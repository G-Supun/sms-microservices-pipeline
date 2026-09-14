import sys
import os
import uuid
import aiofiles
import json
from fastapi import APIRouter, UploadFile, File, HTTPException, status
from fastapi.responses import JSONResponse

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from shared_libs.config import settings
from shared_libs.models.sms_model import SingleSMSRequest
from shared_libs.views.response_view import (
    CSVUploadResponse,
    SinglePredictResponse,
    SingleClassificationResult
)
from grpc_client.client import grpc_client
from services.broker_service import gateway_broker
from services.job_manager import job_manager

router = APIRouter(prefix="/classify", tags=["SMS Classification"])


@router.post(
    "-csv",
    response_model=CSVUploadResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Asynchronous Batch CSV Classification Upload"
)
async def classify_csv(file: UploadFile = File(...)):
    if not file.filename.endswith(".csv"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid file type. Please upload a CSV file."
        )

    job_id = f"job_{uuid.uuid4().hex[:8]}"
    file_path = os.path.join(settings.TEMP_UPLOAD_DIR, f"{job_id}_{file.filename}")

    # Stream file to disk in chunks to keep memory usage near zero
    try:
        async with aiofiles.open(file_path, "wb") as out_file:
            while chunk := await file.read(1024 * 1024):  # 1MB buffer
                await out_file.write(chunk)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to persist upload: {str(e)}"
        )
    finally:
        await file.close()

    # Track job and push metadata to RabbitMQ
    job_manager.create_job(job_id, file.filename)
    await gateway_broker.publish_batch_job(job_id=job_id, file_path=file_path)

    return CSVUploadResponse(
        status="queued",
        message="CSV uploaded and scheduled for multi-worker parallel inference.",
        job_id=job_id
    )


@router.post(
    "-single",
    response_model=SinglePredictResponse,
    status_code=status.HTTP_200_OK,
    summary="Low-Latency Real-Time Single SMS Prediction via gRPC"
)
async def classify_single(payload: SingleSMSRequest):
    try:
        response = await grpc_client.classify_single(
            mobile=payload.mobile,
            message=payload.message
        )
        res = response.result
        return SinglePredictResponse(
            status="success",
            result=SingleClassificationResult(
                mobile=res.mobile or None,
                message=res.message,
                category=res.category,
                is_transactional=res.is_transactional,
                confidence=res.confidence
            )
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Real-time ML service unavailable via gRPC: {str(e)}"
        )

@router.get(
    "/jobs/{job_id}",
    summary="Get Batch Job Status and Telemetry Report"
)
async def get_job_status(job_id: str):
    storage_dir = settings.TEMP_UPLOAD_DIR
    report_path = os.path.join(storage_dir, f"{job_id}_report.json")
    
    # 1. If the JSON report exists, the job is finished
    if os.path.exists(report_path):
        with open(report_path, "r") as f:
            report_data = json.load(f)
        return JSONResponse(content=report_data)
    
    # 2. Check if the CSV is still in the folder (meaning it is currently processing)
    try:
        is_processing = any(
            f.startswith(job_id) and f.endswith(".csv") 
            for f in os.listdir(storage_dir)
        )
    except FileNotFoundError:
        is_processing = False

    if is_processing:
        return JSONResponse(
            content={
                "job_id": job_id,
                "status": "processing",
                "message": "The machine learning worker is currently processing this dataset."
            }, 
            status_code=status.HTTP_202_ACCEPTED
        )
        
    # 3. If neither exists, the job ID is invalid or deleted
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND, 
        detail="Job ID not found or already purged."
    )