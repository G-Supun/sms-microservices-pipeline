import sys
import os
import time
import psutil
import asyncpg
from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel
from typing import Optional

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from shared_libs.config import settings

router = APIRouter(prefix="/records", tags=["Database Records & CRUD"])

# Track when the worker started for uptime metrics
APP_START_TIME = time.time()

class SMSUpdateModel(BaseModel):
    mobile: Optional[str] = None
    message: Optional[str] = None
    is_transactional: Optional[bool] = None

async def get_db_connection():
    return await asyncpg.connect(
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
        database=settings.DB_NAME,
        host=settings.DB_HOST,
        port=settings.DB_PORT
    )

@router.get("/stats", summary="Get classification statistics")
async def get_classification_stats():
    conn = await get_db_connection()
    try:
        row = await conn.fetchrow("""
            SELECT 
                COUNT(*) as total,
                COALESCE(SUM(CASE WHEN is_transactional = TRUE THEN 1 ELSE 0 END), 0) as transactional,
                COALESCE(SUM(CASE WHEN is_transactional = FALSE THEN 1 ELSE 0 END), 0) as promotional
            FROM sms_classifications
        """)
        
        total = row['total']
        trans = row['transactional']
        promo = row['promotional']
        
        return {
            "total_records": total,
            "transactional": {
                "count": trans,
                "percentage": round((trans / total * 100), 2) if total > 0 else 0.0
            },
            "promotional": {
                "count": promo,
                "percentage": round((promo / total * 100), 2) if total > 0 else 0.0
            }
        }
    finally:
        await conn.close()

@router.get("/metrics/live", summary="Get live system metrics")
async def get_live_metrics():
    # Current process for RAM
    process = psutil.Process(os.getpid())
    ram_mb = process.memory_info().rss / (1024 * 1024)
    
    # Host CPU percentage
    cpu_percent = psutil.cpu_percent(interval=0.1)
    
    # Uptime in seconds
    uptime = time.time() - APP_START_TIME
    
    return {
        "container_ram_mb": round(ram_mb, 2),
        "host_cpu_percent": cpu_percent,
        "uptime_seconds": round(uptime, 2)
    }

@router.get("/", summary="Get paginated classification records")
async def get_paginated_records(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page")
):
    offset = (page - 1) * limit
    conn = await get_db_connection()
    try:
        total_records = await conn.fetchval("SELECT COUNT(*) FROM sms_classifications")
        rows = await conn.fetch(
            "SELECT id, mobile, message, is_transactional FROM sms_classifications ORDER BY id LIMIT $1 OFFSET $2",
            limit, offset
        )
        return {
            "total_records": total_records,
            "page": page,
            "limit": limit,
            "total_pages": (total_records + limit - 1) // limit if total_records > 0 else 0,
            "data": [dict(row) for row in rows]
        }
    finally:
        await conn.close()

@router.get("/{record_id}", summary="Get single record by ID")
async def get_record(record_id: int):
    conn = await get_db_connection()
    try:
        row = await conn.fetchrow(
            "SELECT id, mobile, message, is_transactional FROM sms_classifications WHERE id = $1",
            record_id
        )
        if not row:
            raise HTTPException(status_code=404, detail="Record not found")
        return dict(row)
    finally:
        await conn.close()

@router.put("/{record_id}", summary="Update a classification record")
async def update_record(record_id: int, payload: SMSUpdateModel):
    conn = await get_db_connection()
    try:
        row = await conn.fetchrow("SELECT id FROM sms_classifications WHERE id = $1", record_id)
        if not row:
            raise HTTPException(status_code=404, detail="Record not found")
        
        await conn.execute(
            """
            UPDATE sms_classifications 
            SET mobile = COALESCE($1, mobile),
                message = COALESCE($2, message),
                is_transactional = COALESCE($3, is_transactional)
            WHERE id = $4
            """,
            payload.mobile, payload.message, payload.is_transactional, record_id
        )
        return {"status": "success", "message": f"Record {record_id} updated successfully"}
    finally:
        await conn.close()

@router.delete("/{record_id}", summary="Delete a classification record")
async def delete_record(record_id: int):
    conn = await get_db_connection()
    try:
        result = await conn.execute("DELETE FROM sms_classifications WHERE id = $1", record_id)
        if result == "DELETE 0":
            raise HTTPException(status_code=404, detail="Record not found")
        return {"status": "success", "message": f"Record {record_id} deleted successfully"}
    finally:
        await conn.close()