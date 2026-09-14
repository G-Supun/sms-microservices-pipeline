import sys
import os
import asyncpg
from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel
from typing import Optional

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from shared_libs.config import settings

router = APIRouter(prefix="/records", tags=["Database Records & CRUD"])

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