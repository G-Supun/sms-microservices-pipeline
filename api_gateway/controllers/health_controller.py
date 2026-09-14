from fastapi import APIRouter

router = APIRouter(prefix="/health", tags=["System Health"])


@router.get("", summary="Service Health Check")
async def health_check():
    return {
        "status": "healthy",
        "service": "api_gateway",
        "version": "2.0.0"
    }