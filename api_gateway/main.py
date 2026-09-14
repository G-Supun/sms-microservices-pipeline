import sys
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from shared_libs.config import settings
from grpc_client.client import grpc_client
from services.broker_service import gateway_broker
from controllers.classification_controller import router as classification_router
from controllers.health_controller import router as health_router
from controllers.db_records_controller import router as db_records_router

@asynccontextmanager
async def lifespan(app: FastAPI):
    print(f"[*] Starting {settings.APP_NAME} API Gateway...")
    print("[*] Connecting to RabbitMQ Broker...")
    await gateway_broker.connect()

    print(f"[*] Initializing gRPC client link to {settings.GRPC_SERVER_HOST}...")
    grpc_client.connect()

    yield

    print("[*] Closing API Gateway connections...")
    await gateway_broker.close()
    await grpc_client.close()
    print("[*] Gateway shut down cleanly.")


app = FastAPI(
    title="SMS Classifier - API Gateway",
    version=settings.APP_VERSION,
    description="Lightweight Reverse Proxy & Event Producer with gRPC & RabbitMQ",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(classification_router, prefix="/api/v1")
app.include_router(health_router, prefix="/api/v1")
app.include_router(db_records_router, prefix="/api/v1")


@app.get("/", tags=["Root"])
async def root():
    return {
        "service": "API Gateway",
        "docs": "/docs",
        "health": "/api/v1/health"
    }