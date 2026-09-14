import os
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    APP_NAME: str = "SMS Microservices Architecture"
    APP_VERSION: str = "2.0.0"
    DEBUG: bool = False

    # Database Configuration (Used by DB Worker)
    DB_NAME: str = os.getenv("DB_NAME", "practise")
    DB_USER: str = os.getenv("DB_USER", "postgres")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "S@post")
    DB_HOST: str = os.getenv("DB_HOST", "localhost")
    DB_PORT: int = int(os.getenv("DB_PORT", 5432))
    MIN_CONN: int = int(os.getenv("MIN_CONN", 2))
    MAX_CONN: int = int(os.getenv("MAX_CONN", 10))

    # Machine Learning Settings (Used by ML Workers)
    MODEL_PATH: str = os.getenv("MODEL_PATH", "ml/student_cnn.onnx")
    TOKENIZER_DIR: str = os.getenv("TOKENIZER_DIR", "ml")
    NUM_WORKERS: int = int(os.getenv("NUM_WORKERS", 3))
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", 1024))
    MAX_SEQUENCE_LENGTH: int = 64
    DECISION_THRESHOLD: float = float(os.getenv("DECISION_THRESHOLD", 0.65))

    # Internal Network / gRPC
    GRPC_SERVER_HOST: str = os.getenv("GRPC_SERVER_HOST", "localhost:50051")

    # RabbitMQ Broker & Queue Routing
    RABBITMQ_URL: str = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
    JOB_QUEUE_NAME: str = os.getenv("JOB_QUEUE_NAME", "sms_classification_jobs")
    DB_QUEUE_NAME: str = os.getenv("DB_QUEUE_NAME", "sms_db_ingestion")
    
    # Shared Storage 
    TEMP_UPLOAD_DIR: str = os.getenv("TEMP_UPLOAD_DIR", "storage/uploads")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

settings = Settings()

# Ensure the upload directory exists for the API Gateway
os.makedirs(settings.TEMP_UPLOAD_DIR, exist_ok=True)