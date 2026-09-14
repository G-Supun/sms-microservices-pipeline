# SMS Microservices Pipeline

A high-performance, containerized microservices pipeline for batch and real-time SMS classification.

## Tech Stack

- **API Gateway:** FastAPI routing and telemetry endpoints.
- **Message Broker:** RabbitMQ for decoupled asynchronous task queuing.
- **Background Workers:** Python services (`ml_batch_worker`, `db_worker`) handling ONNX model inference and data ingestion.
- **Database:** PostgreSQL (asyncpg pooled) with persistent Docker volumes.

## How to Run

1. Clone the repository.
2. Build and start the cluster: `docker compose up -d --build`
3. Access the interactive API documentation at `http://localhost:8000/docs`.
