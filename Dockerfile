FROM python:3.10-slim

WORKDIR /app

# Install runtime PostgreSQL shared library
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Copy workspace source files
COPY . /app

# Install dependencies using pre-built wheels with high network timeout
RUN pip install --no-cache-dir --default-timeout=1000 --retries=10 \
    -r api_gateway/requirements.txt \
    -r ml_realtime_service/requirements.txt \
    -r ml_batch_worker/requirements.txt \
    -r db_worker/requirements.txt

ENV PYTHONPATH=/app