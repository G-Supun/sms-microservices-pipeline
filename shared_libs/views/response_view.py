from typing import List, Optional
from pydantic import BaseModel

class WorkerChunkTiming(BaseModel):
    worker_id: int
    records_count: int
    inference_wall_time_seconds: float
    cpu_compute_time_seconds: float
    worker_load_share_pct: float

class PerformanceMetrics(BaseModel):
    total_records: int
    total_process_wall_time_seconds: float
    ml_inference_wall_time_seconds: float
    ml_pure_cpu_compute_time_seconds: float
    task_cpu_saturation_pct: float
    db_ingestion_time_seconds: float
    csv_parsing_time_seconds: float
    throughput_msgs_per_sec: float
    avg_latency_ms: float
    ram_overhead_mb: float
    peak_ram_mb: float
    worker_chunk_timings: List[WorkerChunkTiming] = []

class ClassificationSummary(BaseModel):
    transactional_count: int
    promotional_count: int
    transactional_percentage: float
    promotional_percentage: float

class CSVUploadResponse(BaseModel):
    status: str
    message: str
    job_id: Optional[str] = None
    metrics: Optional[PerformanceMetrics] = None
    summary: Optional[ClassificationSummary] = None

class SingleClassificationResult(BaseModel):
    mobile: Optional[str] = None
    message: str
    category: str
    is_transactional: bool
    confidence: float

class SinglePredictResponse(BaseModel):
    status: str
    result: SingleClassificationResult