import time
from typing import Dict, Any


class JobManager:
    def __init__(self):
        self._jobs: Dict[str, Dict[str, Any]] = {}

    def create_job(self, job_id: str, filename: str) -> Dict[str, Any]:
        job_info = {
            "job_id": job_id,
            "filename": filename,
            "status": "queued",
            "created_at": time.time(),
            "metrics": None,
            "summary": None,
            "error": None
        }
        self._jobs[job_id] = job_info
        return job_info

    def get_job(self, job_id: str) -> Dict[str, Any] | None:
        return self._jobs.get(job_id)


job_manager = JobManager()