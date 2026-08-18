"""
Simple in-memory job tracker for long-running background tasks.
Fine for a single-instance personal app.
"""

import uuid
from typing import Any

_jobs: dict[str, dict] = {}


def create_job() -> str:
    job_id = str(uuid.uuid4())
    _jobs[job_id] = {"status": "running", "result": None, "error": None, "logs": [], "failures": [], "cancelled": False, "process": None}
    return job_id


def complete_job(job_id: str, result: Any = None) -> None:
    if job_id in _jobs:
        _jobs[job_id]["status"] = "complete"
        _jobs[job_id]["result"] = result


def fail_job(job_id: str, error: str) -> None:
    if job_id in _jobs:
        _jobs[job_id]["status"] = "failed"
        _jobs[job_id]["error"] = error


def cancel_job(job_id: str) -> bool:
    """Signal a job to cancel and kill any registered subprocess. Returns False if job not found."""
    if job_id not in _jobs:
        return False
    _jobs[job_id]["cancelled"] = True
    _jobs[job_id]["status"] = "cancelled"
    proc = _jobs[job_id].get("process")
    if proc is not None:
        try:
            proc.kill()
        except Exception:
            pass
    return True


def is_cancelled(job_id: str) -> bool:
    return _jobs.get(job_id, {}).get("cancelled", False)


def set_job_process(job_id: str, proc: Any) -> None:
    if job_id in _jobs:
        _jobs[job_id]["process"] = proc


def clear_job_process(job_id: str) -> None:
    if job_id in _jobs:
        _jobs[job_id]["process"] = None


def log_job(job_id: str, message: str) -> None:
    if job_id in _jobs:
        _jobs[job_id]["logs"].append(message)


def log_failure_job(job_id: str, source: str, player: str, reason: str) -> None:
    if job_id in _jobs:
        _jobs[job_id]["failures"].append({"source": source, "player": player, "reason": reason})


def get_job(job_id: str) -> dict | None:
    job = _jobs.get(job_id)
    if job is None:
        return None
    # Don't expose internal process handle to API callers
    return {k: v for k, v in job.items() if k != "process"}


def prune_old_jobs(keep: int = 100) -> None:
    """Keep only the most recent N jobs."""
    if len(_jobs) > keep:
        oldest = list(_jobs.keys())[: len(_jobs) - keep]
        for job_id in oldest:
            del _jobs[job_id]
