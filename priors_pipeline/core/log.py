"""Context-aware job logger. Call set_job_context(job_id) at the start of a
background task, then use job_log() instead of print() to emit messages that
appear both on stdout and in the job's log list."""

from contextvars import ContextVar
from core.jobs import log_job, log_failure_job

_current_job: ContextVar[str | None] = ContextVar("current_job", default=None)
_current_failures: ContextVar[list | None] = ContextVar("current_failures", default=None)


def set_job_context(job_id: str) -> None:
    _current_job.set(job_id)
    _current_failures.set([])


def job_log(message: str) -> None:
    print(message)
    job_id = _current_job.get()
    if job_id:
        log_job(job_id, message)


def log_failure(source: str, player: str, reason: str) -> None:
    """Record a per-player failure. Also emits a job_log line."""
    job_log(f"  [{source}] {player}: {reason}")
    failures = _current_failures.get()
    if failures is not None:
        failures.append({"source": source, "player": player, "reason": reason})
    job_id = _current_job.get()
    if job_id:
        log_failure_job(job_id, source, player, reason)


def get_failures() -> list:
    """Return failures accumulated since set_job_context() was last called."""
    return list(_current_failures.get() or [])
