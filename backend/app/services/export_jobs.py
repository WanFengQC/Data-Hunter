from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Lock, Thread
from typing import Any
from uuid import uuid4

from app.services.postgres_table import stream_items_csv

EXPORT_DIR = Path(__file__).resolve().parents[3] / "tmp_exports"
EXPORT_DIR.mkdir(parents=True, exist_ok=True)
EXPORT_TTL_HOURS = 24


class PgExportJobManager:
    def __init__(self) -> None:
        self._jobs: dict[str, dict[str, Any]] = {}
        self._lock = Lock()

    def _now(self) -> datetime:
        return datetime.now(timezone.utc)

    def _cleanup(self) -> None:
        expire_before = self._now() - timedelta(hours=EXPORT_TTL_HOURS)
        remove_ids: list[str] = []

        for job_id, job in self._jobs.items():
            updated_at = job.get("updated_at")
            if not isinstance(updated_at, datetime):
                continue
            if updated_at >= expire_before:
                continue
            remove_ids.append(job_id)

        for job_id in remove_ids:
            job = self._jobs.pop(job_id, None)
            if not job:
                continue
            file_path = job.get("file_path")
            if file_path:
                try:
                    Path(file_path).unlink(missing_ok=True)
                except Exception:
                    pass

    def _to_public(self, job: dict[str, Any]) -> dict[str, Any]:
        return {
            "job_id": job["job_id"],
            "status": job["status"],
            "created_at": job["created_at"].isoformat(),
            "updated_at": job["updated_at"].isoformat(),
            "file_name": job.get("file_name"),
            "file_size": job.get("file_size"),
            "error": job.get("error"),
        }

    def create_job(self, params: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            self._cleanup()
            job_id = uuid4().hex
            now = self._now()
            year = params.get("year") or "all"
            month = params.get("month") or "all"
            file_name = f"pg_items_full_{year}_{month}_{job_id[:8]}.csv"
            file_path = str(EXPORT_DIR / file_name)

            job = {
                "job_id": job_id,
                "status": "pending",
                "created_at": now,
                "updated_at": now,
                "file_name": file_name,
                "file_path": file_path,
                "file_size": None,
                "error": None,
                "params": params,
            }
            self._jobs[job_id] = job

        t = Thread(target=self._run_job, args=(job_id,), daemon=True)
        t.start()
        return self._to_public(job)

    def _run_job(self, job_id: str) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            job["status"] = "running"
            job["updated_at"] = self._now()
            params = dict(job["params"])
            file_path = Path(job["file_path"])

        try:
            with file_path.open("w", encoding="utf-8", newline="") as f:
                for chunk in stream_items_csv(**params):
                    f.write(chunk)

            size = file_path.stat().st_size if file_path.exists() else 0
            with self._lock:
                cur = self._jobs.get(job_id)
                if not cur:
                    return
                cur["status"] = "completed"
                cur["updated_at"] = self._now()
                cur["file_size"] = int(size)
        except Exception as exc:
            try:
                file_path.unlink(missing_ok=True)
            except Exception:
                pass
            with self._lock:
                cur = self._jobs.get(job_id)
                if not cur:
                    return
                cur["status"] = "failed"
                cur["updated_at"] = self._now()
                cur["error"] = str(exc)

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        with self._lock:
            self._cleanup()
            job = self._jobs.get(job_id)
            if not job:
                return None
            return self._to_public(job)

    def get_job_file_path(self, job_id: str) -> str | None:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return None
            if job.get("status") != "completed":
                return None
            return str(job.get("file_path") or "")


pg_export_jobs = PgExportJobManager()
