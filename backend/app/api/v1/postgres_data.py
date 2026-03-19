import json
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse

from app.core.config import settings
from app.services.export_jobs import pg_export_jobs
from app.services.postgres_table import (
    fetch_filter_options,
    fetch_items,
    stream_items_csv,
    fetch_year_months,
)

router = APIRouter(prefix="/pg", tags=["postgres"])


def _parse_text_filters(raw: str | None) -> dict[str, str]:
    if not raw:
        return {}
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        return {}
    return {str(k): str(v) for k, v in parsed.items()}


def _parse_value_filters(raw: str | None) -> dict[str, list[str]]:
    if not raw:
        return {}
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        return {}

    output: dict[str, list[str]] = {}
    for key, value in parsed.items():
        if isinstance(value, list):
            output[str(key)] = [str(v) for v in value]
    return output


@router.get("/year-months")
def list_year_months(
    schema: str = Query(default=settings.pg_schema),
    table: str = Query(default=settings.pg_table),
) -> dict:
    try:
        values = fetch_year_months(schema_name=schema, table_name=table)
        return {"items": values}
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/filter-options")
def list_filter_options(
    column: str = Query(...),
    year: int | None = Query(default=None, ge=2000, le=2100),
    month: int | None = Query(default=None, ge=1, le=12),
    keyword: str | None = Query(default=None),
    limit: int = Query(default=300, ge=10, le=1000),
    text_filters: str | None = Query(default=None),
    value_filters: str | None = Query(default=None),
    schema: str = Query(default=settings.pg_schema),
    table: str = Query(default=settings.pg_table),
) -> dict:
    try:
        options = fetch_filter_options(
            schema_name=schema,
            table_name=table,
            column=column,
            year=year,
            month=month,
            text_filters=_parse_text_filters(text_filters),
            value_filters=_parse_value_filters(value_filters),
            keyword=keyword,
            limit=limit,
        )
        return {"items": options}
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid filters JSON: {exc}") from exc
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/export-csv/jobs")
def create_export_job(
    year: int | None = Query(default=None, ge=2000, le=2100),
    month: int | None = Query(default=None, ge=1, le=12),
    sort_by: str | None = Query(default=None),
    sort_dir: str = Query(default="desc"),
    filters: str | None = Query(default=None),
    text_filters: str | None = Query(default=None),
    value_filters: str | None = Query(default=None),
    schema: str = Query(default=settings.pg_schema),
    table: str = Query(default=settings.pg_table),
) -> dict:
    try:
        merged_text_filters: dict[str, str] = {}
        merged_text_filters.update(_parse_text_filters(filters))
        merged_text_filters.update(_parse_text_filters(text_filters))
        parsed_value_filters = _parse_value_filters(value_filters)

        job = pg_export_jobs.create_job(
            {
                "schema_name": schema,
                "table_name": table,
                "year": year,
                "month": month,
                "sort_by": sort_by,
                "sort_dir": sort_dir.lower(),
                "text_filters": merged_text_filters,
                "value_filters": parsed_value_filters,
            }
        )
        return job
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid filters JSON: {exc}") from exc
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/export-csv/jobs/{job_id}")
def get_export_job(job_id: str) -> dict:
    job = pg_export_jobs.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Export job not found")
    return job


@router.get("/export-csv/jobs/{job_id}/download")
def download_export_job(job_id: str):
    job = pg_export_jobs.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Export job not found")
    if job.get("status") != "completed":
        raise HTTPException(status_code=409, detail="Export job is not completed yet")

    file_path = pg_export_jobs.get_job_file_path(job_id)
    if not file_path or not Path(file_path).exists():
        raise HTTPException(status_code=410, detail="Export file has expired or is missing")

    return FileResponse(
        path=file_path,
        media_type="text/csv; charset=utf-8",
        filename=str(job.get("file_name") or f"{job_id}.csv"),
    )


@router.get("/items")
def list_pg_items(
    year: int | None = Query(default=None, ge=2000, le=2100),
    month: int | None = Query(default=None, ge=1, le=12),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1),
    sort_by: str | None = Query(default=None),
    sort_dir: str = Query(default="desc"),
    filters: str | None = Query(default=None),  # backward-compatible text filters
    text_filters: str | None = Query(default=None),
    value_filters: str | None = Query(default=None),
    schema: str = Query(default=settings.pg_schema),
    table: str = Query(default=settings.pg_table),
) -> dict:
    try:
        merged_text_filters: dict[str, str] = {}
        merged_text_filters.update(_parse_text_filters(filters))
        merged_text_filters.update(_parse_text_filters(text_filters))

        parsed_value_filters = _parse_value_filters(value_filters)

        result = fetch_items(
            schema_name=schema,
            table_name=table,
            page=page,
            page_size=page_size,
            year=year,
            month=month,
            sort_by=sort_by,
            sort_dir=sort_dir.lower(),
            text_filters=merged_text_filters,
            value_filters=parsed_value_filters,
        )
        return result
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid filters JSON: {exc}") from exc
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/export-csv")
def export_pg_csv(
    year: int | None = Query(default=None, ge=2000, le=2100),
    month: int | None = Query(default=None, ge=1, le=12),
    sort_by: str | None = Query(default=None),
    sort_dir: str = Query(default="desc"),
    filters: str | None = Query(default=None),
    text_filters: str | None = Query(default=None),
    value_filters: str | None = Query(default=None),
    schema: str = Query(default=settings.pg_schema),
    table: str = Query(default=settings.pg_table),
):
    try:
        merged_text_filters: dict[str, str] = {}
        merged_text_filters.update(_parse_text_filters(filters))
        merged_text_filters.update(_parse_text_filters(text_filters))
        parsed_value_filters = _parse_value_filters(value_filters)

        stream = stream_items_csv(
            schema_name=schema,
            table_name=table,
            year=year,
            month=month,
            sort_by=sort_by,
            sort_dir=sort_dir.lower(),
            text_filters=merged_text_filters,
            value_filters=parsed_value_filters,
        )

        ym = f"{year or 'all'}_{month or 'all'}"
        filename = f"pg_items_full_{ym}.csv"
        headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
        return StreamingResponse(
            stream,
            media_type="text/csv; charset=utf-8",
            headers=headers,
        )
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid filters JSON: {exc}") from exc
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=str(exc)) from exc
