import json
from io import StringIO
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from app.core.config import settings
from app.services.postgres_table import (
    fetch_all_items,
    fetch_filter_options,
    fetch_items,
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
        import csv

        merged_text_filters: dict[str, str] = {}
        merged_text_filters.update(_parse_text_filters(filters))
        merged_text_filters.update(_parse_text_filters(text_filters))
        parsed_value_filters = _parse_value_filters(value_filters)

        result = fetch_all_items(
            schema_name=schema,
            table_name=table,
            year=year,
            month=month,
            sort_by=sort_by,
            sort_dir=sort_dir.lower(),
            text_filters=merged_text_filters,
            value_filters=parsed_value_filters,
        )

        columns = result.get("columns", [])
        rows = result.get("items", [])

        sio = StringIO()
        sio.write("\ufeff")
        writer = csv.writer(sio)
        writer.writerow(columns)
        for row in rows:
            writer.writerow([row.get(col, "") for col in columns])
        sio.seek(0)

        ym = f"{year or 'all'}_{month or 'all'}"
        filename = f"pg_items_full_{ym}.csv"
        headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
        return StreamingResponse(
            iter([sio.getvalue()]),
            media_type="text/csv; charset=utf-8",
            headers=headers,
        )
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid filters JSON: {exc}") from exc
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=str(exc)) from exc
