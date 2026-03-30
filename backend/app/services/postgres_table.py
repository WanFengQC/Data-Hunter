import csv
import json
import re
from collections.abc import Iterator
from datetime import date, datetime
from decimal import Decimal
from io import StringIO
from threading import Lock
from typing import Any
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from app.core.config import settings

BLANK_TOKEN = "__BLANK__"
NUMERIC_COLUMN_TYPES = {
    "smallint",
    "integer",
    "bigint",
    "decimal",
    "numeric",
    "real",
    "double precision",
}
_TABLE_PROFILE_CACHE: dict[tuple[str, str], dict[str, Any]] = {}
_TABLE_PROFILE_LOCK = Lock()


def pg_connect() -> psycopg.Connection:
    return psycopg.connect(
        host=settings.pg_host,
        user=settings.pg_user,
        password=settings.pg_pass,
        port=settings.pg_port,
        dbname=settings.pg_db,
        row_factory=dict_row,
    )


def _build_table_profile(
    conn: psycopg.Connection, schema_name: str, table_name: str
) -> dict[str, Any]:
    exists = _table_exists(conn, schema_name, table_name)
    if not exists:
        return {
            "exists": False,
            "column_meta": [],
            "columns": [],
            "column_types": {},
            "valid_columns": set(),
        }

    column_meta = _get_column_meta(conn, schema_name, table_name)
    columns = [row["column_name"] for row in column_meta]
    return {
        "exists": True,
        "column_meta": column_meta,
        "columns": columns,
        "column_types": {row["column_name"]: row["data_type"] for row in column_meta},
        "valid_columns": set(columns),
    }


def _get_table_profile(conn: psycopg.Connection, schema_name: str, table_name: str) -> dict[str, Any]:
    key = (schema_name, table_name)
    with _TABLE_PROFILE_LOCK:
        cached = _TABLE_PROFILE_CACHE.get(key)
    if cached is not None:
        return cached

    profile = _build_table_profile(conn, schema_name, table_name)
    if profile["exists"]:
        with _TABLE_PROFILE_LOCK:
            _TABLE_PROFILE_CACHE[key] = profile
    return profile


def _resolve_selected_columns(
    selected_columns: list[str] | None,
    columns: list[str],
    valid_columns: set[str],
) -> list[str]:
    if not selected_columns:
        return columns

    resolved: list[str] = []
    seen: set[str] = set()
    for column in selected_columns:
        if column not in valid_columns or column in seen:
            continue
        resolved.append(column)
        seen.add(column)
    return resolved


def _serialize_value(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def _table_exists(conn: psycopg.Connection, schema_name: str, table_name: str) -> bool:
    query = """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        ) AS exists
    """
    with conn.cursor() as cur:
        cur.execute(query, (schema_name, table_name))
        row = cur.fetchone()
    return bool(row["exists"])


def _get_column_meta(
    conn: psycopg.Connection, schema_name: str, table_name: str
) -> list[dict[str, str]]:
    query = """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(query, (schema_name, table_name))
        return cur.fetchall()


def _build_time_clauses(year: int | None, month: int | None) -> tuple[list[sql.SQL], list[Any]]:
    clauses: list[sql.SQL] = []
    params: list[Any] = []

    if year is not None and month is not None:
        clauses.append(sql.SQL("year_month = %s"))
        params.append(year * 100 + month)
    elif year is not None:
        clauses.append(sql.SQL("year_month BETWEEN %s AND %s"))
        params.extend([year * 100, year * 100 + 99])
    elif month is not None:
        clauses.append(sql.SQL("(year_month % 100) = %s"))
        params.append(month)

    return clauses, params


def _build_text_filter_clauses(
    text_filters: dict[str, Any],
    valid_columns: set[str],
    column_types: dict[str, str] | None = None,
) -> tuple[list[sql.SQL], list[Any]]:
    clauses: list[sql.SQL] = []
    params: list[Any] = []
    normalized_column_types = {key: str(value).lower() for key, value in (column_types or {}).items()}

    for key, raw_value in text_filters.items():
        if key not in valid_columns:
            continue
        col_text = sql.SQL("CAST({} AS TEXT)").format(sql.Identifier(key))
        col_trimmed_text = sql.SQL("trim(CAST({} AS TEXT))").format(sql.Identifier(key))
        if normalized_column_types.get(key) in NUMERIC_COLUMN_TYPES:
            col_numeric = sql.SQL("CAST({} AS DOUBLE PRECISION)").format(sql.Identifier(key))
        else:
            col_numeric = sql.SQL(
                "(CASE WHEN {} ~ '^[+-]?(?:\\d+(?:\\.\\d+)?|\\.\\d+)(?:[eE][+-]?\\d+)?$' "
                "THEN CAST({} AS DOUBLE PRECISION) ELSE NULL END)"
            ).format(col_trimmed_text, col_trimmed_text)

        # Backward-compatible: plain string means "contains"
        if isinstance(raw_value, str):
            value = raw_value.strip()
            if not value:
                continue
            clauses.append(sql.SQL("{} ILIKE %s").format(col_text))
            params.append(f"%{value}%")
            continue

        if not isinstance(raw_value, dict):
            continue

        op = str(raw_value.get("op") or "contains").strip().lower()
        value = str(raw_value.get("value") or "").strip()

        if op == "contains":
            if not value:
                continue
            clauses.append(sql.SQL("{} ILIKE %s").format(col_text))
            params.append(f"%{value}%")
        elif op == "not_contains":
            if not value:
                continue
            clauses.append(sql.SQL("{} NOT ILIKE %s").format(col_text))
            params.append(f"%{value}%")
        elif op == "starts_with":
            if not value:
                continue
            clauses.append(sql.SQL("{} ILIKE %s").format(col_text))
            params.append(f"{value}%")
        elif op == "ends_with":
            if not value:
                continue
            clauses.append(sql.SQL("{} ILIKE %s").format(col_text))
            params.append(f"%{value}")
        elif op == "contains_word":
            if not value:
                continue
            # Match whole token boundaries, e.g. car matches "remote car" but not "card".
            pattern = rf"(^|[^[:alnum:]_]){re.escape(value)}([^[:alnum:]_]|$)"
            clauses.append(sql.SQL("{} ~* %s").format(col_text))
            params.append(pattern)
        elif op == "equals":
            if not value:
                continue
            clauses.append(sql.SQL("lower({}) = lower(%s)").format(col_text))
            params.append(value)
        elif op == "not_equals":
            if not value:
                continue
            clauses.append(
                sql.SQL("({} IS NULL OR lower({}) <> lower(%s))").format(sql.Identifier(key), col_text)
            )
            params.append(value)
        elif op == "greater_than":
            if not value:
                continue
            try:
                num_value = float(value)
            except (TypeError, ValueError):
                continue
            clauses.append(sql.SQL("{} > %s").format(col_numeric))
            params.append(num_value)
        elif op == "less_than":
            if not value:
                continue
            try:
                num_value = float(value)
            except (TypeError, ValueError):
                continue
            clauses.append(sql.SQL("{} < %s").format(col_numeric))
            params.append(num_value)
        elif op == "range":
            raw_min = raw_value.get("min")
            raw_max = raw_value.get("max")
            has_min = raw_min not in (None, "")
            has_max = raw_max not in (None, "")
            if not has_min and not has_max:
                continue
            if has_min:
                try:
                    min_value = float(raw_min)
                except (TypeError, ValueError):
                    continue
                clauses.append(sql.SQL("{} >= %s").format(col_numeric))
                params.append(min_value)
            if has_max:
                try:
                    max_value = float(raw_max)
                except (TypeError, ValueError):
                    continue
                clauses.append(sql.SQL("{} <= %s").format(col_numeric))
                params.append(max_value)
        elif op == "is_blank":
            clauses.append(sql.SQL("({} IS NULL OR {} = '')").format(sql.Identifier(key), col_text))
        elif op == "is_not_blank":
            clauses.append(sql.SQL("({} IS NOT NULL AND {} <> '')").format(sql.Identifier(key), col_text))

    return clauses, params


def _build_value_filter_clauses(
    value_filters: dict[str, list[str]],
    valid_columns: set[str],
    exclude_column: str | None = None,
) -> tuple[list[sql.SQL], list[Any]]:
    clauses: list[sql.SQL] = []
    params: list[Any] = []

    for key, values in value_filters.items():
        if key not in valid_columns:
            continue
        if exclude_column and key == exclude_column:
            continue
        if not isinstance(values, list):
            continue

        cleaned = [str(v) for v in values if str(v).strip()]
        if not cleaned:
            continue

        include_blank = BLANK_TOKEN in cleaned
        normal_values = [v for v in cleaned if v != BLANK_TOKEN]

        sub_clauses: list[sql.SQL] = []
        if normal_values:
            sub_clauses.append(
                sql.SQL("CAST({} AS TEXT) = ANY(%s)").format(sql.Identifier(key))
            )
            params.append(normal_values)
        if include_blank:
            sub_clauses.append(sql.SQL("{} IS NULL").format(sql.Identifier(key)))

        if sub_clauses:
            clauses.append(sql.SQL("(") + sql.SQL(" OR ").join(sub_clauses) + sql.SQL(")"))

    return clauses, params


def _build_where_sql(
    year: int | None,
    month: int | None,
    valid_columns: set[str],
    column_types: dict[str, str] | None = None,
    text_filters: dict[str, Any] | None = None,
    value_filters: dict[str, list[str]] | None = None,
    exclude_value_filter_column: str | None = None,
) -> tuple[sql.SQL, list[Any]]:
    clauses: list[sql.SQL] = []
    params: list[Any] = []

    t_clauses, t_params = _build_time_clauses(year, month)
    clauses.extend(t_clauses)
    params.extend(t_params)

    f_clauses, f_params = _build_text_filter_clauses(
        text_filters or {}, valid_columns, column_types=column_types
    )
    clauses.extend(f_clauses)
    params.extend(f_params)

    v_clauses, v_params = _build_value_filter_clauses(
        value_filters or {}, valid_columns, exclude_column=exclude_value_filter_column
    )
    clauses.extend(v_clauses)
    params.extend(v_params)

    if not clauses:
        return sql.SQL(""), params

    return sql.SQL(" WHERE ") + sql.SQL(" AND ").join(clauses), params


def _build_order_sql(
    sort_by: str | None,
    sort_dir: str,
    valid_columns: set[str],
    column_types: dict[str, str],
) -> sql.SQL:
    direction = sql.SQL("ASC") if str(sort_dir).lower() == "asc" else sql.SQL("DESC")
    if sort_by and sort_by in valid_columns:
        if column_types.get(sort_by) == "jsonb":
            # JSONB columns in this table often store numeric metrics.
            # Sort by numeric value first; fallback to text for non-numeric JSON.
            numeric_expr = sql.SQL(
                """
                CASE
                    WHEN jsonb_typeof({col}) = 'number' THEN ({col}::text)::numeric
                    WHEN jsonb_typeof({col}) = 'string'
                         AND trim(both '"' from {col}::text) ~ '^-?\\d+(\\.\\d+)?$'
                    THEN trim(both '"' from {col}::text)::numeric
                    ELSE NULL
                END
                """
            ).format(col=sql.Identifier(sort_by))
            text_expr = sql.SQL("trim(both '\"' from {}::text)").format(sql.Identifier(sort_by))
            return sql.SQL(" ORDER BY {} {} NULLS LAST, {} {} NULLS LAST").format(
                numeric_expr,
                direction,
                text_expr,
                direction,
            )
        else:
            order_expr = sql.Identifier(sort_by)
        return sql.SQL(" ORDER BY {} {} NULLS LAST").format(order_expr, direction)
    return sql.SQL(" ORDER BY year_month DESC, id DESC")


def fetch_year_months(schema_name: str, table_name: str) -> list[int]:
    query = sql.SQL(
        "SELECT DISTINCT year_month FROM {}.{} "
        "WHERE year_month IS NOT NULL "
        "ORDER BY year_month DESC"
    ).format(sql.Identifier(schema_name), sql.Identifier(table_name))

    with pg_connect() as conn:
        profile = _get_table_profile(conn, schema_name, table_name)
        if not profile["exists"]:
            return []
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

    return [int(row["year_month"]) for row in rows if row["year_month"] is not None]


def fetch_filter_options(
    schema_name: str,
    table_name: str,
    column: str,
    year: int | None = None,
    month: int | None = None,
    text_filters: dict[str, Any] | None = None,
    value_filters: dict[str, list[str]] | None = None,
    keyword: str | None = None,
    limit: int = 300,
) -> list[dict[str, Any]]:
    limit = min(max(limit, 10), 20000)

    with pg_connect() as conn:
        profile = _get_table_profile(conn, schema_name, table_name)
        if not profile["exists"]:
            return []

        column_types = profile["column_types"]
        valid_columns = profile["valid_columns"]
        if column not in valid_columns:
            return []

        merged_text_filters: dict[str, Any] = {}
        if text_filters:
            merged_text_filters.update(text_filters)
        keyword_norm = (keyword or "").strip()
        if keyword_norm:
            merged_text_filters[column] = keyword_norm

        where_sql, where_params = _build_where_sql(
            year=year,
            month=month,
            valid_columns=valid_columns,
            column_types=column_types,
            text_filters=merged_text_filters,
            value_filters=value_filters,
            exclude_value_filter_column=column,
        )

        query = (
            sql.SQL(
                """
                SELECT
                    CASE WHEN {col} IS NULL THEN %s ELSE CAST({col} AS TEXT) END AS value,
                    COUNT(*) AS count
                FROM {schema}.{table}
                """
            ).format(
                col=sql.Identifier(column),
                schema=sql.Identifier(schema_name),
                table=sql.Identifier(table_name),
            )
            + where_sql
            + sql.SQL(" GROUP BY value ORDER BY count DESC, value ASC LIMIT %s")
        )

        with conn.cursor() as cur:
            cur.execute(query, [BLANK_TOKEN, *where_params, limit])
            rows = cur.fetchall()

    result: list[dict[str, Any]] = []
    for row in rows:
        value = row["value"]
        label = "(空白)" if value == BLANK_TOKEN else str(value)
        result.append(
            {
                "value": value,
                "label": label,
                "count": int(row["count"]),
            }
        )
    return result


def fetch_items(
    schema_name: str,
    table_name: str,
    page: int,
    page_size: int,
    year: int | None = None,
    month: int | None = None,
    sort_by: str | None = None,
    sort_dir: str = "desc",
    filters: dict[str, Any] | None = None,  # backward-compatible alias
    text_filters: dict[str, Any] | None = None,
    value_filters: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    page = max(page, 1)
    page_size = max(page_size, 1)
    offset = (page - 1) * page_size

    merged_text_filters: dict[str, Any] = {}
    if filters:
        merged_text_filters.update(filters)
    if text_filters:
        merged_text_filters.update(text_filters)

    with pg_connect() as conn:
        profile = _get_table_profile(conn, schema_name, table_name)
        if not profile["exists"]:
            return {"columns": [], "items": [], "total": 0, "page": page, "page_size": page_size}

        columns = profile["columns"]
        column_types = profile["column_types"]
        valid_columns = profile["valid_columns"]

        where_sql, where_params = _build_where_sql(
            year=year,
            month=month,
            valid_columns=valid_columns,
            column_types=column_types,
            text_filters=merged_text_filters,
            value_filters=value_filters,
        )

        order_sql = _build_order_sql(
            sort_by=sort_by,
            sort_dir=sort_dir,
            valid_columns=valid_columns,
            column_types=column_types,
        )

        count_query = (
            sql.SQL("SELECT COUNT(*) AS total FROM {}.{}").format(
                sql.Identifier(schema_name), sql.Identifier(table_name)
            )
            + where_sql
        )
        data_query = (
            sql.SQL("SELECT * FROM {}.{}").format(
                sql.Identifier(schema_name), sql.Identifier(table_name)
            )
            + where_sql
            + order_sql
            + sql.SQL(" LIMIT %s OFFSET %s")
        )

        with conn.cursor() as cur:
            cur.execute(count_query, where_params)
            total = int(cur.fetchone()["total"])

            cur.execute(data_query, [*where_params, page_size, offset])
            rows = cur.fetchall()

    items: list[dict[str, Any]] = []
    for row in rows:
        item: dict[str, Any] = {}
        for key, value in row.items():
            item[key] = _serialize_value(value)
        items.append(item)

    return {
        "columns": columns,
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
    }


def fetch_growth_top10_items(
    schema_name: str,
    table_name: str,
    mode: str,
    year: int | None = None,
    month: int | None = None,
    search_min: float | None = None,
    search_max: float | None = None,
    limit: int = 10,
    tag_label: str = "2外形",
) -> dict[str, Any]:
    sort_by_map = {
        "monthly": "total_searches_growth_rate",
        "quarterly": "total_searches_quarter_avg_growth_rate",
        "searches": "total_searches",
    }
    sort_by = sort_by_map.get(str(mode).lower(), "total_searches_growth_rate")
    limit = max(1, min(int(limit or 10), 100))

    text_filters: dict[str, Any] = {
        "标签": {"op": "equals", "value": tag_label},
    }
    if search_min is not None or search_max is not None:
        range_filter: dict[str, Any] = {"op": "range"}
        if search_min is not None:
            range_filter["min"] = search_min
        if search_max is not None:
            range_filter["max"] = search_max
        text_filters["total_searches"] = range_filter

    with pg_connect() as conn:
        profile = _get_table_profile(conn, schema_name, table_name)
        if not profile["exists"]:
            return {
                "columns": [],
                "items": [],
                "total": 0,
                "page": 1,
                "page_size": limit,
                "average_total_searches": None,
                "average_label": "平均总搜索量",
            }

        columns = profile["columns"]
        column_types = profile["column_types"]
        valid_columns = profile["valid_columns"]

        where_sql, where_params = _build_where_sql(
            year=year,
            month=month,
            valid_columns=valid_columns,
            column_types=column_types,
            text_filters=text_filters,
            value_filters={},
        )
        order_sql = _build_order_sql(
            sort_by=sort_by,
            sort_dir="desc",
            valid_columns=valid_columns,
            column_types=column_types,
        )
        data_query = (
            sql.SQL("SELECT * FROM {}.{}").format(
                sql.Identifier(schema_name), sql.Identifier(table_name)
            )
            + where_sql
            + order_sql
            + sql.SQL(" LIMIT %s")
        )
        average_label = "当月平均总搜索量" if mode != "quarterly" else "当季平均总搜索量"
        avg_where_sql = where_sql
        avg_params = list(where_params)
        if mode == "quarterly" and year is not None and month is not None:
            quarter_start = ((month - 1) // 3) * 3 + 1
            quarter_end = quarter_start + 2
            avg_clauses: list[sql.SQL] = [
                sql.SQL("year_month BETWEEN %s AND %s"),
            ]
            avg_params = [year * 100 + quarter_start, year * 100 + quarter_end]
            text_clauses, text_params = _build_text_filter_clauses(
                text_filters,
                valid_columns,
                column_types=column_types,
            )
            avg_clauses.extend(text_clauses)
            avg_params.extend(text_params)
            avg_where_sql = sql.SQL(" WHERE ") + sql.SQL(" AND ").join(avg_clauses)

        avg_query = (
            sql.SQL("SELECT AVG(CAST(total_searches AS DOUBLE PRECISION)) AS avg_total_searches FROM {}.{}").format(
                sql.Identifier(schema_name), sql.Identifier(table_name)
            )
            + avg_where_sql
        )

        with conn.cursor() as cur:
            cur.execute(data_query, [*where_params, limit])
            rows = cur.fetchall()
            cur.execute(avg_query, avg_params)
            avg_row = cur.fetchone()

    items: list[dict[str, Any]] = []
    for row in rows:
        item: dict[str, Any] = {}
        for key, value in row.items():
            item[key] = _serialize_value(value)
        items.append(item)

    avg_total_searches = None
    if avg_row:
        avg_total_searches = _serialize_value(avg_row.get("avg_total_searches"))

    return {
        "columns": columns,
        "items": items,
        "total": len(items),
        "page": 1,
        "page_size": limit,
        "average_total_searches": avg_total_searches,
        "average_label": average_label,
    }


def fetch_all_items(
    schema_name: str,
    table_name: str,
    year: int | None = None,
    month: int | None = None,
    sort_by: str | None = None,
    sort_dir: str = "desc",
    filters: dict[str, Any] | None = None,  # backward-compatible alias
    text_filters: dict[str, Any] | None = None,
    value_filters: dict[str, list[str]] | None = None,
    selected_columns: list[str] | None = None,
) -> dict[str, Any]:
    merged_text_filters: dict[str, Any] = {}
    if filters:
        merged_text_filters.update(filters)
    if text_filters:
        merged_text_filters.update(text_filters)

    with pg_connect() as conn:
        profile = _get_table_profile(conn, schema_name, table_name)
        if not profile["exists"]:
            return {"columns": [], "items": [], "total": 0}

        columns = profile["columns"]
        column_types = profile["column_types"]
        valid_columns = profile["valid_columns"]
        output_columns = _resolve_selected_columns(selected_columns, columns, valid_columns)
        if not output_columns:
            return {"columns": [], "items": [], "total": 0}

        where_sql, where_params = _build_where_sql(
            year=year,
            month=month,
            valid_columns=valid_columns,
            column_types=column_types,
            text_filters=merged_text_filters,
            value_filters=value_filters,
        )
        order_sql = _build_order_sql(
            sort_by=sort_by,
            sort_dir=sort_dir,
            valid_columns=valid_columns,
            column_types=column_types,
        )

        data_query = (
            sql.SQL("SELECT {} FROM {}.{}").format(
                sql.SQL(", ").join(sql.Identifier(col) for col in output_columns),
                sql.Identifier(schema_name), sql.Identifier(table_name)
            )
            + where_sql
            + order_sql
        )

        with conn.cursor() as cur:
            cur.execute(data_query, where_params)
            rows = cur.fetchall()

    items: list[dict[str, Any]] = []
    for row in rows:
        item: dict[str, Any] = {}
        for key, value in row.items():
            item[key] = _serialize_value(value)
        items.append(item)

    return {"columns": output_columns, "items": items, "total": len(items)}


def stream_items_csv(
    schema_name: str,
    table_name: str,
    year: int | None = None,
    month: int | None = None,
    sort_by: str | None = None,
    sort_dir: str = "desc",
    filters: dict[str, Any] | None = None,  # backward-compatible alias
    text_filters: dict[str, Any] | None = None,
    value_filters: dict[str, list[str]] | None = None,
    chunk_rows: int = 2000,
) -> Iterator[str]:
    chunk_rows = max(100, int(chunk_rows or 2000))

    merged_text_filters: dict[str, Any] = {}
    if filters:
        merged_text_filters.update(filters)
    if text_filters:
        merged_text_filters.update(text_filters)

    out = StringIO()
    writer = csv.writer(out)
    out.write("\ufeff")

    def flush_buffer() -> str:
        data = out.getvalue()
        out.seek(0)
        out.truncate(0)
        return data

    with pg_connect() as conn:
        profile = _get_table_profile(conn, schema_name, table_name)
        if not profile["exists"]:
            writer.writerow([])
            yield flush_buffer()
            return

        columns = profile["columns"]
        column_types = profile["column_types"]
        valid_columns = profile["valid_columns"]

        where_sql, where_params = _build_where_sql(
            year=year,
            month=month,
            valid_columns=valid_columns,
            column_types=column_types,
            text_filters=merged_text_filters,
            value_filters=value_filters,
        )
        order_sql = _build_order_sql(
            sort_by=sort_by,
            sort_dir=sort_dir,
            valid_columns=valid_columns,
            column_types=column_types,
        )
        data_query = (
            sql.SQL("SELECT * FROM {}.{}").format(
                sql.Identifier(schema_name), sql.Identifier(table_name)
            )
            + where_sql
            + order_sql
        )

        writer.writerow(columns)
        yield flush_buffer()

        cursor_name = f"csv_export_{uuid4().hex}"
        with conn.cursor(name=cursor_name) as cur:
            cur.itersize = chunk_rows
            cur.execute(data_query, where_params)

            while True:
                rows = cur.fetchmany(chunk_rows)
                if not rows:
                    break

                for row in rows:
                    writer.writerow([_serialize_value(row.get(col)) for col in columns])

                chunk = flush_buffer()
                if chunk:
                    yield chunk


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def fetch_word_frequency_trend(
    schema_name: str,
    freq_table_name: str,
    cache_table_name: str,
    items_table_name: str,
    word: str,
) -> dict[str, Any]:
    normalized_word = str(word or "").strip().lower()
    if not normalized_word:
        return {"word": "", "info": {}, "points": [], "latest_year_month": None}

    freq_points: list[dict[str, Any]] = []
    info: dict[str, Any] = {}
    latest_year_month: int | None = None

    with pg_connect() as conn:
        freq_profile = _get_table_profile(conn, schema_name, freq_table_name)
        if freq_profile["exists"]:
            freq_columns = freq_profile["valid_columns"]
            has_word_zh = "word_zh" in freq_columns
            has_tag = "标签" in freq_columns
            has_reason = "原因" in freq_columns
            has_category = "category" in freq_columns
            has_plushable = "plushable" in freq_columns

            select_fields: list[sql.SQL] = [
                sql.SQL("year_month"),
                sql.SQL("year"),
                sql.SQL("month"),
                sql.SQL("freq"),
                sql.SQL("freq_ratio"),
                sql.SQL("coverage"),
                sql.SQL("total_searches"),
                sql.SQL("word_zh") if has_word_zh else sql.SQL("NULL::text AS word_zh"),
            ]
            if has_tag:
                select_fields.append(sql.SQL("{} AS tag_label").format(sql.Identifier("标签")))
            elif has_category:
                select_fields.append(sql.SQL("category AS tag_label"))
            else:
                select_fields.append(sql.SQL("NULL::text AS tag_label"))

            if has_reason:
                select_fields.append(sql.SQL("{} AS tag_reason").format(sql.Identifier("原因")))
            else:
                select_fields.append(sql.SQL("NULL::text AS tag_reason"))

            if has_category:
                select_fields.append(sql.SQL("category"))
            else:
                select_fields.append(sql.SQL("NULL::text AS category"))

            if has_plushable:
                select_fields.append(sql.SQL("plushable"))
            else:
                select_fields.append(sql.SQL("NULL::text AS plushable"))

            points_query = sql.SQL(
                """
                SELECT
                    {}
                FROM {}.{}
                WHERE word = %s
                ORDER BY year_month ASC
                """
            ).format(
                sql.SQL(", ").join(select_fields),
                sql.Identifier(schema_name),
                sql.Identifier(freq_table_name),
            )
            with conn.cursor() as cur:
                cur.execute(points_query, (normalized_word,))
                rows = cur.fetchall()
                for row in rows:
                    ym = _to_int(row.get("year_month"))
                    point = {
                        "year_month": ym,
                        "year": _to_int(row.get("year")),
                        "month": _to_int(row.get("month")),
                        "freq": _to_int(row.get("freq")),
                        "freq_ratio": _to_float(row.get("freq_ratio")),
                        "coverage": _to_int(row.get("coverage")),
                        "total_searches": _to_float(row.get("total_searches")),
                        "rank": None,
                        "rank_growth_rate": None,
                        "searches_growth_rate": None,
                    }
                    freq_points.append(point)
                    if ym is not None:
                        latest_year_month = ym

                if rows:
                    latest_row = rows[-1]
                    tag_label = latest_row.get("tag_label")
                    info = {
                        "translation_zh": latest_row.get("word_zh"),
                        "tag_label": tag_label,
                        "reason": latest_row.get("tag_reason"),
                        "object_category": latest_row.get("category") or tag_label,
                        "plushable": latest_row.get("plushable"),
                        "plushable_bool": None,
                    }

        cache_profile = _get_table_profile(conn, schema_name, cache_table_name)
        if cache_profile["exists"]:
            cache_columns = cache_profile["valid_columns"]
            cache_select: list[sql.SQL] = [
                sql.SQL("translation_zh")
                if "translation_zh" in cache_columns
                else sql.SQL("NULL::text AS translation_zh"),
            ]
            if "tag_label" in cache_columns:
                cache_select.append(sql.SQL("tag_label"))
            elif "标签" in cache_columns:
                cache_select.append(sql.SQL("{} AS tag_label").format(sql.Identifier("标签")))
            else:
                cache_select.append(sql.SQL("NULL::text AS tag_label"))

            if "tag_reason" in cache_columns:
                cache_select.append(sql.SQL("tag_reason"))
            elif "原因" in cache_columns:
                cache_select.append(sql.SQL("{} AS tag_reason").format(sql.Identifier("原因")))
            else:
                cache_select.append(sql.SQL("NULL::text AS tag_reason"))

            cache_select.extend(
                [
                    sql.SQL("object_category")
                    if "object_category" in cache_columns
                    else sql.SQL("NULL::text AS object_category"),
                    sql.SQL("plushable")
                    if "plushable" in cache_columns
                    else sql.SQL("NULL::text AS plushable"),
                    sql.SQL("plushable_bool")
                    if "plushable_bool" in cache_columns
                    else sql.SQL("NULL::boolean AS plushable_bool"),
                ]
            )
            cache_query = sql.SQL(
                """
                SELECT {}
                FROM {}.{}
                WHERE word = %s
                LIMIT 1
                """
            ).format(
                sql.SQL(", ").join(cache_select),
                sql.Identifier(schema_name),
                sql.Identifier(cache_table_name),
            )
            with conn.cursor() as cur:
                cur.execute(cache_query, (normalized_word,))
                row = cur.fetchone()
                if row:
                    cached_tag_label = row.get("tag_label")
                    object_category = row.get("object_category") or info.get("object_category")
                    info = {
                        "translation_zh": row.get("translation_zh") or info.get("translation_zh"),
                        "tag_label": cached_tag_label or info.get("tag_label") or object_category,
                        "reason": row.get("tag_reason") or info.get("reason"),
                        "object_category": object_category,
                        "plushable": row.get("plushable"),
                        "plushable_bool": row.get("plushable_bool"),
                    }

        # `items_table_name` kept for API compatibility, but trend query has been removed.
        points = freq_points

    return {
        "word": normalized_word,
        "info": info,
        "points": points,
        "latest_year_month": latest_year_month,
    }


def _normalize_asin(value: Any) -> str:
    return str(value or "").strip().upper()


def _normalize_keyword_text(value: Any) -> str:
    return str(value or "").replace("\ufffc", "").strip().lower()


def _parse_gkdatas_rows(value: Any) -> list[dict[str, Any]]:
    parsed = value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return []

    if isinstance(parsed, list):
        return [item for item in parsed if isinstance(item, dict)]
    if isinstance(parsed, dict):
        for key in ("items", "data", "list", "gkdatas"):
            val = parsed.get(key)
            if isinstance(val, list):
                return [item for item in val if isinstance(item, dict)]
    return []


def _parse_top3_asin_rows(value: Any) -> list[dict[str, Any]]:
    parsed = value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return []

    if isinstance(parsed, list):
        return [item for item in parsed if isinstance(item, dict)]
    if isinstance(parsed, dict):
        for key in ("items", "data", "list", "top3", "top3asindtolist"):
            val = parsed.get(key)
            if isinstance(val, list):
                return [item for item in val if isinstance(item, dict)]
    return []


def _extract_top3_rank(top3_value: Any, asin_norm: str) -> int | None:
    rows = _parse_top3_asin_rows(top3_value)
    if not rows:
        return None
    for idx, item in enumerate(rows, start=1):
        item_asin = _normalize_asin(item.get("asin"))
        if item_asin == asin_norm:
            return idx
    return None


def _parse_brand_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return [str(v).strip() for v in parsed if str(v).strip()]
        except json.JSONDecodeError:
            pass
        return [x.strip() for x in re.split(r"[,\n|]+", text) if x.strip()]
    return []


def _extract_top3_brand_hint(top3_asin_value: Any, top3_brands_value: Any, asin_norm: str) -> str:
    rows = _parse_top3_asin_rows(top3_asin_value)
    brands = _parse_brand_list(top3_brands_value)
    if not rows or not brands:
        return ""
    for idx, item in enumerate(rows):
        item_asin = _normalize_asin(item.get("asin"))
        if item_asin == asin_norm and idx < len(brands):
            return brands[idx]
    return ""


def fetch_asin_aba_history(
    schema_name: str,
    table_name: str,
    keyword: str,
    asin: str,
    limit_months: int = 36,
) -> dict[str, Any]:
    keyword_norm = _normalize_keyword_text(keyword)
    asin_norm = _normalize_asin(asin)
    if not keyword_norm or not asin_norm:
        return {"keyword": keyword_norm, "asin": asin_norm, "count": 0, "items": []}

    with pg_connect() as conn:
        profile = _get_table_profile(conn, schema_name, table_name)
        if not profile["exists"]:
            return {"keyword": keyword_norm, "asin": asin_norm, "count": 0, "items": []}

        columns = profile["valid_columns"]
        if "keyword" not in columns or "top3asindtolist" not in columns or "year_month" not in columns:
            return {"keyword": keyword_norm, "asin": asin_norm, "count": 0, "items": []}

        query = sql.SQL(
            """
            SELECT year_month, id, top3asindtolist
            FROM {}.{}
            WHERE lower(trim(both '"' from replace(CAST(keyword AS TEXT), chr(65532), ''))) = %s
              AND top3asindtolist IS NOT NULL
            ORDER BY year_month DESC, id ASC
            LIMIT 5000
            """
        ).format(sql.Identifier(schema_name), sql.Identifier(table_name))

        with conn.cursor() as cur:
            cur.execute(query, (keyword_norm,))
            rows = cur.fetchall()

    month_rank: dict[int, int] = {}
    for row in rows:
        ym = _to_int(row.get("year_month"))
        if ym is None or ym <= 0:
            continue
        if ym in month_rank:
            continue
        rank = _extract_top3_rank(row.get("top3asindtolist"), asin_norm)
        if rank is None:
            continue
        month_rank[ym] = rank

    yms = sorted(month_rank.keys(), reverse=True)
    if limit_months > 0:
        yms = yms[:limit_months]

    items = [
        {
            "year_month": ym,
            "label": f"{ym // 100}-{ym % 100:02d}",
            "rank": month_rank.get(ym),
        }
        for ym in yms
    ]
    return {
        "keyword": keyword_norm,
        "asin": asin_norm,
        "count": len(items),
        "items": items,
    }


def fetch_asin_detail(
    schema_name: str,
    table_name: str,
    asin: str,
    year_month: int | None = None,
    keyword: str | None = None,
    limit_rows: int = 3000,
) -> dict[str, Any]:
    asin_norm = _normalize_asin(asin)
    keyword_norm = _normalize_keyword_text(keyword or "") if keyword else ""
    if not asin_norm:
        return {"asin": "", "found": False, "detail": None}

    with pg_connect() as conn:
        profile = _get_table_profile(conn, schema_name, table_name)
        if not profile["exists"]:
            return {"asin": asin_norm, "found": False, "detail": None}

        columns = profile["valid_columns"]
        required_columns = {"year_month", "keyword", "gkdatas"}
        if not required_columns.issubset(columns):
            return {"asin": asin_norm, "found": False, "detail": None}

        has_top3 = "top3asindtolist" in columns and "top3brands" in columns

        select_cols = ["id", "year_month", "keyword", "gkdatas"]
        if has_top3:
            select_cols.extend(["top3asindtolist", "top3brands"])
        query = (
            sql.SQL("SELECT {} FROM {}.{} WHERE gkdatas IS NOT NULL AND CAST(gkdatas AS TEXT) ILIKE %s").format(
                sql.SQL(", ").join(sql.Identifier(col) for col in select_cols),
                sql.Identifier(schema_name),
                sql.Identifier(table_name),
            )
        )
        params: list[Any] = [f"%{asin_norm}%"]

        if year_month is not None:
            query += sql.SQL(" AND year_month = %s")
            params.append(year_month)

        query += sql.SQL(" ORDER BY year_month DESC, id DESC LIMIT %s")
        params.append(max(100, min(int(limit_rows or 3000), 10000)))

        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()

    best_score: tuple[int, int, int, int] | None = None
    best_detail: dict[str, Any] | None = None

    for row in rows:
        ym = _to_int(row.get("year_month")) or 0
        row_keyword = _normalize_keyword_text(row.get("keyword"))
        keyword_hit = 1 if keyword_norm and row_keyword == keyword_norm else 0

        top3_brand_hint = ""
        if "top3asindtolist" in row and "top3brands" in row:
            top3_brand_hint = _extract_top3_brand_hint(
                row.get("top3asindtolist"),
                row.get("top3brands"),
                asin_norm,
            )

        for item in _parse_gkdatas_rows(row.get("gkdatas")):
            item_asin = _normalize_asin(item.get("asin"))
            if item_asin != asin_norm:
                continue

            title = str(item.get("asinTitle") or item.get("asinUrl") or "").strip()
            image_url = str(item.get("asinImage") or item.get("bigAsinImage") or "").strip()
            brand = str(item.get("asinBrand") or item.get("brand") or top3_brand_hint or "").strip()
            station = str(item.get("station") or "").strip()
            badges = str(item.get("badges") or "").strip()
            price = _to_float(item.get("asinPrice"))
            rating = _to_float(item.get("asinRating"))
            reviews = _to_int(item.get("asinReviews"))
            position = _to_int(item.get("position")) or _to_int(item.get("rankIndex")) or _to_int(item.get("rank"))
            rank_page = _to_int(item.get("rankPage"))
            products = _to_int(item.get("products"))

            completeness = sum(
                1
                for value in (
                    title,
                    image_url,
                    brand,
                    station,
                    badges,
                    price,
                    rating,
                    reviews,
                    position,
                    rank_page,
                    products,
                )
                if value not in ("", None)
            )

            score = (keyword_hit, completeness, ym, -(position or 10**9))
            if best_score is None or score > best_score:
                best_score = score
                best_detail = {
                    "asin": asin_norm,
                    "title": title,
                    "imageUrl": image_url,
                    "brand": brand,
                    "station": station,
                    "badges": badges,
                    "price": price,
                    "rating": rating,
                    "reviews": reviews,
                    "position": position,
                    "rankPage": rank_page,
                    "products": products,
                    "year_month": ym if ym > 0 else None,
                    "keyword": row_keyword or None,
                }

    return {
        "asin": asin_norm,
        "found": best_detail is not None,
        "detail": best_detail,
    }
