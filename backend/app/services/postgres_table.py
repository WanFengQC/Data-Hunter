from datetime import date, datetime
from decimal import Decimal
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from app.core.config import settings

BLANK_TOKEN = "__BLANK__"


def pg_connect() -> psycopg.Connection:
    return psycopg.connect(
        host=settings.pg_host,
        user=settings.pg_user,
        password=settings.pg_pass,
        port=settings.pg_port,
        dbname=settings.pg_db,
        row_factory=dict_row,
    )


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
    text_filters: dict[str, str],
    valid_columns: set[str],
) -> tuple[list[sql.SQL], list[Any]]:
    clauses: list[sql.SQL] = []
    params: list[Any] = []

    for key, raw_value in text_filters.items():
        if key not in valid_columns:
            continue
        value = str(raw_value).strip()
        if not value:
            continue
        clauses.append(sql.SQL("CAST({} AS TEXT) ILIKE %s").format(sql.Identifier(key)))
        params.append(f"%{value}%")

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
    text_filters: dict[str, str] | None = None,
    value_filters: dict[str, list[str]] | None = None,
    exclude_value_filter_column: str | None = None,
) -> tuple[sql.SQL, list[Any]]:
    clauses: list[sql.SQL] = []
    params: list[Any] = []

    t_clauses, t_params = _build_time_clauses(year, month)
    clauses.extend(t_clauses)
    params.extend(t_params)

    f_clauses, f_params = _build_text_filter_clauses(text_filters or {}, valid_columns)
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
            order_expr = sql.SQL("CAST({} AS TEXT)").format(sql.Identifier(sort_by))
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
        if not _table_exists(conn, schema_name, table_name):
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
    text_filters: dict[str, str] | None = None,
    value_filters: dict[str, list[str]] | None = None,
    keyword: str | None = None,
    limit: int = 300,
) -> list[dict[str, Any]]:
    limit = min(max(limit, 10), 1000)

    with pg_connect() as conn:
        if not _table_exists(conn, schema_name, table_name):
            return []

        column_meta = _get_column_meta(conn, schema_name, table_name)
        valid_columns = {row["column_name"] for row in column_meta}
        if column not in valid_columns:
            return []

        where_sql, where_params = _build_where_sql(
            year=year,
            month=month,
            valid_columns=valid_columns,
            text_filters=text_filters,
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
    keyword_norm = (keyword or "").strip().lower()
    for row in rows:
        value = row["value"]
        label = "(空白)" if value == BLANK_TOKEN else str(value)
        if keyword_norm and keyword_norm not in label.lower():
            continue
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
    filters: dict[str, str] | None = None,  # backward-compatible alias
    text_filters: dict[str, str] | None = None,
    value_filters: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    page = max(page, 1)
    page_size = max(page_size, 1)
    offset = (page - 1) * page_size

    merged_text_filters: dict[str, str] = {}
    if filters:
        merged_text_filters.update(filters)
    if text_filters:
        merged_text_filters.update(text_filters)

    with pg_connect() as conn:
        if not _table_exists(conn, schema_name, table_name):
            return {"columns": [], "items": [], "total": 0, "page": page, "page_size": page_size}

        column_meta = _get_column_meta(conn, schema_name, table_name)
        columns = [row["column_name"] for row in column_meta]
        column_types = {row["column_name"]: row["data_type"] for row in column_meta}
        valid_columns = set(columns)

        where_sql, where_params = _build_where_sql(
            year=year,
            month=month,
            valid_columns=valid_columns,
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


def fetch_all_items(
    schema_name: str,
    table_name: str,
    year: int | None = None,
    month: int | None = None,
    sort_by: str | None = None,
    sort_dir: str = "desc",
    filters: dict[str, str] | None = None,  # backward-compatible alias
    text_filters: dict[str, str] | None = None,
    value_filters: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    merged_text_filters: dict[str, str] = {}
    if filters:
        merged_text_filters.update(filters)
    if text_filters:
        merged_text_filters.update(text_filters)

    with pg_connect() as conn:
        if not _table_exists(conn, schema_name, table_name):
            return {"columns": [], "items": [], "total": 0}

        column_meta = _get_column_meta(conn, schema_name, table_name)
        columns = [row["column_name"] for row in column_meta]
        column_types = {row["column_name"]: row["data_type"] for row in column_meta}
        valid_columns = set(columns)

        where_sql, where_params = _build_where_sql(
            year=year,
            month=month,
            valid_columns=valid_columns,
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
        )

        with conn.cursor() as cur:
            cur.execute(count_query, where_params)
            total = int(cur.fetchone()["total"])
            cur.execute(data_query, where_params)
            rows = cur.fetchall()

    items: list[dict[str, Any]] = []
    for row in rows:
        item: dict[str, Any] = {}
        for key, value in row.items():
            item[key] = _serialize_value(value)
        items.append(item)

    return {"columns": columns, "items": items, "total": total}
