import csv
from io import StringIO
from time import monotonic
from typing import Any, Iterator

from psycopg import sql

from app.infrastructure.postgres.database import pg_connect
from app.services.postgres.common import (
    _YEAR_MONTHS_CACHE,
    _YEAR_MONTHS_CACHE_LOCK,
    _YEAR_MONTHS_CACHE_TTL_SECONDS,
    _build_order_sql,
    _build_where_sql,
    _ensure_aba_keyword_norm_index,
    _ensure_default_year_month_index,
    _get_table_profile,
    _resolve_selected_columns,
    _serialize_value,
)
def fetch_year_months(schema_name: str, table_name: str) -> list[int]:
    cache_key = (schema_name, table_name)
    now = monotonic()
    with _YEAR_MONTHS_CACHE_LOCK:
        cached = _YEAR_MONTHS_CACHE.get(cache_key)
        if cached is not None and float(cached.get("expires_at", 0)) > now:
            return list(cached.get("items", []))

    _ensure_default_year_month_index(schema_name, table_name)

    query = sql.SQL(
        "SELECT DISTINCT year_month FROM {}.{} "
        "WHERE year_month IS NOT NULL "
        "ORDER BY year_month DESC"
    ).format(sql.Identifier(schema_name), sql.Identifier(table_name))

    with pg_connect() as conn:
        profile = _get_table_profile(conn, schema_name, table_name)
        if not profile["exists"] or "year_month" not in profile["valid_columns"]:
            return []
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

    values = [int(row["year_month"]) for row in rows if row["year_month"] is not None]
    with _YEAR_MONTHS_CACHE_LOCK:
        _YEAR_MONTHS_CACHE[cache_key] = {
            "items": list(values),
            "expires_at": monotonic() + _YEAR_MONTHS_CACHE_TTL_SECONDS,
        }
    return values

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
        label = "(绌虹櫧)" if value == BLANK_TOKEN else str(value)
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

    response_columns: list[str] = []
    trend_map: dict[str, Any] = {}
    include_trends = False

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

        response_columns = list(columns)
        if table_name == "seller_sprite_word_frequency" and rows:
            cache_profile = _get_table_profile(conn, schema_name, "seller_sprite_word_cache")
            if cache_profile["exists"] and "google_trends_points" in cache_profile["valid_columns"]:
                row_words = sorted(
                    {
                        str(row.get("word") or "").strip().lower()
                        for row in rows
                        if str(row.get("word") or "").strip()
                    }
                )
                if row_words:
                    trend_query = sql.SQL(
                        """
                        SELECT word, google_trends_points
                        FROM {}.{}
                        WHERE word = ANY(%s)
                        """
                    ).format(sql.Identifier(schema_name), sql.Identifier("seller_sprite_word_cache"))
                    with conn.cursor() as cur:
                        cur.execute(trend_query, (row_words,))
                        trend_rows = cur.fetchall()
                    for trend_row in trend_rows:
                        word = str(trend_row.get("word") or "").strip().lower()
                        if word:
                            trend_map[word] = _serialize_value(trend_row.get("google_trends_points"))
                include_trends = True
                if "trends" not in response_columns:
                    response_columns.append("trends")

    items: list[dict[str, Any]] = []
    for row in rows:
        item: dict[str, Any] = {}
        for key, value in row.items():
            item[key] = _serialize_value(value)
        if include_trends:
            row_word = str(row.get("word") or "").strip().lower()
            item["trends"] = trend_map.get(row_word)
        items.append(item)

    return {
        "columns": response_columns,
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

