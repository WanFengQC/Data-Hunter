import json
import re
from datetime import date, datetime
from decimal import Decimal
from threading import Lock
from time import monotonic
from typing import Any

import psycopg
from psycopg import sql

from app.core.config import settings
from app.infrastructure.postgres.database import pg_connect, pg_connect_autocommit
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
_YEAR_MONTHS_CACHE_TTL_SECONDS = 300.0
_YEAR_MONTHS_CACHE: dict[tuple[str, str], dict[str, Any]] = {}
_YEAR_MONTHS_CACHE_LOCK = Lock()
_YEAR_MONTH_INDEX_ATTEMPTS: set[tuple[str, str]] = set()
_YEAR_MONTH_INDEX_ATTEMPTS_LOCK = Lock()
_ABA_KEYWORD_INDEX_ATTEMPTS: set[tuple[str, str]] = set()
_ABA_KEYWORD_INDEX_ATTEMPTS_LOCK = Lock()
_WORD_SHIELD_TABLE_SUFFIX = "_shield"
WORD_SHIELD_SCOPE_WORD_FREQUENCY = "word_frequency"
WORD_SHIELD_SCOPE_ABA = "aba"
DEFAULT_WORD_CACHE_TABLE = "seller_sprite_word_cache"


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


def _invalidate_table_profile(schema_name: str, table_name: str) -> None:
    key = (schema_name, table_name)
    with _TABLE_PROFILE_LOCK:
        _TABLE_PROFILE_CACHE.pop(key, None)


def _ensure_bool_column(
    conn: psycopg.Connection,
    schema_name: str,
    table_name: str,
    profile: dict[str, Any],
    column_name: str,
) -> dict[str, Any]:
    if not profile["exists"]:
        return profile
    if column_name in profile["valid_columns"]:
        return profile

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS {} BOOLEAN NOT NULL DEFAULT FALSE").format(
                sql.Identifier(schema_name),
                sql.Identifier(table_name),
                sql.Identifier(column_name),
            )
        )
    conn.commit()

    _invalidate_table_profile(schema_name, table_name)
    return _get_table_profile(conn, schema_name, table_name)


def _word_shield_table_name(table_name: str) -> str:
    normalized = str(table_name or "").strip()
    if not normalized:
        raise ValueError("Table name is required")
    return f"{normalized}{_WORD_SHIELD_TABLE_SUFFIX}"


def _ensure_word_shield_table(
    conn: psycopg.Connection,
    schema_name: str,
    table_name: str,
) -> str:
    shield_table_name = _word_shield_table_name(table_name)
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {}.{} (
                    word TEXT NOT NULL,
                    source_scope TEXT NOT NULL DEFAULT 'word_frequency',
                    word_zh TEXT NULL,
                    tag_label TEXT NULL,
                    reason TEXT NULL,
                    shielded BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
                )
                """
            ).format(
                sql.Identifier(schema_name),
                sql.Identifier(shield_table_name),
            )
        )
        cur.execute(
            sql.SQL(
                'ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS source_scope TEXT NOT NULL DEFAULT {}'
            ).format(
                sql.Identifier(schema_name),
                sql.Identifier(shield_table_name),
                sql.Literal(WORD_SHIELD_SCOPE_WORD_FREQUENCY),
            )
        )
        cur.execute(
            sql.SQL("ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS word_zh TEXT NULL").format(
                sql.Identifier(schema_name),
                sql.Identifier(shield_table_name),
            )
        )
        cur.execute(
            sql.SQL("ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS tag_label TEXT NULL").format(
                sql.Identifier(schema_name),
                sql.Identifier(shield_table_name),
            )
        )
        cur.execute(
            sql.SQL("ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS reason TEXT NULL").format(
                sql.Identifier(schema_name),
                sql.Identifier(shield_table_name),
            )
        )
        cur.execute(
            sql.SQL("UPDATE {}.{} SET source_scope = {} WHERE source_scope IS NULL OR trim(source_scope) = ''").format(
                sql.Identifier(schema_name),
                sql.Identifier(shield_table_name),
                sql.Literal(WORD_SHIELD_SCOPE_WORD_FREQUENCY),
            )
        )
        cur.execute(
            sql.SQL("ALTER TABLE {}.{} DROP CONSTRAINT IF EXISTS {}").format(
                sql.Identifier(schema_name),
                sql.Identifier(shield_table_name),
                sql.Identifier(f"{shield_table_name}_pkey"),
            )
        )
        cur.execute(
            sql.SQL("CREATE UNIQUE INDEX IF NOT EXISTS {} ON {}.{} (word, source_scope)").format(
                sql.Identifier(f"{shield_table_name}_word_scope_uidx"),
                sql.Identifier(schema_name),
                sql.Identifier(shield_table_name),
            )
        )
    conn.commit()
    return shield_table_name


def _ensure_word_cache_table(
    conn: psycopg.Connection,
    schema_name: str,
    table_name: str = DEFAULT_WORD_CACHE_TABLE,
) -> str:
    normalized_table_name = str(table_name or DEFAULT_WORD_CACHE_TABLE).strip() or DEFAULT_WORD_CACHE_TABLE
    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema_name)))
        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {}.{} (
                    word TEXT PRIMARY KEY,
                    translation_zh TEXT NULL,
                    tag_label TEXT NULL,
                    tag_reason TEXT NULL,
                    source_scope TEXT NOT NULL DEFAULT 'word_frequency',
                    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
                )
                """
            ).format(
                sql.Identifier(schema_name),
                sql.Identifier(normalized_table_name),
            )
        )
        cur.execute(
            sql.SQL("ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS translation_zh TEXT NULL").format(
                sql.Identifier(schema_name),
                sql.Identifier(normalized_table_name),
            )
        )
        cur.execute(
            sql.SQL("ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS tag_label TEXT NULL").format(
                sql.Identifier(schema_name),
                sql.Identifier(normalized_table_name),
            )
        )
        cur.execute(
            sql.SQL("ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS tag_reason TEXT NULL").format(
                sql.Identifier(schema_name),
                sql.Identifier(normalized_table_name),
            )
        )
        cur.execute(
            sql.SQL(
                "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS source_scope TEXT NOT NULL DEFAULT {}"
            ).format(
                sql.Identifier(schema_name),
                sql.Identifier(normalized_table_name),
                sql.Literal(WORD_SHIELD_SCOPE_WORD_FREQUENCY),
            )
        )
        cur.execute(
            sql.SQL(
                "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()"
            ).format(
                sql.Identifier(schema_name),
                sql.Identifier(normalized_table_name),
            )
        )
        cur.execute(
            sql.SQL(
                "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()"
            ).format(
                sql.Identifier(schema_name),
                sql.Identifier(normalized_table_name),
            )
        )
    conn.commit()
    _invalidate_table_profile(schema_name, normalized_table_name)
    return normalized_table_name


def _upsert_word_cache_row(
    conn: psycopg.Connection,
    schema_name: str,
    table_name: str,
    *,
    word: str,
    translation_zh: str | None = None,
    tag_label: str | None = None,
    tag_reason: str | None = None,
) -> None:
    normalized_word = str(word or "").strip().lower()
    if not normalized_word:
        return

    normalized_translation = str(translation_zh or "").strip() or None
    normalized_tag_label = str(tag_label or "").strip() or None
    normalized_tag_reason = str(tag_reason or "").strip() or None
    query = sql.SQL(
        """
        INSERT INTO {}.{} (word, translation_zh, tag_label, tag_reason, source_scope, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
        ON CONFLICT (word)
        DO UPDATE SET
            translation_zh = COALESCE(EXCLUDED.translation_zh, {}.{}.translation_zh),
            tag_label = COALESCE(EXCLUDED.tag_label, {}.{}.tag_label),
            tag_reason = COALESCE(EXCLUDED.tag_reason, {}.{}.tag_reason),
            updated_at = NOW()
        """
    ).format(
        sql.Identifier(schema_name),
        sql.Identifier(table_name),
        sql.Identifier(schema_name),
        sql.Identifier(table_name),
        sql.Identifier(schema_name),
        sql.Identifier(table_name),
        sql.Identifier(schema_name),
        sql.Identifier(table_name),
    )

    with conn.cursor() as cur:
        cur.execute(
            query,
            (
                normalized_word,
                normalized_translation,
                normalized_tag_label,
                normalized_tag_reason,
                WORD_SHIELD_SCOPE_WORD_FREQUENCY,
            ),
        )


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


def _index_exists(
    conn: psycopg.Connection,
    schema_name: str,
    table_name: str,
    index_name: str,
) -> bool:
    query = """
        SELECT EXISTS (
            SELECT 1
            FROM pg_indexes
            WHERE schemaname = %s
              AND tablename = %s
              AND indexname = %s
        ) AS exists
    """
    with conn.cursor() as cur:
        cur.execute(query, (schema_name, table_name, index_name))
        row = cur.fetchone()
    return bool(row["exists"])


def _ensure_default_year_month_index(schema_name: str, table_name: str) -> None:
    if table_name != settings.pg_table:
        return

    cache_key = (schema_name, table_name)
    with _YEAR_MONTH_INDEX_ATTEMPTS_LOCK:
        if cache_key in _YEAR_MONTH_INDEX_ATTEMPTS:
            return

    index_name = f"idx_{table_name}_year_month"
    try:
        with pg_connect() as conn:
            profile = _get_table_profile(conn, schema_name, table_name)
            if not profile["exists"] or "year_month" not in profile["valid_columns"]:
                return
            if _index_exists(conn, schema_name, table_name, index_name):
                with _YEAR_MONTH_INDEX_ATTEMPTS_LOCK:
                    _YEAR_MONTH_INDEX_ATTEMPTS.add(cache_key)
                return

        # Old seller_sprite_items imports did not create a year_month index.
        with pg_connect_autocommit() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("CREATE INDEX CONCURRENTLY IF NOT EXISTS {} ON {}.{} (year_month)").format(
                        sql.Identifier(index_name),
                        sql.Identifier(schema_name),
                        sql.Identifier(table_name),
                    )
                )
    except Exception:
        with _YEAR_MONTH_INDEX_ATTEMPTS_LOCK:
            _YEAR_MONTH_INDEX_ATTEMPTS.add(cache_key)
        return

    with _YEAR_MONTH_INDEX_ATTEMPTS_LOCK:
        _YEAR_MONTH_INDEX_ATTEMPTS.add(cache_key)


def _ensure_aba_keyword_norm_index(schema_name: str, table_name: str) -> None:
    if table_name != settings.pg_table:
        return

    cache_key = (schema_name, table_name)
    with _ABA_KEYWORD_INDEX_ATTEMPTS_LOCK:
        if cache_key in _ABA_KEYWORD_INDEX_ATTEMPTS:
            return

    index_name = f"idx_{table_name}_keyword_norm"
    try:
        with pg_connect() as conn:
            profile = _get_table_profile(conn, schema_name, table_name)
            if not profile["exists"] or "keyword" not in profile["valid_columns"]:
                return
            if _index_exists(conn, schema_name, table_name, index_name):
                with _ABA_KEYWORD_INDEX_ATTEMPTS_LOCK:
                    _ABA_KEYWORD_INDEX_ATTEMPTS.add(cache_key)
                return

        with pg_connect_autocommit() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        CREATE INDEX CONCURRENTLY IF NOT EXISTS {} ON {}.{} (
                            lower(trim(both '"' from replace(CAST(keyword AS TEXT), chr(65532), '')))
                        )
                        """
                    ).format(
                        sql.Identifier(index_name),
                        sql.Identifier(schema_name),
                        sql.Identifier(table_name),
                    )
                )
    except Exception:
        with _ABA_KEYWORD_INDEX_ATTEMPTS_LOCK:
            _ABA_KEYWORD_INDEX_ATTEMPTS.add(cache_key)
        return

    with _ABA_KEYWORD_INDEX_ATTEMPTS_LOCK:
        _ABA_KEYWORD_INDEX_ATTEMPTS.add(cache_key)


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

