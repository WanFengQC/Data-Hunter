from typing import Any

from psycopg import sql

from app.infrastructure.postgres.database import pg_connect
from app.services.postgres.common import (
    DEFAULT_WORD_CACHE_TABLE,
    WORD_SHIELD_SCOPE_ABA,
    WORD_SHIELD_SCOPE_WORD_FREQUENCY,
    _build_order_sql,
    _build_text_filter_clauses,
    _build_where_sql,
    _ensure_bool_column,
    _ensure_word_cache_table,
    _ensure_word_shield_table,
    _get_table_profile,
    _serialize_value,
    _to_float,
    _to_int,
    _upsert_word_cache_row,
)
def update_word_frequency_item(
    schema_name: str,
    table_name: str,
    item_id: int,
    word_zh: str | None,
    tag_label: str | None,
    reason: str | None,
) -> dict[str, Any]:
    normalized_word_zh = str(word_zh or "").strip() or None
    normalized_tag_label = str(tag_label or "").strip() or None
    normalized_reason = str(reason or "").strip() or None

    with pg_connect() as conn:
        profile = _get_table_profile(conn, schema_name, table_name)
        if not profile["exists"]:
            raise ValueError("Table does not exist")

        valid_columns = profile["valid_columns"]
        if "id" not in valid_columns:
            raise ValueError("Table does not support id updates")

        assignments: list[sql.SQL] = []
        params: list[Any] = []
        for column_name, value in (("word_zh", normalized_word_zh), ("标签", normalized_tag_label), ("原因", normalized_reason)):
            if column_name in valid_columns:
                assignments.append(sql.SQL("{} = %s").format(sql.Identifier(column_name)))
                params.append(value)

        if not assignments:
            raise ValueError("Editable columns are missing")

        if "updated_at" in valid_columns:
            assignments.append(sql.SQL("{} = NOW()").format(sql.Identifier("updated_at")))

        query = (
            sql.SQL("UPDATE {}.{} SET ").format(sql.Identifier(schema_name), sql.Identifier(table_name))
            + sql.SQL(", ").join(assignments)
            + sql.SQL(" WHERE id = %s RETURNING *")
        )

        with conn.cursor() as cur:
            cur.execute(query, [*params, item_id])
            row = cur.fetchone()

        if not row:
            raise ValueError("Item not found")

        cache_table_name = _ensure_word_cache_table(conn, schema_name, DEFAULT_WORD_CACHE_TABLE)
        _upsert_word_cache_row(
            conn,
            schema_name,
            cache_table_name,
            word=str(row.get("word") or ""),
            translation_zh=str(row.get("word_zh") or "").strip() or None,
            tag_label=str(row.get("标签") or "").strip() or None,
            tag_reason=str(row.get("原因") or "").strip() or None,
        )
        conn.commit()

    item: dict[str, Any] = {}
    for key, value in row.items():
        item[key] = _serialize_value(value)
    return item


def shield_word_frequency_items_by_word(
    schema_name: str,
    table_name: str,
    word: str,
    source_scope: str = WORD_SHIELD_SCOPE_WORD_FREQUENCY,
    shielded: bool = True,
    word_zh: str | None = None,
    tag_label: str | None = None,
    reason: str | None = None,
) -> dict[str, Any]:
    normalized_word = str(word or "").strip().lower()
    normalized_scope = str(source_scope or "").strip().lower()
    normalized_word_zh = str(word_zh or "").strip() or None
    normalized_tag_label = str(tag_label or "").strip() or None
    normalized_reason = str(reason or "").strip() or None
    if not normalized_word:
        raise ValueError("Word is required")
    if normalized_scope not in {WORD_SHIELD_SCOPE_WORD_FREQUENCY, WORD_SHIELD_SCOPE_ABA}:
        raise ValueError("Invalid shield source scope")

    with pg_connect() as conn:
        shield_table_name = _ensure_word_shield_table(conn, schema_name, table_name)
        query = sql.SQL(
            """
            INSERT INTO {}.{} (word, source_scope, word_zh, tag_label, reason, shielded, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (word, source_scope)
            DO UPDATE SET
                word_zh = COALESCE(EXCLUDED.word_zh, {}.{}.word_zh),
                tag_label = COALESCE(EXCLUDED.tag_label, {}.{}.tag_label),
                reason = COALESCE(EXCLUDED.reason, {}.{}.reason),
                shielded = EXCLUDED.shielded,
                updated_at = NOW()
            """
        ).format(
            sql.Identifier(schema_name),
            sql.Identifier(shield_table_name),
            sql.Identifier(schema_name),
            sql.Identifier(shield_table_name),
            sql.Identifier(schema_name),
            sql.Identifier(shield_table_name),
            sql.Identifier(schema_name),
            sql.Identifier(shield_table_name),
        )

        with conn.cursor() as cur:
            cur.execute(
                query,
                (
                    normalized_word,
                    normalized_scope,
                    normalized_word_zh,
                    normalized_tag_label,
                    normalized_reason,
                    shielded,
                ),
            )
            updated_count = cur.rowcount or 0
        conn.commit()

    return {
        "word": normalized_word,
        "source_scope": normalized_scope,
        "word_zh": normalized_word_zh,
        "tag_label": normalized_tag_label,
        "reason": normalized_reason,
        "shielded": shielded,
        "updated_count": int(updated_count),
    }

def fetch_shielded_word_frequency_items(
    schema_name: str,
    table_name: str,
    source_scope: str | None = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    normalized_limit = max(1, min(int(limit or 500), 5000))
    normalized_scope = str(source_scope or "").strip().lower()
    if normalized_scope and normalized_scope not in {WORD_SHIELD_SCOPE_WORD_FREQUENCY, WORD_SHIELD_SCOPE_ABA}:
        raise ValueError("Invalid shield source scope")

    with pg_connect() as conn:
        shield_table_name = _ensure_word_shield_table(conn, schema_name, table_name)

        if normalized_scope == WORD_SHIELD_SCOPE_ABA:
            shield_query = sql.SQL(
                """
                SELECT shield.word, shield.source_scope, shield.word_zh, shield.tag_label, shield.reason, shield.updated_at
                FROM {}.{} AS shield
                WHERE shield.shielded = TRUE
                  AND shield.source_scope = %s
                ORDER BY shield.updated_at DESC NULLS LAST, shield.word ASC
                LIMIT %s
                """
            ).format(
                sql.Identifier(schema_name),
                sql.Identifier(shield_table_name),
            )
            with conn.cursor() as cur:
                cur.execute(shield_query, (WORD_SHIELD_SCOPE_ABA, normalized_limit))
                shield_rows = cur.fetchall()

            detail_map: dict[str, dict[str, Any]] = {}
            aba_words = sorted(
                {
                    str(row.get("word") or "").strip().lower()
                    for row in shield_rows
                    if str(row.get("word") or "").strip()
                }
            )
            missing_zh_words = sorted(
                {
                    str(row.get("word") or "").strip().lower()
                    for row in shield_rows
                    if str(row.get("word") or "").strip() and not str(row.get("word_zh") or "").strip()
                }
            )
            aba_table_name = settings.pg_table
            aba_profile = _get_table_profile(conn, schema_name, aba_table_name)
            aba_valid_columns = aba_profile["valid_columns"] if aba_profile["exists"] else set()
            if missing_zh_words and len(missing_zh_words) <= 10 and aba_profile["exists"] and "keyword" in aba_valid_columns:
                keywordcn_select = (
                    sql.SQL("""trim(both '"' from CAST(src.keywordcn AS TEXT)) AS word_zh""")
                    if "keywordcn" in aba_valid_columns
                    else sql.SQL("NULL::text AS word_zh")
                )
                year_month_select = (
                    sql.SQL("src.year_month AS year_month")
                    if "year_month" in aba_valid_columns
                    else sql.SQL("NULL::int AS year_month")
                )
                id_order = sql.SQL(", src.id DESC NULLS LAST") if "id" in aba_valid_columns else sql.SQL("")
                aba_detail_query = sql.SQL(
                    """
                    SELECT DISTINCT ON (src.normalized_keyword)
                        src.normalized_keyword AS word,
                        {},
                        {}
                    FROM (
                        SELECT
                            lower(trim(both '"' from replace(CAST(aba.keyword AS TEXT), chr(65532), ''))) AS normalized_keyword,
                            aba.keywordcn,
                            {},
                            {}
                        FROM {}.{} AS aba
                        WHERE lower(trim(both '"' from replace(CAST(aba.keyword AS TEXT), chr(65532), ''))) = ANY(%s)
                    ) AS src
                    ORDER BY src.normalized_keyword, src.year_month DESC NULLS LAST{}
                    """
                ).format(
                    keywordcn_select,
                    year_month_select,
                    sql.SQL("aba.year_month") if "year_month" in aba_valid_columns else sql.SQL("NULL::int AS year_month"),
                    sql.SQL("aba.id") if "id" in aba_valid_columns else sql.SQL("NULL::bigint AS id"),
                    sql.Identifier(schema_name),
                    sql.Identifier(aba_table_name),
                    id_order,
                )
                with conn.cursor() as cur:
                    cur.execute(aba_detail_query, (missing_zh_words,))
                    for row in cur.fetchall():
                        normalized_word = str(row.get("word") or "").strip().lower()
                        if normalized_word:
                            detail_map[normalized_word] = {
                                "word_zh": row.get("word_zh"),
                                "tag_label": None,
                                "reason": None,
                                "year_month": row.get("year_month"),
                            }
                if detail_map:
                    update_query = sql.SQL(
                        """
                        UPDATE {}.{} AS shield
                        SET word_zh = src.word_zh,
                            updated_at = shield.updated_at
                        FROM (SELECT unnest(%s::text[]) AS word, unnest(%s::text[]) AS word_zh) AS src
                        WHERE shield.word = src.word
                          AND shield.source_scope = %s
                          AND (shield.word_zh IS NULL OR trim(shield.word_zh) = '')
                        """
                    ).format(
                        sql.Identifier(schema_name),
                        sql.Identifier(shield_table_name),
                    )
                    update_words = list(detail_map.keys())
                    update_word_zh = [str(detail_map[word].get("word_zh") or "") for word in update_words]
                    with conn.cursor() as cur:
                        cur.execute(update_query, (update_words, update_word_zh, WORD_SHIELD_SCOPE_ABA))
                    conn.commit()

            rows: list[dict[str, Any]] = []
            for shield_row in shield_rows:
                normalized_word = str(shield_row.get("word") or "").strip().lower()
                detail = detail_map.get(normalized_word, {})
                rows.append(
                    {
                        "word": normalized_word,
                        "source_scope": WORD_SHIELD_SCOPE_ABA,
                        "word_zh": str(shield_row.get("word_zh") or "").strip() or detail.get("word_zh"),
                        "tag_label": str(shield_row.get("tag_label") or "").strip() or detail.get("tag_label"),
                        "reason": str(shield_row.get("reason") or "").strip() or detail.get("reason"),
                        "year_month": detail.get("year_month"),
                        "updated_at": shield_row.get("updated_at"),
                    }
                )
        else:
            query = sql.SQL(
                """
                SELECT
                    shield.word,
                    shield.source_scope,
                    shield.word_zh,
                    shield.tag_label,
                    shield.reason,
                    NULL::int AS year_month,
                    shield.updated_at
                FROM {}.{} AS shield
                WHERE shield.shielded = TRUE
                {}
                ORDER BY shield.updated_at DESC NULLS LAST, shield.word ASC
                LIMIT %s
                """
            ).format(
                sql.Identifier(schema_name),
                sql.Identifier(shield_table_name),
                sql.SQL(" AND shield.source_scope = %s") if normalized_scope else sql.SQL(""),
            )

            with conn.cursor() as cur:
                query_params: list[Any] = []
                if normalized_scope:
                    query_params.append(normalized_scope)
                query_params.append(normalized_limit)
                cur.execute(query, query_params)
                rows = cur.fetchall()

    items: list[dict[str, Any]] = []
    seen_words: set[tuple[str, str]] = set()
    for row in rows:
        normalized_word = str(row.get("word") or "").strip().lower()
        row_scope = str(row.get("source_scope") or "").strip().lower() or WORD_SHIELD_SCOPE_WORD_FREQUENCY
        dedupe_key = (row_scope, normalized_word)
        if not normalized_word or dedupe_key in seen_words:
            continue
        seen_words.add(dedupe_key)
        item: dict[str, Any] = {}
        for key, value in row.items():
            item[key] = _serialize_value(value)
        items.append(item)
    return items
def fetch_growth_top10_items(
    schema_name: str,
    table_name: str,
    mode: str,
    year: int | None = None,
    month: int | None = None,
    search_min: float | None = None,
    search_max: float | None = None,
    page: int = 1,
    page_size: int = 10,
    tag_label: str = "2外形",
) -> dict[str, Any]:
    sort_by_map = {
        "monthly": "total_searches_growth_rate",
        "quarterly": "total_searches_quarter_avg_growth_rate",
        "searches": "total_searches",
    }
    sort_by = sort_by_map.get(str(mode).lower(), "total_searches_growth_rate")
    page = max(1, int(page or 1))
    page_size = max(1, min(int(page_size or 10), 100))
    offset = (page - 1) * page_size

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
        shield_table_name = _ensure_word_shield_table(conn, schema_name, table_name)
        profile = _get_table_profile(conn, schema_name, table_name)
        if not profile["exists"]:
            return {
                "columns": [],
                "items": [],
                "total": 0,
                "page": page,
                "page_size": page_size,
                "average_total_searches": None,
                "average_label": "骞冲潎鎬绘悳绱㈤噺",
            }

        columns = profile["columns"]
        response_columns = list(columns)
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
        shield_sql = sql.SQL(
            """
            NOT EXISTS (
                SELECT 1
                FROM {}.{} AS shield
                WHERE shield.shielded = TRUE
                  AND shield.source_scope = {}
                  AND shield.word = LOWER(TRIM(CAST({}.{}.word AS TEXT)))
            )
            """
        ).format(
            sql.Identifier(schema_name),
            sql.Identifier(shield_table_name),
            sql.Literal(WORD_SHIELD_SCOPE_WORD_FREQUENCY),
            sql.Identifier(schema_name),
            sql.Identifier(table_name),
        )
        if not where_params and where_sql.as_string(conn).strip() == "":
            where_sql = sql.SQL(" WHERE ") + shield_sql
        else:
            where_sql = where_sql + sql.SQL(" AND ") + shield_sql
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
            + sql.SQL(" LIMIT %s OFFSET %s")
        )
        count_query = (
            sql.SQL("SELECT COUNT(*) AS total FROM {}.{}").format(
                sql.Identifier(schema_name), sql.Identifier(table_name)
            )
            + where_sql
        )
        average_label = "褰撴湀骞冲潎鎬绘悳绱㈤噺" if mode != "quarterly" else "褰撳骞冲潎鎬绘悳绱㈤噺"
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
            avg_clauses.append(shield_sql)
            avg_params.extend(text_params)
            avg_where_sql = sql.SQL(" WHERE ") + sql.SQL(" AND ").join(avg_clauses)

        avg_query = (
            sql.SQL("SELECT AVG(CAST(total_searches AS DOUBLE PRECISION)) AS avg_total_searches FROM {}.{}").format(
                sql.Identifier(schema_name), sql.Identifier(table_name)
            )
            + avg_where_sql
        )

        with conn.cursor() as cur:
            cur.execute(count_query, where_params)
            count_row = cur.fetchone()
            total = int(count_row.get("total") or 0) if count_row else 0
            cur.execute(data_query, [*where_params, page_size, offset])
            rows = cur.fetchall()
            cur.execute(avg_query, avg_params)
            avg_row = cur.fetchone()

        trend_map: dict[str, Any] = {}
        if rows:
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
                if "trends" not in response_columns:
                    response_columns.append("trends")

    items: list[dict[str, Any]] = []
    for row in rows:
        item: dict[str, Any] = {}
        for key, value in row.items():
            item[key] = _serialize_value(value)
        row_word = str(row.get("word") or "").strip().lower()
        if row_word:
            item["trends"] = trend_map.get(row_word)
        items.append(item)

    avg_total_searches = None
    if avg_row:
        avg_total_searches = _serialize_value(avg_row.get("avg_total_searches"))

    return {
        "columns": response_columns,
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "average_total_searches": avg_total_searches,
        "average_label": average_label,
    }

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


def fetch_word_cache_batch(
    schema_name: str,
    cache_table_name: str,
    words: list[str],
) -> dict[str, dict[str, Any]]:
    normalized_words = sorted({str(item or "").strip().lower() for item in (words or []) if str(item or "").strip()})
    if not normalized_words:
        return {}

    output: dict[str, dict[str, Any]] = {}
    with pg_connect() as conn:
        profile = _get_table_profile(conn, schema_name, cache_table_name)
        if not profile["exists"]:
            return {}

        valid_columns = profile["valid_columns"]
        select_fields: list[sql.SQL] = [sql.SQL("word")]
        if "translation_zh" in valid_columns:
            select_fields.append(sql.SQL("translation_zh"))
        elif "word_zh" in valid_columns:
            select_fields.append(sql.SQL("word_zh AS translation_zh"))
        else:
            select_fields.append(sql.SQL("NULL::text AS translation_zh"))

        if "tag_label" in valid_columns:
            select_fields.append(sql.SQL("tag_label"))
        elif "标签" in valid_columns:
            select_fields.append(sql.SQL("{} AS tag_label").format(sql.Identifier("标签")))
        else:
            select_fields.append(sql.SQL("NULL::text AS tag_label"))

        if "tag_reason" in valid_columns:
            select_fields.append(sql.SQL("tag_reason"))
        elif "原因" in valid_columns:
            select_fields.append(sql.SQL("{} AS tag_reason").format(sql.Identifier("原因")))
        else:
            select_fields.append(sql.SQL("NULL::text AS tag_reason"))

        query = sql.SQL(
            """
            SELECT {}
            FROM {}.{}
            WHERE word = ANY(%s)
            """
        ).format(
            sql.SQL(", ").join(select_fields),
            sql.Identifier(schema_name),
            sql.Identifier(cache_table_name),
        )
        with conn.cursor() as cur:
            cur.execute(query, (normalized_words,))
            rows = cur.fetchall()

    for row in rows:
        word = str(row.get("word") or "").strip().lower()
        if not word:
            continue
        output[word] = {
            "translation_zh": str(row.get("translation_zh") or "").strip() or None,
            "tag_label": str(row.get("tag_label") or "").strip() or None,
            "reason": str(row.get("tag_reason") or "").strip() or None,
        }
    return output


def upsert_word_cache_batch(
    schema_name: str,
    cache_table_name: str,
    items: list[dict[str, Any]],
) -> dict[str, int]:
    normalized_items: list[dict[str, Any]] = []
    for raw in items or []:
        if not isinstance(raw, dict):
            continue
        word = str(raw.get("word") or "").strip().lower()
        if not word:
            continue
        normalized_items.append(
            {
                "word": word,
                "translation_zh": str(raw.get("translation_zh") or "").strip() or None,
                "tag_label": str(raw.get("tag_label") or "").strip() or None,
                "tag_reason": str(raw.get("reason") or raw.get("tag_reason") or "").strip() or None,
            }
        )

    if not normalized_items:
        return {"upserted_count": 0}

    with pg_connect() as conn:
        table_name = _ensure_word_cache_table(conn, schema_name, cache_table_name)
        for item in normalized_items:
            _upsert_word_cache_row(
                conn,
                schema_name,
                table_name,
                word=item["word"],
                translation_zh=item["translation_zh"],
                tag_label=item["tag_label"],
                tag_reason=item["tag_reason"],
            )
        conn.commit()

    return {"upserted_count": len(normalized_items)}


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

