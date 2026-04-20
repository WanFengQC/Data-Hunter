from typing import Any

from psycopg import sql

from app.infrastructure.postgres.database import pg_connect
from app.services.postgres.common import _get_table_profile, _to_float, _to_int


def _normalize_pounds_view(view: str | None) -> str:
    normalized = str(view or "yearly").strip().lower()
    if normalized not in {"yearly", "monthly"}:
        raise ValueError("Invalid pounds view")
    return normalized


def _weighted_blankets_required_columns(profile: dict[str, Any]) -> bool:
    required_columns = {
        "asin",
        "parent",
        "title",
        "brand",
        "imageurl",
        "weight",
        "dimensions",
        "sellername",
        "price",
        "totalunits",
        "totalamount",
        "year_month",
    }
    return required_columns.issubset(profile.get("valid_columns", set()))


def _weighted_blankets_period_clause(view: str, year: int | None, month: int | None) -> tuple[sql.SQL, list[Any]]:
    normalized_view = _normalize_pounds_view(view)
    clauses: list[sql.SQL] = []
    params: list[Any] = []

    if normalized_view == "monthly":
        if year is None or month is None:
            raise ValueError("Monthly pounds view requires both year and month")
        clauses.append(sql.SQL("year_month = %s"))
        params.append(year * 100 + month)
    elif year is not None:
        clauses.append(sql.SQL("year_month BETWEEN %s AND %s"))
        params.extend([year * 100, year * 100 + 99])

    if not clauses:
        return sql.SQL(""), params

    return sql.SQL(" WHERE ") + sql.SQL(" AND ").join(clauses), params


def _normalize_bucket_step(bucket_step: float | None) -> float | None:
    if bucket_step is None:
        return None
    step = float(bucket_step)
    if step <= 0:
        raise ValueError("bucket_step must be greater than 0")
    if step > 10:
        raise ValueError("bucket_step is too large")
    return step


def _pounds_bucket_expr(bucket_step: float | None) -> tuple[sql.SQL, list[Any]]:
    normalized_step = _normalize_bucket_step(bucket_step)
    if normalized_step is None:
        return sql.SQL("f.pounds_value"), []
    return sql.SQL("FLOOR((f.pounds_value - 1.0) / %s) * %s + 1.0"), [normalized_step, normalized_step]


def _build_weighted_blankets_scope_sql(
    schema_name: str,
    table_name: str,
    view: str,
    year: int | None,
    month: int | None,
    bucket_step: float | None = None,
) -> tuple[sql.SQL, list[Any]]:
    period_where_sql, period_params = _weighted_blankets_period_clause(view=view, year=year, month=month)
    bucket_expr_sql, bucket_expr_params = _pounds_bucket_expr(bucket_step)
    base_sql = sql.SQL(
        """
        WITH filtered AS (
            SELECT
                t.*,
                CASE
                    WHEN substring(lower(coalesce(CAST(t.title AS TEXT), '')) FROM '([0-9]+(\\.[0-9]+)?)\\s*(lb|lbs|pound|pounds)') IS NOT NULL
                    THEN CAST(substring(lower(coalesce(CAST(t.title AS TEXT), '')) FROM '([0-9]+(\\.[0-9]+)?)\\s*(lb|lbs|pound|pounds)') AS DOUBLE PRECISION)
                    ELSE NULL
                END AS pounds_value
            FROM {}.{} t
            {}
        ),
        scoped AS (
            SELECT
                f.*,
                {} AS pounds_bucket
            FROM filtered f
            WHERE f.pounds_value IS NOT NULL
              AND f.pounds_value BETWEEN 1 AND 35
              AND NOT EXISTS (
                  SELECT 1
                  FROM {}.{} c
                  WHERE c.year_month = f.year_month
                    AND trim(coalesce(CAST(c.parent AS TEXT), '')) <> ''
                    AND upper(trim(CAST(c.parent AS TEXT))) = upper(trim(CAST(f.asin AS TEXT)))
              )
        )
        """
    ).format(
        sql.Identifier(schema_name),
        sql.Identifier(table_name),
        period_where_sql,
        bucket_expr_sql,
        sql.Identifier(schema_name),
        sql.Identifier(table_name),
    )
    return base_sql, [*period_params, *bucket_expr_params]


def _format_pounds_label(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.2f}".rstrip("0").rstrip(".")


def _format_pounds_range_label(lower: float | None, step: float | None) -> str:
    if lower is None:
        return ""
    if step is None:
        return _format_pounds_label(lower)
    upper = lower + step
    return f"{_format_pounds_label(lower)}-{_format_pounds_label(upper)}"


def fetch_weighted_blankets_pounds_summary(
    schema_name: str,
    table_name: str,
    view: str = "yearly",
    year: int | None = None,
    month: int | None = None,
    bucket_step: float | None = None,
) -> dict[str, Any]:
    normalized_view = _normalize_pounds_view(view)
    normalized_bucket_step = _normalize_bucket_step(bucket_step)

    with pg_connect() as conn:
        profile = _get_table_profile(conn, schema_name, table_name)
        if not profile["exists"] or not _weighted_blankets_required_columns(profile):
            return {
                "view": normalized_view,
                "year": year,
                "month": month,
                "bucket_step": normalized_bucket_step,
                "items": [],
                "total_products": 0,
                "total_units": 0.0,
                "total_amount": 0.0,
            }

        scope_sql, scope_params = _build_weighted_blankets_scope_sql(
            schema_name=schema_name,
            table_name=table_name,
            view=normalized_view,
            year=year,
            month=month,
            bucket_step=normalized_bucket_step,
        )
        query = (
            scope_sql
            + sql.SQL(
                """
                SELECT
                    pounds_bucket AS pounds,
                    (pounds_bucket + %s) AS pounds_to,
                    COUNT(*) AS product_count,
                    COALESCE(SUM(totalunits), 0) AS total_units,
                    COALESCE(SUM(totalamount), 0) AS total_amount,
                    AVG(price) AS avg_price,
                    COUNT(DISTINCT year_month) AS active_periods
                FROM scoped
                GROUP BY pounds_bucket
                ORDER BY pounds_bucket ASC
                """
            )
        )

        with conn.cursor() as cur:
            cur.execute(query, [*scope_params, normalized_bucket_step or 0.0])
            rows = cur.fetchall()

    items: list[dict[str, Any]] = []
    total_products = 0
    total_units = 0.0
    total_amount = 0.0
    for row in rows:
        pounds_value = _to_float(row.get("pounds"))
        product_count = _to_int(row.get("product_count")) or 0
        units_value = _to_float(row.get("total_units")) or 0.0
        amount_value = _to_float(row.get("total_amount")) or 0.0
        total_products += product_count
        total_units += units_value
        total_amount += amount_value
        items.append(
            {
                "pounds": pounds_value,
                "pounds_to": _to_float(row.get("pounds_to")),
                "pounds_label": _format_pounds_range_label(pounds_value, normalized_bucket_step),
                "product_count": product_count,
                "total_units": units_value,
                "total_amount": amount_value,
                "avg_price": _to_float(row.get("avg_price")),
                "active_periods": _to_int(row.get("active_periods")) or 0,
            }
        )

    return {
        "view": normalized_view,
        "year": year,
        "month": month,
        "bucket_step": normalized_bucket_step,
        "items": items,
        "total_products": total_products,
        "total_units": total_units,
        "total_amount": total_amount,
    }


def fetch_weighted_blankets_pounds_detail(
    schema_name: str,
    table_name: str,
    pounds: float,
    view: str = "yearly",
    year: int | None = None,
    month: int | None = None,
    limit: int = 100,
    bucket_step: float | None = None,
) -> dict[str, Any]:
    normalized_view = _normalize_pounds_view(view)
    normalized_bucket_step = _normalize_bucket_step(bucket_step)
    safe_limit = max(1, min(int(limit or 100), 300))

    with pg_connect() as conn:
        profile = _get_table_profile(conn, schema_name, table_name)
        if not profile["exists"] or not _weighted_blankets_required_columns(profile):
            return {
                "view": normalized_view,
                "year": year,
                "month": month,
                "bucket_step": normalized_bucket_step,
                "pounds": pounds,
                "pounds_label": _format_pounds_label(pounds),
                "summary": None,
                "products": [],
            }

        scope_sql, scope_params = _build_weighted_blankets_scope_sql(
            schema_name=schema_name,
            table_name=table_name,
            view=normalized_view,
            year=year,
            month=month,
            bucket_step=normalized_bucket_step,
        )
        summary_template = sql.SQL(
            """
            SELECT
                COUNT(*) AS product_count,
                COALESCE(SUM(totalunits), 0) AS total_units,
                COALESCE(SUM(totalamount), 0) AS total_amount,
                AVG(price) AS avg_price,
                COUNT(DISTINCT year_month) AS active_periods
            FROM scoped
            WHERE {}
            """
        )
        products_template = sql.SQL(
            """
            SELECT
                asin,
                MAX(title) AS title,
                MAX(brand) AS brand,
                MAX(imageurl) AS imageurl,
                MAX(weight) AS weight,
                MAX(dimensions) AS dimensions,
                MAX(sellername) AS sellername,
                MAX(parent) AS parent,
                COALESCE(SUM(totalunits), 0) AS total_units,
                COALESCE(SUM(totalamount), 0) AS total_amount,
                AVG(price) AS avg_price,
                COUNT(DISTINCT year_month) AS active_periods,
                MAX(year_month) AS latest_year_month
            FROM scoped
            WHERE {}
            GROUP BY asin
            ORDER BY total_units DESC, total_amount DESC, asin ASC
            LIMIT %s
            """
        )

        if normalized_bucket_step is None:
            bucket_match_sql = sql.SQL("ABS(pounds_bucket - %s) < 0.0001")
            summary_params = [*scope_params, pounds]
            products_params = [*scope_params, pounds, safe_limit]
        else:
            bucket_upper = pounds + normalized_bucket_step
            bucket_match_sql = sql.SQL("pounds_bucket >= %s AND pounds_bucket < %s")
            summary_params = [*scope_params, pounds, bucket_upper]
            products_params = [*scope_params, pounds, bucket_upper, safe_limit]

        summary_query = scope_sql + summary_template.format(bucket_match_sql)
        products_query = scope_sql + products_template.format(bucket_match_sql)

        with conn.cursor() as cur:
            cur.execute(summary_query, summary_params)
            summary_row = cur.fetchone()
            cur.execute(products_query, products_params)
            product_rows = cur.fetchall()

    summary = None
    if summary_row and (_to_int(summary_row.get("product_count")) or 0) > 0:
        summary = {
            "product_count": _to_int(summary_row.get("product_count")) or 0,
            "total_units": _to_float(summary_row.get("total_units")) or 0.0,
            "total_amount": _to_float(summary_row.get("total_amount")) or 0.0,
            "avg_price": _to_float(summary_row.get("avg_price")),
            "active_periods": _to_int(summary_row.get("active_periods")) or 0,
        }

    products = [
        {
            "asin": str(row.get("asin") or "").strip(),
            "title": row.get("title"),
            "brand": row.get("brand"),
            "imageurl": row.get("imageurl"),
            "weight": row.get("weight"),
            "dimensions": row.get("dimensions"),
            "sellername": row.get("sellername"),
            "parent": row.get("parent"),
            "total_units": _to_float(row.get("total_units")) or 0.0,
            "total_amount": _to_float(row.get("total_amount")) or 0.0,
            "avg_price": _to_float(row.get("avg_price")),
            "active_periods": _to_int(row.get("active_periods")) or 0,
            "latest_year_month": _to_int(row.get("latest_year_month")),
        }
        for row in product_rows
    ]

    return {
        "view": normalized_view,
        "year": year,
        "month": month,
        "bucket_step": normalized_bucket_step,
        "pounds": pounds,
        "pounds_to": pounds + normalized_bucket_step if normalized_bucket_step is not None else None,
        "pounds_label": _format_pounds_range_label(pounds, normalized_bucket_step),
        "summary": summary,
        "products": products,
    }
