import json
import re
from typing import Any

from psycopg import sql

from app.infrastructure.postgres.database import pg_connect
from app.services.postgres.common import _get_table_profile, _to_float, _to_int


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
