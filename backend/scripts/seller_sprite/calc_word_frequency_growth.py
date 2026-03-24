import argparse
import os
from collections import deque
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
import psycopg
from psycopg import sql


SCRIPT_DIR = Path(__file__).resolve().parent
BACKEND_DIR = SCRIPT_DIR.parent.parent
REPO_DIR = BACKEND_DIR.parent


def load_env_files() -> None:
    for env_file in (BACKEND_DIR / ".env", REPO_DIR / ".env"):
        if env_file.exists():
            load_dotenv(env_file, override=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compute total_searches monthly growth rate and quarter avg growth rate "
            "and write back into the existing word frequency table."
        )
    )
    parser.add_argument("--host", default=os.getenv("PG_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("PG_PORT", "5432")))
    parser.add_argument("--user", default=os.getenv("PG_USER", "postgres"))
    parser.add_argument("--password", default=os.getenv("PG_PASS", ""))
    parser.add_argument("--db", default=os.getenv("PG_DB", "postgres"))
    parser.add_argument("--schema", default=os.getenv("PG_SCHEMA", "public"))
    parser.add_argument("--table", default="seller_sprite_word_frequency")
    parser.add_argument("--min-year-month", type=int, default=None)
    parser.add_argument("--max-year-month", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=5000)
    return parser.parse_args()


def connect_db(args: argparse.Namespace) -> psycopg.Connection:
    return psycopg.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        dbname=args.db,
    )


def safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def year_month_to_index(year_month: int | None) -> int | None:
    if year_month is None:
        return None
    year = int(year_month) // 100
    month = int(year_month) % 100
    if month < 1 or month > 12:
        return None
    return year * 12 + month


def ensure_growth_columns(conn: psycopg.Connection, schema_name: str, table_name: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS total_searches_growth_rate DOUBLE PRECISION").format(
                sql.Identifier(schema_name),
                sql.Identifier(table_name),
            )
        )
        cur.execute(
            sql.SQL(
                "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS total_searches_quarter_avg_growth_rate DOUBLE PRECISION"
            ).format(
                sql.Identifier(schema_name),
                sql.Identifier(table_name),
            )
        )
    conn.commit()


def fetch_rows(conn: psycopg.Connection, args: argparse.Namespace) -> list[dict[str, Any]]:
    query = sql.SQL(
        """
        SELECT id, word, year_month, total_searches
        FROM {}.{}
        """
    ).format(sql.Identifier(args.schema), sql.Identifier(args.table))

    clauses = []
    params: list[Any] = []
    if args.min_year_month is not None:
        clauses.append(sql.SQL("year_month >= %s"))
        params.append(args.min_year_month)
    if args.max_year_month is not None:
        clauses.append(sql.SQL("year_month <= %s"))
        params.append(args.max_year_month)
    if clauses:
        query += sql.SQL(" WHERE ") + sql.SQL(" AND ").join(clauses)

    query += sql.SQL(" ORDER BY word ASC, year_month ASC, id ASC")

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(query, params)
        return cur.fetchall()


def compute_updates(rows: list[dict[str, Any]]) -> list[tuple[float | None, float | None, int]]:
    updates: list[tuple[float | None, float | None, int]] = []

    current_word: str | None = None
    prev_total_searches: float | None = None
    prev_month_index: int | None = None
    recent_monthly_rates: deque[float | None] = deque(maxlen=3)

    for row in rows:
        row_id = int(row["id"])
        word = str(row.get("word") or "").strip().lower()
        year_month = int(row["year_month"]) if row.get("year_month") is not None else None
        month_index = year_month_to_index(year_month)
        total_searches = safe_float(row.get("total_searches"))

        if word != current_word:
            current_word = word
            prev_total_searches = None
            prev_month_index = None
            recent_monthly_rates.clear()

        monthly_growth_rate: float | None = None
        is_consecutive_month = (
            month_index is not None
            and prev_month_index is not None
            and month_index - prev_month_index == 1
        )
        if is_consecutive_month and total_searches is not None and prev_total_searches not in (None, 0):
            monthly_growth_rate = (total_searches - prev_total_searches) / prev_total_searches

        recent_monthly_rates.append(monthly_growth_rate)
        valid_rates = [x for x in recent_monthly_rates if x is not None]
        quarter_avg_growth_rate = sum(valid_rates) / len(valid_rates) if valid_rates else None

        updates.append((monthly_growth_rate, quarter_avg_growth_rate, row_id))

        prev_total_searches = total_searches
        prev_month_index = month_index

    return updates


def apply_updates(
    conn: psycopg.Connection,
    args: argparse.Namespace,
    updates: list[tuple[float | None, float | None, int]],
) -> int:
    if not updates:
        return 0

    stmt = sql.SQL(
        """
        UPDATE {}.{}
        SET
            total_searches_growth_rate = %s,
            total_searches_quarter_avg_growth_rate = %s,
            updated_at = NOW()
        WHERE id = %s
        """
    ).format(sql.Identifier(args.schema), sql.Identifier(args.table))

    total = 0
    batch_size = max(1, int(args.batch_size))
    with conn.cursor() as cur:
        for start in range(0, len(updates), batch_size):
            chunk = updates[start : start + batch_size]
            cur.executemany(stmt, chunk)
            total += len(chunk)
    conn.commit()
    return total


def main() -> None:
    load_env_files()
    args = parse_args()

    conn = connect_db(args)
    try:
        print(f"Target(in-place): {args.host}:{args.port}/{args.db}.{args.schema}.{args.table}")
        ensure_growth_columns(conn, args.schema, args.table)
        rows = fetch_rows(conn, args)
        print(f"Loaded rows: {len(rows)}")
        updates = compute_updates(rows)
        updated = apply_updates(conn, args, updates)
        print(f"Updated rows: {updated}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
