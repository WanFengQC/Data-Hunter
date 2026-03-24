import argparse
import csv
import os
import re
import sys
from pathlib import Path
from typing import Any, Iterable

from dotenv import load_dotenv
import psycopg
from psycopg import sql


SCRIPT_DIR = Path(__file__).resolve().parent
BACKEND_DIR = SCRIPT_DIR.parent.parent
REPO_DIR = BACKEND_DIR.parent


def load_env_files() -> None:
    candidates = [
        BACKEND_DIR / ".env",
        REPO_DIR / ".env",
    ]
    for env_file in candidates:
        if env_file.exists():
            load_dotenv(env_file, override=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Import get_keywords output CSV files (word_frequency_analysis_YYYYMM.csv) "
            "into PostgreSQL with year/month columns."
        )
    )
    parser.add_argument(
        "--base-dir",
        default=str(SCRIPT_DIR),
        help="Directory containing csv files (default: script directory).",
    )
    parser.add_argument(
        "--pattern",
        default="word_frequency_analysis_*.csv",
        help="Glob pattern when --files is not provided.",
    )
    parser.add_argument(
        "--files",
        nargs="+",
        help="Explicit csv files to import (absolute or relative to --base-dir).",
    )
    parser.add_argument(
        "--schema",
        default=os.getenv("PG_SCHEMA", "public"),
        help="PostgreSQL schema.",
    )
    parser.add_argument(
        "--table",
        default="seller_sprite_word_frequency",
        help="Target PostgreSQL table.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Upsert batch size (default: 1000).",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Truncate table before import.",
    )
    parser.add_argument(
        "--recreate",
        action="store_true",
        help="Drop and recreate table before import.",
    )
    return parser.parse_args()


def pg_connect() -> psycopg.Connection:
    host = os.getenv("PG_HOST", "127.0.0.1")
    user = os.getenv("PG_USER", "postgres")
    password = os.getenv("PG_PASS", "")
    port = int(os.getenv("PG_PORT", "5432"))
    dbname = os.getenv("PG_DB", "postgres")
    return psycopg.connect(
        host=host,
        user=user,
        password=password,
        port=port,
        dbname=dbname,
    )


def resolve_files(base_dir: Path, pattern: str, files: list[str] | None) -> list[Path]:
    if files:
        resolved: list[Path] = []
        for item in files:
            p = Path(item)
            if not p.is_absolute():
                p = (base_dir / p).resolve()
            if not p.exists() or not p.is_file():
                raise FileNotFoundError(f"CSV file not found: {p}")
            resolved.append(p)
        return resolved

    return sorted(base_dir.glob(pattern), key=lambda p: p.name)


def parse_year_month_from_file_name(file_name: str) -> tuple[int, int, int]:
    match = re.search(r"(\d{6})", file_name)
    if not match:
        raise ValueError(f"Cannot parse year_month from file name: {file_name}")

    ym = int(match.group(1))
    year = ym // 100
    month = ym % 100
    if month < 1 or month > 12:
        raise ValueError(f"Invalid month in file name: {file_name}")
    return year, month, ym


def to_int(value: Any) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except Exception:
        return None


def to_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except Exception:
        return None


def normalize_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def read_csv_rows(file_path: Path, year: int, month: int, year_month: int) -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []
    with file_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            word = normalize_text(row.get("word"))
            if not word:
                continue
            rows.append(
                (
                    word.lower(),
                    normalize_text(row.get("word_zh")),
                    normalize_text(row.get("pos")),
                    normalize_text(row.get("标签"))
                    or normalize_text(row.get("对应标签"))
                    or normalize_text(row.get("category")),
                    normalize_text(row.get("原因")) or normalize_text(row.get("原因备注")),
                    to_int(row.get("freq")),
                    to_float(row.get("freq_ratio")),
                    normalize_text(row.get("freq_ratio_percent")),
                    to_int(row.get("coverage")),
                    normalize_text(row.get("coverage_percent")),
                    to_float(row.get("total_searches")),
                    year,
                    month,
                    year_month,
                    file_path.name,
                )
            )
    return rows


def ensure_table(conn: psycopg.Connection, schema_name: str, table_name: str) -> None:
    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema_name)))
        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {}.{} (
                    id BIGSERIAL PRIMARY KEY,
                    word TEXT NOT NULL,
                    word_zh TEXT,
                    pos TEXT,
                    "标签" TEXT,
                    "原因" TEXT,
                    freq BIGINT,
                    freq_ratio DOUBLE PRECISION,
                    freq_ratio_percent TEXT,
                    coverage BIGINT,
                    coverage_percent TEXT,
                    total_searches DOUBLE PRECISION,
                    year INTEGER NOT NULL,
                    month INTEGER NOT NULL,
                    year_month INTEGER NOT NULL,
                    source_file TEXT NOT NULL,
                    imported_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CONSTRAINT uq_word_year_month UNIQUE (word, year_month)
                )
                """
            ).format(sql.Identifier(schema_name), sql.Identifier(table_name))
        )
        cur.execute(
            sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {}.{} (year_month)").format(
                sql.Identifier(f"idx_{table_name}_year_month"),
                sql.Identifier(schema_name),
                sql.Identifier(table_name),
            )
        )
        cur.execute(
            sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {}.{} (word)").format(
                sql.Identifier(f"idx_{table_name}_word"),
                sql.Identifier(schema_name),
                sql.Identifier(table_name),
            )
        )
    conn.commit()


def drop_table(conn: psycopg.Connection, schema_name: str, table_name: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                sql.Identifier(schema_name),
                sql.Identifier(table_name),
            )
        )
    conn.commit()


def truncate_table(conn: psycopg.Connection, schema_name: str, table_name: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("TRUNCATE TABLE {}.{}").format(
                sql.Identifier(schema_name),
                sql.Identifier(table_name),
            )
        )
    conn.commit()


def batched(items: list[tuple[Any, ...]], size: int) -> Iterable[list[tuple[Any, ...]]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def upsert_rows(
    conn: psycopg.Connection,
    schema_name: str,
    table_name: str,
    rows: list[tuple[Any, ...]],
    batch_size: int,
) -> int:
    batch_size = max(1, int(batch_size))
    stmt = sql.SQL(
        """
        INSERT INTO {}.{} (
            word, word_zh, pos, "标签", "原因",
            freq, freq_ratio, freq_ratio_percent,
            coverage, coverage_percent, total_searches,
            year, month, year_month, source_file
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s
        )
        ON CONFLICT (word, year_month) DO UPDATE SET
            word_zh = EXCLUDED.word_zh,
            pos = EXCLUDED.pos,
            "标签" = EXCLUDED."标签",
            "原因" = EXCLUDED."原因",
            freq = EXCLUDED.freq,
            freq_ratio = EXCLUDED.freq_ratio,
            freq_ratio_percent = EXCLUDED.freq_ratio_percent,
            coverage = EXCLUDED.coverage,
            coverage_percent = EXCLUDED.coverage_percent,
            total_searches = EXCLUDED.total_searches,
            year = EXCLUDED.year,
            month = EXCLUDED.month,
            source_file = EXCLUDED.source_file,
            updated_at = NOW()
        """
    ).format(sql.Identifier(schema_name), sql.Identifier(table_name))

    total = 0
    with conn.cursor() as cur:
        for chunk in batched(rows, batch_size):
            cur.executemany(stmt, chunk)
            total += len(chunk)
    conn.commit()
    return total


def main() -> None:
    load_env_files()
    args = parse_args()

    base_dir = Path(args.base_dir).resolve()
    if not base_dir.exists():
        raise FileNotFoundError(f"Base dir not found: {base_dir}")

    files = resolve_files(base_dir, args.pattern, args.files)
    if not files:
        raise RuntimeError("No csv files found for import.")

    print("Import files:")
    for i, p in enumerate(files, start=1):
        print(f"{i}. {p.name}")

    all_rows: list[tuple[Any, ...]] = []
    for file_path in files:
        year, month, year_month = parse_year_month_from_file_name(file_path.name)
        file_rows = read_csv_rows(file_path, year, month, year_month)
        all_rows.extend(file_rows)
        print(f"{file_path.name}: rows={len(file_rows)}, year={year}, month={month}")

    conn = pg_connect()
    try:
        if args.recreate:
            drop_table(conn, args.schema, args.table)
            print(f"Dropped table: {args.schema}.{args.table}")
        ensure_table(conn, args.schema, args.table)
        if args.truncate and not args.recreate:
            truncate_table(conn, args.schema, args.table)
            print(f"Truncated table: {args.schema}.{args.table}")

        upserted = upsert_rows(
            conn=conn,
            schema_name=args.schema,
            table_name=args.table,
            rows=all_rows,
            batch_size=args.batch_size,
        )
        print(f"Done. Upserted rows: {upserted}")
        print(f"Target table: {args.schema}.{args.table}")
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"[ERROR] {exc}")
        sys.exit(1)
