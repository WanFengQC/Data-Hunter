import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

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
            "Import translation_cache.json / object_category_cache.json / plushable_cache.json "
            "into PostgreSQL with word as PRIMARY KEY."
        )
    )
    parser.add_argument(
        "--base-dir",
        default=str(SCRIPT_DIR),
        help="Directory that contains the 3 json files (default: script directory).",
    )
    parser.add_argument(
        "--translation-file",
        default="translation_cache.json",
        help="Translation cache json file name.",
    )
    parser.add_argument(
        "--category-file",
        default="object_category_cache.json",
        help="Object category cache json file name.",
    )
    parser.add_argument(
        "--plushable-file",
        default="plushable_cache.json",
        help="Plushable cache json file name.",
    )
    parser.add_argument(
        "--schema",
        default=os.getenv("PG_SCHEMA", "public"),
        help="PostgreSQL schema (default: env PG_SCHEMA or public).",
    )
    parser.add_argument(
        "--table",
        default="seller_sprite_word_cache",
        help="PostgreSQL table name.",
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
    return parser.parse_args()


def normalize_word(word: Any) -> str:
    return str(word).strip().lower()


def normalize_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def normalize_plushable_bool(value: Any) -> bool | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"是", "yes", "y", "true", "1"}:
        return True
    if text in {"否", "no", "n", "false", "0"}:
        return False
    return None


def load_json_map(file_path: Path, label: str) -> dict[str, str]:
    if not file_path.exists():
        raise FileNotFoundError(f"{label} file not found: {file_path}")

    raw = json.loads(file_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"{label} must be a JSON object: {file_path}")

    result: dict[str, str] = {}
    for k, v in raw.items():
        key = normalize_word(k)
        if not key:
            continue
        result[key] = "" if v is None else str(v)
    return result


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


def ensure_table(conn: psycopg.Connection, schema_name: str, table_name: str) -> None:
    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema_name)))
        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {}.{} (
                    word TEXT PRIMARY KEY,
                    translation_zh TEXT,
                    object_category TEXT,
                    plushable TEXT,
                    plushable_bool BOOLEAN,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            ).format(sql.Identifier(schema_name), sql.Identifier(table_name))
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


def build_rows(
    translation_map: dict[str, str],
    category_map: dict[str, str],
    plushable_map: dict[str, str],
) -> list[tuple[Any, ...]]:
    words = sorted(set(translation_map) | set(category_map) | set(plushable_map))
    rows: list[tuple[Any, ...]] = []
    for word in words:
        translation_zh = normalize_text(translation_map.get(word))
        object_category = normalize_text(category_map.get(word))
        plushable = normalize_text(plushable_map.get(word))
        plushable_bool = normalize_plushable_bool(plushable)
        rows.append((word, translation_zh, object_category, plushable, plushable_bool))
    return rows


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
            word, translation_zh, object_category, plushable, plushable_bool
        ) VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (word) DO UPDATE SET
            translation_zh = EXCLUDED.translation_zh,
            object_category = EXCLUDED.object_category,
            plushable = EXCLUDED.plushable,
            plushable_bool = EXCLUDED.plushable_bool,
            updated_at = NOW()
        """
    ).format(sql.Identifier(schema_name), sql.Identifier(table_name))

    processed = 0
    with conn.cursor() as cur:
        for i in range(0, len(rows), batch_size):
            chunk = rows[i : i + batch_size]
            cur.executemany(stmt, chunk)
            processed += len(chunk)
    conn.commit()
    return processed


def main() -> None:
    load_env_files()
    args = parse_args()

    base_dir = Path(args.base_dir).resolve()
    translation_file = base_dir / args.translation_file
    category_file = base_dir / args.category_file
    plushable_file = base_dir / args.plushable_file

    translation_map = load_json_map(translation_file, "translation")
    category_map = load_json_map(category_file, "object_category")
    plushable_map = load_json_map(plushable_file, "plushable")

    rows = build_rows(translation_map, category_map, plushable_map)
    print(f"Loaded rows from JSON: {len(rows)}")
    print(
        "Source sizes: "
        f"translation={len(translation_map)}, "
        f"object_category={len(category_map)}, "
        f"plushable={len(plushable_map)}"
    )

    conn = pg_connect()
    try:
        ensure_table(conn, args.schema, args.table)
        if args.truncate:
            truncate_table(conn, args.schema, args.table)
            print(f"Truncated table: {args.schema}.{args.table}")

        upserted = upsert_rows(
            conn=conn,
            schema_name=args.schema,
            table_name=args.table,
            rows=rows,
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
