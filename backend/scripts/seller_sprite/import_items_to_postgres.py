import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Iterable

from dotenv import load_dotenv
import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb


SCRIPT_DIR = Path(__file__).resolve().parent
BACKEND_DIR = SCRIPT_DIR.parent.parent
REPO_DIR = BACKEND_DIR.parent

# =========================
# Manual Config (Edit Here)
# =========================
# If CLI args `--folder/--folders` are not provided, these folders will be used.
# Example: ["ara_202512"] or ["ara_202512", "ara_202601"]
IMPORT_FOLDERS: list[str] = ["ara_202508"]


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
            "Import SellerSprite HTML(JSON) files into PostgreSQL. "
            "Each item key becomes a table column (JSONB), plus year_month."
        )
    )
    parser.add_argument(
        "--base-dir",
        default=str(SCRIPT_DIR),
        help="Base directory containing month folders (default: script directory).",
    )
    parser.add_argument(
        "--folders",
        nargs="+",
        help=(
            "Folders to import in exact order, e.g. "
            "--folders ara_202512 ara_202601"
        ),
    )
    parser.add_argument(
        "--folder",
        action="append",
        default=[],
        help="Single folder to import. Can be used multiple times.",
    )
    parser.add_argument("--schema", default="public", help="PostgreSQL schema.")
    parser.add_argument("--table", default="seller_sprite_items", help="Table name.")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Insert batch size (default: 500).",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Truncate target table before insert.",
    )
    parser.add_argument(
        "--mapping-file",
        default="",
        help="Optional JSON path to write item-key -> column mapping.",
    )
    return parser.parse_args()


def resolve_folders(base_dir: Path, folders: list[str] | None) -> list[Path]:
    if folders:
        resolved: list[Path] = []
        for item in folders:
            candidate = Path(item)
            if not candidate.is_absolute():
                candidate = base_dir / item
            candidate = candidate.resolve()
            if not candidate.exists() or not candidate.is_dir():
                raise FileNotFoundError(f"Folder not found: {candidate}")
            resolved.append(candidate)
        return resolved

    auto = [p for p in base_dir.iterdir() if p.is_dir() and p.name.startswith("ara_")]
    return sorted(auto, key=lambda p: p.name)


def extract_year_month(folder_name: str) -> int:
    match = re.search(r"(\d{6})", folder_name)
    if not match:
        raise ValueError(f"Cannot extract year_month from folder name: {folder_name}")
    return int(match.group(1))


def page_no_from_filename(file_name: str) -> int | None:
    match = re.search(r"(\d+)(?!.*\d)", file_name)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def sort_files(files: Iterable[Path]) -> list[Path]:
    def _key(path: Path) -> tuple[int, str]:
        page_no = page_no_from_filename(path.name)
        return (page_no if page_no is not None else 10**9, path.name)

    return sorted(files, key=_key)


def parse_json_payload(raw_text: str) -> dict:
    text = raw_text.strip()
    if not text:
        return {}

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or end <= start:
            return {}
        try:
            return json.loads(text[start : end + 1])
        except json.JSONDecodeError:
            return {}


def load_items_from_file(file_path: Path) -> tuple[list[dict], int | None]:
    text = file_path.read_text(encoding="utf-8", errors="ignore")
    payload = parse_json_payload(text)
    data = payload.get("data", {})
    if not isinstance(data, dict):
        return [], page_no_from_filename(file_path.name)

    items = data.get("items", [])
    if not isinstance(items, list):
        items = []

    page_val = data.get("page")
    try:
        page_no = int(page_val)
    except (TypeError, ValueError):
        page_no = page_no_from_filename(file_path.name)

    dict_items = [item for item in items if isinstance(item, dict)]
    return dict_items, page_no


def normalize_column_name(raw_name: str) -> str:
    name = raw_name.strip().lower()
    name = re.sub(r"[^a-z0-9_]+", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    if not name:
        name = "field"
    if name[0].isdigit():
        name = f"f_{name}"
    return name


def build_column_mapping(item_keys: list[str]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    used: set[str] = set()
    for key in item_keys:
        base = normalize_column_name(key)
        candidate = base
        index = 2
        while candidate in used:
            candidate = f"{base}_{index}"
            index += 1
        mapping[key] = candidate
        used.add(candidate)
    return mapping


def discover_item_keys(folders: list[Path]) -> list[str]:
    discovered: list[str] = []
    seen: set[str] = set()
    total_folders = len(folders)
    for folder_index, folder in enumerate(folders, start=1):
        html_files = sort_files(folder.glob("*.html"))
        total_files = len(html_files)
        print(
            f"[Discover] Folder {folder_index}/{total_folders}: {folder.name}, "
            f"files={total_files}"
        )
        for file_index, file_path in enumerate(html_files, start=1):
            items, _ = load_items_from_file(file_path)
            for item in items:
                for key in item.keys():
                    if key not in seen:
                        seen.add(key)
                        discovered.append(key)
            if file_index % 20 == 0 or file_index == total_files:
                print(
                    f"[Discover] {folder.name}: {file_index}/{total_files} files, "
                    f"keys={len(discovered)}"
                )
    return discovered


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


def ensure_table(
    conn: psycopg.Connection,
    schema_name: str,
    table_name: str,
    dynamic_columns: list[str],
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {}.{} (
                    id BIGSERIAL PRIMARY KEY,
                    source_folder TEXT NOT NULL,
                    source_file TEXT NOT NULL,
                    page_no INTEGER,
                    year_month INTEGER NOT NULL,
                    imported_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            ).format(sql.Identifier(schema_name), sql.Identifier(table_name))
        )

        for col in dynamic_columns:
            cur.execute(
                sql.SQL("ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS {} JSONB").format(
                    sql.Identifier(schema_name),
                    sql.Identifier(table_name),
                    sql.Identifier(col),
                )
            )
    conn.commit()


def truncate_table(conn: psycopg.Connection, schema_name: str, table_name: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("TRUNCATE TABLE {}.{}").format(
                sql.Identifier(schema_name), sql.Identifier(table_name)
            )
        )
    conn.commit()


def insert_rows(
    conn: psycopg.Connection,
    schema_name: str,
    table_name: str,
    folders: list[Path],
    key_mapping: dict[str, str],
    batch_size: int,
) -> int:
    base_cols = ["source_folder", "source_file", "page_no", "year_month"]
    original_keys = list(key_mapping.keys())
    mapped_cols = [key_mapping[k] for k in original_keys]
    all_columns = base_cols + mapped_cols

    placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in all_columns)
    insert_stmt = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
        sql.Identifier(schema_name),
        sql.Identifier(table_name),
        sql.SQL(", ").join(sql.Identifier(c) for c in all_columns),
        placeholders,
    )

    inserted = 0
    batch: list[tuple] = []
    flush_times = 0
    total_files_all = 0
    for folder in folders:
        total_files_all += len(sort_files(folder.glob("*.html")))
    processed_files = 0

    with conn.cursor() as cur:
        for folder_index, folder in enumerate(folders, start=1):
            year_month = extract_year_month(folder.name)
            html_files = sort_files(folder.glob("*.html"))
            folder_files = len(html_files)
            print(
                f"[Import] Folder {folder_index}/{len(folders)} start: {folder.name}, "
                f"files={folder_files}, year_month={year_month}"
            )
            for file_index, file_path in enumerate(html_files, start=1):
                items, page_no = load_items_from_file(file_path)
                file_item_count = 0
                for item in items:
                    row: list = [folder.name, file_path.name, page_no, year_month]
                    for key in original_keys:
                        value = item.get(key)
                        row.append(None if value is None else Jsonb(value))
                    batch.append(tuple(row))
                    file_item_count += 1

                    if len(batch) >= batch_size:
                        cur.executemany(insert_stmt, batch)
                        inserted += len(batch)
                        batch.clear()
                        flush_times += 1
                        if flush_times % 10 == 0:
                            print(
                                f"[Import] Progress: inserted={inserted}, "
                                f"processed_files={processed_files}/{total_files_all}"
                            )

                processed_files += 1
                print(
                    f"[Import] File {file_index}/{folder_files} done: {file_path.name}, "
                    f"items={file_item_count}, total_inserted={inserted}"
                )

        if batch:
            cur.executemany(insert_stmt, batch)
            inserted += len(batch)
            print(
                f"[Import] Final batch committed. inserted={inserted}, "
                f"processed_files={processed_files}/{total_files_all}"
            )

    conn.commit()
    return inserted


def main() -> None:
    load_env_files()
    args = parse_args()

    base_dir = Path(args.base_dir).resolve()
    if not base_dir.exists():
        raise FileNotFoundError(f"Base directory not found: {base_dir}")

    selected_folders = []
    if args.folders:
        selected_folders.extend(args.folders)
    if args.folder:
        selected_folders.extend(args.folder)
    if not selected_folders and IMPORT_FOLDERS:
        selected_folders.extend(IMPORT_FOLDERS)

    folders = resolve_folders(base_dir, selected_folders or None)
    if not folders:
        raise RuntimeError("No folders found to import.")

    print("Import folder order:")
    for index, folder in enumerate(folders, start=1):
        print(f"{index}. {folder}")

    item_keys = discover_item_keys(folders)
    if not item_keys:
        raise RuntimeError("No item keys discovered. Check file content and folder path.")

    key_mapping = build_column_mapping(item_keys)

    if args.mapping_file:
        mapping_path = Path(args.mapping_file).resolve()
        mapping_path.parent.mkdir(parents=True, exist_ok=True)
        mapping_path.write_text(
            json.dumps(key_mapping, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(f"Column mapping written: {mapping_path}")

    conn = pg_connect()
    try:
        ensure_table(conn, args.schema, args.table, list(key_mapping.values()))
        if args.truncate:
            truncate_table(conn, args.schema, args.table)

        inserted = insert_rows(
            conn=conn,
            schema_name=args.schema,
            table_name=args.table,
            folders=folders,
            key_mapping=key_mapping,
            batch_size=max(1, args.batch_size),
        )
        print(
            f"Done. Inserted rows: {inserted}, table: {args.schema}.{args.table}, "
            f"dynamic columns: {len(key_mapping)}"
        )
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # pragma: no cover
        print(f"[ERROR] {exc}")
        sys.exit(1)
