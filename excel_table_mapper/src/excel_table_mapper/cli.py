from __future__ import annotations

import argparse
import os
from pathlib import Path

from .processor import (
    DEFAULT_STOPWORDS,
    build_table2,
    normalize_input_rows,
    read_input_rows,
    write_output,
)
from .tagger import PgConfig, Tagger


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="将表1搜索词数据转换为表2词根聚合结果（离线/在线模式）。"
    )
    parser.add_argument("--input", required=True, help="输入文件路径（xlsx/csv）")
    parser.add_argument("--sheet", default="", help="输入sheet名（xlsx时可选）")
    parser.add_argument("--output", required=True, help="输出文件路径（xlsx/csv）")
    parser.add_argument("--mode", default="online", choices=["online", "offline"], help="运行模式")
    parser.add_argument(
        "--local-cache-db",
        default=str(Path(os.getenv("LOCALAPPDATA", str(Path.home()))) / "ExcelTableMapper" / "word_cache.sqlite3"),
        help="本地SQLite缓存路径",
    )

    parser.add_argument("--pg-host", default=os.getenv("PG_HOST", "192.168.110.107"))
    parser.add_argument("--pg-port", type=int, default=int(os.getenv("PG_PORT", "5432")))
    parser.add_argument("--pg-db", default=os.getenv("PG_DB", "hunter"))
    parser.add_argument("--pg-user", default=os.getenv("PG_USER", "postgres"))
    parser.add_argument("--pg-pass", default=os.getenv("PG_PASS", "123456"))
    parser.add_argument("--pg-schema", default=os.getenv("PG_SCHEMA", "public"))
    parser.add_argument("--pg-table", default=os.getenv("PG_CACHE_TABLE", "seller_sprite_word_cache"))

    parser.add_argument("--enable-ai", action="store_true", help="缓存缺失时启用AI打标")
    parser.add_argument("--model", default=os.getenv("AI_MODEL", "gpt-5.4"))
    parser.add_argument("--base-url", default=os.getenv("AI_BASE_URL", "https://api.wfqc8.cn/v1"))
    parser.add_argument("--batch-size", type=int, default=50, help="AI批量大小")
    parser.add_argument("--keep-stopwords", action="store_true", help="保留停用词，不进行过滤")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    input_path = Path(args.input).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve()
    local_cache_db = Path(args.local_cache_db).expanduser().resolve()
    local_cache_db.parent.mkdir(parents=True, exist_ok=True)

    if not input_path.exists():
        raise FileNotFoundError(f"输入文件不存在: {input_path}")

    pg = PgConfig(
        enabled=args.mode == "online",
        host=args.pg_host,
        port=int(args.pg_port),
        dbname=args.pg_db,
        user=args.pg_user,
        password=args.pg_pass,
        schema=args.pg_schema,
        table=args.pg_table,
    )
    tagger = Tagger(
        enable_ai=bool(args.enable_ai),
        model=args.model,
        base_url=args.base_url,
        batch_size=max(1, int(args.batch_size)),
        local_cache_db=local_cache_db,
        pg_config=pg,
        persist_local_on_ai=(args.mode == "online"),
    )

    raw_rows = read_input_rows(input_path, args.sheet.strip() or None)
    stopwords = set() if args.keep_stopwords else DEFAULT_STOPWORDS
    normalized = normalize_input_rows(raw_rows, stopwords)
    result_rows = build_table2(normalized, tagger)
    write_output(result_rows, output_path)

    print(f"输入行数: {len(normalized)}")
    print(f"输出词根数: {len(result_rows)}")
    print(f"输出文件: {output_path}")
