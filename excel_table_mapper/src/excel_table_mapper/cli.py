from __future__ import annotations

import argparse
import os
from pathlib import Path

from .processor import (
    DEFAULT_STOPWORDS,
    build_table2_with_options,
    discover_phrase_normalization_candidates,
    normalize_input_rows_with_map,
    read_input_rows,
    write_output_with_options,
)
from .tagger import PgConfig, Tagger


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="将表1数据转换为表2词根聚合结果（离线/在线模式）。")
    parser.add_argument("--input", required=True, help="输入文件路径（xlsx/csv）")
    parser.add_argument("--sheet", default="", help="输入 sheet 名（xlsx 时可选）")
    parser.add_argument("--output", required=True, help="输出文件路径（xlsx/csv）")
    parser.add_argument("--mode", default="online", choices=["online", "offline"], help="运行模式")
    parser.add_argument(
        "--local-cache-db",
        default=str(Path(os.getenv("LOCALAPPDATA", str(Path.home()))) / "TrafficAnalysisTool" / "traffic_analysis_tool_cache.sqlite3"),
        help="本地 SQLite 缓存路径",
    )

    parser.add_argument("--pg-host", default=os.getenv("PG_HOST", "192.168.110.107"))
    parser.add_argument("--pg-port", type=int, default=int(os.getenv("PG_PORT", "5432")))
    parser.add_argument("--pg-db", default=os.getenv("PG_DB", "hunter"))
    parser.add_argument("--pg-user", default=os.getenv("PG_USER", "postgres"))
    parser.add_argument("--pg-pass", default=os.getenv("PG_PASS", "123456"))
    parser.add_argument("--pg-schema", default=os.getenv("PG_SCHEMA", "public"))
    parser.add_argument("--pg-table", default=os.getenv("PG_CACHE_TABLE", "traffic_analysis_word_cache"))

    parser.add_argument("--enable-ai", action="store_true", help="缓存缺失时启用 AI 打标")
    parser.add_argument("--use-history-cache", action="store_true", help="启用历史缓存读取")
    parser.add_argument("--model", default=os.getenv("AI_MODEL", "gpt-5.4"))
    parser.add_argument("--base-url", default=os.getenv("AI_BASE_URL", "https://api.uniapi.io"))
    parser.add_argument("--batch-size", type=int, default=50, help="AI 批量大小")
    parser.add_argument("--keep-stopwords", action="store_true", help="保留停用词，不进行过滤")
    parser.add_argument("--debug-mode", action="store_true", help="输出调试列（导入短语数据）")
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
        use_history_cache=bool(args.use_history_cache),
        model=args.model,
        base_url=args.base_url,
        batch_size=max(1, int(args.batch_size)),
        local_cache_db=local_cache_db,
        pg_config=pg,
        persist_local_on_ai=(args.mode == "online"),
    )

    raw_rows = read_input_rows(input_path, args.sheet.strip() or None)
    stopwords = set() if args.keep_stopwords else DEFAULT_STOPWORDS
    phrase_map: dict[str, str] = {}
    if args.mode == "online":
        candidates = discover_phrase_normalization_candidates(
            raw_rows,
            stopwords,
            source_scope=f"{args.sheet.strip() or 'default'}::{input_path.name}",
        )
        tagger.record_phrase_normalization_candidates(candidates)
    # Online: read confirmed map from DB (and backfill local).
    # Offline: read confirmed map from local cache (synced from DB before).
    phrase_map = tagger.load_phrase_normalization_map()
    normalized = normalize_input_rows_with_map(raw_rows, stopwords, phrase_map)
    source_scope = f"{args.sheet.strip() or 'default'}::{input_path.name}"
    result_rows = build_table2_with_options(
        normalized,
        tagger,
        source_scope=source_scope,
        debug_mode=bool(args.debug_mode),
    )
    write_output_with_options(
        result_rows,
        output_path,
        include_debug=bool(args.debug_mode),
        source_raw_rows=raw_rows,
        source_sheet_name=args.sheet.strip() or None,
    )

    print(f"输入行数: {len(normalized)}")
    print(f"输出词根数: {len(result_rows)}")
    print(f"输出文件: {output_path}")
