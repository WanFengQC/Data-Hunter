from __future__ import annotations

import asyncio
import json
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psycopg
from dotenv import load_dotenv
from openai import AsyncOpenAI


@dataclass
class TagResult:
    tag_label: str
    reason: str


@dataclass
class PgConfig:
    enabled: bool
    host: str
    port: int
    dbname: str
    user: str
    password: str
    schema: str
    table: str


def _norm(word: str) -> str:
    return str(word or "").strip().lower()


def _clean_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _normalize_entry(word: str, raw: Any) -> TagResult:
    if isinstance(raw, str):
        tag = raw.strip() or "无效词"
        return TagResult(tag_label=tag, reason=f"“{word}”，来自缓存。")
    if isinstance(raw, dict):
        tag = (
            str(
                raw.get("标签")
                or raw.get("对应标签")
                or raw.get("tag_label")
                or raw.get("label")
                or raw.get("tag")
                or ""
            ).strip()
            or "无效词"
        )
        reason = (
            raw.get("原因")
            or raw.get("原因备注")
            or raw.get("reason")
            or raw.get("tag_reason")
            or raw.get("mark")
            or f"“{word}”，来自缓存。"
        )
        reason_text = str(reason or "").strip() or "无说明。"
        return TagResult(tag_label=tag, reason=reason_text)
    return TagResult(tag_label="解析失败", reason=f"“{word}”，缓存格式无效。")


def _parse_response(content: str) -> dict[str, dict[str, str]]:
    text = str(content or "").strip()
    if not text:
        return {}
    if text.startswith("```"):
        lines = text.splitlines()
        if len(lines) >= 3:
            text = "\n".join(lines[1:-1]).strip()
    try:
        payload = json.loads(text)
    except Exception:
        return {}
    rows = payload.get("rows") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        return {}
    out: dict[str, dict[str, str]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        word = _norm(row.get("关键词") or row.get("keyword") or row.get("word"))
        if not word:
            continue
        out[word] = {
            "tag_label": str(row.get("对应标签") or row.get("标签") or row.get("label") or "").strip(),
            "reason": str(row.get("原因备注") or row.get("原因") or row.get("reason") or "").strip(),
        }
    return out


class Tagger:
    def __init__(
        self,
        *,
        enable_ai: bool,
        model: str,
        base_url: str,
        batch_size: int,
        local_cache_db: Path,
        pg_config: PgConfig,
        persist_local_on_ai: bool = True,
        api_key: str | None = None,
    ) -> None:
        load_dotenv()
        self.enable_ai = enable_ai
        self.model = model
        self.base_url = base_url
        self.batch_size = max(1, int(batch_size))
        self.local_cache_db = local_cache_db
        self.pg = pg_config
        self.persist_local_on_ai = bool(persist_local_on_ai)

        self.local_cache_db.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_local_table()

        self.client: AsyncOpenAI | None = None
        if enable_ai:
            resolved_api_key = str(api_key or "").strip() or os.getenv("OPENAI_API_KEY", "").strip()
            if not resolved_api_key:
                raise RuntimeError("启用 AI 打标时必须设置 OPENAI_API_KEY。")
            self.client = AsyncOpenAI(api_key=resolved_api_key, base_url=base_url)

    def _sqlite(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.local_cache_db)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_local_table(self) -> None:
        with self._sqlite() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS word_cache (
                    word TEXT PRIMARY KEY,
                    translation_zh TEXT NULL,
                    tag_label TEXT NULL,
                    reason TEXT NULL,
                    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
                )
                """
            )
            conn.commit()

    def _upsert_local(self, items: list[dict[str, Any]]) -> int:
        normalized: list[tuple[str, str | None, str | None, str | None]] = []
        for item in items:
            word = _norm(item.get("word") or "")
            if not word:
                continue
            normalized.append(
                (
                    word,
                    _clean_text(item.get("translation_zh")),
                    _clean_text(item.get("tag_label")),
                    _clean_text(item.get("reason")),
                )
            )
        if not normalized:
            return 0

        with self._sqlite() as conn:
            conn.executemany(
                """
                INSERT INTO word_cache (word, translation_zh, tag_label, reason, updated_at)
                VALUES (?, ?, ?, ?, datetime('now'))
                ON CONFLICT(word) DO UPDATE SET
                    translation_zh = COALESCE(excluded.translation_zh, word_cache.translation_zh),
                    tag_label = COALESCE(excluded.tag_label, word_cache.tag_label),
                    reason = COALESCE(excluded.reason, word_cache.reason),
                    updated_at = datetime('now')
                """,
                normalized,
            )
            conn.commit()
        return len(normalized)

    def _query_local(self, words: list[str]) -> dict[str, dict[str, Any]]:
        normalized_words = sorted({_norm(w) for w in words if _norm(w)})
        if not normalized_words:
            return {}
        placeholders = ",".join(["?"] * len(normalized_words))
        with self._sqlite() as conn:
            rows = conn.execute(
                f"""
                SELECT word, translation_zh, tag_label, reason
                FROM word_cache
                WHERE word IN ({placeholders})
                """,
                normalized_words,
            ).fetchall()
        output: dict[str, dict[str, Any]] = {}
        for row in rows:
            output[str(row["word"])] = {
                "translation_zh": row["translation_zh"],
                "tag_label": row["tag_label"],
                "reason": row["reason"],
            }
        return output

    def _connect_pg(self):
        return psycopg.connect(
            host=self.pg.host,
            user=self.pg.user,
            password=self.pg.password,
            port=self.pg.port,
            dbname=self.pg.dbname,
            connect_timeout=8,
        )

    def _ensure_pg_table(self) -> None:
        if not self.pg.enabled:
            return
        with self._connect_pg() as conn:
            with conn.cursor() as cur:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.pg.schema}")
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self.pg.schema}.{self.pg.table} (
                        word TEXT PRIMARY KEY,
                        translation_zh TEXT NULL,
                        tag_label TEXT NULL,
                        tag_reason TEXT NULL,
                        created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
                    )
                    """
                )
            conn.commit()

    def _query_pg(self, words: list[str]) -> dict[str, dict[str, Any]]:
        if not self.pg.enabled:
            return {}
        normalized_words = sorted({_norm(w) for w in words if _norm(w)})
        if not normalized_words:
            return {}
        self._ensure_pg_table()
        with self._connect_pg() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT word, translation_zh, tag_label, tag_reason
                    FROM {self.pg.schema}.{self.pg.table}
                    WHERE word = ANY(%s)
                    """,
                    (normalized_words,),
                )
                rows = cur.fetchall()
        output: dict[str, dict[str, Any]] = {}
        for row in rows:
            word = _norm(row[0])
            if not word:
                continue
            output[word] = {
                "translation_zh": _clean_text(row[1]),
                "tag_label": _clean_text(row[2]),
                "reason": _clean_text(row[3]),
            }
        return output

    def _upsert_pg(self, items: list[dict[str, Any]]) -> int:
        if not self.pg.enabled:
            return 0
        normalized: list[tuple[str, str | None, str | None, str | None]] = []
        for item in items:
            word = _norm(item.get("word") or "")
            if not word:
                continue
            normalized.append(
                (
                    word,
                    _clean_text(item.get("translation_zh")),
                    _clean_text(item.get("tag_label")),
                    _clean_text(item.get("reason")),
                )
            )
        if not normalized:
            return 0
        self._ensure_pg_table()
        with self._connect_pg() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    f"""
                    INSERT INTO {self.pg.schema}.{self.pg.table}
                    (word, translation_zh, tag_label, tag_reason, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (word)
                    DO UPDATE SET
                        translation_zh = COALESCE(EXCLUDED.translation_zh, {self.pg.schema}.{self.pg.table}.translation_zh),
                        tag_label = COALESCE(EXCLUDED.tag_label, {self.pg.schema}.{self.pg.table}.tag_label),
                        tag_reason = COALESCE(EXCLUDED.tag_reason, {self.pg.schema}.{self.pg.table}.tag_reason),
                        updated_at = NOW()
                    """,
                    [(w, zh, tag, reason) for (w, zh, tag, reason) in normalized],
                )
            conn.commit()
        return len(normalized)

    def _fetch_cache(self, words: list[str]) -> dict[str, dict[str, Any]]:
        local_hits = self._query_local(words)
        missing = [w for w in words if _norm(w) not in local_hits]
        if not missing:
            return local_hits

        pg_hits = self._query_pg(missing)
        if pg_hits:
            self._upsert_local(
                [
                    {
                        "word": word,
                        "translation_zh": entry.get("translation_zh"),
                        "tag_label": entry.get("tag_label"),
                        "reason": entry.get("reason"),
                    }
                    for word, entry in pg_hits.items()
                ]
            )
        merged = dict(local_hits)
        merged.update(pg_hits)
        return merged

    async def _classify_batch(self, words: list[str]) -> dict[str, dict[str, str]]:
        if not self.client or not words:
            return {}
        words_text = "\n".join(words)
        prompt = f"""
# 亚马逊词根打标

请将下面英文词根映射为唯一标签，并给出简短中文原因。

标签必须是以下之一：
1核心词 / 2外形 / 3属性 / 4痛点 / 5规格 / 6受众 / 7场景 / 8品牌 / 无效词

返回要求：
1. 只返回 JSON。
2. 顺序必须和输入一致。

格式：
{{
  "rows": [
    {{"关键词": "stuffed", "对应标签": "1核心词", "原因备注": "“填充”，类目核心词。"}}
  ]
}}

词根列表：
{words_text}
""".strip()
        response = await self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": "你是严格的电商词根打标助手，只输出JSON。"},
                {"role": "user", "content": prompt},
            ],
            temperature=0,
        )
        content = (response.choices[0].message.content or "").strip()
        return _parse_response(content)

    def sync_all_from_db_to_local(self, progress_callback=None) -> dict[str, int]:
        if not self.pg.enabled:
            return {"synced_count": 0}
        self._ensure_pg_table()
        with self._connect_pg() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT word, translation_zh, tag_label, tag_reason
                    FROM {self.pg.schema}.{self.pg.table}
                    """
                )
                rows = cur.fetchall()
        items = []
        for idx, row in enumerate(rows, start=1):
            items.append(
                {
                    "word": _norm(row[0]),
                    "translation_zh": _clean_text(row[1]),
                    "tag_label": _clean_text(row[2]),
                    "reason": _clean_text(row[3]),
                }
            )
            if callable(progress_callback) and idx % 1000 == 0:
                progress_callback({"stage": "db_sync", "done": idx, "total": len(rows)})
        upserted = self._upsert_local(items)
        if callable(progress_callback):
            progress_callback({"stage": "db_sync_done", "done": upserted, "total": len(rows)})
        return {"synced_count": upserted}

    def classify(self, words: list[str], progress_callback=None) -> dict[str, TagResult]:
        result: dict[str, TagResult] = {}
        missing: list[str] = []
        cached = self._fetch_cache(words)

        for word in words:
            entry = cached.get(_norm(word))
            if entry is None:
                missing.append(word)
                continue
            result[word] = _normalize_entry(word, entry)

        if callable(progress_callback):
            progress_callback(
                {
                    "stage": "cache_checked",
                    "total_words": len(words),
                    "cache_hit": len(result),
                    "missing": len(missing),
                }
            )

        if missing and self.enable_ai:
            batches = [missing[i : i + self.batch_size] for i in range(0, len(missing), self.batch_size)]
            total_batches = len(batches)
            ai_items: list[dict[str, Any]] = []

            async def _run() -> None:
                done = 0
                for idx, batch in enumerate(batches, start=1):
                    payload = await self._classify_batch(batch)
                    for word in batch:
                        item = payload.get(_norm(word))
                        if not item:
                            result[word] = TagResult("解析失败", f"“{word}”，AI未返回结果。")
                            continue
                        normalized = _normalize_entry(word, item)
                        result[word] = normalized
                        ai_items.append(
                            {
                                "word": word,
                                "tag_label": normalized.tag_label,
                                "reason": normalized.reason,
                            }
                        )
                    done += len(batch)
                    if callable(progress_callback):
                        progress_callback(
                            {
                                "stage": "ai_batch",
                                "batch_index": idx,
                                "total_batches": total_batches,
                                "done_words": done,
                                "total_missing": len(missing),
                            }
                        )

            asyncio.run(_run())
            if self.persist_local_on_ai:
                self._upsert_local(ai_items)
            self._upsert_pg(ai_items)
            if callable(progress_callback):
                progress_callback({"stage": "ai_done", "done_words": len(missing), "total_missing": len(missing)})

        for word in missing:
            if word in result:
                continue
            result[word] = TagResult("解析失败", f"“{word}”，无缓存且未启用AI。")
        return result
