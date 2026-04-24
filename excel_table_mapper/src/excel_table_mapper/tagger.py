from __future__ import annotations

import asyncio
import json
import os
import re
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
    def _format_reason(w: str, reason_text: str) -> str:
        text = str(reason_text or "").strip().replace('"', "“").replace("'", "")
        text = text.replace("““", "“").replace("””", "”")
        if not text:
            return f"“{w}”，词义待确认。"
        text = text.replace("表示", "").replace("指", "").strip("，,。 ")
        first_sentence = text.split("。")[0].strip("，, ")
        if "，" in first_sentence:
            left, right = first_sentence.split("，", 1)
            meaning = left.strip("“” ,，")
            reason = right.strip(" ,，")
            if meaning and reason:
                return f"“{meaning}”，{reason}。"
        return f"“{w}”，{first_sentence or '词义待确认'}。"

    if isinstance(raw, str):
        tag = raw.strip() or "无效词"
        return TagResult(tag_label=tag, reason=_format_reason(word, f"{word}，来自缓存"))

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
        return TagResult(tag_label=tag, reason=_format_reason(word, str(reason or "").strip()))

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


def _ensure_reason_format(word: str, reason: str) -> str:
    text = str(reason or "").strip()
    if not text:
        return f"“{word}”，词义待确认。"
    text = text.replace('"', "“").replace("'", "")
    text = text.replace("通常表示", "").replace("表示", "").replace("指的是", "").replace("可用于", "")
    text = text.strip("，,。 ")
    m = re.match(r"^“([^”]+)”\s*[，,]\s*(.+)$", text)
    if m:
        meaning = m.group(1).strip("，,。 ")
        why = m.group(2).strip("，,。 ")
        if meaning and why:
            return f"“{meaning}”，{why}。"
    if "，" in text:
        left, right = text.split("，", 1)
        meaning = left.strip("“” ,，。")
        why = right.strip(" ,，。")
        if meaning and why:
            return f"“{meaning}”，{why}。"
    return f"“{word}”，{text}。"


class Tagger:
    def __init__(
        self,
        *,
        enable_ai: bool,
        use_history_cache: bool = True,
        model: str,
        base_url: str,
        batch_size: int,
        local_cache_db: Path,
        pg_config: PgConfig,
        persist_local_on_ai: bool = True,
        api_key: str | None = None,
    ) -> None:
        load_dotenv()
        self.enable_ai = bool(enable_ai)
        self.use_history_cache = bool(use_history_cache)
        self.model = model
        self.base_url = base_url
        self.batch_size = max(1, int(batch_size))
        self.local_cache_db = local_cache_db
        self.pg = pg_config
        self.pg_verified_table = f"{self.pg.table}_verified"
        self.pg_phrase_norm_table = f"{self.pg.table}_phrase_norm"
        self.pg_phrase_norm_source_table = f"{self.pg.table}_phrase_norm_sources"
        self.pg_source_table = f"{self.pg.table}_sources"
        self.persist_local_on_ai = bool(persist_local_on_ai)
        self.seed_phrase_norms = [
            {
                "raw_phrase": "jelly cat",
                "normalized_phrase": "jellycat",
                "status": "manual_confirmed",
                "source": "seed",
                "source_scope": "seed:jellycat",
                "confidence": 1.0,
                "hit_count": 1,
            }
        ]

        self.local_cache_db.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_local_table()
        self._upsert_local_phrase_norm(self.seed_phrase_norms)
        if self.pg.enabled:
            self._upsert_pg_phrase_norm(self.seed_phrase_norms)

        self.client: AsyncOpenAI | None = None
        if self.enable_ai:
            resolved_api_key = str(api_key or "").strip() or os.getenv("OPENAI_API_KEY", "").strip()
            if not resolved_api_key:
                raise RuntimeError("启用 AI 打标时必须设置 OPENAI_API_KEY。")
            self.client = AsyncOpenAI(api_key=resolved_api_key, base_url=base_url)

    @staticmethod
    def _phrase_norm_conflicts_from_map(mapping: dict[str, str]) -> set[tuple[str, str]]:
        conflicts: set[tuple[str, str]] = set()
        for raw, normalized in mapping.items():
            if not raw or not normalized or raw == normalized:
                continue
            if mapping.get(normalized) == raw:
                a, b = sorted((raw, normalized))
                conflicts.add((a, b))
        return conflicts

    @staticmethod
    def _raise_if_new_phrase_norm_conflicts(
        before_map: dict[str, str],
        after_map: dict[str, str],
        *,
        context: str,
    ) -> None:
        before_conflicts = Tagger._phrase_norm_conflicts_from_map(before_map)
        after_conflicts = Tagger._phrase_norm_conflicts_from_map(after_map)
        new_conflicts = sorted(after_conflicts - before_conflicts)
        if not new_conflicts:
            return
        preview = "；".join([f"{a} <-> {b}" for a, b in new_conflicts[:5]])
        raise RuntimeError(f"{context}失败：检测到归一化冲突，禁止保存。冲突示例：{preview}")

    @staticmethod
    def _apply_phrase_norm_overrides(
        base_map: dict[str, str],
        overrides: list[tuple[str, str, str]],
    ) -> dict[str, str]:
        result = dict(base_map)
        for raw_phrase, normalized_phrase, status in overrides:
            status_text = str(status or "").strip().lower()
            if status_text == "rejected":
                result.pop(raw_phrase, None)
                continue
            if raw_phrase and normalized_phrase and raw_phrase != normalized_phrase:
                result[raw_phrase] = normalized_phrase
        return result

    def _sqlite(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.local_cache_db)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_local_table(self) -> None:
        with self._sqlite() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS phrase_normalization (
                    raw_phrase TEXT PRIMARY KEY,
                    normalized_phrase TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    source TEXT NULL,
                    confidence REAL NULL,
                    hit_count INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS phrase_normalization_sources (
                    raw_phrase TEXT NOT NULL,
                    source_scope TEXT NOT NULL,
                    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                    PRIMARY KEY (raw_phrase, source_scope)
                )
                """
            )
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

    def list_verified_cache(self, keyword: str | None = None, limit: int = 5000) -> list[dict[str, Any]]:
        limit_val = max(1, min(int(limit or 5000), 20000))
        keyword_text = _norm(keyword or "")

        if self.pg.enabled:
            self._ensure_pg_table()
            with self._connect_pg() as conn:
                with conn.cursor() as cur:
                    if keyword_text:
                        like = f"%{keyword_text}%"
                        cur.execute(
                            f"""
                            SELECT word, tag_label, tag_reason, updated_at
                            FROM {self.pg.schema}.{self.pg_verified_table}
                            WHERE word ILIKE %s
                               OR tag_label ILIKE %s
                               OR COALESCE(tag_reason, '') ILIKE %s
                            ORDER BY updated_at DESC
                            LIMIT %s
                            """,
                            (like, like, like, limit_val),
                        )
                    else:
                        cur.execute(
                            f"""
                            SELECT word, tag_label, tag_reason, updated_at
                            FROM {self.pg.schema}.{self.pg_verified_table}
                            ORDER BY updated_at DESC
                            LIMIT %s
                            """,
                            (limit_val,),
                        )
                    rows = cur.fetchall()
            items = [
                {
                    "word": str(row[0] or ""),
                    "tag_label": str(row[1] or ""),
                    "reason": str(row[2] or ""),
                    "updated_at": row[3],
                }
                for row in rows
                if _norm(row[0])
            ]
            if items:
                self._upsert_local(
                    [
                        {
                            "word": item["word"],
                            "tag_label": item["tag_label"],
                            "reason": item["reason"],
                        }
                        for item in items
                    ]
                )
            return items

        with self._sqlite() as conn:
            if keyword_text:
                like = f"%{keyword_text}%"
                rows = conn.execute(
                    """
                    SELECT word, tag_label, reason, updated_at
                    FROM word_cache
                    WHERE tag_label IS NOT NULL
                      AND (
                        lower(word) LIKE ?
                        OR lower(COALESCE(tag_label, '')) LIKE ?
                        OR lower(COALESCE(reason, '')) LIKE ?
                      )
                    ORDER BY updated_at DESC
                    LIMIT ?
                    """,
                    (like, like, like, limit_val),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT word, tag_label, reason, updated_at
                    FROM word_cache
                    WHERE tag_label IS NOT NULL
                    ORDER BY updated_at DESC
                    LIMIT ?
                    """,
                    (limit_val,),
                ).fetchall()
        return [
            {
                "word": str(row["word"] or ""),
                "tag_label": str(row["tag_label"] or ""),
                "reason": str(row["reason"] or ""),
                "updated_at": row["updated_at"],
            }
            for row in rows
            if _norm(row["word"])
        ]

    def update_verified_cache_entry(self, word: str, tag_label: str, reason: str) -> None:
        norm_word = _norm(word)
        if not norm_word:
            raise ValueError("word is required")
        label = str(tag_label or "").strip()
        if not label:
            raise ValueError("tag_label is required")
        fixed_reason = _ensure_reason_format(norm_word, str(reason or "").strip())

        if self.pg.enabled:
            self._ensure_pg_table()
            with self._connect_pg() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        INSERT INTO {self.pg.schema}.{self.pg_verified_table}
                        (word, tag_label, tag_reason, promoted_at, updated_at)
                        VALUES (%s, %s, %s, NOW(), NOW())
                        ON CONFLICT (word)
                        DO UPDATE SET
                            tag_label = EXCLUDED.tag_label,
                            tag_reason = EXCLUDED.tag_reason,
                            updated_at = NOW()
                        """,
                        (norm_word, label, fixed_reason),
                    )
                    cur.execute(
                        f"""
                        UPDATE {self.pg.schema}.{self.pg.table}
                        SET tag_label = %s, tag_reason = %s, updated_at = NOW()
                        WHERE word = %s
                        """,
                        (label, fixed_reason, norm_word),
                    )
                conn.commit()

        self._upsert_local(
            [
                {
                    "word": norm_word,
                    "tag_label": label,
                    "reason": fixed_reason,
                }
            ]
        )

    def _local_phrase_norm_map_all_active(self) -> dict[str, str]:
        with self._sqlite() as conn:
            rows = conn.execute(
                """
                SELECT raw_phrase, normalized_phrase, status
                FROM phrase_normalization
                """
            ).fetchall()
        out: dict[str, str] = {}
        for row in rows:
            raw_phrase = " ".join(_norm(row["raw_phrase"]).split())
            normalized_phrase = " ".join(_norm(row["normalized_phrase"]).split())
            status = str(row["status"] or "").strip().lower()
            if not raw_phrase or not normalized_phrase or raw_phrase == normalized_phrase:
                continue
            if status == "rejected":
                continue
            out[raw_phrase] = normalized_phrase
        return out

    def _upsert_local_phrase_norm(self, items: list[dict[str, Any]]) -> int:
        rows: list[tuple[str, str, str, str | None, float | None, int]] = []
        source_rows: list[tuple[str, str]] = []
        for item in items:
            raw_phrase = " ".join(_norm(item.get("raw_phrase") or "").split())
            normalized_phrase = " ".join(_norm(item.get("normalized_phrase") or "").split())
            if not raw_phrase or not normalized_phrase or raw_phrase == normalized_phrase:
                continue
            status = str(item.get("status") or "pending").strip() or "pending"
            if status not in {"pending", "auto_confirmed", "manual_confirmed", "rejected"}:
                status = "pending"
            source = _clean_text(item.get("source"))
            confidence = item.get("confidence")
            try:
                confidence_val = float(confidence) if confidence is not None else None
            except Exception:
                confidence_val = None
            hit_count = int(item.get("hit_count") or 1)
            rows.append((raw_phrase, normalized_phrase, status, source, confidence_val, max(1, hit_count)))
            source_scope = " ".join(_norm(item.get("source_scope") or "").split())
            if source_scope:
                source_rows.append((raw_phrase, source_scope))
        if not rows:
            return 0

        current_map = self._local_phrase_norm_map_all_active()
        next_map = self._apply_phrase_norm_overrides(
            current_map,
            [(raw, normalized, status) for (raw, normalized, status, _, _, _) in rows],
        )
        self._raise_if_new_phrase_norm_conflicts(current_map, next_map, context="本地归一化保存")

        with self._sqlite() as conn:
            conn.executemany(
                """
                INSERT INTO phrase_normalization
                (raw_phrase, normalized_phrase, status, source, confidence, hit_count, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
                ON CONFLICT(raw_phrase) DO UPDATE SET
                    normalized_phrase = CASE
                        WHEN phrase_normalization.status IN ('manual_confirmed', 'rejected')
                        THEN phrase_normalization.normalized_phrase
                        ELSE excluded.normalized_phrase
                    END,
                    status = CASE
                        WHEN phrase_normalization.status IN ('manual_confirmed', 'rejected')
                        THEN phrase_normalization.status
                        ELSE excluded.status
                    END,
                    source = COALESCE(excluded.source, phrase_normalization.source),
                    confidence = COALESCE(excluded.confidence, phrase_normalization.confidence),
                    hit_count = CASE
                        WHEN phrase_normalization.hit_count > excluded.hit_count
                        THEN phrase_normalization.hit_count
                        ELSE excluded.hit_count
                    END,
                    updated_at = datetime('now')
                """,
                rows,
            )
            if source_rows:
                conn.executemany(
                    """
                    INSERT INTO phrase_normalization_sources (raw_phrase, source_scope, updated_at)
                    VALUES (?, ?, datetime('now'))
                    ON CONFLICT(raw_phrase, source_scope) DO UPDATE SET
                        updated_at = datetime('now')
                    """,
                    source_rows,
                )
                conn.executemany(
                    """
                    UPDATE phrase_normalization
                    SET hit_count = (
                        SELECT COUNT(1) FROM phrase_normalization_sources s
                        WHERE s.raw_phrase = phrase_normalization.raw_phrase
                    ),
                    updated_at = datetime('now')
                    WHERE raw_phrase = ?
                    """,
                    [(raw_phrase,) for raw_phrase, _ in source_rows],
                )
            conn.commit()
        return len(rows)

    def _query_local_phrase_norm(self, *, confirmed_only: bool = True) -> list[dict[str, Any]]:
        where = "WHERE status IN ('auto_confirmed', 'manual_confirmed')" if confirmed_only else ""
        with self._sqlite() as conn:
            fetched = conn.execute(
                f"""
                SELECT raw_phrase, normalized_phrase, status, source, confidence, hit_count, updated_at
                FROM phrase_normalization
                {where}
                """
            ).fetchall()
        return [
            {
                "raw_phrase": str(row["raw_phrase"]),
                "normalized_phrase": str(row["normalized_phrase"]),
                "status": str(row["status"]),
                "source": row["source"],
                "confidence": row["confidence"],
                "hit_count": int(row["hit_count"] or 0),
                "updated_at": row["updated_at"],
            }
            for row in fetched
        ]

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
                        tag_hit_count INTEGER NOT NULL DEFAULT 0,
                        created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
                    )
                    """
                )
                cur.execute(
                    f"""
                    ALTER TABLE {self.pg.schema}.{self.pg.table}
                    ADD COLUMN IF NOT EXISTS tag_hit_count INTEGER NOT NULL DEFAULT 0
                    """
                )
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self.pg.schema}.{self.pg_source_table} (
                        word TEXT NOT NULL,
                        tag_label TEXT NOT NULL,
                        source_scope TEXT NOT NULL,
                        created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
                        PRIMARY KEY (word, tag_label, source_scope)
                    )
                    """
                )
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self.pg.schema}.{self.pg_phrase_norm_table} (
                        raw_phrase TEXT PRIMARY KEY,
                        normalized_phrase TEXT NOT NULL,
                        status TEXT NOT NULL DEFAULT 'pending',
                        source TEXT NULL,
                        confidence DOUBLE PRECISION NULL,
                        hit_count INTEGER NOT NULL DEFAULT 0,
                        created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
                    )
                    """
                )
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self.pg.schema}.{self.pg_phrase_norm_source_table} (
                        raw_phrase TEXT NOT NULL,
                        source_scope TEXT NOT NULL,
                        created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
                        PRIMARY KEY (raw_phrase, source_scope)
                    )
                    """
                )
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self.pg.schema}.{self.pg_verified_table} (
                        word TEXT PRIMARY KEY,
                        translation_zh TEXT NULL,
                        tag_label TEXT NOT NULL,
                        tag_reason TEXT NULL,
                        promoted_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
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
                    FROM {self.pg.schema}.{self.pg_verified_table}
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

    def _pg_phrase_norm_map_all_active(self) -> dict[str, str]:
        if not self.pg.enabled:
            return {}
        self._ensure_pg_table()
        with self._connect_pg() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT raw_phrase, normalized_phrase, status
                    FROM {self.pg.schema}.{self.pg_phrase_norm_table}
                    """
                )
                rows = cur.fetchall()
        out: dict[str, str] = {}
        for raw_phrase, normalized_phrase, status in rows:
            raw = " ".join(_norm(raw_phrase).split())
            normalized = " ".join(_norm(normalized_phrase).split())
            status_text = str(status or "").strip().lower()
            if not raw or not normalized or raw == normalized:
                continue
            if status_text == "rejected":
                continue
            out[raw] = normalized
        return out

    def _upsert_pg_phrase_norm(self, items: list[dict[str, Any]]) -> int:
        if not self.pg.enabled:
            return 0
        normalized_rows: list[tuple[str, str, str, str | None, float | None, int]] = []
        source_rows: list[tuple[str, str]] = []
        for item in items:
            raw_phrase = " ".join(_norm(item.get("raw_phrase") or "").split())
            normalized_phrase = " ".join(_norm(item.get("normalized_phrase") or "").split())
            if not raw_phrase or not normalized_phrase or raw_phrase == normalized_phrase:
                continue
            status = str(item.get("status") or "pending").strip() or "pending"
            if status not in {"pending", "auto_confirmed", "manual_confirmed", "rejected"}:
                status = "pending"
            source = _clean_text(item.get("source"))
            confidence = item.get("confidence")
            try:
                confidence_val = float(confidence) if confidence is not None else None
            except Exception:
                confidence_val = None
            hit_count = int(item.get("hit_count") or 1)
            normalized_rows.append((raw_phrase, normalized_phrase, status, source, confidence_val, max(1, hit_count)))
            source_scope = " ".join(_norm(item.get("source_scope") or "").split())
            if source_scope:
                source_rows.append((raw_phrase, source_scope))
        if not normalized_rows:
            return 0

        current_map = self._pg_phrase_norm_map_all_active()
        next_map = self._apply_phrase_norm_overrides(
            current_map,
            [(raw, normalized, status) for (raw, normalized, status, _, _, _) in normalized_rows],
        )
        self._raise_if_new_phrase_norm_conflicts(current_map, next_map, context="云端归一化保存")

        self._ensure_pg_table()
        with self._connect_pg() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    f"""
                    INSERT INTO {self.pg.schema}.{self.pg_phrase_norm_table}
                    (raw_phrase, normalized_phrase, status, source, confidence, hit_count, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (raw_phrase)
                    DO UPDATE SET
                        normalized_phrase = CASE
                            WHEN {self.pg.schema}.{self.pg_phrase_norm_table}.status IN ('manual_confirmed', 'rejected')
                            THEN {self.pg.schema}.{self.pg_phrase_norm_table}.normalized_phrase
                            ELSE EXCLUDED.normalized_phrase
                        END,
                        status = CASE
                            WHEN {self.pg.schema}.{self.pg_phrase_norm_table}.status IN ('manual_confirmed', 'rejected')
                            THEN {self.pg.schema}.{self.pg_phrase_norm_table}.status
                            ELSE EXCLUDED.status
                        END,
                        source = COALESCE(EXCLUDED.source, {self.pg.schema}.{self.pg_phrase_norm_table}.source),
                        confidence = COALESCE(EXCLUDED.confidence, {self.pg.schema}.{self.pg_phrase_norm_table}.confidence),
                        hit_count = GREATEST({self.pg.schema}.{self.pg_phrase_norm_table}.hit_count, EXCLUDED.hit_count),
                        updated_at = NOW()
                    """,
                    normalized_rows,
                )
                if source_rows:
                    cur.executemany(
                        f"""
                        INSERT INTO {self.pg.schema}.{self.pg_phrase_norm_source_table}
                        (raw_phrase, source_scope, created_at)
                        VALUES (%s, %s, NOW())
                        ON CONFLICT (raw_phrase, source_scope) DO NOTHING
                        """,
                        source_rows,
                    )
                    cur.execute(
                        f"""
                        UPDATE {self.pg.schema}.{self.pg_phrase_norm_table} p
                        SET hit_count = s.cnt, updated_at = NOW()
                        FROM (
                            SELECT raw_phrase, COUNT(DISTINCT source_scope) AS cnt
                            FROM {self.pg.schema}.{self.pg_phrase_norm_source_table}
                            GROUP BY raw_phrase
                        ) s
                        WHERE p.raw_phrase = s.raw_phrase
                          AND p.raw_phrase = ANY(%s)
                        """,
                        ([raw_phrase for raw_phrase, _ in source_rows],),
                    )
            conn.commit()
        return len(normalized_rows)

    def _query_pg_phrase_norm(self, *, confirmed_only: bool = True) -> list[dict[str, Any]]:
        if not self.pg.enabled:
            return []
        self._ensure_pg_table()
        where = "WHERE status IN ('auto_confirmed', 'manual_confirmed')" if confirmed_only else ""
        with self._connect_pg() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT raw_phrase, normalized_phrase, status, source, confidence, hit_count, updated_at
                    FROM {self.pg.schema}.{self.pg_phrase_norm_table}
                    {where}
                    """
                )
                rows = cur.fetchall()
        return [
            {
                "raw_phrase": str(r[0]),
                "normalized_phrase": str(r[1]),
                "status": str(r[2]),
                "source": r[3],
                "confidence": r[4],
                "hit_count": int(r[5] or 0),
                "updated_at": r[6],
            }
            for r in rows
        ]

    def list_phrase_normalization(self, status: str | None = None) -> list[dict[str, Any]]:
        status_text = str(status or "").strip().lower()
        allowed = {"pending", "auto_confirmed", "manual_confirmed", "rejected"}
        if status_text and status_text not in allowed:
            status_text = ""

        if self.pg.enabled:
            self._ensure_pg_table()
            with self._connect_pg() as conn:
                with conn.cursor() as cur:
                    if status_text:
                        cur.execute(
                            f"""
                            SELECT raw_phrase, normalized_phrase, status, source, confidence, hit_count, updated_at
                            FROM {self.pg.schema}.{self.pg_phrase_norm_table}
                            WHERE status = %s
                            ORDER BY hit_count DESC, updated_at DESC
                            """,
                            (status_text,),
                        )
                    else:
                        cur.execute(
                            f"""
                            SELECT raw_phrase, normalized_phrase, status, source, confidence, hit_count, updated_at
                            FROM {self.pg.schema}.{self.pg_phrase_norm_table}
                            ORDER BY hit_count DESC, updated_at DESC
                            """
                        )
                    rows = cur.fetchall()
            items = [
                {
                    "raw_phrase": str(r[0]),
                    "normalized_phrase": str(r[1]),
                    "status": str(r[2]),
                    "source": r[3],
                    "confidence": r[4],
                    "hit_count": int(r[5] or 0),
                    "updated_at": r[6],
                }
                for r in rows
            ]
            if items:
                self._upsert_local_phrase_norm(items)
            return items

        items = self._query_local_phrase_norm(confirmed_only=False)
        if status_text:
            return [x for x in items if str(x.get("status", "")).lower() == status_text]
        return items

    def update_phrase_normalization_status(self, raw_phrases: list[str], status: str) -> int:
        target = str(status or "").strip().lower()
        if target not in {"pending", "manual_confirmed", "rejected"}:
            raise ValueError("invalid normalization status")
        keys = sorted({" ".join(_norm(p).split()) for p in raw_phrases if _norm(p)})
        if not keys:
            return 0

        updated = 0
        if self.pg.enabled:
            if target != "rejected":
                current_map = self._pg_phrase_norm_map_all_active()
                self._ensure_pg_table()
                with self._connect_pg() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            f"""
                            SELECT raw_phrase, normalized_phrase
                            FROM {self.pg.schema}.{self.pg_phrase_norm_table}
                            WHERE raw_phrase = ANY(%s)
                            """,
                            (keys,),
                        )
                        rows = cur.fetchall()
                overrides = [
                    (" ".join(_norm(r[0]).split()), " ".join(_norm(r[1]).split()), target)
                    for r in rows
                    if _norm(r[0]) and _norm(r[1])
                ]
                next_map = self._apply_phrase_norm_overrides(current_map, overrides)
                self._raise_if_new_phrase_norm_conflicts(current_map, next_map, context="云端归一化状态更新")
            self._ensure_pg_table()
            with self._connect_pg() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        UPDATE {self.pg.schema}.{self.pg_phrase_norm_table}
                        SET status = %s, updated_at = NOW()
                        WHERE raw_phrase = ANY(%s)
                        """,
                        (target, keys),
                    )
                    updated = int(cur.rowcount or 0)
                conn.commit()
            rows = self.list_phrase_normalization()
            if rows:
                self._upsert_local_phrase_norm(rows)
            return updated

        if target != "rejected":
            current_map = self._local_phrase_norm_map_all_active()
            placeholders = ",".join(["?"] * len(keys))
            with self._sqlite() as conn:
                rows = conn.execute(
                    f"""
                    SELECT raw_phrase, normalized_phrase
                    FROM phrase_normalization
                    WHERE raw_phrase IN ({placeholders})
                    """,
                    keys,
                ).fetchall()
            overrides = [
                (" ".join(_norm(r["raw_phrase"]).split()), " ".join(_norm(r["normalized_phrase"]).split()), target)
                for r in rows
                if _norm(r["raw_phrase"]) and _norm(r["normalized_phrase"])
            ]
            next_map = self._apply_phrase_norm_overrides(current_map, overrides)
            self._raise_if_new_phrase_norm_conflicts(current_map, next_map, context="本地归一化状态更新")

        with self._sqlite() as conn:
            placeholders = ",".join(["?"] * len(keys))
            cur = conn.execute(
                f"""
                UPDATE phrase_normalization
                SET status = ?, updated_at = datetime('now')
                WHERE raw_phrase IN ({placeholders})
                """,
                [target, *keys],
            )
            updated = int(cur.rowcount or 0)
            conn.commit()
        return updated

    def load_phrase_normalization_map(self) -> dict[str, str]:
        if self.pg.enabled:
            pg_rows = self._query_pg_phrase_norm(confirmed_only=True)
            if pg_rows:
                self._upsert_local_phrase_norm(pg_rows)
                return {row["raw_phrase"]: row["normalized_phrase"] for row in pg_rows}
            return {}
        local_rows = self._query_local_phrase_norm(confirmed_only=True)
        return {row["raw_phrase"]: row["normalized_phrase"] for row in local_rows}

    def record_phrase_normalization_candidates(self, items: list[dict[str, Any]]) -> int:
        if not items:
            return 0
        if self.pg.enabled:
            written = self._upsert_pg_phrase_norm(items)
            confirmed_rows = self._query_pg_phrase_norm(confirmed_only=True)
            if confirmed_rows:
                self._upsert_local_phrase_norm(confirmed_rows)
            return written
        return self._upsert_local_phrase_norm(items)

    def _promote_verified_from_raw(self, words: list[str]) -> int:
        if not self.pg.enabled:
            return 0

        normalized_words = sorted({_norm(w) for w in words if _norm(w)})
        if not normalized_words:
            return 0

        self._ensure_pg_table()
        with self._connect_pg() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {self.pg.schema}.{self.pg_verified_table}
                    (word, translation_zh, tag_label, tag_reason, promoted_at, updated_at)
                    SELECT word, translation_zh, tag_label, tag_reason, NOW(), NOW()
                    FROM {self.pg.schema}.{self.pg.table}
                    WHERE word = ANY(%s)
                      AND tag_label IS NOT NULL
                      AND tag_hit_count >= 3
                    ON CONFLICT (word)
                    DO UPDATE SET
                        translation_zh = COALESCE(EXCLUDED.translation_zh, {self.pg.schema}.{self.pg_verified_table}.translation_zh),
                        tag_label = EXCLUDED.tag_label,
                        tag_reason = COALESCE(EXCLUDED.tag_reason, {self.pg.schema}.{self.pg_verified_table}.tag_reason),
                        updated_at = NOW()
                    """,
                    (normalized_words,),
                )
                promoted = cur.rowcount or 0
            conn.commit()

        return int(promoted)

    def _upsert_pg(self, items: list[dict[str, Any]], source_scope: str | None = None) -> int:
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
                    (word, translation_zh, tag_label, tag_reason, tag_hit_count, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (word)
                    DO UPDATE SET
                        translation_zh = COALESCE(EXCLUDED.translation_zh, {self.pg.schema}.{self.pg.table}.translation_zh),
                        tag_label = COALESCE(EXCLUDED.tag_label, {self.pg.schema}.{self.pg.table}.tag_label),
                        tag_reason = COALESCE(EXCLUDED.tag_reason, {self.pg.schema}.{self.pg.table}.tag_reason),
                        tag_hit_count = CASE
                            WHEN EXCLUDED.tag_label IS NOT NULL
                             AND {self.pg.schema}.{self.pg.table}.tag_label = EXCLUDED.tag_label
                            THEN {self.pg.schema}.{self.pg.table}.tag_hit_count + 1
                            ELSE 1
                        END,
                        updated_at = NOW()
                    """,
                    [(w, zh, tag, reason, 1) for (w, zh, tag, reason) in normalized],
                )
                source_scope_text = str(source_scope or "").strip()
                if source_scope_text:
                    cur.executemany(
                        f"""
                        INSERT INTO {self.pg.schema}.{self.pg_source_table}
                        (word, tag_label, source_scope, created_at)
                        VALUES (%s, %s, %s, NOW())
                        ON CONFLICT (word, tag_label, source_scope) DO NOTHING
                        """,
                        [(w, tag, source_scope_text) for (w, _, tag, _) in normalized if tag],
                    )
                    cur.execute(
                        f"""
                        UPDATE {self.pg.schema}.{self.pg.table} t
                        SET tag_hit_count = s.cnt
                        FROM (
                            SELECT word, tag_label, COUNT(DISTINCT source_scope) AS cnt
                            FROM {self.pg.schema}.{self.pg_source_table}
                            GROUP BY word, tag_label
                        ) s
                        WHERE t.word = s.word
                          AND t.tag_label = s.tag_label
                          AND t.word = ANY(%s)
                        """,
                        ([w for (w, _, _, _) in normalized],),
                    )
            conn.commit()

        promoted_words = [w for (w, _, _, _) in normalized]
        self._promote_verified_from_raw(promoted_words)
        if self.use_history_cache:
            promoted_hits = self._query_pg(promoted_words)
            if promoted_hits:
                self._upsert_local(
                    [
                        {
                            "word": word,
                            "translation_zh": entry.get("translation_zh"),
                            "tag_label": entry.get("tag_label"),
                            "reason": entry.get("reason"),
                        }
                        for word, entry in promoted_hits.items()
                    ]
                )
        return len(normalized)

    def _fetch_cache(self, words: list[str]) -> dict[str, dict[str, Any]]:
        if not self.use_history_cache:
            return {}

        if self.pg.enabled:
            # Online mode: prioritize DB verified cache only, then backfill local.
            pg_hits = self._query_pg(words)
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
            return pg_hits

        # Offline mode: local cache only.
        return self._query_local(words)

    async def _classify_batch(self, words: list[str], phrase_hints: dict[str, list[str]] | None = None) -> dict[str, dict[str, str]]:
        if not self.client or not words:
            return {}

        phrase_hints = phrase_hints or {}
        lines: list[str] = []
        for word in words:
            phrases = phrase_hints.get(_norm(word), [])[:10]
            refs_text = "; ".join(phrases) if phrases else "无"
            lines.append(f"关键词: {word} | 参考短语(按搜索量降序): {refs_text}")
        words_text = "\n\n".join(lines)
        prompt = f"""
# 亚马逊词根打标
请将下面的英文词根映射为唯一标签。站在10年跨境PM视角，为每个标签简述为何选择该标签。
标签必须是以下之一：
1核心词 / 2外形 / 3属性 / 4痛点 / 5规格 / 6受众 / 7场景 / 8品牌 / 无效词

标签定义：
- 1核心词：产品的核心名称或类别名（如：stuffed, plush, animal, toy, pillow）。
- 2外形：视觉主体形象、物种名（如：bear, panda, cat, unicorn, dinosaur）。
- 3属性：产品的物理特性、改性工艺或硬件卖点（如：weighted, heated, warmable, cooling）。
- 4痛点：用户主观渴望解决的情绪、生理问题或购买诉求（如：anxiety, sleep, sensory, stress）。
- 5规格：描述产品的大小、重量或厚度规格（如：giant, large, 5lbs, mini）。
- 6受众：描述产品的使用人群（如：adults, kids, toddler, baby）。
- 7场景：描述产品的使用时机或赠送节点（如：gift, birthday, valentine）。
- 8品牌：描述特定的品牌名称或受保护的IP（如：warmies, disney, squishmallow）。
- 无效词：语义不完整或业务价值低，按无效词处理。

返回要求：
1. 只返回 JSON。
2. 顺序必须和输入一致。
3. 原因备注用简短中文说明，不要输出多余解释。
4. 颜色词统一归为 2外形，例如 pink / blue / red / black / white / rainbow / beige 等配色词，不归 5规格。
5. 每行有“关键词 + 参考短语(Top10)”；参考短语仅作为该关键词语义判定依据，不改变标签集合。
6. 如关键词为西班牙语（或主要语义来自西班牙语），请在原因备注中明确标注“西班牙语”。

格式：
{{
  "rows": [
    {{
      "关键词": "stuffed",
      "对应标签": "1核心词",
      "原因备注": "“填充”，类目核心词。"
    }},
    {{
      "关键词": "weighted",
      "对应标签": "3属性",
      "原因备注": "“加重”，产品核心属性。"
    }},
    {{
      "关键词": "pink",
      "对应标签": "2外形",
      "原因备注": "“粉色”，外观形象词，用于区分视觉风格。"
    }}
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
        if not self.pg.enabled or not self.use_history_cache:
            return {"synced_count": 0}

        self._ensure_pg_table()
        with self._connect_pg() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT word, translation_zh, tag_label, tag_reason
                    FROM {self.pg.schema}.{self.pg_verified_table}
                    """
                )
                rows = cur.fetchall()
                cur.execute(
                    f"""
                    SELECT raw_phrase, normalized_phrase, status, source, confidence, hit_count
                    FROM {self.pg.schema}.{self.pg_phrase_norm_table}
                    """
                )
                norm_rows = cur.fetchall()

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
        norm_items = [
            {
                "raw_phrase": _clean_text(row[0]) or "",
                "normalized_phrase": _clean_text(row[1]) or "",
                "status": _clean_text(row[2]) or "pending",
                "source": _clean_text(row[3]),
                "confidence": row[4],
                "hit_count": int(row[5] or 1),
            }
            for row in norm_rows
        ]
        norm_upserted = self._upsert_local_phrase_norm(norm_items)
        if callable(progress_callback):
            progress_callback({"stage": "db_sync_done", "done": upserted, "total": len(rows)})
        return {"synced_count": upserted, "synced_norm_count": norm_upserted}

    def classify(
        self,
        words: list[str],
        *,
        phrase_hints: dict[str, list[str]] | None = None,
        progress_callback=None,
        source_scope: str | None = None,
    ) -> dict[str, TagResult]:
        result: dict[str, TagResult] = {}
        missing: list[str] = []
        cached = self._fetch_cache(words)
        confirmed_in_db: dict[str, dict[str, Any]] = {}
        if self.pg.enabled:
            # Always check confirmed table in online mode to avoid re-labeling/re-writing existing confirmed words.
            confirmed_in_db = self._query_pg(words)

        for word in words:
            norm_word = _norm(word)
            entry = cached.get(norm_word)
            if entry is None and norm_word in confirmed_in_db:
                entry = confirmed_in_db[norm_word]
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
                    payload = await self._classify_batch(batch, phrase_hints=phrase_hints)
                    for word in batch:
                        item = payload.get(_norm(word))
                        if not item:
                            result[word] = TagResult("解析失败", f"“{word}”，AI 未返回结果。")
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

            if self.persist_local_on_ai and not self.pg.enabled:
                self._upsert_local(ai_items)
            self._upsert_pg(ai_items, source_scope=source_scope)

            if callable(progress_callback):
                progress_callback({"stage": "ai_done", "done_words": len(missing), "total_missing": len(missing)})

        for word in missing:
            if word in result:
                continue
            result[word] = TagResult("解析失败", f"“{word}”，无缓存且未启用 AI。")

        # Final fallback: enforce output reason format for all sources.
        for word, row in list(result.items()):
            result[word] = TagResult(tag_label=row.tag_label, reason=_ensure_reason_format(word, row.reason))

        return result
