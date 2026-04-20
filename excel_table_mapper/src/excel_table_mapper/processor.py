from __future__ import annotations

import csv
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from openpyxl import Workbook, load_workbook
from openpyxl.formatting.rule import ColorScaleRule
from openpyxl.utils import get_column_letter

from .tagger import Tagger, TagResult

DEFAULT_STOPWORDS = {
    "a",
    "an",
    "the",
    "and",
    "or",
    "to",
    "of",
    "for",
    "with",
    "in",
    "on",
    "at",
    "by",
    "from",
    "is",
    "are",
    "be",
    "as",
    "it",
}

TOKEN_PATTERN = re.compile(r"[a-z0-9]+")


@dataclass
class RawRow:
    search_term: str
    weekly_exposure: float
    impressions: float
    clicks: float
    tokens: list[str]
    unique_tokens: set[str]


def _safe_float(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "")
    if not text:
        return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def split_words(text: str, *, stopwords: set[str] | None = None, to_lower: bool = True) -> list[str]:
    if not isinstance(text, str):
        return []
    source = text.lower() if to_lower else text
    tokens = TOKEN_PATTERN.findall(source)
    out: list[str] = []
    for token in tokens:
        if token.isdigit():
            continue
        if any(ch.isdigit() for ch in token):
            continue
        if len(token) <= 1:
            continue
        if stopwords and token in stopwords:
            continue
        out.append(token)
    return out


def read_input_rows(input_path: Path, sheet_name: str | None) -> list[dict[str, Any]]:
    if input_path.suffix.lower() == ".csv":
        with input_path.open("r", encoding="utf-8-sig", newline="") as f:
            return list(csv.DictReader(f))

    wb = load_workbook(input_path, data_only=True, read_only=True)
    ws = wb[sheet_name] if sheet_name else wb[wb.sheetnames[0]]
    rows = ws.iter_rows(values_only=True)
    headers = [str(h).strip() if h is not None else "" for h in next(rows, [])]
    result: list[dict[str, Any]] = []
    for row in rows:
        record = {}
        for i, key in enumerate(headers):
            if not key:
                continue
            record[key] = row[i] if i < len(row) else None
        if record:
            result.append(record)
    return result


def normalize_input_rows(raw_rows: list[dict[str, Any]], stopwords: set[str]) -> list[RawRow]:
    required_aliases = {
        "search_term": ("搜索词", "keyword", "关键词", "search_term"),
        "weekly_exposure": ("预估周曝光量", "weekly_exposure"),
        "impressions": ("展示量", "impressions"),
        "clicks": ("点击量", "clicks"),
    }

    def pick(row: dict[str, Any], aliases: tuple[str, ...]) -> Any:
        for alias in aliases:
            if alias in row:
                return row.get(alias)
        return None

    normalized: list[RawRow] = []
    for row in raw_rows:
        search_term = str(pick(row, required_aliases["search_term"]) or "").strip()
        if not search_term:
            continue
        tokens = split_words(search_term, stopwords=stopwords, to_lower=True)
        if not tokens:
            continue
        normalized.append(
            RawRow(
                search_term=search_term,
                weekly_exposure=_safe_float(pick(row, required_aliases["weekly_exposure"])),
                impressions=_safe_float(pick(row, required_aliases["impressions"])),
                clicks=_safe_float(pick(row, required_aliases["clicks"])),
                tokens=tokens,
                unique_tokens=set(tokens),
            )
        )

    if not normalized:
        raise RuntimeError("输入数据为空，或缺少可解析的“搜索词”列。")
    return normalized


def build_table2(rows: list[RawRow], tagger: Tagger, progress_callback=None) -> list[dict[str, Any]]:
    freq_counter = Counter()
    for row in rows:
        freq_counter.update(row.tokens)

    word_total_clicks = defaultdict(float)
    word_total_impressions = defaultdict(float)
    word_total_weekly_exposure = defaultdict(float)

    for row in rows:
        for word in row.unique_tokens:
            word_total_clicks[word] += row.clicks
            word_total_impressions[word] += row.impressions
            word_total_weekly_exposure[word] += row.weekly_exposure

    words = [w for w, _ in freq_counter.most_common()]
    tags: dict[str, TagResult] = tagger.classify(words, progress_callback=progress_callback)

    output: list[dict[str, Any]] = []
    for word in words:
        total = word_total_clicks[word]
        denominator = word_total_impressions[word]
        exposure_sum = word_total_weekly_exposure[word]

        weight: float | str = ""
        ratio: float | str = ""
        if denominator > 0 and total > 0:
            weight = round((total / denominator) * (4.3 * exposure_sum) * 10, 0)
            if total:
                ratio = weight / total

        tag = tags.get(word) or TagResult(tag_label="解析失败", reason=f"“{word}”，无缓存且未启用AI。")
        output.append(
            {
                "单词": word,
                "频次": int(freq_counter[word]),
                "打标": tag.tag_label,
                "mark": tag.reason,
                "Weight": weight,
                "Total": round(total, 6),
                "占比": ratio,
            }
        )

    output.sort(key=lambda x: (x["频次"], float(x["Total"] or 0)), reverse=True)
    return output


def write_output(rows: list[dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    columns = ["单词", "频次", "打标", "mark", "Weight", "Total", "占比"]
    if output_path.suffix.lower() == ".csv":
        with output_path.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=columns)
            writer.writeheader()
            writer.writerows(rows)
        return

    wb = Workbook()
    ws = wb.active
    ws.title = "表2结果"

    # Row 1 blank, row 2 summary, row 3 headers, row 4+ data.
    ws.append([])
    weight_sum = sum(float(r.get("Weight") or 0) for r in rows if isinstance(r.get("Weight"), (int, float)))
    total_sum = sum(float(r.get("Total") or 0) for r in rows if isinstance(r.get("Total"), (int, float)))
    ratio_sum = (weight_sum / total_sum) if total_sum else ""
    ws.append(["", "", "", "", weight_sum if weight_sum else "", total_sum if total_sum else "", ratio_sum])
    ws["G2"].number_format = "0%"

    ws.append(columns)
    for row in rows:
        ws.append([row.get(col, "") for col in columns])

    last_row = ws.max_row
    for idx in range(4, last_row + 1):
        cell_val = ws[f"G{idx}"].value
        if isinstance(cell_val, (int, float)):
            ws[f"G{idx}"].number_format = "0%"
    if last_row >= 4:
        ws.conditional_formatting.add(
            f"G4:G{last_row}",
            ColorScaleRule(
                start_type="num",
                start_value=0,
                start_color="00B050",
                mid_type="num",
                mid_value=0.5,
                mid_color="FFD966",
                end_type="num",
                end_value=1.5,
                end_color="FF0000",
            ),
        )

    # Auto-fit column widths based on rendered text length.
    for col_idx in range(1, len(columns) + 1):
        col_letter = get_column_letter(col_idx)
        max_len = 0
        for row_idx in range(1, ws.max_row + 1):
            value = ws[f"{col_letter}{row_idx}"].value
            text = "" if value is None else str(value)
            max_len = max(max_len, len(text))
        ws.column_dimensions[col_letter].width = min(80, max(8, max_len + 2))

    wb.save(output_path)

