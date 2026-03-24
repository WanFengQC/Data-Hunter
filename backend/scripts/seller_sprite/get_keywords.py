import asyncio
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import nltk
import pandas as pd
from deep_translator import GoogleTranslator
from nltk import pos_tag
from nltk.corpus import wordnet as wn

from deepseek_classify_batch import async_client as deepseek_async_client

# =========================
# 配置
# =========================

def get_env_int(name, default):
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def get_env_float(name, default):
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def safe_float(value, default=0.0):
    if value is None:
        return default
    if isinstance(value, (int, float)):
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    text = str(value).strip().replace(",", "")
    if not text:
        return default
    try:
        return float(text)
    except (TypeError, ValueError):
        return default


def resolve_worker_count(total_batches, requested_workers, auto_workers, max_workers):
    if total_batches <= 0:
        return 1

    hard_cap = max(1, int(max_workers or 1))
    if requested_workers and requested_workers > 0:
        return max(1, min(int(requested_workers), total_batches, hard_cap))

    auto = max(1, int(auto_workers or 1))
    return max(1, min(auto, total_batches, hard_cap))


BASE_DIR = Path(__file__).resolve().parent
ARA_BASE_DIR = Path(os.getenv("ARA_BASE_DIR", r"D:\ara"))
INPUT_DIR = ARA_BASE_DIR / "ara_202503"
OUTPUT_FILE = "word_frequency_analysis_202503.csv"

TRANSLATION_CACHE_FILE = BASE_DIR / "translation_cache.json"
OBJECT_CACHE_FILE = BASE_DIR / "object_category_cache.json"
TAG_REASON_CACHE_FILE = BASE_DIR / "tag_reason_cache.json"
DEFAULT_TAG_KB_FILE = Path(r"C:\Users\EDY\Downloads\毛绒玩具品类打标知识库.txt")

BATCH_SIZE = get_env_int("TRANSLATE_BATCH_SIZE", 100)
TRANSLATE_WORKERS = get_env_int("TRANSLATE_WORKERS", 4)
TRANSLATE_SLEEP_SECONDS = get_env_float("TRANSLATE_SLEEP_SECONDS", 0)
DEEPSEEK_BATCH_SIZE = get_env_int("DEEPSEEK_BATCH_SIZE", 500)
DEEPSEEK_WORKERS = get_env_int("DEEPSEEK_WORKERS", 0)
DEEPSEEK_MAX_WORKERS = get_env_int("DEEPSEEK_MAX_WORKERS", 64)
DEEPSEEK_AUTO_WORKERS = get_env_int("DEEPSEEK_AUTO_WORKERS", 24)
DEEPSEEK_TAG_BATCH_SIZE = get_env_int("DEEPSEEK_TAG_BATCH_SIZE", 100)
DEEPSEEK_TAG_MAX_WORKERS = get_env_int("DEEPSEEK_TAG_MAX_WORKERS", 128)
DEEPSEEK_TAG_AUTO_WORKERS = get_env_int("DEEPSEEK_TAG_AUTO_WORKERS", 48)
DEEPSEEK_TAG_CACHE_SAVE_EVERY = get_env_int("DEEPSEEK_TAG_CACHE_SAVE_EVERY", 5)
DEEPSEEK_DEBUG_PROMPT = str(os.getenv("DEEPSEEK_DEBUG_PROMPT", "0")).strip().lower()
DEEPSEEK_DEBUG_LOG_FILE = BASE_DIR / "deepseek_prompt_debug.log"
USE_LOCAL_TAG_KB = str(os.getenv("USE_LOCAL_TAG_KB", "0")).strip().lower() in {"1", "true", "yes", "y", "on"}
TO_LOWER = True
REMOVE_STOPWORDS = True

STOPWORDS = {
    "",
    "a",
    "an",
    "the",
    "and",
    "or",
    "for",
    "with",
    "of",
    "to",
    "in",
    "on",
    "at",
    "by",
}

TAG_LABELS = [
    "1核心词",
    "2外形",
    "3属性",
    "4痛点",
    "5规格",
    "6受众",
    "7场景",
    "8品牌",
    "无效词",
]

TAG_LABEL_BY_INDEX = {
    "1": "1核心词",
    "2": "2外形",
    "3": "3属性",
    "4": "4痛点",
    "5": "5规格",
    "6": "6受众",
    "7": "7场景",
    "8": "8品牌",
}

TAG_LABEL_ALIASES = {
    "1核心词": "1核心词",
    "核心词": "1核心词",
    "核心": "1核心词",
    "2外形": "2外形",
    "外形": "2外形",
    "形象": "2外形",
    "物种": "2外形",
    "3属性": "3属性",
    "属性": "3属性",
    "3工艺": "3属性",
    "工艺": "3属性",
    "4痛点": "4痛点",
    "痛点": "4痛点",
    "诉求": "4痛点",
    "5规格": "5规格",
    "规格": "5规格",
    "尺寸": "5规格",
    "6受众": "6受众",
    "受众": "6受众",
    "人群": "6受众",
    "7场景": "7场景",
    "场景": "7场景",
    "8品牌": "8品牌",
    "品牌": "8品牌",
    "ip": "8品牌",
    "无效词": "无效词",
    "无效": "无效词",
    "噪音词": "无效词",
    "invalid": "无效词",
    "noise": "无效词",
}

TAG_REASON_HINTS = {
    "1核心词": "类目核心词，直接定义产品大类。",
    "2外形": "主体形象词，决定视觉与物种识别。",
    "3属性": "功能或工艺属性词，体现差异化卖点。",
    "4痛点": "需求痛点词，直接承接购买动机。",
    "5规格": "尺寸或重量规格词，用于量级筛选。",
    "6受众": "目标使用人群词，限定受众范围。",
    "7场景": "使用或送礼场景词，指向消费节点。",
    "8品牌": "品牌或IP词，指向特定品牌资产。",
    "无效词": "语义不完整或业务价值低，按无效词处理。",
}

_TAG_KB_CACHE: dict[str, dict[str, str]] | None = None
LOW_CONFIDENCE_INVALID_REASON = "语义不完整或业务价值低，按无效词处理。"

# =========================
# NLTK
# =========================


def ensure_nltk():
    nltk.download("punkt", quiet=True)
    nltk.download("averaged_perceptron_tagger", quiet=True)
    nltk.download("wordnet", quiet=True)
    nltk.download("omw-1.4", quiet=True)


# =========================
# 拆词
# =========================


def split_words(text):
    if not isinstance(text, str):
        return []

    if TO_LOWER:
        text = text.lower()

    words = re.findall(r"[a-z0-9]+", text)
    result = []

    for word in words:
        if REMOVE_STOPWORDS and word in STOPWORDS:
            continue
        if word.isdigit():
            continue
        if any(char.isdigit() for char in word):
            continue
        if len(word) <= 1:
            continue

        result.append(word)

    return result


# =========================
# 词性
# =========================


def get_pos(word):
    try:
        tag = pos_tag([word])[0][1]
    except Exception:
        return "", "未知"

    if tag.startswith("NN"):
        return tag, "名词"
    if tag.startswith("JJ"):
        return tag, "形容词"
    if tag.startswith("VB"):
        return tag, "动词"
    if tag.startswith("RB"):
        return tag, "副词"

    return tag, "其他"


# =========================
# WordNet 分类
# =========================

MAP = {
    "animal": "动物",
    "vehicle": "交通工具",
    "person": "人物",
    "food": "食物",
    "plant": "植物",
    "artifact": "物体",
    "shape": "形状",
}

CANONICAL_CATEGORIES = [
    "交通工具",
    "非名词",
    "动物",
    "人物",
    "食物",
    "植物",
    "形状",
    "物体",
    "抽象",
]

CATEGORY_ALIASES = {
    "animal": "动物",
    "vehicle": "交通工具",
    "person": "人物",
    "food": "食物",
    "plant": "植物",
    "shape": "形状",
    "artifact": "物体",
    "object": "物体",
    "abstract": "抽象",
    "nonnoun": "非名词",
    "non_noun": "非名词",
    "noun": "物体",
}


def wordnet_classify(word):
    synsets = wn.synsets(word, pos=wn.NOUN)[:1]

    if not synsets:
        return None

    for syn in synsets:
        for path in syn.hypernym_paths():
            for node in path:
                name = node.name().split(".")[0]
                if name in MAP:
                    return MAP[name]

    for syn in synsets:
        for path in syn.hypernym_paths():
            for node in path:
                if node.name().startswith("physical_entity"):
                    return "物体"

    return None


def normalize_category_label(value, fallback="物体"):
    text = str(value or "").strip()
    if not text:
        return fallback

    text = text.replace("\ufeff", "").replace("\\", "")
    text = re.sub(r'["\'`“”‘’＂]', "", text)
    text = text.strip().strip(",:;，。：；")
    text = re.sub(r"\s+", "", text)
    if not text:
        return fallback

    low = text.lower()
    if low in CATEGORY_ALIASES:
        return CATEGORY_ALIASES[low]

    for label in CANONICAL_CATEGORIES:
        if label in text:
            return label

    return fallback


# =========================
# 缓存
# =========================


def load_cache(file_path):
    if not file_path.exists():
        return {}

    try:
        raw = json.loads(file_path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return {}
        return {str(k).strip().lower(): v for k, v in raw.items()}
    except Exception:
        return {}


def save_cache(cache, file_path):
    file_path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


def normalize_cache_key(word):
    return str(word).strip().lower()


def is_true_flag(value):
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def debug_deepseek_prompt(scope, model, system_prompt, user_prompt, words):
    if not is_true_flag(DEEPSEEK_DEBUG_PROMPT):
        return

    payload = {
        "scope": scope,
        "model": model,
        "words_count": len(words or []),
        "words": list(words or []),
        "system_prompt": system_prompt,
        "user_prompt": user_prompt,
    }
    message = json.dumps(payload, ensure_ascii=False, indent=2)

    print("\n===== DeepSeek Debug Prompt Begin =====")
    print(message)
    print("===== DeepSeek Debug Prompt End =====\n")

    try:
        with DEEPSEEK_DEBUG_LOG_FILE.open("a", encoding="utf-8") as fp:
            fp.write(message)
            fp.write("\n\n")
    except Exception:
        pass


def debug_deepseek_response(scope, raw_content, parsed_payload=None, normalized_payload=None):
    if not is_true_flag(DEEPSEEK_DEBUG_PROMPT):
        return

    payload = {
        "scope": scope,
        "raw_content": raw_content,
        "parsed_payload": parsed_payload,
        "normalized_payload": normalized_payload,
    }
    message = json.dumps(payload, ensure_ascii=False, indent=2)

    print("\n===== DeepSeek Debug Response Begin =====")
    print(message)
    print("===== DeepSeek Debug Response End =====\n")

    try:
        with DEEPSEEK_DEBUG_LOG_FILE.open("a", encoding="utf-8") as fp:
            fp.write(message)
            fp.write("\n\n")
    except Exception:
        pass


def resolve_tag_kb_file():
    env_path = str(os.getenv("PLUSH_TAG_KB_FILE", "")).strip()
    candidates = []
    if env_path:
        candidates.append(Path(env_path))
    candidates.append(DEFAULT_TAG_KB_FILE)
    candidates.append(BASE_DIR / "毛绒玩具品类打标知识库.txt")

    for path in candidates:
        if path.exists():
            return path

    return candidates[0]


def normalize_tag_label(value):
    text = str(value or "").strip()
    if not text:
        return "无效词"

    compact = re.sub(r"\s+", "", text).lower()
    alias_hit = TAG_LABEL_ALIASES.get(compact)
    if alias_hit:
        return alias_hit

    for idx, label in TAG_LABEL_BY_INDEX.items():
        if compact == idx or compact.startswith(idx):
            return label

    if "无效" in text or "noise" in compact or "invalid" in compact:
        return "无效词"

    return "无效词"


def parse_tag_kb():
    global _TAG_KB_CACHE
    if _TAG_KB_CACHE is not None:
        return _TAG_KB_CACHE

    file_path = resolve_tag_kb_file()
    if not file_path.exists():
        _TAG_KB_CACHE = {}
        return _TAG_KB_CACHE

    try:
        lines = file_path.read_text(encoding="utf-8").splitlines()
    except Exception:
        _TAG_KB_CACHE = {}
        return _TAG_KB_CACHE

    kb_map = {}
    current_label = ""
    current_terms = []
    current_reason = ""
    current_related = ""

    def flush_entry():
        if not current_label or not current_terms:
            return
        for term in current_terms:
            if term and term not in kb_map:
                kb_map[term] = {
                    "标签": current_label,
                    "原因备注": current_reason,
                    "关联词组": current_related,
                }

    def split_after_colon(text):
        for sep in ("：", ":"):
            if sep in text:
                return text.split(sep, 1)[1].strip()
        return ""

    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue

        if line.startswith("# "):
            flush_entry()
            current_terms = []
            current_reason = ""
            current_related = ""

            title = line[2:].strip()
            title_compact = title.replace(" ", "")
            head_match = re.match(r"([1-8])", title_compact)
            if head_match:
                current_label = TAG_LABEL_BY_INDEX.get(head_match.group(1), "")
            elif ("噪音" in title) or ("待定" in title) or ("无效" in title):
                current_label = "无效词"
            else:
                current_label = ""
            continue

        if line.startswith("### ["):
            flush_entry()
            current_reason = ""
            current_related = ""
            bracket_match = re.search(r"\[(.*?)\]", line)
            if bracket_match:
                raw_terms = re.split(r"[\\/、,，]", bracket_match.group(1))
                current_terms = [normalize_cache_key(term) for term in raw_terms if normalize_cache_key(term)]
            else:
                current_terms = []
            continue

        if "原因备注" in line:
            maybe = split_after_colon(line)
            if maybe:
                current_reason = maybe
            continue

        if "关联词组" in line:
            maybe = split_after_colon(line)
            if maybe:
                current_related = maybe
            continue

        if "处理动作" in line:
            maybe = split_after_colon(line)
            if maybe:
                current_reason = maybe
            continue

    flush_entry()
    _TAG_KB_CACHE = kb_map
    return _TAG_KB_CACHE


def clean_reason_text(value):
    text = str(value or "").strip()
    if not text:
        return ""
    text = text.strip("`").strip()
    text = re.sub(r"\s+", " ", text)
    text = text.lstrip("，。；：")
    return text


def format_tag_reason(word, zh_map, label, reason_text="", kb_record=None):
    meaning = str((zh_map or {}).get(word, "")).strip() or word

    if kb_record:
        parts = []
        reason_note = clean_reason_text(kb_record.get("原因备注", ""))
        related = clean_reason_text(kb_record.get("关联词组", ""))
        if reason_note:
            parts.append(reason_note)
        if related:
            parts.append(f"关联词组：{related}")
        kb_reason = "；".join(parts) if parts else "命中知识库规则。"
        return f"“{meaning}”，知识库数据：{kb_reason}"

    core = clean_reason_text(reason_text)
    if not core:
        core = TAG_REASON_HINTS.get(label, TAG_REASON_HINTS["无效词"])

    if core.startswith("“") and "”，" in core:
        return core
    return f"“{meaning}”，{core}"


def parse_deepseek_json(content):
    body = str(content or "").strip()
    if not body:
        return {}

    if body.startswith("```"):
        lines = body.splitlines()
        if len(lines) >= 3:
            body = "\n".join(lines[1:-1]).strip()

    candidates = [body]
    left = body.find("{")
    right = body.rfind("}")
    if left >= 0 and right > left:
        candidates.append(body[left : right + 1])

    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
            if isinstance(parsed, dict):
                return parsed
            if isinstance(parsed, list):
                return {"rows": parsed}
        except Exception:
            continue

    return {}


def _decode_json_like_string(value):
    text = str(value or "")
    try:
        return json.loads(f"\"{text}\"")
    except Exception:
        return text.replace('\\"', '"').replace("\\n", "\n").replace("\\t", "\t")


def parse_tag_rows_loose(content):
    body = str(content or "")
    if not body:
        return {}

    rows = {}

    candidates = [body]
    # 有些模型会把整段 JSON 再转义一次，补一份反转义候选。
    if '\\"' in body:
        candidates.append(body.replace('\\"', '"').replace("\\n", "\n").replace("\\t", "\t"))

    for text in candidates:
        # 先抽取每个对象块，即便整段被截断，也能拿到前半段完整对象。
        object_blocks = re.findall(r"\{[^{}]*\}", text, flags=re.S)
        for block in object_blocks:
            normalized_block = re.sub(r'"关\s*键\s*词"', '"关键词"', block)
            normalized_block = re.sub(r'"对\s*应\s*标\s*签"', '"对应标签"', normalized_block)
            normalized_block = re.sub(r'"原\s*因\s*备\s*注"', '"原因备注"', normalized_block)

            keyword = ""
            label = ""
            reason = ""

            try:
                data = json.loads(normalized_block)
                if isinstance(data, dict):
                    keyword = str(
                        data.get("关键词") or data.get("keyword") or data.get("词根") or data.get("word") or ""
                    ).strip()
                    label = str(data.get("对应标签") or data.get("标签") or data.get("label") or "").strip()
                    reason = str(data.get("原因备注") or data.get("原因") or data.get("reason") or "").strip()
            except Exception:
                # JSON 失败则按字段回退抽取。
                k_match = re.search(r'"关键词"\s*:\s*"(?P<v>(?:\\.|[^"\\])*)"', normalized_block, flags=re.S)
                l_match = re.search(r'"对应标签"\s*:\s*"(?P<v>(?:\\.|[^"\\])*)"', normalized_block, flags=re.S)
                r_match = re.search(r'"原因备注"\s*:\s*"(?P<v>(?:\\.|[^"\\])*)"', normalized_block, flags=re.S)
                keyword = _decode_json_like_string(k_match.group("v")) if k_match else ""
                label = _decode_json_like_string(l_match.group("v")) if l_match else ""
                reason = _decode_json_like_string(r_match.group("v")) if r_match else ""

            keyword_key = normalize_cache_key(keyword)
            if not keyword_key:
                continue
            rows[keyword_key] = {"标签": label, "原因": reason}

        if rows:
            break

    return rows


def normalize_tag_entry(word, entry, zh_map):
    label_raw = ""
    reason_raw = ""

    if isinstance(entry, dict):
        label_raw = (
            entry.get("标签")
            or entry.get("label")
            or entry.get("对应标签")
            or entry.get("tag")
            or ""
        )
        reason_raw = (
            entry.get("原因")
            or entry.get("reason")
            or entry.get("原因备注")
            or ""
        )
    elif isinstance(entry, str):
        label_raw = entry

    label = normalize_tag_label(label_raw)
    reason = format_tag_reason(word, zh_map, label, reason_raw)
    return {"标签": label, "原因": reason}


def build_parse_failed_entry(word, zh_map):
    meaning = str((zh_map or {}).get(word, "")).strip() or word
    return {"标签": "解析失败", "原因": f"“{meaning}”，DeepSeek返回截断或格式异常，需重试。"}


def is_low_confidence_fallback_entry(entry):
    if not isinstance(entry, dict):
        return False
    label = str(entry.get("标签", "")).strip()
    reason = str(entry.get("原因", "")).strip()
    return label == "无效词" and LOW_CONFIDENCE_INVALID_REASON in reason and "知识库数据" not in reason


async def classify_tag_reason_batch_async(words):
    if not words:
        return {}

    words_text = "\n".join(words)
    prompt = f"""
# 亚马逊毛绒玩具词根自动打标专家 (Prompt V1.1)

## ⚖️ 角色定义
- Role: 资深亚马逊数据分析师 & 毛绒玩具品类高级PM。
- Core Value: 坚持“效率优先、数据驱动”，拒绝语义幻觉。通过建立精准的8维坐标系，将非结构化的词根转化为可量化的业务指标。
- Tone: 极度理性、策略导向。输出优先级：准确率 > 格式对齐 > 专业备注。

## 📥 输入数据
1. 数据格式：以换行符分隔的单个词根列表（Root Words）。
2. 强制约束：
- 必须严格保持原始词根的输入顺序，严禁擅自打乱或去重。
- 严禁基于主观臆测补全词组中未出现的标签维度。

## 🚦 工作流：三阶段审计逻辑
### PHASE 1: 知识库优先检索 (KB Priority)
在处理任何词根前，必须优先检索挂载的《毛绒玩具品类打标知识库.txt》。若命中库中定义的拦截逻辑，必须强制覆盖后续通用规则。

### PHASE 2: 8维坐标系映射 (Tagging Logic)
若知识库未拦截，则按以下优先级进行唯一性归类（1个词根仅对应1个标签）：
1. 1核心词：产品的核心名称或类别名（如：stuffed, plush, animal, toy, pillow）。
2. 2外形：视觉主体形象、物种名（如：bear, panda, cat, unicorn, dinosaur）。
3. 3属性：产品的物理特性、改性工艺或硬件卖点（如：weighted, heated, warmable, cooling）。
4. 4痛点：用户主观渴望解决的情绪、生理问题或购买诉求（如：anxiety, sleep, sensory, stress）。
5. 5规格：描述产品的大小、重量或厚度规格（如：giant, large, 5lbs, mini）。
6. 6受众：描述产品的使用人群（如：adults, kids, toddler, baby）。
7. 7场景：描述产品的使用时机或赠送节点（如：gift, birthday, valentine）。
8. 8品牌：描述特定的品牌名称或受保护的IP（如：warmies, disney, squishmallow）。

### PHASE 3: 专业归因与生成
站在10年跨境PM视角，为每个标签简述为何选择该标签。

## 🗂️ 输出标准
1. 关键词顺序必须与输入完全一致。
2. 无法识别的词根标签标注为“无效词”，备注写明原因。
3. 原因备注必须遵循“中文意思+简述”格式，例如：“填充”，类目大词。
4. 若命中知识库，备注格式为：“中文意思”，知识库数据：原因备注+关联词组。

## ⚠️ 约束原则
1. 顺序一致性：严禁错位。
2. 唯一性：一个词根只能对应一个标签。
3. 拒绝废话：备注直击业务定性，不要啰嗦。

## 🔡 返回格式（程序接收专用）
只返回 JSON，不要 Markdown，不要代码块，不要额外说明。
格式如下：
{{
  "rows": [
    {{"关键词": "stuffed", "对应标签": "1核心词", "原因备注": "“填充”，类目大词。"}},
    {{"关键词": "weighted", "对应标签": "3属性", "原因备注": "“加重”，核心差异化卖点。"}}
  ]
}}

标签值必须是以下之一：
1核心词 / 2外形 / 3属性 / 4痛点 / 5规格 / 6受众 / 7场景 / 8品牌 / 无效词

## Root Words（按行输入）
{words_text}
"""

    system_prompt = "你是严格的词根打标助手，只输出JSON。"
    debug_deepseek_prompt("tag_reason", "gpt-5.4", system_prompt, prompt, words)

    try:
        response = await deepseek_async_client.chat.completions.create(
            model="gpt-5.4",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt},
            ],
            temperature=0,
        )
    except Exception as exc:
        print("DeepSeek tag error:", exc)
        return {}

    content = (response.choices[0].message.content or "").strip()
    parsed = parse_deepseek_json(content)
    normalized_result = {}
    rows_map = {}

    if isinstance(parsed, dict):
        rows = parsed.get("rows")
        if isinstance(rows, list):
            for row in rows:
                if not isinstance(row, dict):
                    continue
                keyword = str(
                    row.get("关键词")
                    or row.get("keyword")
                    or row.get("词根")
                    or row.get("word")
                    or ""
                ).strip()
                if not keyword:
                    continue
                rows_map[normalize_cache_key(keyword)] = {
                    "标签": row.get("对应标签") or row.get("标签") or row.get("label") or "",
                    "原因": row.get("原因备注") or row.get("原因") or row.get("reason") or "",
                }

    if not rows_map:
        rows_map = parse_tag_rows_loose(content)

    for word in words:
        key_lower = word.lower()
        payload_entry: Any = rows_map.get(key_lower)
        if payload_entry is not None:
            normalized_result[word] = payload_entry
            continue

        payload_entry = parsed.get(word)
        if payload_entry is None:
            payload_entry = parsed.get(key_lower)
        if payload_entry is None and isinstance(parsed, dict):
            for k, v in parsed.items():
                if str(k).strip().lower() == key_lower:
                    payload_entry = v
                    break

        normalized_result[word] = payload_entry if payload_entry is not None else {}

    debug_deepseek_response(
        "tag_reason",
        raw_content=content,
        parsed_payload=parsed,
        normalized_payload=normalized_result,
    )

    return normalized_result


def classify_tag_reason(words, zh_map):
    kb_map = parse_tag_kb() if USE_LOCAL_TAG_KB else {}
    cache = load_cache(TAG_REASON_CACHE_FILE)
    if not USE_LOCAL_TAG_KB:
        print("本地知识库已忽略：仅使用缓存+DeepSeek")

    # 清理历史“解析失败被误写为无效词”的低置信缓存
    dirty_keys = []
    for ck, cv in list(cache.items()):
        if isinstance(cv, dict) and is_low_confidence_fallback_entry(cv):
            dirty_keys.append(ck)
    for ck in dirty_keys:
        cache.pop(ck, None)
    if dirty_keys:
        print("已清理低置信tag缓存:", len(dirty_keys))

    result = {}
    need_ai = []

    for word in words:
        cache_key = normalize_cache_key(word)

        if kb_map:
            kb_record = kb_map.get(cache_key)
            if kb_record:
                label = normalize_tag_label(kb_record.get("标签", "无效词"))
                reason = format_tag_reason(word, zh_map, label, kb_record=kb_record)
                result[word] = {"标签": label, "原因": reason}
                cache[cache_key] = result[word]
                continue

        cached_entry = cache.get(cache_key)
        if isinstance(cached_entry, (dict, str)):
            normalized_cached = normalize_tag_entry(word, cached_entry, zh_map)
            if is_low_confidence_fallback_entry(normalized_cached):
                need_ai.append(word)
            else:
                result[word] = normalized_cached
                cache[cache_key] = result[word]
            continue

        need_ai.append(word)

    print("DeepSeek tag pending:", len(need_ai))

    if need_ai:
        batch_size = max(1, DEEPSEEK_TAG_BATCH_SIZE)
        batches = [need_ai[i : i + batch_size] for i in range(0, len(need_ai), batch_size)]
        workers = resolve_worker_count(
            total_batches=len(batches),
            requested_workers=DEEPSEEK_WORKERS,
            auto_workers=DEEPSEEK_TAG_AUTO_WORKERS,
            max_workers=DEEPSEEK_TAG_MAX_WORKERS,
        )
        print("DeepSeek tag workers:", workers, "batch_size:", batch_size, "batches:", len(batches))
        async def _run_tag_batches():
            done_count = 0
            done_batches = 0
            sem = asyncio.Semaphore(max(1, workers))
            missing_words_all = []
            missing_seen = set()

            async def _run_one(batch):
                async with sem:
                    try:
                        batch_result = await classify_tag_reason_batch_async(batch) or {}
                    except Exception as exc:
                        print("DeepSeek tag error:", exc)
                        batch_result = {}
                    return batch, batch_result

            tasks = [asyncio.create_task(_run_one(batch)) for batch in batches]

            for task in asyncio.as_completed(tasks):
                batch, batch_result = await task

                for word in batch:
                    cache_key = normalize_cache_key(word)
                    raw_entry = batch_result.get(word, {})
                    if not raw_entry:
                        if word not in missing_seen:
                            missing_seen.add(word)
                            missing_words_all.append(word)
                        continue

                    normalized_entry = normalize_tag_entry(word, raw_entry, zh_map)
                    result[word] = normalized_entry
                    cache[cache_key] = normalized_entry

                done_count += len(batch)
                done_batches += 1
                print("DeepSeek tag progress:", done_count, "/", len(need_ai))

                if done_batches % max(1, DEEPSEEK_TAG_CACHE_SAVE_EVERY) == 0:
                    save_cache(cache, TAG_REASON_CACHE_FILE)
                    print("DeepSeek tag cache checkpoint:", done_batches, "/", len(batches))

            if missing_words_all:
                print("DeepSeek tag final retry missing:", len(missing_words_all))
                final_retry_result = await classify_tag_reason_batch_async(missing_words_all) or {}
                recovered_count = 0
                for word in missing_words_all:
                    cache_key = normalize_cache_key(word)
                    raw_entry = final_retry_result.get(word, {})
                    if not raw_entry:
                        result[word] = build_parse_failed_entry(word, zh_map)
                        continue

                    normalized_entry = normalize_tag_entry(word, raw_entry, zh_map)
                    result[word] = normalized_entry
                    cache[cache_key] = normalized_entry
                    recovered_count += 1
                print("DeepSeek tag final retry recovered:", recovered_count, "/", len(missing_words_all))

        asyncio.run(_run_tag_batches())

    save_cache(cache, TAG_REASON_CACHE_FILE)
    return result


# =========================
# 翻译
# =========================


def translate(words):
    cache = load_cache(TRANSLATION_CACHE_FILE)

    result = {}
    need = []

    for word in words:
        cache_key = normalize_cache_key(word)
        if cache_key in cache:
            result[word] = cache[cache_key]
        else:
            need.append(word)

    print("翻译待处理:", len(need))

    def translate_batch_worker(batch_words):
        translator = GoogleTranslator(source="en", target="zh-CN")
        try:
            translated = translator.translate_batch(batch_words)
        except Exception:
            translated = [""] * len(batch_words)

        if not isinstance(translated, list):
            translated = [""] * len(batch_words)
        if len(translated) < len(batch_words):
            translated.extend([""] * (len(batch_words) - len(translated)))

        return {word: zh for word, zh in zip(batch_words, translated)}

    batches = [need[i : i + BATCH_SIZE] for i in range(0, len(need), BATCH_SIZE)]
    workers = max(1, TRANSLATE_WORKERS)
    done_count = 0

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_batch = {executor.submit(translate_batch_worker, batch): batch for batch in batches}

        for future in as_completed(future_to_batch):
            batch = future_to_batch[future]
            try:
                batch_map = future.result()
            except Exception:
                batch_map = {word: "" for word in batch}

            for word in batch:
                cache_key = normalize_cache_key(word)
                zh = batch_map.get(word, "")
                result[word] = zh
                cache[cache_key] = zh

            done_count += len(batch)
            save_cache(cache, TRANSLATION_CACHE_FILE)
            print("翻译进度:", done_count, "/", len(need))

            if TRANSLATE_SLEEP_SECONDS > 0:
                time.sleep(TRANSLATE_SLEEP_SECONDS)

    return result


# =========================
# 主程序
# =========================


def main():
    ensure_nltk()

    files = list(INPUT_DIR.glob("*.html")) + list(INPUT_DIR.glob("*.json"))
    rows = []

    for file_path in files:
        try:
            data = json.loads(file_path.read_text(encoding="utf-8"))
            if data.get("code") != "OK":
                continue
            items = data["data"]["items"]
        except Exception:
            continue

        for item in items:
            rows.append(
                {
                    "keyword": item.get("keyword", ""),
                    "searches": safe_float(item.get("searches"), 0.0),
                }
            )

    df = pd.DataFrame(rows)
    print("keyword数:", len(df))

    freq = Counter()
    searches = defaultdict(float)
    coverage = defaultdict(int)

    for _, row in df.iterrows():
        words = split_words(row["keyword"])
        freq.update(words)

        for word in set(words):
            searches[word] += row["searches"]
            coverage[word] += 1

    total_freq = sum(freq.values())
    total_kw = len(df)
    words = [word for word, _ in freq.most_common()]

    pos_zh = {}

    for word in words:
        tag, zh = get_pos(word)
        pos_zh[word] = zh

    zh_map = translate(words)
    tag_reason = classify_tag_reason(words, zh_map)

    result_rows = []

    for word in words:
        frequency = freq[word]

        result_rows.append(
            {
                "word": word,
                "word_zh": zh_map.get(word, ""),
                "pos": pos_zh[word],
                "标签": tag_reason.get(word, {}).get("标签", "解析失败"),
                "原因": tag_reason.get(word, {}).get("原因", build_parse_failed_entry(word, zh_map).get("原因")),
                "freq": frequency,
                "freq_ratio": round(frequency / total_freq, 6),
                "freq_ratio_percent": f"{frequency / total_freq:.2%}",
                "coverage": coverage[word],
                "coverage_percent": f"{coverage[word] / total_kw:.2%}",
                "total_searches": round(searches[word], 2),
            }
        )

    output_columns = [
        "word",
        "word_zh",
        "pos",
        "标签",
        "原因",
        "freq",
        "freq_ratio",
        "freq_ratio_percent",
        "coverage",
        "coverage_percent",
        "total_searches",
    ]
    output_df = pd.DataFrame(result_rows, columns=output_columns)
    if not output_df.empty:
        output_df = output_df.sort_values(by=["freq", "total_searches"], ascending=[False, False])
    output_df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

    print("完成:", OUTPUT_FILE)
    print(output_df.head(20))


if __name__ == "__main__":
    main()
