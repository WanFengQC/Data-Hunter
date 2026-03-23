import asyncio
import json
import os
from pathlib import Path

from openai import OpenAI, AsyncOpenAI


# =========================
# Config
# =========================

def get_env_int(name, default):
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


DEEPSEEK_API_KEY = "sk-bMvckI4omhQZsd6eb"
BASE_URL = "http://127.0.0.1:8317/v1"
DEEPSEEK_BATCH_SIZE = get_env_int("DEEPSEEK_BATCH_SIZE", 500)
DEEPSEEK_WORKERS = get_env_int("DEEPSEEK_WORKERS", 0)
DEEPSEEK_MAX_WORKERS = get_env_int("DEEPSEEK_MAX_WORKERS", 64)
DEEPSEEK_AUTO_WORKERS = get_env_int("DEEPSEEK_AUTO_WORKERS", 24)
DEEPSEEK_DEBUG_PROMPT = str(os.getenv("DEEPSEEK_DEBUG_PROMPT", "0")).strip().lower()

BASE_DIR = Path(__file__).resolve().parent
CACHE_FILE = BASE_DIR / "object_category_cache.json"
DEEPSEEK_DEBUG_LOG_FILE = BASE_DIR / "deepseek_prompt_debug.log"

client = OpenAI(api_key=DEEPSEEK_API_KEY, base_url=BASE_URL)
async_client = AsyncOpenAI(api_key=DEEPSEEK_API_KEY, base_url=BASE_URL)


# =========================
# Cache
# =========================

def load_cache():
    if not CACHE_FILE.exists():
        return {}
    try:
        raw = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return {}
        return {str(k).strip().lower(): v for k, v in raw.items()}
    except Exception:
        return {}


def save_cache(cache):
    CACHE_FILE.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


def normalize_cache_key(word):
    return str(word).strip().lower()


def resolve_worker_count(total_batches, requested_workers, auto_workers, max_workers):
    if total_batches <= 0:
        return 1
    hard_cap = max(1, int(max_workers or 1))
    if requested_workers and requested_workers > 0:
        return max(1, min(int(requested_workers), total_batches, hard_cap))
    auto = max(1, int(auto_workers or 1))
    return max(1, min(auto, total_batches, hard_cap))


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


# =========================
# Batch Classify
# =========================

def build_classify_prompt(words):
    return f"""
你是一个玩具行业专家。

请判断这些英文单词属于哪一类，分类必须严格从以下选项中选择：
动物 / 交通工具 / 人物 / 食物 / 植物 / 形状 / 物体 / 抽象

要求：
1. 每个词必须有分类
2. 不要解释
3. 只返回 JSON
4. key 是英文词，value 是分类（中文）

示例：
{{
  "dog": "动物",
  "car": "交通工具",
  "apple": "食物",
  "love": "抽象"
}}

待判断词：
{words}
"""


def parse_classify_content(content):
    text = str(content or "").strip()
    if not text:
        return {}

    try:
        data = json.loads(text)
        if isinstance(data, dict):
            return data
    except Exception:
        pass

    result = {}
    for line in text.split("\n"):
        if ":" in line:
            k, v = line.split(":", 1)
            result[k.strip().strip('"')] = v.strip().strip('"').replace(",", "")
    return result


async def classify_batch_async(words):
    if not words:
        return {}

    prompt = build_classify_prompt(words)
    system_prompt = "你是分类助手"
    debug_deepseek_prompt("object_category", "gpt-5.4", system_prompt, prompt, words)

    try:
        resp = await async_client.chat.completions.create(
            model="gpt-5.4",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt},
            ],
            temperature=0,
        )
        content = (resp.choices[0].message.content or "").strip()
        parsed = parse_classify_content(content)
        debug_deepseek_response(
            "object_category",
            raw_content=content,
            parsed_payload=parsed if isinstance(parsed, dict) else None,
            normalized_payload=parsed,
        )
        return parsed
    except Exception as exc:
        print("DeepSeek错误:", exc)
        return {}


def classify_with_cache(words):
    cache = load_cache()

    result = {}
    need = []

    for w in words:
        cache_key = normalize_cache_key(w)
        if cache_key in cache:
            result[w] = cache[cache_key]
        else:
            need.append(w)

    print("DeepSeek待处理:", len(need))

    batch_size = max(1, DEEPSEEK_BATCH_SIZE)
    batches = [need[i : i + batch_size] for i in range(0, len(need), batch_size)]
    if not batches:
        return result

    workers = resolve_worker_count(
        total_batches=len(batches),
        requested_workers=DEEPSEEK_WORKERS,
        auto_workers=DEEPSEEK_AUTO_WORKERS,
        max_workers=DEEPSEEK_MAX_WORKERS,
    )
    print("DeepSeek workers:", workers, "batch_size:", batch_size, "batches:", len(batches))

    async def _run_batches():
        done_count = 0
        sem = asyncio.Semaphore(max(1, workers))

        async def _run_one(batch):
            async with sem:
                return batch, await classify_batch_async(batch)

        tasks = [asyncio.create_task(_run_one(batch)) for batch in batches]

        nonlocal result, cache
        for task in asyncio.as_completed(tasks):
            batch, res = await task
            normalized_res = {normalize_cache_key(k): v for k, v in (res or {}).items()}

            for w in batch:
                cache_key = normalize_cache_key(w)
                c = normalized_res.get(cache_key, "物体")
                result[w] = c
                cache[cache_key] = c

            done_count += len(batch)
            print("DeepSeek进度:", done_count, "/", len(need))

    try:
        asyncio.run(_run_batches())
    except RuntimeError as exc:
        # Fallback for environments already running an event loop.
        print("DeepSeek async loop warning:", exc)
        for batch in batches:
            res = client.chat.completions.create(
                model="gpt-5.4",
                messages=[
                    {"role": "system", "content": "你是分类助手"},
                    {"role": "user", "content": build_classify_prompt(batch)},
                ],
                temperature=0,
            )
            content = (res.choices[0].message.content or "").strip()
            parsed = parse_classify_content(content)
            normalized_res = {normalize_cache_key(k): v for k, v in (parsed or {}).items()}
            for w in batch:
                cache_key = normalize_cache_key(w)
                c = normalized_res.get(cache_key, "物体")
                result[w] = c
                cache[cache_key] = c

    save_cache(cache)
    return result
