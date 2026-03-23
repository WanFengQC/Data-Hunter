import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from openai import OpenAI


# =========================
# 配置
# =========================

def get_env_int(name, default):
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


DEEPSEEK_API_KEY = "sk-f93c5cc7df984f2ea501017091e1e633"
BASE_URL = "https://api.deepseek.com"
DEEPSEEK_BATCH_SIZE = get_env_int("DEEPSEEK_BATCH_SIZE", 500)
DEEPSEEK_WORKERS = get_env_int("DEEPSEEK_WORKERS", 0)
DEEPSEEK_DEBUG_PROMPT = str(os.getenv("DEEPSEEK_DEBUG_PROMPT", "0")).strip().lower()

BASE_DIR = Path(__file__).resolve().parent
CACHE_FILE = BASE_DIR / "object_category_cache.json"
DEEPSEEK_DEBUG_LOG_FILE = BASE_DIR / "deepseek_prompt_debug.log"

client = OpenAI(
    api_key=DEEPSEEK_API_KEY,
    base_url=BASE_URL
)


# =========================
# 缓存
# =========================

def load_cache():

    if not CACHE_FILE.exists():
        return {}

    try:
        raw = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return {}
        return {str(k).strip().lower(): v for k, v in raw.items()}
    except:
        return {}


def save_cache(cache):

    CACHE_FILE.write_text(
        json.dumps(cache, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )


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


# =========================
# DeepSeek批量分类
# =========================

def classify_batch(words):

    if not words:
        return {}

    prompt = f"""
你是一个玩具行业专家。

请判断这些英文单词是否属于“可以做成玩具形状的具体物体”，并分类。

分类必须严格从以下选项中选择：

动物
交通工具
人物
食物
植物
形状
物体
抽象

要求：
1. 每个词必须有分类
2. 不要解释
3. 只返回JSON
4. key是英文词，value是分类（中文）

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

    system_prompt = "你是分类助手"
    debug_deepseek_prompt("object_category", "deepseek-chat", system_prompt, prompt, words)

    try:

        resp = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt}
            ],
            temperature=0
        )

        content = resp.choices[0].message.content.strip()

        # 尝试JSON解析
        try:
            data = json.loads(content)
            if isinstance(data, dict):
                debug_deepseek_response(
                    "object_category",
                    raw_content=content,
                    parsed_payload=data,
                    normalized_payload=data,
                )
                return data
        except:
            pass

        # fallback解析
        result = {}

        for line in content.split("\n"):
            if ":" in line:
                k, v = line.split(":", 1)
                result[k.strip().strip('"')] = v.strip().strip('"').replace(",", "")

        debug_deepseek_response(
            "object_category",
            raw_content=content,
            parsed_payload=None,
            normalized_payload=result,
        )
        return result

    except Exception as e:
        print("DeepSeek错误:", e)
        return {}


# =========================
# 对外接口（带缓存）
# =========================

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

    batch_size = DEEPSEEK_BATCH_SIZE
    batches = [need[i : i + batch_size] for i in range(0, len(need), batch_size)]
    if not batches:
        return result

    workers = len(batches) if DEEPSEEK_WORKERS <= 0 else min(DEEPSEEK_WORKERS, len(batches))
    done_count = 0

    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        future_to_batch = {executor.submit(classify_batch, batch): batch for batch in batches}

        for future in as_completed(future_to_batch):
            batch = future_to_batch[future]
            try:
                res = future.result()
            except Exception as e:
                print("DeepSeek错误:", e)
                res = {}

            normalized_res = {normalize_cache_key(k): v for k, v in (res or {}).items()}

            for w in batch:
                cache_key = normalize_cache_key(w)
                c = normalized_res.get(cache_key, "物体")
                result[w] = c
                cache[cache_key] = c

            done_count += len(batch)
            print("DeepSeek进度:", done_count, "/", len(need))

    save_cache(cache)

    return result
