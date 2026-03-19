import json
import os
import time
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

BASE_DIR = Path(__file__).resolve().parent
CACHE_FILE = BASE_DIR / "object_category_cache.json"

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

    try:

        resp = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": "你是分类助手"},
                {"role": "user", "content": prompt}
            ],
            temperature=0
        )

        content = resp.choices[0].message.content.strip()

        # 尝试JSON解析
        try:
            data = json.loads(content)
            if isinstance(data, dict):
                return data
        except:
            pass

        # fallback解析
        result = {}

        for line in content.split("\n"):
            if ":" in line:
                k, v = line.split(":", 1)
                result[k.strip().strip('"')] = v.strip().strip('"').replace(",", "")

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

    for i in range(0, len(need), batch_size):

        batch = need[i:i+batch_size]

        res = classify_batch(batch)
        normalized_res = {normalize_cache_key(k): v for k, v in res.items()}

        for w in batch:
            cache_key = normalize_cache_key(w)

            c = normalized_res.get(cache_key, "物体")

            result[w] = c
            cache[cache_key] = c

        save_cache(cache)

        print("DeepSeek进度:", i+len(batch), "/", len(need))

        time.sleep(0.5)

    return result
