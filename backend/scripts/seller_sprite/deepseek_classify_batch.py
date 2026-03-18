import json
import time
from pathlib import Path
from openai import OpenAI


# =========================
# 配置
# =========================

DEEPSEEK_API_KEY = "sk-f93c5cc7df984f2ea501017091e1e633"
BASE_URL = "https://api.deepseek.com"

CACHE_FILE = Path("object_category_cache.json")

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
        return json.loads(
            CACHE_FILE.read_text(encoding="utf-8")
        )
    except:
        return {}


def save_cache(cache):

    CACHE_FILE.write_text(
        json.dumps(cache, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )


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

        if w in cache:
            result[w] = cache[w]
        else:
            need.append(w)

    print("DeepSeek待处理:", len(need))

    batch_size = 20

    for i in range(0, len(need), batch_size):

        batch = need[i:i+batch_size]

        res = classify_batch(batch)

        for w in batch:

            c = res.get(w, "物体")

            result[w] = c
            cache[w] = c

        save_cache(cache)

        print("DeepSeek进度:", i+len(batch), "/", len(need))

        time.sleep(0.5)

    return result