import json
import re
import time
from collections import Counter, defaultdict
from pathlib import Path

import nltk
import pandas as pd
from deep_translator import GoogleTranslator
from nltk import pos_tag
from nltk.corpus import wordnet as wn

from deepseek_classify_batch import classify_with_cache, client as deepseek_client

# =========================
# 配置
# =========================

INPUT_DIR = Path("ara_202512/test")
OUTPUT_FILE = "ara_202512/test/word_frequency_analysis.csv"

TRANSLATION_CACHE_FILE = Path("translation_cache.json")
OBJECT_CACHE_FILE = Path("object_category_cache.json")
PLUSHABLE_CACHE_FILE = Path("plushable_cache.json")

BATCH_SIZE = 50
DEEPSEEK_BATCH_SIZE = 20
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
# WordNet分类
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


# =========================
# 缓存
# =========================


def load_cache(file_path):
    if not file_path.exists():
        return {}

    try:
        return json.loads(file_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_cache(cache, file_path):
    file_path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


# =========================
# 分类（核心）
# =========================


def classify_words(words, pos_map):
    cache = load_cache(OBJECT_CACHE_FILE)

    result = {}
    need_ai = []

    for word in words:
        pos = pos_map[word]

        if not pos.startswith("NN"):
            result[word] = "非名词"
            continue

        if word in cache:
            result[word] = cache[word]
            continue

        category = wordnet_classify(word)
        if category:
            result[word] = category
            cache[word] = category
            continue

        need_ai.append(word)

    print("需要AI分类:", len(need_ai))

    if need_ai:
        ai_result = classify_with_cache(need_ai)

        for word in need_ai:
            result[word] = ai_result.get(word, "物体")

    save_cache(cache, OBJECT_CACHE_FILE)
    return result


# =========================
# DeepSeek毛绒化判断
# =========================


def normalize_plushable(value):
    if isinstance(value, bool):
        return "是" if value else "否"

    if value is None:
        return "否"

    text = str(value).strip().lower()

    yes_set = {"是", "yes", "y", "true", "1", "可", "可以", "能", "shape"}
    no_set = {"否", "no", "n", "false", "0", "不", "不可以", "不能"}

    if text in yes_set:
        return "是"
    if text in no_set:
        return "否"
    if any(token in text for token in ["yes", "true", "shape", "plush", "可", "能"]):
        return "是"

    return "否"


def parse_deepseek_dict(content):
    body = (content or "").strip()

    if body.startswith("```"):
        lines = body.splitlines()
        if len(lines) >= 3:
            body = "\n".join(lines[1:-1]).strip()

    try:
        data = json.loads(body)
        if isinstance(data, dict):
            return data
    except Exception:
        pass

    result = {}

    for line in body.splitlines():
        if ":" not in line:
            continue

        key, value = line.split(":", 1)
        key = key.strip().strip('"').strip("'").strip(",")
        value = value.strip().strip('"').strip("'").strip(",")

        if key:
            result[key] = value

    return result


def classify_plushable_batch(words):
    if not words:
        return {}

    prompt = f"""
你是毛绒玩具选品助手。

请判断每个英文单词是否满足以下任意条件：
1. 是“形状”词（例如 heart, star, circle）
2. 是可以做成毛绒玩具的具体事物（动物、人物、食物、植物、交通工具、日常物体等）

输出要求：
1. 只返回 JSON，不要解释
2. key 为英文单词，value 只能是“是”或“否”
3. 每个单词都必须给出结果

待判断单词：
{words}
"""

    try:
        response = deepseek_client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": "你是严谨的二分类助手。"},
                {"role": "user", "content": prompt},
            ],
            temperature=0,
        )

        content = (response.choices[0].message.content or "").strip()
        raw = parse_deepseek_dict(content)
        normalized_raw = {str(k).lower(): v for k, v in raw.items()}

        return {word: normalize_plushable(normalized_raw.get(word.lower())) for word in words}

    except Exception as exc:
        print("DeepSeek plush error:", exc)
        return {word: "否" for word in words}


def classify_plushable(words):
    cache = load_cache(PLUSHABLE_CACHE_FILE)

    result = {}
    need_ai = []

    for word in words:
        if word in cache:
            result[word] = normalize_plushable(cache[word])
        else:
            need_ai.append(word)

    print("DeepSeek plush pending:", len(need_ai))

    for index in range(0, len(need_ai), DEEPSEEK_BATCH_SIZE):
        batch = need_ai[index : index + DEEPSEEK_BATCH_SIZE]
        batch_result = classify_plushable_batch(batch)

        for word in batch:
            decision = normalize_plushable(batch_result.get(word, "否"))
            result[word] = decision
            cache[word] = decision

        save_cache(cache, PLUSHABLE_CACHE_FILE)
        print("DeepSeek plush progress:", index + len(batch), "/", len(need_ai))

        time.sleep(0.5)

    return result


# =========================
# 翻译
# =========================


def translate(words):
    cache = load_cache(TRANSLATION_CACHE_FILE)
    translator = GoogleTranslator(source="en", target="zh-CN")

    result = {}
    need = []

    for word in words:
        if word in cache:
            result[word] = cache[word]
        else:
            need.append(word)

    print("翻译待处理:", len(need))

    for index in range(0, len(need), BATCH_SIZE):
        batch = need[index : index + BATCH_SIZE]

        try:
            translated = translator.translate_batch(batch)
        except Exception:
            translated = [""] * len(batch)

        for word, zh in zip(batch, translated):
            result[word] = zh
            cache[word] = zh

        save_cache(cache, TRANSLATION_CACHE_FILE)
        print("翻译进度:", index + len(batch), "/", len(need))

        time.sleep(0.2)

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
                    "searches": float(item.get("searches", 0)),
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

    pos_en = {}
    pos_zh = {}

    for word in words:
        tag, zh = get_pos(word)
        pos_en[word] = tag
        pos_zh[word] = zh

    category = classify_words(words, pos_en)
    plushable = classify_plushable(words)
    zh_map = translate(words)

    result_rows = []

    for word in words:
        frequency = freq[word]

        result_rows.append(
            {
                "word": word,
                "word_zh": zh_map.get(word, ""),
                "pos": pos_zh[word],
                "category": category[word],
                "plushable": plushable.get(word, "否"),
                "freq": frequency,
                "freq_ratio": round(frequency / total_freq, 6),
                "freq_ratio_percent": f"{frequency / total_freq:.2%}",
                "coverage": coverage[word],
                "coverage_percent": f"{coverage[word] / total_kw:.2%}",
                "total_searches": round(searches[word], 2),
            }
        )

    output_df = pd.DataFrame(result_rows)
    output_df = output_df.sort_values(by=["freq", "total_searches"], ascending=[False, False])
    output_df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

    print("完成:", OUTPUT_FILE)
    print(output_df.head(20))


if __name__ == "__main__":
    main()