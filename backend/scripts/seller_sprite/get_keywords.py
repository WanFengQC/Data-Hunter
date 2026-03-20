import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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


BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "ara_202509"
OUTPUT_FILE = "word_frequency_analysis_202509.csv"

TRANSLATION_CACHE_FILE = BASE_DIR / "translation_cache.json"
OBJECT_CACHE_FILE = BASE_DIR / "object_category_cache.json"
PLUSHABLE_CACHE_FILE = BASE_DIR / "plushable_cache.json"

BATCH_SIZE = get_env_int("TRANSLATE_BATCH_SIZE", 100)
TRANSLATE_WORKERS = get_env_int("TRANSLATE_WORKERS", 4)
TRANSLATE_SLEEP_SECONDS = get_env_float("TRANSLATE_SLEEP_SECONDS", 0)
DEEPSEEK_BATCH_SIZE = get_env_int("DEEPSEEK_BATCH_SIZE", 500)
DEEPSEEK_WORKERS = get_env_int("DEEPSEEK_WORKERS", 0)
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


# =========================
# 分类（核心）
# =========================


def classify_words(words, pos_map):
    cache = load_cache(OBJECT_CACHE_FILE)

    result = {}
    need_ai = []

    for word in words:
        pos = pos_map[word]
        cache_key = normalize_cache_key(word)

        if not pos.startswith("NN"):
            result[word] = "非名词"
            continue

        if cache_key in cache:
            result[word] = cache[cache_key]
            continue

        category = wordnet_classify(word)
        if category:
            result[word] = category
            cache[cache_key] = category
            continue

        need_ai.append(word)

    print("需要AI分类:", len(need_ai))

    if need_ai:
        ai_result = classify_with_cache(need_ai)

        for word in need_ai:
            cache_key = normalize_cache_key(word)
            decision = ai_result.get(word, ai_result.get(cache_key, "物体"))
            result[word] = decision
            cache[cache_key] = decision

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
        cache_key = normalize_cache_key(word)
        if cache_key in cache:
            result[word] = normalize_plushable(cache[cache_key])
        else:
            need_ai.append(word)

    print("DeepSeek plush pending:", len(need_ai))

    batches = [need_ai[i : i + DEEPSEEK_BATCH_SIZE] for i in range(0, len(need_ai), DEEPSEEK_BATCH_SIZE)]
    if not batches:
        return result

    workers = len(batches) if DEEPSEEK_WORKERS <= 0 else min(DEEPSEEK_WORKERS, len(batches))
    done_count = 0

    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        future_to_batch = {executor.submit(classify_plushable_batch, batch): batch for batch in batches}

        for future in as_completed(future_to_batch):
            batch = future_to_batch[future]
            try:
                batch_result = future.result()
            except Exception as exc:
                print("DeepSeek plush error:", exc)
                batch_result = {}

            for word in batch:
                cache_key = normalize_cache_key(word)
                decision = normalize_plushable((batch_result or {}).get(word, "否"))
                result[word] = decision
                cache[cache_key] = decision

            done_count += len(batch)
            print("DeepSeek plush progress:", done_count, "/", len(need_ai))

    save_cache(cache, PLUSHABLE_CACHE_FILE)

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
