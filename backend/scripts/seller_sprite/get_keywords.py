import json
import re
import time
from pathlib import Path
from collections import Counter, defaultdict

import pandas as pd
import nltk
from nltk import pos_tag
from nltk.corpus import wordnet as wn
from deep_translator import GoogleTranslator
from deepseek_classify_batch import classify_with_cache

# =========================
# 配置
# =========================

INPUT_DIR = Path("ara_202512/test")
OUTPUT_FILE = "ara_202512/test/word_frequency_analysis.csv"

TRANSLATION_CACHE_FILE = Path("translation_cache.json")
OBJECT_CACHE_FILE = Path("object_category_cache.json")

BATCH_SIZE = 50
TO_LOWER = True
REMOVE_STOPWORDS = True

STOPWORDS = {
    "", "a", "an", "the", "and", "or", "for", "with", "of", "to", "in", "on", "at", "by"
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

    if not isinstance(text,str):
        return []

    if TO_LOWER:
        text = text.lower()

    words = re.findall(r"[a-z0-9]+", text)

    result=[]

    for w in words:

        if REMOVE_STOPWORDS and w in STOPWORDS:
            continue

        if w.isdigit():
            continue

        if any(c.isdigit() for c in w):
            continue

        if len(w)<=1:
            continue

        result.append(w)

    return result

# =========================
# 词性
# =========================

def get_pos(word):

    try:
        tag = pos_tag([word])[0][1]
    except:
        return "", "未知"

    if tag.startswith("NN"):
        return tag,"名词"
    if tag.startswith("JJ"):
        return tag,"形容词"
    if tag.startswith("VB"):
        return tag,"动词"
    if tag.startswith("RB"):
        return tag,"副词"

    return tag,"其他"

# =========================
# WordNet分类
# =========================

MAP = {
    "animal":"动物",
    "vehicle":"交通工具",
    "person":"人物",
    "food":"食物",
    "plant":"植物",
    "artifact":"物体",
    "shape":"形状"
}

def wordnet_classify(word):

    synsets = wn.synsets(word,pos=wn.NOUN)[:1]

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

def load_cache(file):
    if not file.exists():
        return {}
    try:
        return json.loads(file.read_text(encoding="utf-8"))
    except:
        return {}

def save_cache(cache,file):
    file.write_text(json.dumps(cache,ensure_ascii=False,indent=2),encoding="utf-8")

# =========================
# 分类（核心）
# =========================

def classify_words(words, pos_map):

    cache = load_cache(OBJECT_CACHE_FILE)

    result = {}
    need_ai = []

    for w in words:

        pos = pos_map[w]

        if not pos.startswith("NN"):
            result[w] = "非名词"
            continue

        # ⭐ 先查统一缓存
        if w in cache:
            result[w] = cache[w]
            continue

        # ⭐ WordNet
        cat = wordnet_classify(w)

        if cat:
            result[w] = cat
            cache[w] = cat
            continue

        need_ai.append(w)

    print("需要AI:", len(need_ai))

    # ⭐ 调用DeepSeek模块
    if need_ai:

        ai_result = classify_with_cache(need_ai)

        for w in need_ai:
            result[w] = ai_result.get(w, "物体")

    save_cache(cache, OBJECT_CACHE_FILE)

    return result

# =========================
# 翻译
# =========================

def translate(words):

    cache = load_cache(TRANSLATION_CACHE_FILE)

    translator = GoogleTranslator(source="en",target="zh-CN")

    result={}
    need=[]

    for w in words:
        if w in cache:
            result[w]=cache[w]
        else:
            need.append(w)

    print("翻译待处理:",len(need))

    for i in range(0,len(need),BATCH_SIZE):

        batch=need[i:i+BATCH_SIZE]

        try:
            res = translator.translate_batch(batch)
        except:
            res=[""]*len(batch)

        for w,zh in zip(batch,res):
            result[w]=zh
            cache[w]=zh

        save_cache(cache,TRANSLATION_CACHE_FILE)

        print("翻译进度:",i+len(batch),"/",len(need))

        time.sleep(0.2)

    return result

# =========================
# 主程序
# =========================

def main():

    ensure_nltk()

    files = list(INPUT_DIR.glob("*.html"))+list(INPUT_DIR.glob("*.json"))

    rows=[]

    for f in files:

        try:
            data=json.loads(f.read_text(encoding="utf-8"))
            if data.get("code")!="OK":
                continue
            items=data["data"]["items"]
        except:
            continue

        for it in items:

            rows.append({
                "keyword":it.get("keyword",""),
                "searches":float(it.get("searches",0))
            })

    df=pd.DataFrame(rows)

    print("keyword数:",len(df))

    # =========================
    # 统计
    # =========================

    freq=Counter()
    searches=defaultdict(float)
    coverage=defaultdict(int)

    for _,r in df.iterrows():

        ws=split_words(r["keyword"])

        freq.update(ws)

        for w in set(ws):
            searches[w]+=r["searches"]
            coverage[w]+=1

    total_freq=sum(freq.values())
    total_kw=len(df)

    words=[w for w,_ in freq.most_common()]

    # =========================
    # 词性
    # =========================

    pos_en={}
    pos_zh={}

    for w in words:
        tag,zh=get_pos(w)
        pos_en[w]=tag
        pos_zh[w]=zh

    # =========================
    # 分类
    # =========================

    category=classify_words(words,pos_en)

    # =========================
    # 翻译
    # =========================

    zh_map=translate(words)

    # =========================
    # 汇总
    # =========================

    res=[]

    for w in words:

        fr=freq[w]

        res.append({
            "word":w,
            "word_zh":zh_map.get(w,""),
            "pos":pos_zh[w],
            "category":category[w],
            "freq":fr,
            "freq_ratio":round(fr/total_freq,6),
            "freq_ratio_percent":f"{fr/total_freq:.2%}",
            "coverage":coverage[w],
            "coverage_percent":f"{coverage[w]/total_kw:.2%}",
            "total_searches":round(searches[w],2)
        })

    df=pd.DataFrame(res)

    df=df.sort_values(by=["freq","total_searches"],ascending=[False,False])

    df.to_csv(OUTPUT_FILE,index=False,encoding="utf-8-sig")

    print("完成:",OUTPUT_FILE)
    print(df.head(20))


if __name__=="__main__":
    main()