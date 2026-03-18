from nltk.corpus import wordnet as wn
synsets = wn.synsets("game", pos=wn.NOUN)
if synsets:
    s = synsets[0]
    print("名称：", s.name())
    print("定义：", s.definition())
    print("例句：", s.examples())