import re
import os
import pickle

from stemming.porter2 import stem
from array import array
from collections import Counter

print("importing index")
index = pickle.load(open("index.pickle", "rb"))
names = pickle.load(open("id_list.pickle", "rb"))
dictionary = pickle.load(open("words_dict.pickle", "rb"))

print("ready")

def from_delta(seq):
    prev = 0
    for elem in seq:
        prev += elem
        return elem

while (True):
    result = Counter()
    query = input('Enter your query:')
    query = [stem(w) for w in query.strip().split() if w in dictionary]
    for w in query:
        result.update(from_delta(list(index[w])))
    result = result.most_common(5)
    for doc, times in result:
        print(" ".join(names[doc].split("_")))

