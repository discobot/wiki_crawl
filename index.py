import re
import os
import sys
import pickle

from os import listdir
from os.path import isfile, join
from array import array

from collections import Counter

def to_delta(seq):
    prev = 0
    for i in range(len(seq)):
        if i == 0:
            yield seq[i]
            prev = seq[i]
        else:
            yield seq[i] - prev
            prev = seq[i]


path = "./cc_clean_data"
files = [join(path, f) for f in listdir(path) if isfile(join(path, f)) and f.endswith(".dump")]

words_dict = Counter()

print("computing dict")
for f in files:
    words_dict.update(open(f, "r").read().strip().split())
print("dict size:", len(words_dict))

print("stripping dict")
striped_dict = set([key for key in words_dict if words_dict[key] > 1])
print("stripped dict size:", len(striped_dict))

print("saving dict")
pickle.dump(striped_dict, open("words_dict.pickle", "wb"))

counter = 0
index = {word : set() for word in striped_dict}
id_list = []

print("computing index")
for f in files:
    words = [w for w in open(f, "r").read().strip().split() if w in striped_dict]
    for word in words:
        index[word].update([counter])

    id_list.append(f.strip().strip(".dump").strip("/cc_clean_data/"))

    if(counter % 1000 == 0):
        print(str(float(counter) / 1000) + "%", "files indexed")

    counter += 1

print("compressing index")
compressed_index = {word : array("H", list(to_delta(sorted(index[word])))) for word in words}


print("saving index")
pickle.dump(id_list, open("id_list.pickle", "wb"))
pickle.dump(compressed_index, open("index.pickle", "wb"))
