import regex as re
import unicodedata
import os
import sys

from bs4 import BeautifulSoup as bs
from bs4 import Comment

from stemming.porter2 import stem


directory = os.path.join(".", "cc_data")
output	= "./cc_clean_data/"

def remove_punctuation(text):
        return re.sub(r"\p{P}+", "", text)
i = 0
print(directory)
for root, dirs, files in os.walk(directory):
    for file in files:
        if file.endswith(".dump"):
            i += 1
            if (i % 1000 == 0):
                print(i / 1000, "%")
            f = open("./cc_data/" + file, 'r')
            soup  = bs(f.read(), "html.parser")

            comments = soup.findAll(text = lambda text:isinstance(text, Comment))
            [comment.extract() for comment in comments]

            ps = [s.get_text() for s in soup.find_all('p')]
            text = " ".join(ps)
            text = re.sub("\s", " ", text)
            text = remove_punctuation(text)
            text = " ".join([stem(w.lower()) for w in text.strip().split()])

            result = open(output + file, 'w')
            result.write(text)
