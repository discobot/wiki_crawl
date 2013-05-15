from hashes.simhash import simhash
from os.path import isfile, join
from os import listdir
import multiprocessing

def sh(f):
    return [simhash(open(f).read()), f]

def get_compare():
    path = "./cc_clean_data"
    pool = multiprocessing.Pool(10)
    files = [join(path, f) for f in listdir(path) if isfile(join(path, f)) and f.endswith(".dump")]

    hashes = []

    print("calculating hashes")
    hashes = pool.map(sh, files)

    print("comparing hashes")
    for i in range(len(hashes)):
        if (i % 1000 == 0):
            print (str(i / 1000), "%")
        for j in range(i + 1, len(hashes)):
            score = hashes[i][0].similarity(hashes[j][0])
            if score > 0.95:
                temp_result = [score, hashes[i][1], hashes[j][1]]
                print(temp_result)
                yield temp_result
