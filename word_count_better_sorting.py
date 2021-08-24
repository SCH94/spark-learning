from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setMaster("local").setAppName("WC-SORT")
sc = SparkContext().getOrCreate(conf = conf)

def parsedata(lines):
    return re.compile(r"\W+", re.UNICODE).split(lines.lower())

dl = sc.textFile("file:///Azuredatabricks/spark/book.txt")

words = dl.flatMap(parsedata)
# wc = words.map(lambda x: (x, 1))
wc = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x+y)
print(wc.top(5))

wcss = wc.map(lambda x: (x[0], x[1]))
print(wcss.top(5))

wcs = wc.map(lambda x: (x[1], x[0])).sortByKey()
wcc = wcs.collect()
# print(wcs.top(5))

for res in wcc:
    count = str(res[0])
    word = res[1].encode('ascii', 'ignore')
    if(word):
        print(f"{word} -> {count}")
        
sc.stop()