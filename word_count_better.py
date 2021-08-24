from pyspark import SparkConf, SparkContext
import re

def parsedata(dl):
    return re.compile(r'[^A-z0-9]+', re.UNICODE).split(dl.lower())

conf = SparkConf().setMaster("local").setAppName("WORD-COUNT-BETTER")
sc = SparkContext().getOrCreate(conf = conf)

dl = sc.textFile("file:///Azuredatabricks/spark/book.txt")

words = dl.flatMap(parsedata)

wc = words.countByValue()

for k, v in wc.items():
    cw = k.encode('ascii', 'ignore')
    if(cw):
        print(f"{cw} -> {v}") 
print(words.top(5))
sc.stop()