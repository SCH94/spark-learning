"""
Key Values in Resilient distributed datasets

"""
from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("Fake-friends")

#sc = SparkContext(conf = conf)
sc = SparkContext.getOrCreate(conf=conf)
def parsedata(lines):
    data = lines.split(',')
    age = int(data[2])
    nfriends = int(data[3])
    return (age, nfriends)


lines = sc.textFile('file:///Azuredatabricks/spark/fakefriends.csv')
rdd = lines.map(parsedata)
#print(rdd.top(num = 5))
sheet = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x,y :(x[0] + y[0], x[1] + y[1]))
ss = sheet.collect()
print(ss[:10])

avg = sheet.mapValues(lambda x: x[0]/x[1])

cdata = avg.collect()

print(cdata[:5])

sc.stop()



