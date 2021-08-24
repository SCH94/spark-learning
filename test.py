from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///Azuredatabricks/spark/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

rs = [f"{x} -> {y}" for x,y in result.items()]
print(rs)

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    # print("%s %i" % (key, value))
    print(f"{key} : {value}")