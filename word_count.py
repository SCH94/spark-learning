from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Word-COUNT")

sc = SparkContext().getOrCreate(conf = conf)

dl = sc.textFile('file:///Azuredatabricks/spark/book.txt')

words = dl.flatMap(lambda x: x.split())

wc = words.countByValue()

for k, v in wc.items():
    cw = k.encode('ascii', 'ignore')
    if(cw):
        print(f"::::> {cw} -> {v}")

sc.stop()
