'''
we are given customer data.
column 1 is customer id
column 2 is item id
column 3 is price
find total ammount spend by customer
'''
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CUSTOMER-ORDER")

sc = SparkContext().getOrCreate(conf = conf)

def parse_data(data):
    data = data.split(',')
    c_id = int(data[0])
    i_id = data[1]
    price = float(data[2])
    return (c_id, price)

dl = sc.textFile('file:///Azuredatabricks/spark/customer-orders.csv')

orders = dl.map(parse_data).reduceByKey(lambda x, y: x+y).sortByKey()

fo = orders.collect()

for o in fo:
    print(f"{o[0]} -> {o[1]:.2f}")
    
sc.stop()