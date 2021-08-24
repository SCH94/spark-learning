# -*- coding: utf-8 -*-
"""
Filetring RDD'S

filter() -> removes imformation, returns boolean.

Created on Sat Aug 21 19:07:45 2021

@author: Arul Vats
"""
from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("Filtering-RDD")
sc = SparkContext().getOrCreate(conf=conf)

def parsedata(dl):
    data = dl.split(',')
    stationID = data[0]
    entrytype = data[2]
    temp = float(data[3]) * 0.1 * (9.0/5.0) +32.0
    return (stationID, entrytype, temp)

dl = sc.textFile('file:///Azuredatabricks/spark/1800.csv')

rdd = dl.map(parsedata)

#mTemp = rdd.filter(lambda x: 'TMIN' in x[1])
mTemp = rdd.filter(lambda x: 'TMAX' in x[1])
sTemp = mTemp.map(lambda x: (x[0], x[2]))

#smTemp = sTemp.reduceByKey(lambda x, y: min(x, y))
smTemp = sTemp.reduceByKey(lambda x, y: max(x, y))
result = smTemp.collect()

print(f"result => {result[:10]}")

sc.stop()

