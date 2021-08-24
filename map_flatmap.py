# -*- coding: utf-8 -*-
"""
map and flap map.

Created on Sun Aug 22 16:02:49 2021

@author: Arul Vats
"""

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MAP-FLATMAP")

sc = SparkContext().getOrCreate(conf = conf)

#dl = sc.textFile('file:///Azuredatabricks/spark/book.txt')
dl = sc.textFile('file:///Azuredatabricks/spark/test.txt')
#wl = dl.map(lambda x: x.upper())
sl = dl.map(lambda x: x.split())
fl = dl.flatMap(lambda x: x.split())
mp = sl.collect()
fmp = fl.collect()
print([x for x in mp])
print("<:::::::::::::::::::::::::>")
print([x for x in fmp])
sc.stop()
