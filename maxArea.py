import sys
import csv
# import pandas as pd
import numpy as np
from pyspark import SparkContext, SparkConf
file_name=sys.argv[1]
conf=SparkConf().setAppName('Counting 2312 RDDs').setMaster('local')
sc=SparkContext(conf=conf)
rdd=sc.textFile(file_name)
neighborhoods=rdd.mapPartitions(lambda x:csv.reader(x,delimiter="|"))
areas=neighborhoods.map(lambda x:x[5])
header=areas.first()
max_element=areas.filter(lambda x:x!=header).max()
print(max_element)
