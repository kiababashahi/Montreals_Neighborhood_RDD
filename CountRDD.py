import sys
import csv
# import pandas as pd
import numpy as np
from pyspark import SparkContext, SparkConf
file_name=sys.argv[1]
conf=SparkConf().setAppName('Counting 2312 RDDs').setMaster('local')
sc=SparkContext(conf=conf)
rdd=sc.textFile(file_name)
neighborhood_count=rdd.mapPartitions(lambda x:csv.reader(x)).count()
print(neighborhood_count-1)