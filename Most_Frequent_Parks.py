import sys
import csv
# import pandas as pd
import numpy as np
"""
In this scrip we are reading a csv file containing the neighborhoods of the city of Montreal. We are particularly,interested
in the 16 th column which contains the list of all of the parks in each neighborhood. The goal is to take the lists, convert it 
to an rdd, then find the most frequent parks in the city . 
Here we will first read from the CSV file,take out the 16th column, remove its header, then flatten the list 
by converting the list of neighborhoods into an RDD and finally, use mapReduce to find the most frequent parks
"""
from pyspark import SparkContext, SparkConf
file_name=sys.argv[1]
conf=SparkConf().setAppName('Counting 2312 RDDs').setMaster('local')
sc=SparkContext(conf=conf)
rdd=sc.textFile(file_name)
#partitioning the rdd which was of a CSV file into its rows or records pay attention to the use of mapPartitions
neighborhoods=rdd.mapPartitions(lambda x:csv.reader(x,delimiter="|"))
park_list=neighborhoods.map(lambda x:x[15]).filter(lambda x:x)
header=park_list.first()
#removing the header
parks=park_list.filter(lambda x:x!=header)
# the output will look like this # [[a;b;c],[a;d;b],....]
# We want to "flatten" this list and feed each [] to a node and let that node break it to (a,1),(b,1),(c,1),(a,1),(d,1),(b,1)
#then in order to find the most frequent element we only need to reduce by key so that the list of
# (a,2),(b,2),(c,1),(d,1),... is acquired
flattened_parks=parks.flatMap(lambda x:x.split(";"))
most_freq=flattened_parks.map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[1],ascending=False).take(10)
for park_id in most_freq:
    print(park_id)