# Montreals_Neighborhood_RDD
In this repository, I will cover some very basic spark ideas using RDDs. When I started with Spark, I remember I had difficulty finding code on RDDs whereas I saw an abundance of code for DataFramse. 
These days, I see a lot of people working with data frames when they can work with RDDs or going back and forth between a DFF and an RDD in their code despite being able to carry everything using RDDs. Even though in some cases it might be simpler or even better to do so, in general, one should avoid doing such a thing.

I felt like a guide on how to code basic spark codes using RDDs and how to use them to read CSV files and manipulate them. And then play with some more advanced stuff, later on, might help people new to spark develop their skills. This is one of the reasons I am moving some of my projects from my private repositories and making them public also why I am writing some tutorials.

This readme is mostly about the Most_Frequent_parks example as it still simple but it has some technical details embedded in it. 

So, let's start describing the data set. The data set that I am currently working with is extracted from the city of Montreal. Here, we are representing the neighborhoods of the city each with their unique Id as well as their type, a list of their parks, and their areas.

 
Lets first set up our spark evnironment 
```ruby
import sys
import csv
from pyspark import SparkContext, SparkConf
file_name=sys.argv[1]
conf=SparkConf().setAppName('Counting 2312 RDDs').setMaster('local')
sc=SparkContext(conf=conf)
rdd=sc.textFile(file_name)

```

Now that our CSV file is parallelized and converted into a spark object we need to treat it like one. I will simplify the explanations. (NOTE: I might oversimplify stuff just to give a person new to the spark world some insight on what some of the methods that I used do. )

We want to use the power of RDDs so we will break the CSV into rows and then will send each row to a given "computational unit". Which can be achieved using the following syntax

```ruby
neighborhoods=rdd.mapPartitions(lambda x:csv.reader(x,delimiter="|"))
```
Each row is now converted to a list. For those of you new to the industry. So what is the lambda thing? Think about it as f(x) here the function is named lambda(x). It takes as input a row of the CSV file and then returns a list by breaking down the row into elements by splitting them using the delimiter specified( "|").

 For this project, we are interested in the list of parks that have been stored on the 16th column of each row. Now that we have each row (think of it as an oneD array) given to a computational unit we can extract the 16th element.

This way if we combine our nodes, for each row, we have selected the 16th column hence the 16th column has been extracted from the matrix and the rest has been discarded. 

Keep in mind that is the goal of MapReduce to break the problem in small sub problems to allow parallelization.  Now that the 16th row is present, take its first element (header) and then remove it.

```ruby
park_list=neighborhoods.map(lambda x:x[15]).filter(lambda x:x)
header=park_list.first()
#removing the header
parks=park_list.filter(lambda x:x!=header)
```
The output of the above will be something like [[a,;b;c;d],[a;e;c;f],....,[f,g]] that is a list of lists.  

Next is to flatten the list obtained from the previous step. Let's remove the nested list structute and just have something like [a,b,c,d,a,e,c,f,...,f,g]. This is done via

```ruby
flattened_parks=parks.flatMap(lambda x:x.split(";"))
```
Pay close attention to the use of flatMaphere. Lastly, we want to know the parks that are present in most of the neighborhoods or what I call frequent parks. Note that a park can be massive and it can be shared between neighborhoods. That being said we need to count these occurrences. And lucky for us, we can use MapReduce.
```ruby
most_freq=flattened_parks.map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[1],ascending=False).take(10)
```
From the previous step we obtained [a,b,c,d,a,e,c,f,...,f,g]. This will be the input of the map method. Each element x (a, b, ...) will be sent to the lambda function, then a one will be concatenated to it and the entire result will be reduced by key via a simple lambda function called in the reduceByKey method. The final output is sorted and the top ten biggest parks are extracted. 
