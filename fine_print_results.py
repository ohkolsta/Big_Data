from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *

#SET SPARKCONTEXT
conf = SparkConf().setAppName("AirBnB datasets")
sc = SparkContext(conf=conf)

#INITIALIZE SPARK SESSION
spark = SparkSession \
    .builder \
    .getOrCreate()

p2 = sc.textFile("distinct_p1.txt").collect()[0]
p2 = p2.replace("[Row(count(DISTINCT ", "")
p2 = p2.replace(")]", "")
p2 = p2.replace(")", "")
p2 = p2.split(",")
#p2 = p2.replace("[Row(count(DISTINCT ", "")
#p2 = p2.replace(")]", "")
for col in p2:
    print(col)
