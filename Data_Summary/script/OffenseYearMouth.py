from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from pyspark.sql import SQLContext

def process(x):
    date = x.split("/")
    year = int(date[2])
    mouth = int(date[0])
    return ((year,mouth), 1)    

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: task <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x : reader(x))
    counts = lines.map(lambda x: x[1].strip()) \
            .filter(lambda x: x!="") \
            .map(process) \
            .reduceByKey(add) \
            .sortByKey() \
            .map(lambda x: (str(x[0][1])+ "/" + str(x[0][0]), x[1]))
    sqlContext = SQLContext(sc)
    df = sqlContext.createDataFrame(counts) 
    df.coalesce(1).write.format('com.databricks.spark.csv').save("OffenseYearMouth.csv")
    sc.stop()            
