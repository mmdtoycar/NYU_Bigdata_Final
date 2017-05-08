from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from pyspark.sql import SQLContext
  

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: task <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x : reader(x))
    counts = lines.filter(lambda x: int(x[2].split(':')[0]) == 12 or int(x[2].split(':')[0]) == 13 or int(x[2].split(':')[0]) == 11) \
            .map(lambda x: ((x[7], int(x[2].split(':')[0])), 1) ) \
            .reduceByKey(add) \
            .sortByKey() \
            .map(lambda x: (x[0][1], x[0][0], x[1])) 
    sqlContext = SQLContext(sc)
    df = sqlContext.createDataFrame(counts) 
    df.coalesce(1).write.format('com.databricks.spark.csv').save("1213Outliers.csv")
    sc.stop()            
