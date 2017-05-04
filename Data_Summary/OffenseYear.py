from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from pyspark.sql import SQLContext

def process(x):
    hour = int(x.split(":")[0])
    
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: task <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    header = lines.first()
    lines = lines.filter(lambda line: line != header)

    lines = lines.mapPartitions(lambda x : reader(x))
    counts = lines.map(lambda x: x[1]) \
            .filter(lambda x: x!="") \
            .map(lambda x: (int(x.split("/")[2]), 1) ) \
            .reduceByKey(add) \
            .map(lambda x: (x[1], x[0])) \
            .sortByKey(False) \
            .map(lambda x: (x[1], x[0]))
    sqlContext = SQLContext(sc)
    df = sqlContext.createDataFrame(counts, ['key','count'])     
    df.coalesce(1).write.format('com.databricks.spark.csv').options(header='true').save("OffenseTime.csv")
    sc.stop()