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
    counts = lines.map(lambda x: (x[13], 1)) \
            .reduceByKey(add) \
            .map(lambda x: (x[1], x[0])) \
            .sortByKey() \
            .map(lambda x: (x[1], x[0]))
    sqlContext = SQLContext(sc)
    df = sqlContext.createDataFrame(counts)
    df.coalesce(1).write.format('com.databricks.spark.csv').save("BoroughNameDistribution.csv")
    sc.stop()