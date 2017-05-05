from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from pyspark.sql import SQLContext


def filter_out_by_year(row):
    return int(row[1].split("/")[2]) >= 2006
    
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: task <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x : reader(x))
    counts = lines.filter(filter_out_by_year)

    sqlContext = SQLContext(sc)
    df = sqlContext.createDataFrame(counts)     
    df.coalesce(1).write.format('com.databricks.spark.csv').save("DataClean.csv")
    sc.stop()