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
    header = lines.first()
    lines = lines.filter(lambda line: line != header)

    lines = lines.mapPartitions(lambda x : reader(x))
    counts = lines.map(lambda x: (x[11], 1)) \
            .reduceByKey(add) \
            .sortByKey()
    sqlContext = SQLContext(sc)
    df = sqlContext.createDataFrame(counts, ['key','count'])
    df.coalesce(1).write.format('com.databricks.spark.csv').options(header='true').save("OffenseLevelDistribution.csv")
    sc.stop()