# which kind of offense done in a particular period of time
from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from pyspark.sql import SQLContext

def output(Pair):
    return "%s\t%s" % (Pair[0], Pair[1])
def process(x):
    hour = int(x[0].split(":")[0])
    if(hour in range(0, 6)):
        return ("00:00 - 06:00", x[1])
    elif(hour in range(6, 12) ):
        return ("06:00 - 12:00", x[1])
    elif(hour in range(12, 18)):
        return ("12:00 - 18:00", x[1])
    else :
        return ("18:00 - 24:00", x[1])

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: task <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    header = lines.first()
    lines = lines.filter(lambda line: line != header)

    lines = lines.mapPartitions(lambda x : reader(x))
    counts = lines.map(lambda x: (x[2], x[7])) \
            .filter(lambda x: x[0]!="") \
    		.map(process) \
    		.map(lambda x: ((x[0],x[1]), 1)) \
            .reduceByKey(add) \
            .map(lambda x: (x[1], x[0])) \
            .sortByKey(False) \
            .map(lambda x: (x[1], x[0])) \
            .map(output)
    sqlContext = SQLContext(sc)
    df = sqlContext.createDataFrame(counts)	
    df.coalesce(1).write.format('com.databricks.spark.csv').options(header='true').save("OffenseTimeAndTypeDistribution.out")
    sc.stop()