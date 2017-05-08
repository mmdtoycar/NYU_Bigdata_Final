from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from pyspark.sql import SQLContext

def process(x):
    date = x[1].split("/")
    year = date[2]
    return (year, 1) 

def toStrip(x):
    for i in range(len(x)):
       x[i] = x[i].strip()
    return x 

def isValidMidtownManhattanCrime(x):
    thisDate = x[1]
    precinct_code = int(x[14])
    return (precinct_code >= 14 and precinct_code <= 18) \
        and thisDate != ""

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: task <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x : reader(x))
    counts = lines.map(toStrip) \
                    .filter(isValidMidtownManhattanCrime) \
                    .map(process) \
                    .reduceByKey(add) \
                    .sortByKey()

    sqlContext = SQLContext(sc)
    df = sqlContext.createDataFrame(counts, ['Year','Number']) 
    df.coalesce(1).write.format('com.databricks.spark.csv').options(header='true').save("MidtownManhattanCrimeStatisticsInEachYear.csv")
    sc.stop()            
