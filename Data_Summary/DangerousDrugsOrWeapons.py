from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from pyspark.sql import SQLContext

def process(x):
    date = x[1].split("/")
    year = date[2]
    OFND_DESC = x[7].upper()
    return ((OFND_DESC,year), 1) 

def toStrip(x):
    for i in range(len(x)):
       x[i] = x[i].strip()
    return x 

def isDangerousDrugsOrWeaponsOnStreets(x):
    thisDate = x[1]
    OFND_DESC = x[7].upper()
    location = x[16].upper()
    return (OFND_DESC == "DANGEROUS DRUGS" or OFND_DESC == "DANGEROUS WEAPONS") \
        and location == "STREET" and thisDate != ""

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: task <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x : reader(x))
    counts = lines.map(toStrip) \
                    .filter(isDangerousDrugsOrWeaponsOnStreets) \
                    .map(process) \
                    .reduceByKey(add) \
                    .sortByKey() \
                    .map(lambda x: (x[0][0], x[0][1], x[1]))

    sqlContext = SQLContext(sc)
    df = sqlContext.createDataFrame(counts, ['Desc','Year','Number']) 
    df.coalesce(1).write.format('com.databricks.spark.csv').options(header='true').save("DangerousDrugsOrWeaponsOnStreets.csv")
    sc.stop()            
