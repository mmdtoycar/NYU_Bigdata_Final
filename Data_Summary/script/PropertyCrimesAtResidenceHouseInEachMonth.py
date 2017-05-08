from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from pyspark.sql import SQLContext

def process(x):
    date = x[1].split("/")
    month = date[0]
    return (month, 1) 

def toStrip(x):
    for i in range(len(x)):
       x[i] = x[i].strip()
    return x 

def isPropertyCrimeAtResidenceHouse(x):
    thisDate = x[1]
    OFND_DESC = x[7].upper()
    location = x[16].upper()
    return ("LARCENY" in OFND_DESC or "BURGLARY" in OFND_DESC or "THEFT" in OFND_DESC) \
        and (location == "RESIDENCE - APT. HOUSE" or location == "RESIDENCE-HOUSE" or location == "RESIDENCE - PUBLIC HOUSING") \
        and thisDate != ""

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: task <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x : reader(x))
    counts = lines.map(toStrip) \
                    .filter(isPropertyCrimeAtResidenceHouse) \
                    .map(process) \
                    .reduceByKey(add) \
                    .sortByKey()

    sqlContext = SQLContext(sc)
    df = sqlContext.createDataFrame(counts, ['Month','Number']) 
    df.coalesce(1).write.format('com.databricks.spark.csv').options(header='true').save("PropertyCrimesAtResidenceHouseInEachMonth.csv")
    sc.stop()            
