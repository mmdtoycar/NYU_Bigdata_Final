from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from pyspark.sql import SQLContext

def process(x):
    hour = int(x.split(":")[0])
    if(hour in range(0, 1)):
        return ("00:00 - 00:59", 1)
    elif(hour in range(1, 2) ):
        return ("01:00 - 01:59", 1)
    elif(hour in range(2, 3)):
        return ("02:00 - 02:59", 1)
    elif(hour in range(3, 4)):
        return ("03:00 - 03:59", 1)
    elif(hour in range(4, 5)):
        return ("04:00 - 04:59", 1)
    elif(hour in range(5, 6)):
        return ("05:00 - 05:59", 1)
    elif(hour in range(6, 7)):
        return ("06:00 - 06:59", 1)    
    elif(hour in range(7, 8)):
        return ("07:00 - 07:59", 1)
    elif(hour in range(8, 9)):
        return ("09:00 - 08:59", 1)
    elif(hour in range(9, 10)):
        return ("09:00 - 09:59", 1)
    elif(hour in range(10, 11)):
        return ("10:00 - 10:59", 1)    
    elif(hour in range(11, 12)):
        return ("11:00 - 11:59", 1)    
    elif(hour in range(12, 13)):
        return ("12:00 - 12:59", 1)
    elif(hour in range(13, 14)):
        return ("13:00 - 13:59", 1)
    elif(hour in range(14, 15)):
        return ("14:00 - 14:59", 1)    
    elif(hour in range(15, 16)):
        return ("15:00 - 15:59", 1)
    elif(hour in range(16, 17)):
        return ("16:00 - 16:59", 1)
    elif(hour in range(17, 18)):
        return ("17:00 - 17:59", 1)
    elif(hour in range(18, 19)):
        return ("18:00 - 18:59", 1)
    elif(hour in range(19, 20)):
        return ("19:00 - 19:59", 1) 
    elif(hour in range(20, 21)):
        return ("20:00 - 20:59", 1)    
    elif(hour in range(21, 22)):
        return ("21:00 - 21:59", 1)
    elif(hour in range(22, 23)):
        return ("22:00 - 22:59", 1)
    elif(hour in range(23, 24)):
        return ("23:00 - 23:59", 1)
    else :
        return ("24:00 - 00:00", 1)
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: task <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x : reader(x))
    counts = lines.map(lambda x: x[2]) \
            .filter(lambda x: x!="") \
            .map(process) \
            .reduceByKey(add) \
            .map(lambda x: (x[1], x[0])) \
            .sortByKey(False) \
            .map(lambda x: (x[1], x[0]))
    sqlContext = SQLContext(sc)
    df = sqlContext.createDataFrame(counts)     
    df.coalesce(1).write.format('com.databricks.spark.csv').save("OffenseTime.csv")
    sc.stop()