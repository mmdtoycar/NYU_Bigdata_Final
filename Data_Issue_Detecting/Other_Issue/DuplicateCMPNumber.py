from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

def output(Pair):
    return "%s\t%s" % (Pair[0], Pair[1])

def process(x):
    invalidType = "Has_Duplicate_Confict_Value"
    return (x[0], "AppearTime: {}\t{}".format(x[1], invalidType))
    
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: task <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    #extract header
    header = lines.first()
    lines = lines.filter(lambda line: line != header)

    lines = lines.mapPartitions(lambda x : reader(x))
    counts = lines.map(lambda x: (x[0].strip(), 1)) 
    counts = counts.reduceByKey(add) \
            .filter(lambda x : (x[1] > 1)) \
            .map(process) \
            .sortByKey() \
            .map(output)
    counts.coalesce(1).saveAsTextFile("DuplicateCMPNumber.out")
    sc.stop()