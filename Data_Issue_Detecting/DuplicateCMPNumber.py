from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

def output(Pair):
    return "%s\t%s" % (Pair[0], Pair[1])

def ifIsNotValidNumberString(str):
    return not str.isdigit()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: task <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x : reader(x))
    counts = lines.map(lambda x: (x[0], 1)) 
    # store the internal result
    element = counts
    counts = counts.reduceByKey(add) \
            .filter(lambda x : (x[1] > 1) or (ifIsNotValidNumberString(x[0]))) \
            .map(lambda x: x[0]) \
            .collect()
    element = element.map(lambda x: (x[0], "INT\tComplaint Num\tValid") if x[0] not in counts
                            else (x[0], "INT\tComplaint Num\tInvalid")) \
            .sortByKey() \
            .map(output)
    element.saveAsTextFile("DuplicateCMPNumber.out")
    sc.stop()