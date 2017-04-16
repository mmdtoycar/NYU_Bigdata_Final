from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

def output(Pair):
    return "%s\t%s" % (Pair[0], Pair[1])

def isInvalid(x):
    allEmpty = ifAllEmpty(x)
    RAPEOrSexCrime = ifRAPEOrSexCrime(x)
    return (allEmpty and not RAPEOrSexCrime) or (not allEmpty and RAPEOrSexCrime)

def ifAllEmpty(x):
    return x[2].strip() == "" and x[3].strip() == "" and x[4].strip() == "" and x[5].strip() == "" and x[6].strip() == ""

def ifRAPEOrSexCrime(x):
    return x[1] == "RAPE" or x[1] == "SEX CRIMES"

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: task <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    header = lines.first()
    lines = lines.filter(lambda line: line != header)

    lines = lines.mapPartitions(lambda x : reader(x))
    counts = lines.map(lambda x: (x[0],x[7],x[19],x[20],x[21],x[22],x[23])) \
            .filter(lambda x: isInvalid(x)) \
            .map(lambda x: (x[0], (x[1],x[2],x[3],x[4],x[5],x[6]))) \
            .sortByKey() \
            .map(output)
    counts.coalesce(1).saveAsTextFile("InvalidSexRape_Geo.out")
    sc.stop()