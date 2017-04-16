from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

def output(Pair):
    return "%s\t%s" % (Pair[0], Pair[1])

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: task <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    header = lines.first()
    lines = lines.filter(lambda line: line != header)

    lines = lines.mapPartitions(lambda x : reader(x))
    counts =  lines.map(lambda x: x[5]) \
            .filter(lambda x: x!="") \
            .map(lambda x: (x.split("/")[0], 1)) \
            .reduceByKey(add) \
            .sortByKey() \
            .map(output)
    counts.coalesce(1).saveAsTextFile("DateReportedToPoliceDistribution.out")
    sc.stop()