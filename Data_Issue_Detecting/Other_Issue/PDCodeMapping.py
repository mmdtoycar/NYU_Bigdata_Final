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
    #extract header
    header = lines.first()
    lines = lines.filter(lambda line: line != header)

    lines = lines.mapPartitions(lambda x : reader(x))
    counts = lines.filter(lambda x: x[8] != "") \
                 .map(lambda x: ((x[8], x[9]), 1)) 
    counts = counts.reduceByKey(add) \
            .sortByKey()

    duplicates = counts.map(lambda x: (x[0][0], 1)) \
                .reduceByKey(add) \
                .filter(lambda x: x[1] > 1 ) \
                .map(output)

    counts = counts.map(output);
    counts.coalesce(1).saveAsTextFile("PDCodeMapping.out")
    duplicates = duplicates.coalesce(1).saveAsTextFile("InvalidPDCodeMapping.out")
    sc.stop()