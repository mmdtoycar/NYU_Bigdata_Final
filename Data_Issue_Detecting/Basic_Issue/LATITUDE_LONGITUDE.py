from __future__ import print_function
import re
import sys
from operator import add
from pyspark import SparkContext
from csv import reader

def output(Pair):
    return "%s\t%s" % (Pair[0], Pair[1])

def process(x):
    baseType = "DECIMAL"
    semanticType = "Latitude&Longitude"
    if x[0] == "" or x[1] == "":
        return (x[2], "{}\t{}\tNULL".format(baseType, semanticType))
    elif x[2] == "({}, {})".format(x[0],x[1]):
        return (x[2], "{}\t{}\tValid".format(baseType, semanticType))
    else :
        return (x[2], "{}\t{}\tInvalid".format(baseType, semanticType))

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: task <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    header = lines.first()
    lines = lines.filter(lambda line: line != header)

    lines = lines.mapPartitions(lambda x : reader(x))
    counts = lines.map(lambda x: (x[21],x[22],x[23])) \
            .map(process) \
            .sortByKey() \
            .map(output)
    counts.coalesce(1).saveAsTextFile("LATITUDE_LONGITUDE.out")
    sc.stop()