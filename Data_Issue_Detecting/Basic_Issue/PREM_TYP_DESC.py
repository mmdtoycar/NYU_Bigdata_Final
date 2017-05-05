from __future__ import print_function
import re
import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from helper.CHECK_BASE_TYPE import checkBaseType

def output(Pair):
    return "%s\t%s" % (Pair[0], Pair[1])

def process(x):
    baseType = checkBaseType(x)
    semanticType = "Premises description"
    if x == "":
        return (x, "{}\t{}\tNULL".format(baseType, semanticType))
    else :
        return (x, "{}\t{}\tValid".format(baseType, semanticType))

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: task <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x : reader(x))
    counts = lines.map(lambda x: x[16].strip()) \
            .map(process) \
            .sortByKey() \
            .map(output)
    counts.coalesce(1).saveAsTextFile("PREM_TYP_DESC.out")
    sc.stop()