from __future__ import print_function
import re
import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from helper.CHECK_BASE_TYPE import isIntOrNot, isFloatOrNot

def output(Pair):
    return "%s\t%s" % (Pair[0], Pair[1])

def checkBaseType(x):
    if isIntOrNot(x):
        return "INT"
    elif isFloatOrNot(x):
        return "DECIMAL"
    elif re.match(r"^(0?[1-9]|1[012])/(0?[1-9]|[12][0-9]|3[01])/\d{4}$", x) \
        or re.match(r"^(0?[1-9]|1[0-9]|2[0-3]):(0?[1-9]|[1-5][0-9]):(0?[1-9]|[1-5][0-9])$", x):
        return "DATETIME"
    else:
        return "TEXT"

def process(x):
    baseType = checkBaseType(x)
    semanticType = "Y-Coordinate"
    if x == "":
        return (x, "{}\t{}\tNULL".format(baseType, semanticType))
    elif isIntOrNot(x) or isFloatOrNot(x):
        return (x, "{}\t{}\tValid".format(baseType, semanticType))
    else :
        return (x, "{}\t{}\tInvalid".format(baseType, semanticType))
 
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: task <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x : reader(x))
    counts = lines.map(lambda x: x[20].strip()) \
            .map(process) \
            .sortByKey() \
            .map(output)
    counts.coalesce(1).saveAsTextFile("Y_COORD_CD.out")
    sc.stop()