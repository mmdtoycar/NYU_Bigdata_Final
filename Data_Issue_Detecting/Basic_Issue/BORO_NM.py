from __future__ import print_function
import re
import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from helper.CHECK_BASE_TYPE import checkBaseType

def output(Pair):
    return "%s\t%s" % (Pair[0], Pair[1])

def ifIsNotValidNumberString(str):
    return not str.isdigit()

def process(x):
    baseType = checkBaseType(x)
    semanticType = "Name of borough"
    if x == "":
        return (x, "{}\t{}\tNULL".format(baseType, semanticType))
    elif x.upper() in area:
        return (x, "{}\t{}\tValid".format(baseType, semanticType))
    else :
        return (x, "{}\t{}\tInvalid".format(baseType, semanticType))
area = ("MANHATTAN", "BRONX", "BROOKLYN", "QUEENS", "STATEN ISLAND")
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: task <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    header = lines.first()
    lines = lines.filter(lambda line: line != header)
    lines = lines.mapPartitions(lambda x : reader(x))
    counts = lines.map(lambda x: x[13].strip()) \
            .map(process) \
            .sortByKey() \
            .map(output)
    counts.saveAsTextFile("BORO_NM.out")
    sc.stop()