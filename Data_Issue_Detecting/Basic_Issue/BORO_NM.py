from __future__ import print_function
import re
import sys
from operator import add
from pyspark import SparkContext
from csv import reader
def output(Pair):
    return "%s\t%s" % (Pair[0], Pair[1])

def ifIsNotValidNumberString(str):
    return not str.isdigit()
def checkBaseType(x):
    if x.isdigit():
        return "INT"
    elif unicode(x).isdecimal():
        return "DECIMAL"
    elif re.match(r"^(0?[1-9]|1[012])/(0?[1-9]|[12][0-9]|3[01])/\d{4}$", x) \
        or re.match(r"^(0?[1-9]|1[0-9]|2[0-3]):(0?[1-9]|[1-5][0-9]):(0?[1-9]|[1-5][0-9])$", x):
        return "DATETIME"
    else:
        return "TEXT"
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
    counts.coalesce(1).saveAsTextFile("BORO_NM.out")
    sc.stop()