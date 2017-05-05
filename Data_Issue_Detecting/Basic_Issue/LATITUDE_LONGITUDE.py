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
    if x[0][0] == "" or x[0][1] == "":
        return (x[1], "{}\t{}\tNULL".format(baseType, semanticType))
    elif x[1] == "({}, {})".format(x[0][0],x[0][1]):
        return (x[1], "{}\t{}\tValid".format(baseType, semanticType))
    else :
        return (x[1], "{}\t{}\tInvalid".format(baseType, semanticType))

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: task <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x : reader(x))
    counts = lines.map(lambda x: ((x[21],x[22]),x[23]) ) 
    invalidMappings = counts.filter(lambda x: x[0][0] != "" and x[0][1] != "" and x[1] !="") \
                     .filter(lambda x: x[1] != "({}, {})".format(x[0][0],x[0][1])) \
                     .map(output)
    counts = counts.map(process) \
            .sortByKey() \
            .map(output)
    invalidMappings.coalesce(1).saveAsTextFile("INVALIDMAPPINGS.out")
    counts.coalesce(1).saveAsTextFile("LATITUDE_LONGITUDE.out")
    sc.stop()