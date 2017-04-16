# this file explores the CRM_ATPT_CPTD_CD column,
# value ranges in completed and attempted. Any value out of this 
# range will be considered Invalid
from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from helper.CHECK_BASE_TYPE import checkBaseType

def output(Pair):
    return "%s\t%s" % (Pair[0], Pair[1])

def process(x):
    baseType = checkBaseType(x)
    semanticType = "Indicator_of_Crime_Completion"
    if x == "":
        return (x, "{}\t{}\tNULL".format(baseType, semanticType))
    elif x.upper() in range:
        return (x, "{}\t{}\tValid".format(baseType, semanticType))
    else :
        return (x, "{}\t{}\tInvalid".format(baseType, semanticType))

range = ("COMPLETED", "ATTEMPTED")
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: task <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    header = lines.first()
    lines = lines.filter(lambda line: line != header)
    lines = lines.mapPartitions(lambda x : reader(x))
    counts = lines.map(lambda x: x[10].strip()) \
            .map(process) \
            .sortByKey() \
            .map(output)
    counts.coalesce(1).saveAsTextFile("CRM_ATPT_CPTD_CD.out")
    sc.stop()