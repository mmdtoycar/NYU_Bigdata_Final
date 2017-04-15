from __future__ import print_function

import re
import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from helper.CHECK_BASE_TYPE import checkBaseType
from helper.CHECK_VALID_DATE import ifIsNotValidDateString

def output(Pair):
    return "%s\t%s" % (Pair[0], Pair[1])

def process(x):
    baseType = checkBaseType(x[1])
    semanticType = "Reporting_Date"
    if x[1] == "":
        return (x[1], "{}\t{}\tNULL".format(baseType, semanticType))
    elif(x[0] not in counts):
        return (x[1], "{}\t{}\tValid".format(baseType, semanticType))
    else :
        return (x[1], "{}\t{}\tInvalid".format(baseType, semanticType))

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
    counts = lines.map(lambda x: (x[0].strip(), x[5].strip())) 
    # store the internal result
    element = counts
    counts = counts.filter(lambda x : (ifIsNotValidDateString(x[1]))) \
            .map(lambda x: x[0]) \
            .collect()
    element = element.map(process) \
            .sortByKey() \
            .map(output)
    element.saveAsTextFile("RPT_DT.out")
    sc.stop()