# this file explores the CMPLNT_FR_TM column

from __future__ import print_function
import re
import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from helper.CHECK_BASE_TYPE import checkBaseType
from helper.CHECK_VALID_DATE import ifIsNotValidTimeString

def output(Pair):
    return "%s\t%s" % (Pair[0], Pair[1])

def process(x):
    data = x[2].strip()
    baseType = checkBaseType(data)
    semanticType = "Complaint_Starting_Time"
    if data == "":
        return (data, "{}\t{}\tNULL".format(baseType, semanticType))
    elif ifIsNotValidTimeString(data):
        return (data, "{}\t{}\tInvalid".format(baseType, semanticType))
    else:
        return (data, "{}\t{}\tValid".format(baseType, semanticType))

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: task <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x : reader(x)) 
    counts = lines.map(process) \
            .sortByKey() \
            .map(output)
    counts.coalesce(1).saveAsTextFile("CMPLNT_FR_TM.out")
    sc.stop()