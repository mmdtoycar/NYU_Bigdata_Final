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
    data = x[4].strip()
    baseType = checkBaseType(data)
    semanticType = "Complaint_Ending_Time"
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
    #extract header
    header = lines.first()
    lines = lines.filter(lambda line: line != header)

    # lines = lines.mapPartitions(lambda x : reader(x))
    # counts = lines.map(lambda x: (x[0].strip(), x[4].strip())) 
    # # store the internal result
    # element = counts
    # counts = counts.filter(lambda x : (len(x[1]) != 0 and ifIsNotValidTimeString(x[1]))) \
    #         .map(lambda x: x[0]) \
    #         .collect()
    # element = element.map(process) \
    #         .sortByKey() \
    #         .map(output)
    lines = lines.mapPartitions(lambda x : reader(x)) 
    counts = lines.map(process) \
            .sortByKey() \
            .map(output)
    counts.coalesce(1).saveAsTextFile("CMPLNT_TO_TM.out")
    sc.stop()