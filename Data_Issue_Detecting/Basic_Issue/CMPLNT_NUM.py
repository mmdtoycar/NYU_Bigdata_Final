from __future__ import print_function

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
    baseType = checkBaseType(x[0])
    semanticType = "Complaint_Num"
    if x[0] == "":
        return (x[0], "{}\t{}\tNULL".format(baseType, semanticType))
    elif(x[0] not in counts):
        return (x[0], "{}\t{}\tValid".format(baseType, semanticType))
    else :
        return (x[0], "{}\t{}\tInvalid".format(baseType, semanticType))

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
    counts = lines.map(lambda x: (x[0].strip(), 1)) 
    # store the internal result
    element = counts
    counts = counts.reduceByKey(add) \
            .filter(lambda x : (ifIsNotValidNumberString(x[0]))) \
            .map(lambda x: x[0]) \
            .collect()
    element = element.map(process) \
            .sortByKey() \
            .map(output)
    element.coalesce(1).saveAsTextFile("CMPLNT_NUM.out")
    sc.stop()