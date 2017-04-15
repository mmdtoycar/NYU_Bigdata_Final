from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

def output(Pair):
    return "%s\t%s" % (Pair[0], Pair[1])

def process(Pair):
    value = list(Pair[1])
    value = len(value)
    return (Pair[0], value)

def ifIsNotValidNumberString(str):
    return not str.isdigit()

def filt(x):
    return (x[0], 1)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: task <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x : reader(x))
    
    counts = lines.map(filt)

    counts = counts.groupByKey().filter(lambda x : len(list(x[1])) > 1 or ifIsNotValidNumberString(x[0])).map(process).sortByKey().map(output)

    counts.saveAsTextFile("task.out")

    sc.stop()