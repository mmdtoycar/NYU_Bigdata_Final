from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from csv import reader

def output(Pair):
    return "%s\t%s" % (Pair[0], Pair[1])

def ifHasConfilt(x):
    return x in invalidCollect

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
    counts = lines.filter(lambda x: x[8].strip() != "" and x[6].strip() != "") \
                 .map(lambda x: ((x[8], x[6]), 1)) 
    conflicts = counts

    counts = counts.reduceByKey(add) \
            .sortByKey()

    invalid2 = lines.filter(lambda x: x[8].strip() != "" and x[6].strip() != "") \
                 .map(lambda x: ((x[8], x[6]), 1)) \
                 .reduceByKey(add) \
                 .map(lambda x: (x[0][0], x[0][1])) \
                 .groupByKey() \
                 .filter(lambda x: len(x[1]) > 1) \
                 .map(lambda x: x[0])

    invalidCollect = invalid2.collect();

    # invalid = counts.map(lambda x: (x[0], 1)) \
    #             .reduceByKey(add) \
    #             .filter(lambda x: x[1] > 1) \
    #             .map(lambda x: x[0]) \
    #             .sortByKey() \
    #             .map(output)

    conflicts = counts.filter(lambda x: (ifHasConfilt(x[0][0]))) \
                    .sortByKey()
                    

    invalidOutput = conflicts.filter(lambda x: int(x[1]) < 50) \
                    .map(lambda x: (x[0][0], x[0][1])) \
                    .sortByKey() \
                    .map(output)

    conflicts.map(output)

    counts = counts.map(output);
    counts.coalesce(1).saveAsTextFile("PD_OFNS_Mapping.out")
    conflicts.coalesce(1).saveAsTextFile("ConflictPD_OFNS_Mapping.out")
    invalidOutput.coalesce(1).saveAsTextFile("InvalidPD_OFNS_Mapping.out")

    sc.stop()