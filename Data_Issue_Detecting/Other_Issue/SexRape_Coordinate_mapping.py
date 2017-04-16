from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

def output(Pair):
    return "%s\t%s" % (Pair[0], Pair[1])

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: task <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    header = lines.first()
    lines = lines.filter(lambda line: line != header)

    lines = lines.mapPartitions(lambda x : reader(x))
    counts = lines.map(lambda x: (x[7],x[19],x[20],x[21],x[22],x[23])) \
            .filter(lambda x: x[0] == "RAPE" or x[0] == "SEX CRIMES") \
            .filter(lambda x: x[1] != "" or x[2] != "" or x[3] != "" or x[4] != "" or x[5] != "") \
            .map(lambda x: (x[0], (x[1],x[2],x[3],x[4],x[5]))) \
            .sortByKey() \
            .map(output)
    counts.coalesce(1).saveAsTextFile("SexRape_Coordinate_mapping.out")
    sc.stop()