from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from csv import reader
def isNullValue(x):
	return not(x[0] == "" or x[1] == "" or x[2] == "" or x[3] =="" or x[5] == "" or
			x[6] == "" or x[7] == "" or x[8] == "" or x[9] == "" or x[10] == "" or 
			x[11] == "" or x[12] == "" or x[13] == "" or x[14] == "" or x[16] == "" 
			x[19] == "" or x[20] == "" or x[21] == "" or x[22] == "" or x[23] == "")
def isInSpecificRange(x):

def dataTypeCheck(x):

def mappingCheck(x):

def toStrip(x):
	for i in range(len(x)):
		x[i] = x[i].strip()


location = ("INSIDE", "OPPOSITE OF", "FRONT OF", "REAR OF", "")
area = ("MANHATTAN", "BRONX", "BROOKLYN", "QUEENS", "STATEN ISLAND")
crimeState = ("COMPLETED", "ATTEMPTED")
offenseLevel = ("FELONY", "MISDEMEANOR", "VIOLATION")


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
    cleanedData = lines.map(lambda x: toStrip) \
    				.filter(lambda x: isNullValue) \
    				.filter(lambda x: )
					                
    cleanedData.coalesce(1).saveAsTextFile("DataClean.out")
    