from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from helper.CHECK_BASE_TYPE import checkBaseType,isIntOrNot, isFloatOrNot
from helper.STRING_UTILS import ifNotValidThreeDigitString
from helper.CHECK_VALID_DATE import ifIsNotValidDateString, ifIsNotValidTimeString

def isNullValue(x):
	return not(x[0] == "" or x[1] == "" or x[2] == "" or x[3] =="" or x[5] == "" or
			x[6] == "" or x[7] == "" or x[8] == "" or x[9] == "" or x[10] == "" or 
			x[11] == "" or x[12] == "" or x[13] == "" or x[14] == "" or x[16] == "" or
			x[19] == "" or x[20] == "" or x[21] == "" or x[22] == "" or x[23] == "")
def isInSpecificRange(x):
    return x[15] in location and x[13] in area and x[10] in crimeState and x[11] in offenseLevel
def dataTypeCheck(x):
    return isIntOrNot(x[0]) and (not ifIsNotValidTimeString(x[2])) and (not ifIsNotValidDateString(x[1])) \
    and (not ifIsNotValidTimeString(x[4])) and (not ifIsNotValidDateString(x[3])) and (not ifIsNotValidDateString(x[5])) \
    and (not ifNotValidThreeDigitString(x[6])) and (not ifNotValidThreeDigitString(x[8])) \
    and isFloatOrNot(x[19]) and isFloatOrNot(x[20]) and isFloatOrNot(x[21]) and \
    isFloatOrNot(x[22])
def mappingCheck(x):
    pass
def toStrip(x):
    for i in range(len(x)):
       x[i] = x[i].strip()
    return x

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
    cleanedData = lines.map(toStrip) \
                    .filter(isNullValue) \
    				.filter(dataTypeCheck) \
                    .filter(isInSpecificRange)
 
    cleanedData.coalesce(1).saveAsTextFile("DataClean.out")
    