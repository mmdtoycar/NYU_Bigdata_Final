from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from Basic_Issue.helper.CHECK_BASE_TYPE import checkBaseType,isIntOrNot, isFloatOrNot
from Basic_Issue.helper.STRING_UTILS import ifNotValidThreeDigitString
from Basic_Issue.helper.CHECK_VALID_DATE import ifIsNotValidDateString, ifIsNotValidTimeString
from Other_Issue.helper.removeConflictPrecinctBoro import ifIsInvalidPrecinctBoroRecord
import os

def loadData():
    if os.path.isfile('./Other_Issue/InvalidPrecinct_BORO_Mapping.out/part-00000'):
        file = open("./Other_Issue/InvalidPrecinct_BORO_Mapping.out/part-00000")
        while 1:
            line = file.readline().rstrip('\n')
            if not line:
                break
            [key, value] = line.split("\t")
            if not invalidPrecinctBoroRecord.has_key(key):
                invalidPrecinctBoroRecord[key] = []
            if value not in invalidPrecinctBoroRecord.get(key):
                invalidPrecinctBoroRecord.get(key).append(value)


def isNullValue(x):
	return not(x[0] == "" or (x[1] == "" and x[2] != "" or x[1] != "" and x[2] == "") or
            (x[3] == "" and x[4] != "" or x[3] != "" and x[4] == "") or x[5] == "" or 
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
invalidPrecinctBoroRecord = {}

loadData()

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
                    .filter(isNullValue and dataTypeCheck and isInSpecificRange) \
                    .filter(lambda x: not ifIsInvalidPrecinctBoroRecord(x[14], x[13], invalidPrecinctBoroRecord))
                    # .filter(mappingCheck)
    cleanedData.coalesce(1).saveAsTextFile("DataClean.out")
    