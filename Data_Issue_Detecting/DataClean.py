from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from Basic_Issue.helper.CHECK_BASE_TYPE import checkBaseType,isIntOrNot, isFloatOrNot
from Basic_Issue.helper.STRING_UTILS import ifNotValidThreeDigitString
from Basic_Issue.helper.CHECK_VALID_DATE import ifIsNotValidDateString, ifIsNotValidTimeString
from Other_Issue.helper.removeInvalid import *
from pyspark.sql import SQLContext

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

    if os.path.isfile('./Other_Issue/InvalidDuplicateCMPNumber.out/part-00000'):
        file = open("./Other_Issue/InvalidDuplicateCMPNumber.out/part-00000")
        while 1:
            line = file.readline().rstrip('\n')
            if not line:
                break
            [key, value] = line.split("\t")
            duplicateCMPNumberList.add(key)

    if os.path.isfile('./Other_Issue/InvalidPDCodeMapping.out/part-00000'):
        file = open("./Other_Issue/InvalidPDCodeMapping.out/part-00000")
        while 1:
            line = file.readline().rstrip('\n')
            if not line:
                break
            [key, value] = line.split("\t")
            invalidPDCodeMappingList.add(key)

    if os.path.isfile('./Other_Issue/InvalidOffenseCodeMapping.out/part-00000'):
        file = open("./Other_Issue/InvalidOffenseCodeMapping.out/part-00000")
        while 1:
            line = file.readline().rstrip('\n')
            if not line:
                break
            [key, value] = line.split("\t")
            if not invalidOffenseCodeDetailRecord.has_key(key):
                invalidOffenseCodeDetailRecord[key] = []
            if value not in invalidOffenseCodeDetailRecord.get(key):
                invalidOffenseCodeDetailRecord.get(key).append(value)

    if os.path.isfile('./Other_Issue/InvalidPDCodeMapping.out/part-00000'):
        file = open("./Other_Issue/InvalidPDCodeMapping.out/part-00000")
        while 1:
            line = file.readline().rstrip('\n')
            if not line:
                break
            [key, value] = line.split("\t")
            if not invalidPDCodeDetailRecord.has_key(key):
                invalidPDCodeDetailRecord[key] = []
            if value not in invalidPDCodeDetailRecord.get(key):
                invalidPDCodeDetailRecord.get(key).append(value)

    if os.path.isfile('./Other_Issue/InvalidPD_OFNS_Mapping.out/part-00000'):
        file = open("./Other_Issue/InvalidPD_OFNS_Mapping.out/part-00000")
        while 1:
            line = file.readline().rstrip('\n')
            if not line:
                break
            [key, value] = line.split("\t")
            if not invalidPD_OFNSRecord.has_key(key):
                invalidPD_OFNSRecord[key] = []
            if value not in invalidPD_OFNSRecord.get(key):
                invalidPD_OFNSRecord.get(key).append(value)

    if os.path.isfile('./Other_Issue/InvalidSexRape_Geo.out/part-00000'):
        file = open("./Other_Issue/InvalidSexRape_Geo.out/part-00000")
        while 1:
            line = file.readline().rstrip('\n')
            if not line:
                break
            [key, value] = line.split("\t")
            invalidEmptyGeoSet.add(key)


def isNullValue(x):
	return not(x[0] == "" or (x[1] == "" and x[2] != "" or x[1] != "" and x[2] == "") or
            (x[3] == "" and x[4] != "" or x[3] != "" and x[4] == "") or x[5] == "" or 
			x[6] == "" or x[7] == "" or x[8] == "" or x[9] == "" or x[10] == "" or 
			x[11] == "" or x[12] == "" or x[13] == "" or x[14] == "" or x[16] == "" or
			not(x[19] == "" and x[20] == "" and x[21] == "" and x[22] == "" and x[23] == "" or
                x[19] != "" and x[20] != "" and x[21] != "" and x[22] != "" and x[23] != ""))
def isInSpecificRange(x):
    return x[15] in location and x[13] in area and x[10] in crimeState and x[11] in offenseLevel
def dataTypeCheck(x):
    return isIntOrNot(x[0]) and (not ifIsNotValidTimeString(x[2])) and (not ifIsNotValidDateString(x[1])) \
    and (not ifIsNotValidTimeString(x[4])) and (not ifIsNotValidDateString(x[3])) and (not ifIsNotValidDateString(x[5])) \
    and (not ifNotValidThreeDigitString(x[6])) and (not ifNotValidThreeDigitString(x[8])) \
    and isFloatOrNot(x[19]) and isFloatOrNot(x[20]) and isFloatOrNot(x[21]) and \
    isFloatOrNot(x[22])
def mappingCheck(x):
    return not ifInvalidMappingRecord(x[14], x[13], invalidPrecinctBoroRecord) \
        and not ifInvalidIndexRecord(x[0], duplicateCMPNumberList) \
        and not ifInvalidIndexRecord(x[8], invalidPDCodeMappingList) \
        and not ifInvalidMappingRecord(x[6], x[7], invalidOffenseCodeDetailRecord) \
        and not ifInvalidMappingRecord(x[8], x[6], invalidPD_OFNSRecord) \
        and not ifInvalidIndexRecord(x[0], invalidEmptyGeoSet)
def toStrip(x):
    for i in range(len(x)):
       x[i] = x[i].strip()
    return x

location = ("INSIDE", "OPPOSITE OF", "FRONT OF", "REAR OF", "")
area = ("MANHATTAN", "BRONX", "BROOKLYN", "QUEENS", "STATEN ISLAND")
crimeState = ("COMPLETED", "ATTEMPTED")
offenseLevel = ("FELONY", "MISDEMEANOR", "VIOLATION")
invalidPrecinctBoroRecord = {}
duplicateCMPNumberList = set()
invalidPDCodeMappingList = set()
invalidOffenseCodeDetailRecord = {}
invalidPDCodeDetailRecord = {}
invalidPD_OFNSRecord = {}
invalidEmptyGeoSet = set()


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
                    .filter(isNullValue) \
    				.filter(dataTypeCheck) \
                    .filter(isInSpecificRange) \
                    .filter(mappingCheck)
    sqlContext = SQLContext(sc)
    df = sqlContext.createDataFrame(cleanedData)
    df.coalesce(1).write.format('com.databricks.spark.csv').options(header='true').save("DataClean.csv")
    # cleanedData.coalesce(1).saveAsTextFile("DataClean.csv")

    