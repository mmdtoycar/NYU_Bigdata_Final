# NYU_Bigdata_Final
NYU Bigdata final project

group members:
Minda Fang mf3308
Qiming Zhang qz718
Mingdi Mao mm8688

All of our codes is done by using Spark!

Approaches to run the code :

spark-submit [filename] NYPD_Complaint_Data_Historic.csv
and it will generate the output file in the same directory.

Codes are divided into three parts below:
	1) data quality issues
	2) data clean code
	3) data summary

data quality issues will examine each column in the data set, and will provide a script that assigns to each value:
1) a base type (i.e.,  INT/LONG, DECIMAL, TEXT, maybe DATETIME)
2) a semantic data type 
3) a label from the set [NULL -> missing or unknown information, VALID -> valid value from the intended domain of the column, INVALID-> suspicious or invalid values]

Data clean code (DataClean.py) will clean the original crime data file.

Data summary will generate some interesting statistics result by using the cleaned data file. 
