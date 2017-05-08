# NYU_Bigdata_Final

`NYU_Bigdata_Final` is a project developed for the `DS1004 Big Data Analytics` at NYU, which aims to analyze [NYC Crime Data](https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i) of the latest 10 years (2006 - 2015). The project includes detecting data issue, data cleaning and data summary, and data hypotheses.
## group members:
- Minda Fang mf3308
- Qiming Zhang qz718
- Mingdi Mao mm8688

## Project structure

The structure of our project is below:

* Data issue detecting
	* [Basic issues](Data_Issue_Detecting/Basic_Issue) 
	* [Other issues](Data_Issue_Detecting/Other_Issue)
* [Data clean code](Data_Cleaning_Code)
* [Data summary](Data_Summary) 
* [DataHypothesis](DataHypothesis)
* Results
	* [Output](Results/Output)
	* [Plot](Results/plot)	 

**Data issue detecting** is to examine the quality of the datasets. Basic issues include checking the existence of null values, range problem, unexpected type problem. Other issues include checking column correlation mapping prolem and some other problems like unique primary key.

**Data clean code** focus on cleaning the dataset based on the issues examined in data issues detecting part. The main script for clean the data is [DataClean.py](Data_Issue_Detecting/DataClean.py).

**Data summary** is to summarize the NYC crime dataset based on space, time, correlation between columns.  

**Data hypothesis** 

###Follow the Readme.md in each section to run the code!

## Approaches to run the code :
All of our codes is done by using **Spark**!

`spark-submit [filename] NYPD_Complaint_Data_Historic.csv`



## Outcomes
All result and outcome will be published after the end of this course. See the partial outcomes in [final report](FinalReport.pdf).