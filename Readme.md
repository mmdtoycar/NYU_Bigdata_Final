# NYU Bigdata Final Project(CS-GY 9223)

**NYU_Bigdata_Final** is a project developed for the **CS-GY_9223 Big Data Analytics** at NYU, which aims to analyze [NYC Crime Data](https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i) of the latest 10 years (2006 - 2015). The project includes detecting data issue, data cleaning and data summary, and data hypotheses.
## Group Members:
- Minda Fang (mf3308)
- Qiming Zhang (qz718)
- Mingdi Mao (mm8688)

## Project Structure

The structure of our project is below:

* [Data issue detecting](Data_Issue_Detecting)
	* [Basic issues](Data_Issue_Detecting/Basic_Issue) 
	* [Other issues](Data_Issue_Detecting/Other_Issue)
* [Data clean code](Data_Issue_Detecting/)
* [Data summary](Data_Summary) 
* [Results](Results)
	* [Output](Results/Output)
	* [Plot](Results/Plot)	 

**Data issue detecting** is to examine the quality of the datasets. Basic issues include checking the existence of null values, range problem, unexpected type problem. Other issues include checking column correlation mapping prolem and some other problems like unique primary key.

**Data clean code** focus on cleaning the dataset based on the issues examined in data issues detecting part. The main script for clean the data is [DataClean.py](Data_Issue_Detecting/DataClean.py). Cleaned Data could be found via this [link](https://drive.google.com/open?id=0B53W-MZXrX4iUGVOSzJKZHF1dFU).

**Data summary** is to summarize the NYC crime dataset based on space, time, correlation between columns.  

## How to run the code:
All of our codes is done by using **Apache Spark**!

Follow the <u>Readme.md</u> in each section to run the code.


## Outcomes
All results and outcomes are in the [final report](FinalReport.pdf).
