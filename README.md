# Zillow-s-Home-Value-Prediction-Zestimate-
Data Science Machine learning Project

# Creating Data as a Service: 

# 1. Data ingestion, EDA, Wrangling:
  - Download the data from Zillow. (https://www.kaggle.com/c/zillow-prize-1)
  
  - Programmatically write the data to a S3 bucket named “ZillowData”. (Use a configuration file to put your Amazon keys)
  
  - Dockeize this image
 
# 2. Create a DBaas (Database as a service)

-  Using the ATHENA, move the clean data to a S3 bucket cloud database.

• Note: Used command line to do this. Created a script that will take the clean data you created earlier and create a DBaas. 

- Jupyter notebook illustrating to run sample queries connecting to the AWS and MongoDBs and getting back the data.


To Run the data wrangling from docker images :

docker run megha8/assignment2:latest
This will get the data wrangling and upload the dataset to S3 bucket.

For Exploratory Data Analysis step Run the EDA.py 
Which will give the detaile analysis of the dataset.

For running the data set in mongoDB using flask use the file 
Flask.py Which will run the querry for nearest latitude and longitude on Ec2 instance on dataset using MongoDB



