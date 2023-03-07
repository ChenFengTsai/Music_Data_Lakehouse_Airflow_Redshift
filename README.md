# Music_Data_Lakehouse_Airflow_Redshift

## Overview
* Extracted music data from Spotify Web API and built end-to-end pipeline from data ingestion to Athena analysis
* Utilized Reshift to load and unload data from S3 and utilized Glue to crawl the data
* Integrated with slack notification function in airflow to gave failure notification when pipeline failed

## Tools
Python, SQL, Airflow, Redshift, Athena, Glue S3

![alt text](https://github.com/ChenFengTsai/Music_Data_Lakehouse_Airflow_Redshift/blob/337ad63f07f44ce8a7ca12290714ef079b413ed1/Data_lakehouse_Architecture.png)

For this section, I would implement a data lakehouse solution in AWS with airflow for Spotify data analytics workloads. We are using MWAA here, which is the AWS version of airflow and it is more convenient to manage with other modules. A more comprehensive walk through is on my [Medium](https://medium.com/@rich.tsai1103/data-lakehouse-with-airflow-for-spotify-analytics-workloads-67ac42f33adb)

## Pre-requirements :
* Save all the finalized dags, sql query, and airflow configuration in one S3 bucket
* Save Spotify data file (retrieved using Spotipy package or Spotify Web API) in another S3 bucket.
* You could configure the airflow and utilize airflow UI to see the process.

## Workflow Steps
* Task 1. Create empty tables in Redshift
* Task 2. Load data from S3 bucket into Redshift table
We first create the staging tables and load corresponding data from S3 into these tables. Then we merge the staging tables into the original tables we created in task 1.
* Task 3. Unload aggregated data from Redshift to S3 bucket
* Task 4. Use Glue to retrieve metadata in S3 and Athena to query data for analysis

We have finalized the data lakehouse workflows from migrating raw data in S3 to Redshift warehouse and saving cleaned unstructured data to S3 for modeling purpose or analysis purpose using Glue and Athena.

