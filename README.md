# 🎧 Spotify Data Pipeline using AWS & Snowflake

This project builds a scalable and automated **ETL pipeline** that extracts playlist data from the **Spotify API**, transforms it using **AWS Glue (Apache Spark)**, and loads it into **Snowflake** using **Snowpipe**. The data is finally visualized using **Power BI** to generate meaningful insights.

---

## 📌 Project Overview

- **Objective:** Automate the extraction, transformation, and loading of Spotify playlist data.
- **Architecture:** Event-driven, serverless, and cloud-native.
- **Outcome:** A production-ready data pipeline that supports real-time ingestion and dashboarding.

---

## 🚀 Tech Stack

| Component     | Tool/Service           |
|---------------|------------------------|
| Data Source   | Spotify Web API        |
| Extraction    | AWS Lambda + CloudWatch |
| Storage       | Amazon S3              |
| Transformation| AWS Glue (Apache Spark)|
| Data Loading  | Snowpipe (Snowflake)   |
| Dashboard     | Power BI               |

---

## 🔄 Workflow

### 1. Extract  
- A **Lambda function** fetches playlist data from the Spotify API.  
- The job is scheduled using **CloudWatch Events** to run every 5 minutes.  
- Raw JSON data is saved to an S3 folder: `s3://<bucket>/raw_data/to_processed/`.

### 2. Transform  
- An **AWS Glue Job (Spark)** picks up the raw data from S3.  
- Data is cleaned and split into structured tables: Album, Artist, Song.  
- Transformed data is saved in:  

### 3. Load  
- Transformed files trigger **Snowpipe** using **S3 event notifications (via SQS)**.  
- Data is loaded into **Snowflake tables** automatically.  
- **Storage integration** is used to securely connect Snowflake with S3.

### 4. Visualize  
- Data from Snowflake is imported into **Power BI**.  
- Dashboards provide real-time insights into song trends, artist performance, and more.
