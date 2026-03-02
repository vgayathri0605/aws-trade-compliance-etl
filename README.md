AWS Trade Compliance ETL (Serverless)
Project Overview:
This project implements a serverless ETL pipeline on AWS to process trade compliance CSV files automatically when uploaded to S3.

Architecture:
S3 → Lambda → CloudWatch

Workflow:
Upload CSV to S3 bucket
Lambda automatically triggers
CSV file is parsed
Trade validation rules applied
Results logged to CloudWatch

Technologies Used:
Python
AWS Lambda
Amazon S3
IAM
CloudWatch

Resume Statement:
Designed and deployed a serverless AWS ETL pipeline using S3-triggered Lambda with IAM-based security and CloudWatch monitoring.