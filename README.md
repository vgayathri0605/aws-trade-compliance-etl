# AWS Trade Compliance ETL (Serverless)

## Project Overview
This project implements a serverless ETL pipeline on AWS to automatically process trade compliance CSV files when uploaded to Amazon S3.

## Architecture
S3 → Lambda → CloudWatch

## Workflow
1. CSV file uploaded to S3 bucket
2. Lambda function triggers automatically
3. File is parsed and validated
4. Trade compliance rules applied
5. Results logged to CloudWatch

## Sample Input Data
A simulated trade feed CSV representing portfolio trades exported from a trading system. Includes portfolio type, instrument, and trade amount fields used for compliance validation.

## Technologies Used
- Python
- AWS Lambda
- Amazon S3
- IAM
- Amazon CloudWatch

## Key Features
- Event-driven serverless architecture
- Automated compliance validation
- Error handling and logging
- Free Tier cost optimized deployment

