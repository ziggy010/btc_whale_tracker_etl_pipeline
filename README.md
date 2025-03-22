# BTC Whale Tracker ETL Pipeline

*An automated pipeline tracking Bitcoin whale transactions in real-time.*

## Overview
This project is an end-to-end ETL pipeline that extracts Bitcoin transactions from the Blockchain.com API, identifies whale movements (>10 BTC), and loads them into Snowflake for analytics. Built to showcase data engineering skills with AWS and Snowflake—automated hourly, it captures wallet activity to spot market movers. Perfect for crypto analytics and Web3 insights!

### Features
- **Extract**: Pulls raw transaction data from Blockchain.com’s latest blocks via AWS Lambda.  
- **Transform**: Filters whale transactions (>10 BTC), adds `tx_day` field, and formats data for storage.  
- **Load**: Uses Snowpipe to auto-ingest processed data into Snowflake’s `staging.whale_txns`.  
- **Automation**: Runs hourly via CloudWatch triggers, with S3 event-driven Snowpipe ingestion.

## Architecture
![Pipeline Architecture](https://via.placeholder.com/600x300.png?text=BTC+Whale+Tracker+Architecture)  

1. **Blockchain.com API**: Fetches latest block transactions → `raw_data/whale_txns_YYYY-MM-DD_HH.json`.  
2. **AWS Lambda (Extract)**: Pulls data and stores raw JSON in S3.  
3. **AWS S3**: Holds raw JSON (`raw_data/`) and transformed CSV (`transformed_data/`).  
4. **AWS Lambda (Transform)**: Processes JSON, filters whales, outputs CSV.  
5. **Snowflake Snowpipe**: Loads CSV into `staging.whale_txns` on S3 updates.  
6. **AWS CloudWatch**: Triggers extract Lambda hourly.

## Tech Stack
- **Python**: `requests`, `pandas`, `boto3`—API calls, data handling, AWS integration.  
- **AWS**:  
  - **Lambda**: Serverless extract/transform functions.  
  - **S3**: Storage bucket (`s3://crypto-etl-bucket/`).  
  - **CloudWatch**: Hourly scheduler.  
- **Snowflake**: Data warehouse with Snowpipe for ingestion.

## Setup Instructions

### Prerequisites
- **Blockchain.com API**: Free access—no key needed (`https://blockchain.info`).  
- **AWS Account**: IAM roles, S3 bucket, Lambda setup—your basics.  
- **Snowflake Account**: Warehouse and database (`STAGING`) configured.

## Thank you!
