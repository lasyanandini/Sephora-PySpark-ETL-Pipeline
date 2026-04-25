# Sephora PySpark ETL Pipeline

## Project Overview

This project focuses on building a real-world ETL pipeline using PySpark on the Sephora Products & Reviews dataset.

The goal was to process large-scale product and customer review data, clean and transform it, generate business-ready insights, and store the final output in optimized Parquet format for faster querying and analytics.

This project helped me understand how production-grade data pipelines work in real Data Engineering environments.

---

## Problem Statement

Raw CSV files often contain:
- Null values
- Duplicate records
- Inconsistent data types
- Multiple datasets that need joining
- Performance issues with large-scale processing

The challenge was to transform this raw Sephora dataset into clean, structured, analytics-ready data using PySpark.

---

## Dataset Used

### Source:
Kaggle - Sephora Products and Skincare Reviews Dataset

### Includes:
- Product details
- Product categories
- Brand information
- Ratings
- Customer reviews
- Review timestamps

---

## Tech Stack

- Python
- PySpark
- Databricks
- Spark SQL
- Parquet
- Kaggle Dataset

---

## ETL Pipeline Flow

### Step 1: Data Ingestion
- Read large CSV datasets into PySpark DataFrames

### Step 2: Data Cleaning
- Handle null values
- Remove duplicate records
- Fix inconsistent data types

### Step 3: Data Transformation
- Standardize product and brand information
- Join product and review datasets using `product_id`
- Create aggregations like:
  - Average rating per product
  - Total reviews per product

### Step 4: Advanced Processing
- Apply Window Functions:
  - ROW_NUMBER()
  - RANK()
- Identify top-performing products
- Find latest customer reviews

### Step 5: Performance Optimization
- Used:
  - repartition()
  - coalesce()
  - broadcast joins
  - caching concepts

### Step 6: Data Storage
- Save final transformed output in Parquet format for optimized querying and storage efficiency

---

## Business Use Case

This pipeline helps businesses answer questions like:

- Which products have the highest ratings?
- Which brands perform best?
- Which categories receive the most customer engagement?
- Which products are trending based on recent reviews?

This supports business teams, analysts, and dashboards with clean and reliable data.

---

## Key Learnings

Through this project, I learned:

- Real-world ETL pipeline development
- Distributed data processing using PySpark
- Performance optimization techniques in Spark
- Handling large-scale data transformations
- Building production-style Data Engineering workflows

---

## Future Improvements

- Load final output into AWS S3
- Connect to Snowflake / Redshift
- Build automated workflows using Airflow
- Create Power BI dashboard on transformed data

---

## Author

Lasya Nandini

Making data simple and practical.