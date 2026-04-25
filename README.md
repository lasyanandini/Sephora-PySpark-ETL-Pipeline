# Sephora PySpark ETL Pipeline

## Project Overview

This project focuses on building a real-world ETL pipeline using PySpark using the Sephora Products & Skincare Reviews dataset.

The objective was to process large-scale product and customer review data, clean and transform raw datasets, generate business-ready insights, and store the final output in optimized Parquet format for faster querying and analytics.

This project helped me understand how production-grade ETL pipelines work in real Data Engineering environments and how PySpark handles large datasets efficiently.

---

## Problem Statement

Raw CSV files usually contain:

- Null values
- Duplicate records
- Incorrect or inconsistent data types
- Multiple datasets that need proper joining
- Performance challenges when processing large-scale data

The challenge was to transform this raw Sephora dataset into clean, structured, analytics-ready data using PySpark and Data Engineering best practices.

---

## Dataset Used

### Source

Kaggle – Sephora Products and Skincare Reviews Dataset

### Dataset Includes

- Product details
- Product categories
- Brand information
- Product pricing
- Ratings
- Customer reviews
- Review timestamps
- Review titles and descriptions

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

- Loaded large CSV datasets into PySpark DataFrames
- Used schema inference for initial data loading
- Verified schema and record counts

### Step 2: Data Cleaning

- Checked null values across all columns
- Removed fully null rows
- Handled missing values using selective `fillna()`
- Fixed incorrect data types:
  - Converted rating to `double`
  - Converted submission date to `date`

### Step 3: Data Transformation

- Joined product and review datasets using `product_id`
- Standardized and prepared the base dataset for analysis
- Created reusable transformed DataFrame for downstream processing

### Step 4: Aggregations

Generated key business metrics such as:

- Average rating per product
- Total number of reviews per product
- Brand-wise product performance
- Category-level review analysis

### Step 5: Window Functions

Applied advanced PySpark window functions like:

- `ROW_NUMBER()`
- `RANK()`
- `DENSE_RANK()`
- `LAG()`

Use cases included:

- Ranking top products within each category
- Finding top 3 products per brand
- Getting the most recent review per product
- Running total of reviews over time
- Comparing current rating vs previous rating

### Step 6: Performance Optimization

Implemented Spark optimization concepts such as:

- `repartition()`
- `coalesce()`
- Broadcast joins
- Caching concepts
- Partition-aware processing

This improved execution efficiency for large-scale datasets.

### Step 7: Data Storage

- Saved final transformed output in Parquet format
- Used optimized storage for faster querying
- Improved performance compared to traditional CSV storage

---

## Final Output Dataset

The final output contains:

- product_id
- product_name
- brand_name
- avg_rating
- total_reviews

This dataset is analytics-ready and can be directly used for dashboards, reporting, and downstream business analysis.

---

## Business Use Case

This pipeline helps businesses answer important questions like:

- Which products have the highest ratings?
- Which brands are performing the best?
- Which categories receive the most customer engagement?
- Which products are trending based on recent reviews?
- Which products need quality improvement based on low ratings?

This supports business teams, analysts, and dashboards with clean, reliable, and decision-ready data.

---

## Key Learnings

Through this project, I learned:

- Real-world ETL pipeline development using PySpark
- Distributed data processing at scale
- Spark performance optimization techniques
- Handling large-scale joins and aggregations
- Window functions for advanced analytics
- Data storage optimization using Parquet
- Building production-style Data Engineering workflows

---

## Future Improvements

Planned future enhancements:

- Load final output into AWS S3
- Integrate with Snowflake / Redshift
- Build automated workflows using Airflow
- Add incremental daily data loading
- Create Power BI dashboard on transformed data
- Deploy the full pipeline as an end-to-end Data Engineering project

---

## Author

Lasya Nandini

Making data simple, practical, and production-ready.
