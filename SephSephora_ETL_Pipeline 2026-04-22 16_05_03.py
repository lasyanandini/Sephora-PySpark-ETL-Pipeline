# Databricks notebook source
# MAGIC %md
# MAGIC ### Section 1: Data Ingestion

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md #### Load product 

# COMMAND ----------

# Load Products Dataset
df_prod = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load("/Volumes/workspace/default/my_files/product_info.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load review

# COMMAND ----------

# Load Reviews Dataset
df_review = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load("/Volumes/workspace/default/my_files/reviews_0-250.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Section 2: Data Cleaning

# COMMAND ----------

# MAGIC %md #### Check & Handle nulls

# COMMAND ----------

df_prod.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in df_prod.columns
]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Remove fully null rows

# COMMAND ----------


df_prod = df_prod.dropna(how="all")
df_review = df_review.dropna(how="all")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Fill only required text columns

# COMMAND ----------

df_prod = df_prod.fillna({
    "brand_name": "Not Available",
    "product_name": "Not Available"
})

# COMMAND ----------

df_review = df_review.fillna({
    "review_text": "Not Available",
    "review_title": "Not Available"
})

# COMMAND ----------

# MAGIC %md
# MAGIC #### Fix rating datatype

# COMMAND ----------

df_review = df_review.withColumn(
    "rating",
    expr("try_cast(rating as double)")
)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Fix date datatype

# COMMAND ----------

df_review = df_review.withColumn(
    "submission_time",
    to_date(col("submission_time"), "yyyy-MM-dd")
)

# COMMAND ----------

# MAGIC %md ### Section 3: Transformations

# COMMAND ----------

df_joined = df_prod.alias("p") \
    .join(df_review.alias("r"), "product_id", "inner") \
    .select(
        col("p.product_id").alias("product_id"),
        col("p.product_name").alias("product_name"),
        col("p.brand_name").alias("brand_name"),
        col("r.rating").alias("rating"),
        col("r.review_text").alias("review_text"),
        col("r.submission_time").alias("submission_time")
    )

df_joined.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Section 4: Aggregations

# COMMAND ----------

# MAGIC %md
# MAGIC #### avg_rating & total_review

# COMMAND ----------

df_prod.display()

# COMMAND ----------

df_agg = df_joined.groupBy(
    "product_id",
    "product_name",
    "brand_name"
).agg(
    avg("rating").alias("avg_rating"),
    count("*").alias("total_reviews")
)

df_agg.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Section 5: Final Output

# COMMAND ----------

# MAGIC %md
# MAGIC #### Select final columns

# COMMAND ----------

# Select final required columns
df_final = df_agg.select(
    "product_id",
    "product_name",
    "brand_name",
    "avg_rating",
    "total_reviews"
)

df_final.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Section 6: Save final output as Parquet file

# COMMAND ----------

df_final.write.format("parquet") \
    .mode("overwrite") \
    .save("/Volumes/workspace/default/my_files/transformed_ETLpipeline_sephora")

# COMMAND ----------

