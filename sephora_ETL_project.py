# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *


# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Data Reading

# COMMAND ----------

df_prod = spark.read.format('csv')\
    .option('header', True)\
    .option('inferschema', True)\
    .load('/Volumes/workspace/default/my_files/product_info.csv') 

df_reviews = spark.read.format('csv')\
    .option('header', True)\
    .option('inferschema', True)\
    .load('/Volumes/workspace/default/my_files/reviews_0-250.csv')


# COMMAND ----------

df_prod.display()

# COMMAND ----------

df_reviews.display()

# COMMAND ----------

df_reviews.count()

# COMMAND ----------

df_prod.select(col('brand_name')).distinct().display()

# COMMAND ----------

df_prod.select(col("size").isNull()).count()

# COMMAND ----------

df_reviews.display()

# COMMAND ----------

# MAGIC %md ### 2. Data Cleaning

# COMMAND ----------

# MAGIC %md
# MAGIC #### Find the columns with null values

# COMMAND ----------

df_reviews.select(
    [ sum(col(c).isNull().cast("int")).alias(c) for c in df_reviews.columns]
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Replace missing ratings with average rating

# COMMAND ----------

df_review_clean = df_reviews.withColumn('rating',\
    when(\
       col('rating').rlike('^[0-9.]+$'),\
       col('rating').cast('double')\
    ).otherwise(None)\
    )

# COMMAND ----------

avg_rating = df_review_clean.select(avg('rating')).first()[0]


# COMMAND ----------

df_final_review = df_review_clean.withColumn('rating', coalesce( col('rating'), lit(avg_rating)))

# COMMAND ----------

df_final_review.display()

# COMMAND ----------

df_final_review.filter(~col('rating').rlike('[0-9.]+$')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Remove duplicate records from reviews dataset

# COMMAND ----------

df_final_review = df_final_review.withColumn('review_text', col('review_text').isNull())
df_final_review.display()

# COMMAND ----------

df_final_review.dropDuplicates().count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Standardize brand names (e.g., "LOREAL", "Loreal" → "Loreal")

# COMMAND ----------

df_final_review.select(initcap('brand_name'))
df_final_review.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Handle empty or null review_text values

# COMMAND ----------

df_final_review.fillna('Not Available', subset=['review_text'])
df_final_review.display()


# COMMAND ----------

# MAGIC %md ### 3. Filtering & Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get all products where price > 2000
# MAGIC

# COMMAND ----------

df_prod.filter(col('price_usd') > 2000).display()

# COMMAND ----------

# MAGIC %md #### Filter reviews where rating = 5

# COMMAND ----------

df_final_review.filter(col('rating') == 5.0).display()

# COMMAND ----------

# MAGIC %md #### Extract all products belonging to category = "Skincare"

# COMMAND ----------

df_prod.filter(col('primary_category') == 'Skincare')

df_final_review.display()

# COMMAND ----------

# MAGIC %md  
# MAGIC #### Create a new column price_category
# MAGIC   price < 1000 → "Low"
# MAGIC   price between 1000 and 3000 → "Medium"
# MAGIC   price > 3000 → "High"
# MAGIC

# COMMAND ----------

df_prod = df_prod.withColumn('price_category', 
                   when(
                       col('price_usd') < 1000, 'Low'
                   ).
                   when(
                       (col('price_usd') > 1000) & (col('price_usd') < 3000), 'Medium price'
                   ).
                   otherwise('High')
)

df_prod.display()

# COMMAND ----------

# MAGIC %md #### Create a column review_length that counts number of characters in review_text

# COMMAND ----------

df_final_review = df_final_review.withColumn('review_length', length(col('review_text')))

df_final_review.display()

# COMMAND ----------

# MAGIC %md ### 4. Joins

# COMMAND ----------

# MAGIC %md #### Join products and reviews datasets using product_id

# COMMAND ----------

df_prod.display()

# COMMAND ----------

df_final_review.display()

# COMMAND ----------

df_prod.join(df_final_review, df_prod['product_id'] == df_final_review['product_id'], 'inner').display()

# COMMAND ----------

# MAGIC %md #### Get product name, brand name, and rating for each review

# COMMAND ----------

p = df_prod.alias('p')
r = df_final_review.alias('r')

p.join(r, p['product_id'] == r['product_id'], 'inner')\
.select(
  col('p.product_name'), col('p.brand_name'), col('r.rating')
  ).display() 

# df_prod.select(col('product_id'), col('brand_name'), col('rating'), col('reviews')).display()

# COMMAND ----------

# DBTITLE 1,er
# MAGIC %md #### Find products that have no reviews

# COMMAND ----------

p.join(r, "product_id", "left").\
    filter(col('r.review_title').isNull()).\
    select(col('p.product_name'), col('r.review_title'))\
        .display()

# COMMAND ----------

# MAGIC %md #### Find reviews that reference a product_id that does not exist in products dataset

# COMMAND ----------

r.join(p, r['product_id'] == p['product_id'], 'anti').display()

# COMMAND ----------

# MAGIC %md #### Count number of reviews per product

# COMMAND ----------

p.withColumn('reviews', expr('try_cast(reviews as int)'))\
    .groupby('product_name')\
        .agg(sum('reviews').alias("total_reviews"))\
            .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Aggregations (GROUP BY)

# COMMAND ----------

# MAGIC %md #### Find average rating per product
# MAGIC

# COMMAND ----------

p.join(r, 'product_id')\
    .groupBy('p.product_name').agg(avg('r.rating').alias('avg_rating'))\
        .display()


# p.withColumn('rating', expr("try_cast(rating as double)")).\
#     groupBy(col('product_name')).agg(avg(col('rating')).alias('avg_rating')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Find top 10 brands with highest number of products

# COMMAND ----------


p.groupBy(col('brand_name')) \
 .agg(count(col('product_name')).alias('highest_products')) \
 .orderBy(col('highest_products').desc()) \
 .limit(10)\
 .display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Count total number of reviews per category

# COMMAND ----------

p.join(r, 'product_id')\
    .groupBy("tertiary_category") \
        .agg(sum("reviews").alias("total_reviews")) \
            .display()

# COMMAND ----------

# MAGIC %md #### Find highest rated product in each category
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import avg, col, row_number
from pyspark.sql.window import Window

# Step 1: Join + aggregate
df = p.join(r, 'product_id') \
    .groupBy('p.product_id', 'p.product_name', 'p.primary_category') \
    .agg(avg('r.rating').alias('avg_rating'))

# Step 2: Define window
window = Window.partitionBy("primary_category") \
               .orderBy(col("avg_rating").desc())

# Step 3: Apply ranking
df_ranked = df.withColumn(
    "rank",
    row_number().over(window)
)

# Step 4 (Optional): Top N per category (example: top 3)
top_n = df_ranked.filter(col('rank') == 1)

# Show results
display(top_n)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Find average price per brand

# COMMAND ----------

p.groupBy('brand_name').agg(avg('price_usd').alias('avg_price')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Window Functions
# MAGIC

# COMMAND ----------

# MAGIC %md #### Rank products within each category based on average rating

# COMMAND ----------

from pyspark.sql.window import Window

df_temp = p.join(r, 'product_id')\
    .groupby('p.product_name', 'primary_category')\
        .agg(avg('r.rating').alias('avg_rating'))

window = window.partitionBy("primary_category").orderBy(col('avg_rating').desc())

df_rank = df_temp.withColumn(
    'rank',
    row_number().over(window) 
    )

df_rank.select(col('rank'), col('product_name'), col('primary_category'), col('avg_rating')).display()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Find top 3 products within each brand based on highest average rating

# COMMAND ----------

from pyspark.sql.window import Window

df_temp = p.join(r, 'product_id')\
    .select(
        col("p.product_name").alias("product_name"),
        col("p.brand_name").alias("brand_name"),
        col("r.rating").alias("rating"))\
    .groupby('product_name', 'brand_name')\
        .agg(avg('rating').alias('avg_rating'))

window = Window.partitionBy('brand_name').orderBy(col('avg_rating').desc())

result = df_temp.withColumn(
    'rank',
    row_number().over(window)
    )

result.filter(col("rank") <= 3).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get the latest review for each product

# COMMAND ----------

#r.select('submission_time').show(5, False) 
r.printSchema()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, to_date

# Step 1: Convert safely (use correct format!)
r.withColumn(
    "submission_time",
    to_date("submission_time", "yyyy-MM-dd") 
)

# Step 2: Join + select
df_temp = p.alias("p").join(r.alias("r"), 'product_id')\
    .select(
        col('p.product_name').alias('product_name'),
        col('r.review_title').alias('review_title'),
        col('r.submission_time').alias('submission_time'),
        col('r.product_id').alias('product_id')
    )

# Step 3: Window
window = Window.partitionBy('product_id')\
               .orderBy(col('submission_time').desc())

# Step 4: Rank
df_temp = df_temp.withColumn(
    'rank',
    row_number().over(window)
)

# Step 5: Filter
result = df_temp.filter(col('rank') == 1)

result.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Running Total No.of Reviews per product

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col, sum, to_date

df_temp = p.join(r, 'product_id')\
    .select( 
            col("p.product_id").alias("product_id"), 
            col("p.product_name").alias("product_name"), 
            col("r.submission_time").alias("submission_time"), 
            col("p.reviews").alias("review_count")
    )

df_temp = df_temp.withColumn( 'submission_time' , expr("try_cast( submission_time as date)"))

window = Window.partitionBy('product_id').orderBy(col('submission_time').asc())\
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Running total
df_temp = df_temp.withColumn(
    "running_total_reviews",
    sum(lit(1)).over(window)
)

result = df_temp.select(
    col('product_id'), col('product_name'), col("submission_time"), col('review_count'), col("running_total_reviews") 
)

result.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Difference between current rating and previous rating

# COMMAND ----------

from pyspark.sql.window import Window

df_temp1= p.alias('p').join(r.alias('r'), 'product_id')\
    .select(
        col('p.product_id').alias('product_id'),
        col('p.product_name').alias('product_name'),
        col('r.submission_time').alias('submission_time'),
        col('r.rating').alias('rating')
    )

df_temp1 = df_temp1.withColumn( 'submission_time' , expr("try_cast( submission_time as date)"))
df_temp1 = df_temp1.withColumn( 'rating' , col("rating").cast('double'))

window = Window.partitionBy('product_id').orderBy(col('submission_time').asc()) 

df_temp1 = df_temp1.withColumn('previous_rating', lag('rating', 1).over(window))

df_temp1 = df_temp1.withColumn('rating_difference', col('rating') - col('previous_rating'))

result = df_temp1.select(
    'product_id',
    'product_name',
    'submission_time',
    'rating',
    'previous_rating',
    'rating_difference'
)

result.display() 

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Performance Optimization

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check how many partitions are present in df_reviews

# COMMAND ----------

r.selectExpr('spark_partition_id()').distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Repartition the reviews dataset into 5 partitions

# COMMAND ----------

r = r.repartition(5)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reduce partitions to 2 using coalesce
# MAGIC

# COMMAND ----------

r = r.coalesce(2)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Perform a broadcast join between products and reviews

# COMMAND ----------

# As p is the smallest table

df_temp = r.join(broadcast(p), "product_id", "right")
df_temp.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cache the joined DataFrame and reuse it (e.g., perform two actions on it)

# COMMAND ----------

 

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Data Storage & Formats

# COMMAND ----------

# MAGIC %md
# MAGIC #### Save df_temp as a CSV file

# COMMAND ----------

# df_temp.write.format('csv')\
#     .mode('append')\
#     .save('Volumes/workspace/default/my_files/sephora_transformed_data.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Save the same DataFrame as Parquet format

# COMMAND ----------

df.write.format('parquet')\
	.mode('overwrite')\
	.option('path',
	'/Volumes/workspace/default/my_files/sephora_transformed_data')\
	.save() 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read the Parquet file back into a DataFrame

# COMMAND ----------

df_temp2 = spark.read.format('parquet')\
    .load('/Volumes/workspace/default/my_files/sephora_transformed_data')

# COMMAND ----------

df_temp2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Save data partitioned by product_name

# COMMAND ----------

df_temp2.write\
    .format("parquet")\
    .mode("overwrite")\
    .partitionBy("product_name")\
    .save("/Volumes/workspace/default/my_files/sephora_partitioned_by_product") 

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

p.display()

# COMMAND ----------

r.display()

# COMMAND ----------

# MAGIC %md
# MAGIC