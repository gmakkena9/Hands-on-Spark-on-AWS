import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date, upper, coalesce, lit
from awsglue.dynamicframe import DynamicFrame

## Initialize contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# --- Define S3 Paths ---
s3_input_path = "s3://handsonfinallanding-unc801438826/"
s3_processed_path = "s3://handsonfinalprocessed-unc801438826/processed-data/"
s3_analytics_base = "s3://handsonfinalprocessed-unc801438826/Athena Results/"

# --- Read the data from the S3 landing zone ---
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [s3_input_path], "recurse": True},
    format="csv",
    format_options={"withHeader": True, "inferSchema": True},
)

df = dynamic_frame.toDF()

# --- Data Cleansing ---
# Cast rating to integer first so non-numeric/blank values become null
df = df.withColumn("rating", col("rating").cast("integer"))

# Drop rows missing a valid rating or customer_id
df_clean = df.dropna(subset=["rating", "customer_id"])

# --- Transformations ---
df_transformed = df_clean.withColumn(
    "review_date", to_date(col("review_date"), "yyyy-MM-dd")
)
df_transformed = df_transformed.withColumn(
    "review_text", coalesce(col("review_text"), lit("No review text"))
)
df_transformed = df_transformed.withColumn(
    "product_id_upper", upper(col("product_id"))
)

# --- Write the full cleaned dataset to S3 as Parquet ---
glue_processed_frame = DynamicFrame.fromDF(df_transformed, glueContext, "transformed_df")
glueContext.write_dynamic_frame.from_options(
    frame=glue_processed_frame,
    connection_type="s3",
    connection_options={"path": s3_processed_path},
    format="parquet",
)

# --- Run Spark SQL Analytics ---
df_transformed.createOrReplaceTempView("product_reviews")


def write_query_result(sql, name):
    """Run a SQL query and write the single-file Parquet result to its own subfolder."""
    result_df = spark.sql(sql).repartition(1)
    result_frame = DynamicFrame.fromDF(result_df, glueContext, name)
    glueContext.write_dynamic_frame.from_options(
        frame=result_frame,
        connection_type="s3",
        connection_options={"path": f"{s3_analytics_base}{name}/"},
        format="parquet",
    )
    print(f"Wrote {name} results to {s3_analytics_base}{name}/")


# 1. Daily Review Counts
write_query_result(
    """
    SELECT review_date, COUNT(*) AS review_count
    FROM product_reviews
    GROUP BY review_date
    ORDER BY review_date
    """,
    "daily_review_counts",
)

# 2. Top 5 Most Active Customers
write_query_result(
    """
    SELECT customer_id, COUNT(*) AS review_count
    FROM product_reviews
    GROUP BY customer_id
    ORDER BY review_count DESC
    LIMIT 5
    """,
    "top_5_customers",
)

# 3. Overall Rating Distribution
write_query_result(
    """
    SELECT rating, COUNT(*) AS count
    FROM product_reviews
    GROUP BY rating
    ORDER BY rating
    """,
    "rating_distribution",
)

# Bonus: keep the original average-rating-per-product query too
write_query_result(
    """
    SELECT product_id_upper, AVG(rating) AS average_rating, COUNT(*) AS review_count
    FROM product_reviews
    GROUP BY product_id_upper
    ORDER BY average_rating DESC
    """,
    "average_rating_by_product",
)

job.commit()