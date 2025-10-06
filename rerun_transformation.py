import argparse
import logging
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, lower, regexp_replace, to_date
from pyspark.sql.types import (BooleanType, DoubleType, FloatType,
                               IntegerType, StringType)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session(gcs_bucket_name: str) -> SparkSession:
    """
    Creates and configures the Spark Session to connect to the Hive Warehouse on GCS.
    """
    warehouse_path = f"gs://{gcs_bucket_name}/hive-warehouse"
    logger.info(f"Connecting to Hive Metastore Warehouse at: {warehouse_path}")

    return SparkSession.builder \
        .appName("InsideAirbnbTransformationOnly") \
        .config("spark.sql.warehouse.dir", warehouse_path) \
        .enableHiveSupport() \
        .getOrCreate()

def clean_price_column(price_col):
    """Cleans the price column by removing '$' and ',', and casts it to Double."""
    return regexp_replace(price_col, "[$,]", "").cast(DoubleType())

def transform_listings_df(df: DataFrame) -> DataFrame:
    """Applies specific transformations to the listings dataset."""
    logger.info("Transforming listings data...")
    numeric_cols = {
        "accommodates": IntegerType(), "bathrooms": FloatType(), "bedrooms": FloatType(),
        "beds": IntegerType(), "review_scores_rating": FloatType(), "number_of_reviews": IntegerType(),
        "minimum_nights": IntegerType(), "maximum_nights": IntegerType()
    }
    for col_name, col_type in numeric_cols.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, col(col_name).cast(col_type))

    if 'price' in df.columns:
        df = df.withColumn("price", clean_price_column(col("price")))

    df = df.withColumn("processed_date", lit(datetime.now().strftime('%Y-%m-%d')))
    return df

def transform_calendar_df(df: DataFrame) -> DataFrame:
    """Applies specific transformations to the calendar dataset."""
    logger.info("Transforming calendar data...")
    if 'date' in df.columns:
        df = df.withColumnRenamed("date", "calendar_date")

    for price_col in ['price', 'adjusted_price']:
        if price_col in df.columns:
            df = df.withColumn(price_col, clean_price_column(col(price_col)))

    if 'available' in df.columns:
        df = df.withColumn("available", lower(col("available")) == "t")

    return df

def transform_reviews_df(df: DataFrame) -> DataFrame:
    """Applies specific transformations to the reviews dataset."""
    logger.info("Transforming reviews data...")
    if 'date' in df.columns:
        df = df.withColumnRenamed("date", "review_date")
    if 'review_date' in df.columns:
        df = df.withColumn("review_date", to_date(col("review_date"), "yyyy-MM-dd"))
    if 'comments' in df.columns:
        df = df.withColumn("comments", regexp_replace(col("comments"), "[\n\r]", " "))
    return df

def overwrite_table(spark: SparkSession, df: DataFrame, table_name: str):
    """
    Overwrites an existing Hive table using a temporary view to avoid read/write conflicts.
    """
    db_name = "inside_airbnb"
    full_table_name = f"{db_name}.{table_name}"
    temp_view_name = f"{table_name}_temp_view"

    logger.info(f"Overwriting table: {full_table_name} using temporary view: {temp_view_name}")

    # Step 1: Create a temporary view from the transformed DataFrame.
    # This holds the results in memory, breaking the dependency on the source files.
    df.createOrReplaceTempView(temp_view_name)

    # Step 2: Use SQL INSERT OVERWRITE to atomically replace the table's contents.
    # This is the standard, safe way to overwrite a table you're also reading from.
    # Spark handles the staging of data behind the scenes.
    overwrite_sql = f"INSERT OVERWRITE TABLE {full_table_name} SELECT * FROM {temp_view_name}"
    spark.sql(overwrite_sql)

    # Step 3: Drop the temporary view to clean up.
    spark.catalog.dropTempView(temp_view_name)

    logger.info(f"Table {full_table_name} overwritten successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--gcs-bucket-name", required=True, help="GCS bucket name (e.g., your-bucket).")
    args = parser.parse_args()

    spark = create_spark_session(args.gcs_bucket_name)
    
    db_name = "inside_airbnb"
    spark.sql(f"USE {db_name}")
    logger.info(f"Using database: {db_name}")

    # --- Listings Transformation ---
    logger.info("--- Starting Listings Transformation ---")
    raw_listings_df = spark.table("listings")
    transformed_listings_df = transform_listings_df(raw_listings_df)
    overwrite_table(spark, transformed_listings_df, "listings")

    # --- Calendar Transformation ---
    logger.info("--- Starting Calendar Transformation ---")
    raw_calendar_df = spark.table("calendar")
    transformed_calendar_df = transform_calendar_df(raw_calendar_df)
    overwrite_table(spark, transformed_calendar_df, "calendar")

    # --- Reviews Transformation ---
    logger.info("--- Starting Reviews Transformation ---")
    raw_reviews_df = spark.table("reviews")
    transformed_reviews_df = transform_reviews_df(raw_reviews_df)
    overwrite_table(spark, transformed_reviews_df, "reviews")

    logger.info("All transformations have been re-applied successfully.")
    spark.stop()