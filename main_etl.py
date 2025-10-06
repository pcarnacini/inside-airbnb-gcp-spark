import argparse
import logging
import uuid
from datetime import datetime

import requests
import yaml
from google.cloud import storage
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, lower, regexp_replace, to_date
from pyspark.sql.types import (DoubleType, FloatType,
                               IntegerType)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session(gcs_bucket_name: str) -> SparkSession:
    """
    Creates and configures the Spark Session with Hive support, a GCS warehouse,
    and dynamic partition overwrite enabled to make the job idempotent.
    """
    warehouse_path = f"gs://{gcs_bucket_name}/hive-warehouse"
    logger.info(f"Setting Hive Metastore Warehouse to: {warehouse_path}")

    return SparkSession.builder \
        .appName("InsideAirbnbETL") \
        .config("spark.sql.warehouse.dir", warehouse_path) \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .enableHiveSupport() \
        .getOrCreate()

def download_and_read_csv(spark: SparkSession, url: str, bucket_name: str) -> DataFrame | None:
    """
    Downloads a .csv.gz file, streams it to a temporary location in GCS,
    reads it into a Spark DataFrame, caches the DataFrame in memory,
    and then deletes the temporary file. This is a robust method for handling
    large files and complex CSVs with multiline fields.
    """
    logger.info(f"Robustly downloading and reading from URL: {url}")
    
    temp_file_name = f"temp/{uuid.uuid4().hex}.csv.gz"
    temp_gcs_path = f"gs://{bucket_name}/{temp_file_name}"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(temp_file_name)
    
    df = None

    try:
        logger.info(f"Downloading {url} and streaming to {temp_gcs_path}")
        with requests.get(url, stream=True, timeout=300) as r:
            r.raise_for_status()
            blob.upload_from_file(r.raw, content_type='application/gzip')
        logger.info("Successfully uploaded to temporary GCS location.")

        df_reader = spark.read.csv(
            temp_gcs_path,
            header=True,
            multiLine=True,
            escape='"',
            quote='"',
            inferSchema=True
        )
        
        df = df_reader.cache()
        logger.info(f"Triggering cache for DataFrame. Row count: {df.count()}")
        
        return df

    except requests.RequestException as e:
        logger.error(f"Failed to download {url}: {e}")
        return None
    except Exception as e:
        logger.error(f"Failed to process file from {url}: {e}")
        if df:
            df.unpersist()
        return None
    finally:
        try:
            if blob.exists():
                logger.info(f"Deleting temporary file: {temp_gcs_path}")
                blob.delete()
                logger.info("Temporary file deleted.")
        except Exception as e:
            logger.warning(f"Could not delete temporary file {temp_gcs_path}: {e}")

def clean_price_column(price_col):
    """Cleans the price column by removing '$' and ',', and casts it to Double."""
    return regexp_replace(price_col, "[$,]", "").cast(DoubleType())

def clean_text_field(text_col):
    """Cleans text fields by removing URLs, newlines, and quote characters."""
    cleaned_col = regexp_replace(text_col, r'https?://\S+|www\.\S+', '')
    cleaned_col = regexp_replace(cleaned_col, "[\n\r]", " ")
    cleaned_col = regexp_replace(cleaned_col, '"', '')
    return cleaned_col

def transform_listings_df(df: DataFrame) -> DataFrame:
    """Applies specific transformations to the listings dataset."""
    logger.info("Transforming listings data...")
    numeric_cols = {
        "accommodates": IntegerType(), "bathrooms": FloatType(), "bedrooms": FloatType(),
        "beds": IntegerType(), "review_scores_rating": FloatType(), "number_of_reviews": IntegerType(),
        "minimum_nights": IntegerType(), "maximum_nights": IntegerType(),
        "reviews_per_month": FloatType()
    }
    for col_name, col_type in numeric_cols.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, col(col_name).cast(col_type))

    if 'price' in df.columns:
        df = df.withColumn("price", clean_price_column(col("price")))

    text_cols_to_clean = ["name", "description", "neighborhood_overview", "host_about"]
    for col_name in text_cols_to_clean:
        if col_name in df.columns:
            df = df.withColumn(col_name, clean_text_field(col(col_name)))

    date_cols = ["last_scraped", "host_since", "first_review", "last_review"]
    for col_name in date_cols:
        if col_name in df.columns:
            df = df.withColumn(col_name, to_date(col(col_name), "yyyy-MM-dd"))
            
    df = df.withColumn("processed_date", lit(datetime.now().strftime('%Y-%m-%d')))
    return df

def transform_calendar_df(df: DataFrame) -> DataFrame:
    """Applies specific transformations to the calendar dataset."""
    logger.info("Transforming calendar data...")
    if 'date' in df.columns:
        df = df.withColumnRenamed("date", "calendar_date")

    if 'calendar_date' in df.columns:
        df = df.withColumn("calendar_date", to_date(col("calendar_date"), "yyyy-MM-dd"))

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
        df = df.withColumn("comments", clean_text_field(col("comments")))

    return df

def save_dataframe_as_table(spark: SparkSession, df: DataFrame, table_name: str, city: str, snapshot: str):
    """Saves a DataFrame as a Hive table, with data and metadata on GCS."""
    db_name = "inside_airbnb"
    full_table_name = f"{db_name}.{table_name}"
    
    df_with_partitions = df.withColumn("city", lit(city)).withColumn("snapshot_date", lit(snapshot))
    
    logger.info(f"Saving table: {full_table_name}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

    df_with_partitions.write.partitionBy("city", "snapshot_date") \
        .mode("overwrite") \
        .format("parquet") \
        .saveAsTable(full_table_name)
    logger.info(f"Table {full_table_name} saved successfully.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--gcs-bucket-name", required=True, help="GCS bucket name (e.g., your-bucket).")
    parser.add_argument("--config-path", required=True, help="GCS path to the cities.yaml config file.")
    args = parser.parse_args()

    spark_session = create_spark_session(args.gcs_bucket_name)

    config_gcs_path = args.config_path.replace("gs://", "")
    bucket_name_from_config, config_blob_path = config_gcs_path.split('/', 1)

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name_from_config)
    blob = bucket.blob(config_blob_path)
    config_content = blob.download_as_text()
    config = yaml.safe_load(config_content)

    snapshot_date = config.get("snapshot")
    if not snapshot_date:
        raise ValueError("Snapshot date not found in config file.")

    for city_config in config.get("cities", []):
        city_name = city_config.get("name")
        base_url = city_config.get("base_url", "").rstrip("/")

        for filename in city_config.get("files", []):
            raw_df = None
            try:
                raw_df = download_and_read_csv(spark_session, f"{base_url}/{snapshot_date}/data/{filename}", args.gcs_bucket_name)
                
                if raw_df:
                    if 'listings' in filename:
                        transformed_df = transform_listings_df(raw_df)
                        save_dataframe_as_table(spark_session, transformed_df, "listings", city_name, snapshot_date)
                    elif 'calendar' in filename:
                        transformed_df = transform_calendar_df(raw_df)
                        save_dataframe_as_table(spark_session, transformed_df, "calendar", city_name, snapshot_date)
                    elif 'reviews' in filename:
                        transformed_df = transform_reviews_df(raw_df)
                        save_dataframe_as_table(spark_session, transformed_df, "reviews", city_name, snapshot_date)
            finally:
                if raw_df:
                    raw_df.unpersist()

    spark_session.stop()