import argparse
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session(gcs_bucket_name: str) -> SparkSession:
    """
    Creates and configures the Spark Session to connect to the Hive Warehouse on GCS.
    """
    warehouse_path = f"gs://{gcs_bucket_name}/hive-warehouse"
    logger.info(f"Connecting to Hive Metastore Warehouse at: {warehouse_path}")

    return SparkSession.builder \
        .appName("InsideAirbnbAnalysisDataFrameAPI") \
        .config("spark.sql.warehouse.dir", warehouse_path) \
        .enableHiveSupport() \
        .getOrCreate()

def save_analysis_result(df: DataFrame, gcs_bucket_name: str, analysis_name: str):
    """Displays a preview of the result and saves it in Parquet format to GCS."""
    logger.info(f"--- Result for: {analysis_name} ---")
    df.show(20, truncate=False)

    output_path = f"gs://{gcs_bucket_name}/results/{analysis_name}"
    df.coalesce(1).write.mode("overwrite").format("parquet").save(output_path)
    logger.info(f"Result for '{analysis_name}' saved to: {output_path}")

def run_price_by_room_type_and_neighborhood(spark: SparkSession, gcs_bucket_name: str):
    """
    Calculates the average price by room type and neighborhood, using daily calendar data.
    """
    logger.info("Running: run_price_by_room_type_and_neighborhood")
    listings_df = spark.table("inside_airbnb.listings").alias("l")
    calendar_df = spark.table("inside_airbnb.calendar").alias("c")

    result_df = listings_df.join(calendar_df, F.col("l.id") == F.col("c.listing_id")) \
        .filter(
            (F.col("c.price").isNotNull()) &
            (F.col("c.price") > 0) &
            (F.col("l.room_type").isNotNull()) &
            (F.col("l.neighbourhood_cleansed").isNotNull())
        ) \
        .groupBy("l.room_type", "l.neighbourhood_cleansed") \
        .agg(
            F.round(F.avg("c.price"), 2).alias("avg_price"),
            F.countDistinct("l.id").alias("total_listings")
        ) \
        .orderBy(F.col("avg_price").desc())

    save_analysis_result(result_df.limit(50), gcs_bucket_name, "price_by_room_type_neighbourhood")

def run_daily_availability_analysis(spark: SparkSession, gcs_bucket_name: str):
    logger.info("Running: run_daily_availability_analysis")
    calendar_df = spark.table("inside_airbnb.calendar")

    result_df = calendar_df \
        .groupBy("calendar_date") \
        .agg(
            F.count("*").alias("total_listings"),
            F.sum(F.when(F.col("available") == True, 1).otherwise(0)).alias("available_listings")
        ) \
        .withColumn("pct_available", F.round((F.col("available_listings") / F.col("total_listings")) * 100, 2)) \
        .orderBy("calendar_date")

    save_analysis_result(result_df.limit(30), gcs_bucket_name, "daily_availability")

def run_top_listings_by_reviews_analysis(spark: SparkSession, gcs_bucket_name: str):
    logger.info("Running: run_top_listings_by_reviews_analysis")
    listings_df = spark.table("inside_airbnb.listings")

    result_df = listings_df \
        .select("id", "name", "number_of_reviews", "price", "city") \
        .orderBy(F.col("number_of_reviews").cast(IntegerType()).desc())
        
    save_analysis_result(result_df.limit(10), gcs_bucket_name, "top_listings_by_reviews")

def run_superhost_impact_analysis(spark: SparkSession, gcs_bucket_name: str):
    logger.info("Running: run_superhost_impact_analysis")
    listings_df = spark.table("inside_airbnb.listings")

    result_df = listings_df \
        .filter(F.col("host_is_superhost").isin(['t', 'f'])) \
        .groupBy("host_is_superhost") \
        .agg(
            F.count("id").alias("total_listings"),
            F.round(F.avg("review_scores_rating"), 2).alias("avg_rating"),
            F.round(F.avg("price"), 2).alias("avg_price")
        ) \
        .orderBy(F.col("total_listings").desc())

    save_analysis_result(result_df, gcs_bucket_name, "superhost_impact")

def run_veteran_hosts_analysis(spark: SparkSession, gcs_bucket_name: str):
    logger.info("Running: run_veteran_hosts_analysis")
    listings_df = spark.table("inside_airbnb.listings")
    
    result_df = listings_df \
        .withColumn("host_since_date", F.to_date(F.col("host_since"), "yyyy-MM-dd")) \
        .filter(F.col("host_since_date").isNotNull()) \
        .groupBy("host_id", "host_name") \
        .agg(
            F.count("id").alias("total_listings"),
            F.min("host_since_date").alias("first_listing_date")
        ) \
        .orderBy(F.col("first_listing_date").asc())

    save_analysis_result(result_df, gcs_bucket_name, "veteran_hosts_analysis")

def run_price_seasonality_analysis(spark: SparkSession, gcs_bucket_name: str):
    logger.info("Running: run_price_seasonality_analysis")
    calendar_df = spark.table("inside_airbnb.calendar")
    
    result_df = calendar_df \
        .filter((F.col("available") == True) & (F.col("price").isNotNull())) \
        .withColumn("month", F.month(F.to_date(F.col("calendar_date"), "yyyy-MM-dd"))) \
        .groupBy("city", "month") \
        .agg(
            F.round(F.avg("price"), 2).alias("average_price")
        ) \
        .orderBy("city", "month")
        
    save_analysis_result(result_df, gcs_bucket_name, "price_seasonality_analysis")

def run_occupancy_rate_estimation_analysis(spark: SparkSession, gcs_bucket_name: str):
    logger.info("Running: run_occupancy_rate_estimation_analysis")
    calendar_df = spark.table("inside_airbnb.calendar")

    result_df = calendar_df \
        .groupBy("listing_id") \
        .agg(
            F.sum(F.when(F.col("available") == False, 1).otherwise(0)).alias("booked_days"),
            F.count("*").alias("total_days")
        ) \
        .withColumn("occupancy_rate", F.round((F.col("booked_days") / F.col("total_days")) * 100, 2)) \
        .orderBy(F.col("occupancy_rate").desc())

    save_analysis_result(result_df.limit(100), gcs_bucket_name, "occupancy_rate_estimation")

def run_most_reviewed_hosts_analysis(spark: SparkSession, gcs_bucket_name: str):
    logger.info("Running: run_most_reviewed_hosts_analysis")
    listings_df = spark.table("inside_airbnb.listings")

    result_df = listings_df \
        .groupBy("host_id", "host_name") \
        .agg(
            F.sum("number_of_reviews").alias("total_reviews"),
            F.count("id").alias("total_listings")
        ) \
        .orderBy(F.col("total_reviews").desc())
        
    save_analysis_result(result_df.limit(10), gcs_bucket_name, "most_reviewed_hosts")

def run_price_distribution_by_property_type(spark: SparkSession, gcs_bucket_name: str):
    logger.info("Running: run_price_distribution_by_property_type")
    listings_df = spark.table("inside_airbnb.listings")
    
    result_df = listings_df \
        .filter(F.col("price").isNotNull()) \
        .groupBy("property_type") \
        .agg(
            F.count("id").alias("listing_count"),
            F.round(F.avg("price"), 2).alias("avg_price"),
            F.round(F.expr("percentile_approx(price, 0.5)"), 2).alias("median_price")
        ) \
        .orderBy(F.col("listing_count").desc())

    save_analysis_result(result_df.limit(20), gcs_bucket_name, "price_distribution_by_property_type")
    
def run_listings_without_recent_reviews(spark: SparkSession, gcs_bucket_name: str):
    logger.info("Running: run_listings_without_recent_reviews")
    listings_df = spark.table("inside_airbnb.listings")

    result_df = listings_df \
        .withColumn("last_review_date", F.to_date(F.col("last_review"), "yyyy-MM-dd")) \
        .filter(F.col("last_review_date") < F.date_sub(F.current_date(), 365)) \
        .select("id", "name", "last_review_date", "number_of_reviews") \
        .orderBy(F.col("last_review_date").asc())

    save_analysis_result(result_df.limit(100), gcs_bucket_name, "listings_without_recent_reviews")

def run_new_listings_trend_analysis(spark: SparkSession, gcs_bucket_name: str):
    logger.info("Running: run_new_listings_trend_analysis")
    listings_df = spark.table("inside_airbnb.listings")

    result_df = listings_df \
        .withColumn("first_review_month", F.trunc(F.to_date(F.col("first_review"), "yyyy-MM-dd"), "month")) \
        .filter(F.col("first_review_month").isNotNull()) \
        .groupBy("first_review_month") \
        .count() \
        .orderBy("first_review_month")

    save_analysis_result(result_df, gcs_bucket_name, "new_listings_trend")
    
def run_general_metrics_analysis(spark: SparkSession, gcs_bucket_name: str):
    logger.info("Running: run_general_metrics_analysis")
    listings_df = spark.table("inside_airbnb.listings")

    result_df = listings_df \
        .agg(
            F.countDistinct("id").alias("total_listings"),
            F.round(F.avg("price"), 2).alias("avg_price"),
            F.min("price").alias("min_price"),
            F.max("price").alias("max_price"),
            F.countDistinct("room_type").alias("unique_room_types"),
            F.countDistinct("neighbourhood_cleansed").alias("unique_neighbourhoods"),
            F.countDistinct("host_id").alias("unique_hosts")
        )

    save_analysis_result(result_df, gcs_bucket_name, "general_metrics")
    
def run_top_neighborhoods_by_price_analysis(spark: SparkSession, gcs_bucket_name: str):
    logger.info("Running: run_top_neighborhoods_by_price_analysis")
    listings_df = spark.table("inside_airbnb.listings")

    result_df = listings_df \
        .filter(F.col("price").isNotNull()) \
        .groupBy("neighbourhood_cleansed") \
        .agg(
            F.count("id").alias("total_listings"),
            F.round(F.avg("price"), 2).alias("avg_price"),
            F.min("price").alias("min_price"),
            F.max("price").alias("max_price")
        ) \
        .orderBy(F.col("avg_price").desc())

    save_analysis_result(result_df.limit(20), gcs_bucket_name, "top_neighborhoods_by_price")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--gcs-bucket-name", required=True, help="GCS bucket name (e.g., your-bucket).")
    args = parser.parse_args()

    spark = create_spark_session(args.gcs_bucket_name)
    
    spark.sql("USE inside_airbnb")
    logger.info("Using database: inside_airbnb")
    
    bucket_name = args.gcs_bucket_name
    
    run_price_by_room_type_and_neighborhood(spark, bucket_name)
    run_daily_availability_analysis(spark, bucket_name)
    run_top_listings_by_reviews_analysis(spark, bucket_name)
    run_superhost_impact_analysis(spark, bucket_name)
    run_occupancy_rate_estimation_analysis(spark, bucket_name)
    run_most_reviewed_hosts_analysis(spark, bucket_name)
    run_price_distribution_by_property_type(spark, bucket_name)
    run_listings_without_recent_reviews(spark, bucket_name)
    run_new_listings_trend_analysis(spark, bucket_name)
    run_general_metrics_analysis(spark, bucket_name)
    run_top_neighborhoods_by_price_analysis(spark, bucket_name)
    run_veteran_hosts_analysis(spark, bucket_name)
    run_price_seasonality_analysis(spark, bucket_name)
    
    logger.info("All analyses completed successfully.")
    spark.stop()