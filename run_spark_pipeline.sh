#!/usr/bin/env bash
set -euo pipefail

# --- Configuration ---
# ATTENTION: Update the variables below with your own values.
REGION="us-central1"
CLUSTER="airbnb-spark-cluster" # Your Dataproc cluster name
BUCKET_NAME="bucket-name" # Just the bucket name, without "gs://"
PROJECT_ID="project-id" # Your GCP project ID

# --- GCS paths for the script files ---
ETL_SCRIPT_GCS="gs://${BUCKET_NAME}/code/main_etl.py"
ANALYSES_SCRIPT_GCS="gs://${BUCKET_NAME}/code/run_spark_analyses.py"
CONFIG_FILE_GCS="gs://${BUCKET_NAME}/code/config/cities.yaml"

echo "=== Spark ETL & Analysis Pipeline (DataFrame API) - Inside Airbnb (GCP) ==="

# Step 0: Upload scripts and configs to GCS
echo "0. Uploading scripts to GCS..."
gsutil cp main_etl.py "$ETL_SCRIPT_GCS"
gsutil cp run_spark_analyses.py "$ANALYSES_SCRIPT_GCS"
gsutil cp config/cities.yaml "$CONFIG_FILE_GCS"
echo "Scripts uploaded."

# Step 1, 2 & 3: Execute the ETL job
echo "1. Submitting ETL job to Dataproc..."
gcloud dataproc jobs submit pyspark "$ETL_SCRIPT_GCS" \
  --cluster="$CLUSTER" \
  --region="$REGION" \
  --project="$PROJECT_ID" \
  -- \
  --gcs-bucket-name="$BUCKET_NAME" \
  --config-path="$CONFIG_FILE_GCS"

echo "ETL job completed."

# Step 4: Execute the Analyses job
echo "2. Submitting Analyses (DataFrame API) job to Dataproc..."
gcloud dataproc jobs submit pyspark "$ANALYSES_SCRIPT_GCS" \
  --cluster="$CLUSTER" \
  --region="$REGION" \
  --project="$PROJECT_ID" \
  -- \
  --gcs-bucket-name="$BUCKET_NAME"

echo "Analyses job completed."
echo "=== Spark pipeline finished successfully! ==="