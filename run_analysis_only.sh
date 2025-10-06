#!/usr/bin/env bash
set -euo pipefail

# --- Configuration ---
# ATTENTION: Update the variables below with your own values.
REGION="us-central1"
CLUSTER="airbnb-spark-cluster" # Your Dataproc cluster name
BUCKET_NAME="bucket-name" # Just the bucket name, without "gs://"
PROJECT_ID="project-id" # Your GCP project ID

# --- GCS path for the analysis script ---
ANALYSES_SCRIPT_GCS="gs://${BUCKET_NAME}/code/run_spark_analyses.py"

echo "=== Spark Analysis-Only Pipeline - Inside Airbnb (GCP) ==="

# Step 1: Upload the analysis script to GCS
echo "1. Uploading analysis script to GCS..."
gsutil cp run_spark_analyses.py "$ANALYSES_SCRIPT_GCS"
echo "Script uploaded."

# Step 2: Submit the Analysis job to Dataproc
echo "2. Submitting Analysis job to Dataproc..."
gcloud dataproc jobs submit pyspark "$ANALYSES_SCRIPT_GCS" \
  --cluster="$CLUSTER" \
  --region="$REGION" \
  --project="$PROJECT_ID" \
  -- \
  --gcs-bucket-name="$BUCKET_NAME"

echo "Analysis job completed."
echo "=== Analysis pipeline finished successfully! ==="