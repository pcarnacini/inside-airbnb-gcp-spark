#!/usr/bin/env bash
set -euo pipefail

# --- Configuration ---
# ATTENTION: Update the variables below with your own values.
REGION="us-central1"
CLUSTER="airbnb-spark-cluster" # Your Dataproc cluster name
BUCKET_NAME="bucket-name" # Just the bucket name, without "gs://"
PROJECT_ID="project-id" # Your GCP project ID

# --- GCS path for the transformation script ---
TRANSFORMATION_SCRIPT_GCS="gs://${BUCKET_NAME}/code/rerun_transformation.py"

echo "=== Spark Transformation-Only Pipeline - Inside Airbnb (GCP) ==="

# Step 1: Upload the transformation script to GCS
echo "1. Uploading transformation script to GCS..."
gsutil cp rerun_transformation.py "$TRANSFORMATION_SCRIPT_GCS"
echo "Script uploaded."

# Step 2: Submit the Transformation job to Dataproc
echo "2. Submitting Transformation job to Dataproc..."
gcloud dataproc jobs submit pyspark "$TRANSFORMATION_SCRIPT_GCS" \
  --cluster="$CLUSTER" \
  --region="$REGION" \
  --project="$PROJECT_ID" \
  -- \
  --gcs-bucket-name="$BUCKET_NAME"

echo "Transformation job completed."
echo "=== Transformation pipeline finished successfully! ==="