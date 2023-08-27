#!/bin/bash

# Add src to PYTHONPATH
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"

# Exit immediately if any command fail
set -e

echo "Loading AWS credentials from AWS CLI profile..."

# Export AWS credentials so Spark executors can access S3
export AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id)
export AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key)
export AWS_DEFAULT_REGION=ap-south-1

# Safety check
if [ -z "$AWS_ACCESS_KEY_ID" ] || [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
  echo "❌ AWS credentials not found. Did you run aws configure?"
  exit 1
fi

echo "AWS credentials loaded successfully."
echo "Starting Spark pipeline..."

# Run pipeline
python -m src.main.main

echo "Pipeline finished."
