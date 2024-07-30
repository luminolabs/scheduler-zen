#!/bin/bash

# This script is used to start the scheduler on dev and production environments, don't use locally

# Exit on errors
set -e

# Set variables
PROJECT_ID="neat-airport-407301"
SECRET_NAME="scheduler-zen-db-config"

# Fetch the secret
echo "Fetching database configuration from Secret Manager"
SECRET_PAYLOAD=$(gcloud secrets versions access latest --secret=$SECRET_NAME --project=$PROJECT_ID)

# Parse the secret payload and set environment variables
eval "$SECRET_PAYLOAD"

# Export the variables so they're available to docker-compose
export SZ_DB_NAME
export SZ_DB_USER
export SZ_DB_PASS

# Start the services using docker-compose
echo "Starting services with docker-compose"
docker compose up -d

echo "Scheduler services started successfully"