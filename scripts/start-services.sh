#!/bin/bash

# This script is used to start the scheduler on dev and production environments, don't use locally

# Exit on errors
set -e

# Go to the /scheduler-zen directory, where we've loaded all necessary files to run the Scheduler
cd /scheduler-zen

# Inputs
COMPOSE_OPTS=$1  # Additional options to pass to docker compose

# Constants
LOCAL_ENV="local"
PROJECT_ID="neat-airport-407301"
SECRET_NAME="scheduler-zen-db-config"

if [[ "$SZ_ENV" == "" ]]; then
  SZ_ENV="$SZ_ENV"
fi

# Export .env environment variables; note, we aren't aware of which environment
# we're running on before importing CAPI_ENV from .env,
# so we can't cd to /pipeline-zen-jobs conditionally above
eval $(cat ./.env | grep -v '^#' | tr -d '\r')
echo "SZ_ENV set to $SZ_ENV"

# Fetch the secret
echo "Fetching database configuration from Secret Manager"
SECRET_PAYLOAD=$(gcloud secrets versions access latest --secret=$SECRET_NAME --project=$PROJECT_ID)

# Parse the secret payload and set environment variables
eval "$SECRET_PAYLOAD"

# Export the variables so they're available to docker-compose
export SZ_DB_NAME
export SZ_DB_USER
export SZ_DB_PASS

# Configure docker to use gcloud as a credential helper
gcloud auth configure-docker us-central1-docker.pkg.dev --quiet

# Start the services using docker-compose
echo "Starting services with docker-compose"
docker compose up --build -d $COMPOSE_OPTS

echo "Scheduler services started successfully"
