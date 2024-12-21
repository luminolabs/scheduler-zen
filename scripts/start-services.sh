#!/bin/bash

# This script is used to start the service on GCP only
# Don't use this script locally

# Exit on errors
set -e

# Export all variables
set -o allexport

# Constants
SERVICE_NAME='scheduler-zen'
ENV_VAR_PREFIX='SZ'

# Inputs
COMPOSE_OPTS="${@:1}"  # Options to pass to docker compose

echo "Starting $SERVICE_NAME..."
echo "ENV_VAR_PREFIX set to $ENV_VAR_PREFIX"
echo "COMPOSE_OPTS set to $COMPOSE_OPTS"

echo "Pulling env variables from metadata server"
SZ_ENV=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/${ENV_VAR_PREFIX}_ENV")
SZ_DB_HOST=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/${ENV_VAR_PREFIX}_DB_PORT")
echo "SZ_ENV set to $SZ_ENV"
echo "SZ_DB_HOST set to $SZ_DB_HOST"

CODE_REPO_DIR="/$SERVICE_NAME"
echo "Changing directory to $CODE_REPO_DIR"
cd "$CODE_REPO_DIR" || exit 0

# Set the project ID and service account based on the environment
PROJECT_ID="eng-ai-$SZ_ENV"
CLOUDSDK_CORE_ACCOUNT="$SERVICE_NAME-sa@$PROJECT_ID.iam.gserviceaccount.com"
echo "PROJECT_ID set to $PROJECT_ID"
echo "CLOUDSDK_CORE_ACCOUNT set to $CLOUDSDK_CORE_ACCOUNT"

echo "Fetching secrets and configuration from Secret Manager"
SECRET_NAME="$SERVICE_NAME-config"
SECRET_PAYLOAD=$(gcloud secrets versions access latest --secret=$SECRET_NAME --project=$PROJECT_ID)
eval "$SECRET_PAYLOAD"

echo "Starting services with docker-compose"
docker compose up --build --no-deps -d $COMPOSE_OPTS

echo "Services started successfully"