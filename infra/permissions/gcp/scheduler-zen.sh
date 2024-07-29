#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Set variables
ENV="dev"
PROJECT_ID="neat-airport-407301"
SERVICE_ACCOUNT="scheduler-zen-$ENV@$PROJECT_ID.iam.gserviceaccount.com"

# Ensure the user is logged in and the correct project is set
gcloud config set project $PROJECT_ID

# Create the service account if it doesn't exist
gcloud iam service-accounts create scheduler-zen-dev --display-name="scheduler-zen-dev" || true

# Create a custom role for MIG operations
gcloud iam roles create scheduler_zen_mig_manager \
  --project=$PROJECT_ID --title="Scheduler Zen MIG Manager" \
  --description="Role for Scheduler Zen MIG operations" \
  --permissions=compute.instanceGroupManagers.get,compute.instanceGroupManagers.list,compute.instanceGroupManagers.update,compute.instances.list

# Assign the custom MIG role to the service account
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SERVICE_ACCOUNT" \
  --role="projects/$PROJECT_ID/roles/scheduler_zen_mig_manager"

# Pub/Sub permissions
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SERVICE_ACCOUNT" \
  --role=roles/pubsub.subscriber
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SERVICE_ACCOUNT" \
  --role=roles/pubsub.publisher

# Allow access to GCP secrets such as `huggingface_token`
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SERVICE_ACCOUNT" \
  --role=roles/secretmanager.secretAccessor
gcloud beta projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SERVICE_ACCOUNT" \
  --role=roles/secretmanager.viewer

# Logging permissions
# Required for:
# - All files: logging is used throughout the application
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT" \
    --role="roles/logging.logWriter"

echo "Permissions set up completed for $SERVICE_ACCOUNT"