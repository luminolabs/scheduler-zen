# Running Pipeline Zen Jobs Scheduler

This document provides instructions for running the Pipeline Zen Jobs Scheduler locally and 
setting it up on a Google Cloud Platform (GCP) Virtual Machine (VM).

For more detailed information on using the Pipeline Zen Jobs Scheduler, 
please refer to the [USAGE.md](USAGE.md) file.

## Running Locally

Before proceeding with either local setup option, you need to create an `.env` file:

1. Navigate to the project root directory.

2. Create an `.env` file in the project root and add the following lines:
   ```
   SZ_ENV=local
   SZ_DB_NAME=scheduler_zen
   SZ_DB_USER=user123
   SZ_DB_PASS=pass123
   SZ_DB_HOST=localhost
   SZ_DB_PORT=35200
   ```

This `.env` file is crucial for both local setup options as it provides necessary configuration 
for the database and application.

You have two options for running the project locally:

### Option 1: Hybrid Local Setup

Recommended for local development; 
this will start the db using docker compose, and will run the API directly with python.

1. Start the PostgreSQL database using Docker Compose:
   ```bash
   docker-compose up -d db
   ```

2. Install the required Python dependencies:
   ```bash
   pip install -Ur requirements.txt
   ```

3. Set the Python path:
   ```bash
   export PYTHONPATH=$(pwd)/src
   ```

4. Start the scheduler and API:
   ```bash
   python src/app/main.py
   ```

### Option 2: Full Docker Compose Setup

This will start both the PostgreSQL database and the API service using docker compose.

1. Build and start all services using Docker Compose:
   ```
   docker-compose up -d
   ```

## Running with the FakeMigClientWithPipeline

- Ensure that the `pipeline-zen` repo is cloned under `../pipeline-zen` relatively to this repo
- Ensure that you can run a dummy job directly with the pipeline locally first
- Ensure that you have a `./.secrets/gcp_key.json` under the `pipeline-zen` folder.
- Import that service account to `gcloud`:
  ```bash
  gcloud auth activate-service-account pipeline-zen-jobs-sa@eng-ai-dev.iam.gserviceaccount.com --key-file=./.secrets/gcp_key.json --project=eng-ai-dev
  ```
- Then revert back to your default login (use your email):
  ```bash
  gcloud config set account vasilis@luminolabs.ai
  ```

The pipeline will use the `pipeline-zen-jobs-sa` service account automatically