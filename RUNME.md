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
   SZ_DB_PORT=5432
   ```

This `.env` file is crucial for both local setup options as it provides necessary configuration 
for the database and application.

You have two options for running the project locally:

### Option 1: Hybrid Local Setup

Recommended for local development; 
this will start the db using docker compose, and will run the API directly with python.

1. Start the PostgreSQL database using Docker Compose:
   ```
   docker-compose up -d db
   ```

2. Install the required Python dependencies:
   ```
   pip install -Ur requirements.txt
   ```

3. Set the Python path:
   ```
   export PYTHONPATH=$(pwd)/src
   ```

4. Start the scheduler and API:
   ```
   python src/app/api.py
   ```

### Option 2: Full Docker Compose Setup

This will start both the PostgreSQL database and the API service using docker compose.

1. Build and start all services using Docker Compose:
   ```
   docker-compose up -d
   ```

## Setting Up on a GCP VM

To set up the project on a GCP VM, follow these steps:

1. Ensure that the VM is configured with the `scheduler-zen-dev` service account.

2. Install Docker and Docker Compose on the VM if they're not already installed.

3. Install the GCP Ops Agent on the VM.

4. Set up log streaming to GCP. Refer to the configuration under the `infra/vm/` directory.

5. Clone the `scheduler-zen` repository to the `/scheduler-zen` directory on the VM:
   ```
   git clone git@github.com:luminolabs/scheduler-zen.git /scheduler-zen
   ```

6. Navigate to the project root:
   ```
   cd /scheduler-zen
   ```

7. Create a `.env` file in the project root with the following content:
   ```
   SZ_ENV=dev
   ```
8. Run the following script to start the services:
    ```
    ./scripts/start-scheduler.sh
    ```

This script will fetch the database configuration from Google Secret Manager, 
set the necessary environment variables, and start the api and db services using Docker Compose.