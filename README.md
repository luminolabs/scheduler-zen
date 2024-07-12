# Pipeline Zen Job Scheduler

## Overview

The Pipeline Zen Job Scheduler is a distributed system designed to manage and execute jobs 
across multiple Google Cloud Platform (GCP) regions. It efficiently utilizes Managed Instance Groups (MIGs) 
to handle job distribution, execution, and resource management.

## Features

- Distributed job scheduling across GCP regions
- Automatic scaling of compute resources based on job demand
- Real-time job and VM status tracking
- Efficient resource utilization and clean-up
- RESTful API for job submission, monitoring, and control
- Support for local development and testing with a fake MIG manager

## Architecture

The system consists of several key components:

1. **API Server**: Handles incoming job requests and provides status information
2. **Scheduler**: Manages job distribution, MIG scaling, and overall system state
3. **Cluster Orchestrator**: Coordinates operations across multiple MIG clusters
4. **Cluster Manager**: Manages resources within a specific MIG cluster configuration
5. **MIG Manager**: Interfaces with GCP to control a single MIG
6. **Pub/Sub Client**: Manages communication between components using Google Cloud Pub/Sub
7. **Database**: Stores job information and status (uses SQLite with async operations)

## Setup

1. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

## Running the Application

1. Start the API server:
   ```
   python api.py
   ```

2. The API will be available at `http://localhost:8000`

## API Endpoints

- `POST /jobs`: Submit a new job
- `GET /jobs/{job_id}`: Get details of a specific job
- `POST /jobs/{job_id}/stop`: Stop a running job
- `GET /status`: Get the overall status of the scheduler

## Development

For local development and testing, set `SZ_ENV=local`. This will use the `FakeMigManager` 
instead of interacting with real GCP resources.