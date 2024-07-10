# LLM Fine-Tuning Job Scheduler

Work in progress!

## Overview

The LLM Fine-Tuning Job Scheduler is a distributed system designed to manage and execute large language model (LLM) 
fine-tuning jobs across multiple Google Cloud Platform (GCP) regions. It efficiently utilizes 
Managed Instance Groups (MIGs) to handle job distribution, execution, and resource management.

## Features

- Distributed job scheduling across GCP regions
- Automatic scaling of compute resources based on job demand
- Real-time job and VM status tracking
- Efficient resource utilization and clean-up
- RESTful API for job submission, monitoring, and control

## Architecture

The system consists of several key components:

1. API Server: Handles incoming job requests and provides status information
2. Scheduler: Manages job distribution, MIG scaling, and overall system state
3. MIG Manager: Interfaces with GCP to control Managed Instance Groups
4. Pub/Sub Client: Manages communication between components using Google Cloud Pub/Sub
5. Database: Stores job information and status
6. Job Executor: Runs on each VM to execute jobs and report status

## API Endpoints

- `POST /jobs`: Submit a new job
- `GET /jobs/{job_id}`: Get details of a specific job
- `GET /jobs`: List all jobs (with optional status filter)
- `POST /jobs/{job_id}/stop`: Stop a running job
- `GET /status`: Get the overall status of the scheduler