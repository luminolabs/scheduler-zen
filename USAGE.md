# Usage Guide for Pipeline Zen Jobs Scheduler

This document provides instructions on how to use the Pipeline Zen Jobs Scheduler, including setting up SSH tunnels, interacting with the API, and querying the database.

## Setting up SSH Tunnels

Before interacting with the API or database on the VM, you need to set up SSH tunnels. This allows you to securely access these services as if they were running on your local machine.

1. For the API (assuming it runs on port 8000 on the VM):
   ```
   gcloud compute ssh --zone "us-central1-a" "scheduler-zen" -- -L 8000:localhost:8000
   ```

2. For the database (assuming PostgreSQL runs on port 5432 on the VM):
   ```
   gcloud compute ssh --zone "us-central1-a" "scheduler-zen" -- -L 5432:localhost:5432
   ```

Keep these terminal windows open to maintain the SSH tunnels.

## Interacting with the API

### Scheduling a New Job

To schedule a new job, use the following curl command as a template:

```bash
curl -X POST http://localhost:8000/jobs -H "Content-Type: application/json" -d '{
  "job_id": "vasilis-protoml1-llama3-8b-lora-4xa100-40gb-run1",  
  "workflow": "torchtunewrapper",
  "args": {
    "job_config_name": "llm_llama3_8b",
    "dataset_id": "scaraveos/protoml1",
    "train_file_path": "text2sql.jsonl",
    "batch_size": 2,
    "shuffle": true,
    "num_epochs": 1,
    "use_lora": true, 
    "use_single_device": false,
    "num_gpus": 4 
  },
  "keep_alive": false,
  "cluster": "4xa100-40gb" 
}'
```

### Stopping a Job

To stop a running job, use the following curl command:

```bash
curl -X POST http://localhost:8000/jobs/{job_id}/stop
```

Replace `{job_id}` with the ID of the job you want to stop.

### Getting Job Status

To get the status of the scheduler, including information about running and pending jobs, use:

```bash
curl http://localhost:8000/status
```

## Interacting with the Database

### Connecting to the Database

To connect to the PostgreSQL database, use the following command:

```bash
psql -h localhost -p 5432 -U reader -d scheduler_zen
```

You'll be prompted to enter the password for the `reader`.

### Querying the Jobs Table

Once connected to the database, you can run SQL queries to interact with the jobs table. Here are some example queries:

1. List all jobs:
   ```sql
   SELECT * FROM jobs;
   ```

2. List all running jobs:
   ```sql
   SELECT * FROM jobs WHERE status = 'RUNNING';
   ```

3. Get the count of jobs by status:
   ```sql
   SELECT status, COUNT(*) FROM jobs GROUP BY status;
   ```

4. Get details of a specific job:
   ```sql
   SELECT * FROM jobs WHERE id = 'your-job-id';
   ```

5. List jobs for a specific cluster:
   ```sql
   SELECT * FROM jobs WHERE cluster = '4xa100-40gb';
   ```

6. List jobs created in the last 24 hours:
   ```sql
   SELECT * FROM jobs WHERE created_at > NOW() - INTERVAL '24 hours';
   ```

Remember to end your SQL queries with a semicolon (;).

For more detailed information about the API endpoints and database schema, please refer to the project documentation.