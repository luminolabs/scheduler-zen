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

To schedule a new job, use the following curl command as a template.

For available clusters look at the [app-config/default.yml](https://github.com/luminolabs/scheduler-zen/blob/main/app-configs/default.yml) file 
and find the `mig_clusters` section.

If you're new to this, ask a team member to verify the configuration and help you schedule your first job.

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

### Installing psql

Install psql:

```bash
brew doctor
brew update
brew install libpq
````

Add the following to your `.bash_profile` or `.zshrc` or wherever you set your environment variables:

```bash
export PATH="/usr/local/opt/libpq/bin:$PATH"
````

Then, reload your shell.

Consider using a PostgreSQL client like 
[Postico](https://eggerapps.at/postico/) or [pgAdmin](https://www.pgadmin.org/) 
for a more user-friendly interface. Or if
you use intellij, you can use the database plugin to connect to the database.

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

## Monitoring Jobs

### Observing MIG Statuses

After scheduling a job, you can monitor the Managed Instance Group (MIG) statuses on the GCP console:

1. Visit the [Instance Groups page](https://console.cloud.google.com/compute/instanceGroups/list?orgonly=true&project=neat-airport-407301&supportedpurview=organizationId,folder,project&rapt=AEjHL4NUVCalN7qo2SUhehSDZEQx1Gl2QtKDw3Gutn_hI-VoGjojng75_mpQj3gyHoPF5Rj1B0xn6Wg09QsM0Rtf1fs1KZK1Qmd8zuYtQu7iNCXCgnbaT58).
2. Use the filter functionality to show only the relevant cluster, e.g., `4xa100-40gb`.

This will allow you to see the current state and any scaling activities of the MIGs associated with your job.

### Viewing Job Logs

Once a job moves to the `RUNNING` status, you can view its logs in the GCP console:

1. First, query the `jobs` table in the database to get the `vm_name` for your job.
2. Visit the [Logs Explorer page](https://console.cloud.google.com/logs/query;query=resource.type%3D%22gce_instance%22%0A%22my-vm-name%22;cursorTimestamp=2024-07-29T00:35:48.915042250Z;duration=PT45M?project=neat-airport-407301&rapt=AEjHL4N0pNnZDgQMawNExGZ6NGqjyjXC8_C5X18V3-Mu-WOEkr4O1MexLNso4fjrMD7ImQV2-Ldyt3u4eYo5nK9Ud3RLkszcispRh9pRrQhE5CTZXcCjMuo).
3. Replace `my-vm-name` in the search bar with the actual VM name from your job.

This will show you the logs from the VM running your job, which can be helpful for monitoring progress or debugging issues.

### Accessing Training Metrics

All training metrics are stored in BigQuery. You can query these metrics using the BigQuery console:

1. Visit the [BigQuery console](https://console.cloud.google.com/bigquery?project=neat-airport-407301).
2. Use a query like the following to see all metrics for a given job_id:

```sql
SELECT *
FROM `neat-airport-407301.pipeline_zen.torchtunewrapper` AS t
WHERE t.job_id = 'your-job-id'
ORDER BY create_ts DESC;
```

Replace `'your-job-id'` with the actual ID of the job you want to investigate.

This query will return all stored metrics for the specified job, ordered by creation timestamp in descending order.

## Accessing Job Results

Once a job is completed or has failed, any logs and model weights are automatically uploaded to a GCP bucket. The specific bucket used depends on the region where the job was executed.

To find the correct bucket for your job results:

1. Query the `jobs` table in the database to get the `region` for your job.
2. Use the region to determine the correct GCP bucket:

   - Asia regions: `lum-pipeline-zen-jobs-asia`
   - Europe regions: `lum-pipeline-zen-jobs-europe`
   - US regions: `lum-pipeline-zen-jobs-us`
   - Middle East (me-west1): `lum-pipeline-zen-jobs-me-west1`

For example, if your job's region is `us-central1`, you would look for your results in the `lum-pipeline-zen-jobs-us` bucket.

To access the bucket:

1. Go to the [Google Cloud Storage browser](https://console.cloud.google.com/storage/browser).
2. Select the appropriate bucket based on your job's region.
3. Navigate through the bucket to find your job's results. They are typically organized by job ID.

Remember to download any important results you need, as bucket contents may be subject to retention policies.

## Best Practices

- Always use unique job IDs when scheduling new jobs.
- Monitor the status of your jobs regularly using both the database queries and the GCP console tools.
- Check the MIG statuses to ensure that resources are being allocated and deallocated as expected.
- Review job logs promptly to catch and address any issues.
- Analyze training metrics in BigQuery to track the progress and performance of your jobs.

For more detailed information about the API endpoints, database schema, or GCP tools, please refer to the project documentation and the GCP documentation.