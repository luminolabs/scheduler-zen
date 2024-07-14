# Pipeline Zen Jobs Scheduler

Pipeline Zen Jobs Scheduler is a system designed to manage and scale compute resources for job processing across multiple clusters and regions in Google Cloud Platform (GCP).

## System Architecture

The system is composed of several key components that work together to manage jobs and scale resources:

1. Scheduler
2. ClusterOrchestrator
3. ClusterManager
4. MigManager
5. PubSub Client

## High Level System Design Diagram

[![high-level-system-design.png](assets%2Fhigh-level-system-design.png)](assets/high-level-system-design.png)

### Scheduler

The Scheduler is the central component of the system. It:

- Manages the overall job scheduling process.
- Interacts with the database to track job statuses.
- Coordinates with the ClusterOrchestrator to manage cluster scaling.
- Uses PubSub to send and receive messages about job statuses.
- Periodically monitors and updates the system state.

### ClusterOrchestrator

The ClusterOrchestrator oversees all clusters in the system. It:

- Maintains a collection of ClusterManagers, one for each cluster configuration.
- Coordinates scaling operations across all clusters.
- Aggregates status information from all clusters.

### ClusterManager

Each ClusterManager is responsible for a specific cluster configuration. It:

- Manages operations for a cluster across multiple regions.
- Handles scaling decisions for its cluster based on running VMs and pending jobs.
- Interacts with the MigManager to perform actual scaling operations.

### MigManager

The MigManager directly interacts with Google Cloud's Managed Instance Groups (MIGs). It:

- Performs API calls to GCP to scale MIGs, get MIG information, and list VMs.
- Caches MIG information to reduce API calls.
- Implements request rate limiting to avoid hitting API quotas.

### PubSub Client

The PubSub Client facilitates asynchronous communication within the system. It:

- Publishes messages about new jobs to be started.
- Listens for heartbeat messages from running jobs.
- Sends stop signals to running jobs when needed.

## Key Terminology

- **Cluster**: A logical grouping of compute resources with similar specifications (e.g., "4xa100-40gb" for a cluster of machines with 4 A100 40GB GPUs each).
- **Region**: A geographic area where GCP resources can be located (e.g., "us-central1").
- **MIG (Managed Instance Group)**: A GCP resource that maintains a group of identical VM instances, allowing for easy scaling and management.
- **VM (Virtual Machine)**: An individual compute instance within a MIG.
- **Job**: A unit of work that needs to be processed on the compute resources.
- **Scaling**: The process of adjusting the number of VMs in a MIG based on workload demands.
- **PubSub**: Google Cloud Pub/Sub, a messaging service used for sending and receiving messages between components.

## Workflow

1. The Scheduler receives a new job request.
2. The job is added to the database and a message is published via PubSub to start the job.
3. The Scheduler determines which cluster should handle the job based on its requirements.
4. The ClusterOrchestrator is notified to check if scaling is necessary.
5. If scaling is needed, the appropriate ClusterManager uses the MigManager to adjust the size of the relevant MIGs.
6. The job starts running on an available VM.
7. The running job sends periodic heartbeat messages via PubSub, which the Scheduler uses to update the job status.
8. The Scheduler continues to monitor running jobs and pending workloads, adjusting resources as needed.
9. When a job completes or needs to be stopped, the Scheduler sends a message via PubSub to the relevant VM.

## Configuration

The system is configured with:

- A list of available clusters and their specifications.
- The regions associated with each cluster.
- Maximum scale limits for each cluster.
- GCP project information and credentials.
- PubSub topic and subscription names for various message types.

This configuration allows the system to make informed decisions about resource allocation and scaling across different types of compute resources and geographic regions, while maintaining efficient communication between components.

## Data Flow

1. Job Requests → Scheduler → Database
2. Scheduler → PubSub → Job Start Messages
3. Running Jobs → PubSub → Heartbeat Messages → Scheduler
4. Scheduler → ClusterOrchestrator → ClusterManagers → MigManager → GCP (for scaling)
5. Scheduler → PubSub → Job Stop Messages (when needed)

This architecture allows for a scalable, responsive system that can handle varying workloads across multiple clusters and regions while maintaining efficient communication and resource management.