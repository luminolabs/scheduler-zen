import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, computed_field
from typing import Dict, Any, List

from app.config_manager import config
from app.fake_mig_client import FakeMigClient
from app.utils import setup_logger
from app.scheduler import Scheduler
from app.cluster_orchestrator import ClusterOrchestrator
from app.database import Database
from app.pubsub_client import PubSubClient
from app.mig_client import MigClient

# Set up logging
logger = setup_logger(__name__)


# Used to create a new job
class CreateJobRequest(BaseModel):
    job_id: str
    workflow: str
    args: Dict[str, Any]
    gpu_type: str  # ex: "a100-40gb"
    num_gpus: int = 1
    keep_alive: bool = False
    user_id: str = "0"  # Default to 0 for internal jobs; the Customer API will set this to the user ID

    @computed_field
    def cluster(self) -> str:
        """
        Return the cluster name based on the GPU type and number of GPUs.
        Returns:
            str: The cluster name. ex: "4xa100-40gb" or "local" if using fake MIG manager.
        """
        return f"{self.num_gpus}x{self.gpu_type}" if not config.use_fake_mig_client \
            else "local"  # When using the fake MIG manager, we have a single "local" cluster


# Used to get jobs by user and IDs
class ListUserJobsRequest(BaseModel):
    user_id: str
    job_ids: List[str]


def init_scheduler():
    """Set up components needed for and initialize the scheduler."""
    db = Database(config.database_url)
    pubsub = PubSubClient(config.gcp_project)

    # Initialize the cluster orchestrator
    cluster_config = {k: config.gpu_regions[v] for k, v in config.mig_clusters.items()}

    # The FakeMigManager simulates VMs and MIGs for testing
    if config.use_fake_mig_client:
        logger.info("Using FakeMigClient for local environment")
        mig_client = FakeMigClient()
    else:
        logger.info("Using real MigClient for non-local environment")
        mig_client = MigClient(config.gcp_project)

    # Initialize the cluster orchestrator with max scale limits
    cluster_orchestrator = ClusterOrchestrator(config.gcp_project, cluster_config, mig_client, config.max_scale_limits, db)
    return Scheduler(db, pubsub, cluster_orchestrator, mig_client)


scheduler = init_scheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Application startup
    logger.info("Connecting to the database")
    await scheduler.db.connect()
    logger.info("Starting scheduler")
    asyncio.create_task(scheduler.start())
    yield
    # Application shutdown
    logger.info("Stopping scheduler")
    await scheduler.stop()
    logger.info("Disconnecting from the database")
    await scheduler.db.close()

# Create the FastAPI application with the lifespan context manager
app = FastAPI(lifespan=lifespan)


@app.post("/jobs")
async def create_job(job: CreateJobRequest) -> Dict[str, Any]:
    """
    Create a new job.

    Args:
        job (CreateJobRequest): The job details.

    Returns:
        dict: A dictionary containing the job_id of the added job.
    """
    # Check if cluster exists
    if not scheduler.cluster_orchestrator.cluster_exists(job.cluster):
        raise HTTPException(status_code=422, detail=f"Cluster '{job.cluster}' does not exist")

    # Check if job_id already exists
    existing_job = await scheduler.db.get_job(job.job_id)
    if existing_job:
        raise HTTPException(status_code=422, detail=f"Job with ID '{job.job_id}' already exists")

    job_id = await scheduler.add_job(job.dict())
    logger.info(f"Added new job with ID: {job_id}")
    return {"job_id": job_id, "status": "new"}


@app.post("/jobs/{job_id}/stop")
async def stop_job(job_id: str) -> Dict[str, Any]:
    """
    Stop a running job.

    Args:
        job_id (str): The ID of the job to stop.

    Returns:
        dict: A dictionary indicating the job was stopped.
    """
    success = await scheduler.stop_job(job_id)
    if not success:
        logger.warning(f"Failed to stop job with ID: {job_id}")
        raise HTTPException(status_code=404, detail="Job not found or not running")
    logger.info(f"Stopped job with ID: {job_id}")
    return {"status": "stopping"}


@app.post("/jobs/get_by_user_and_ids")
async def get_jobs_by_user_and_ids(request: ListUserJobsRequest) -> List[Dict[str, Any]]:
    """
    Get jobs for a specific user with the given job IDs.

    This is primarily used by the Lumino API to refresh job details for a given user.
    TODO: In the future, let's use webhooks to notify the Customer API of job updates.

    Args:
        request (ListUserJobsRequest): The request containing user_id and job_ids.

    Returns:
        list: A list of jobs.
    """
    jobs = await scheduler.db.get_jobs_by_user_and_ids(request.user_id, request.job_ids)
    logger.info(f"Retrieved {len(jobs)} jobs for user {request.user_id}")
    return jobs


if __name__ == "__main__":
    import uvicorn
    logger.info(f"Starting API server")
    uvicorn.run(app, host="0.0.0.0", port=5200)
