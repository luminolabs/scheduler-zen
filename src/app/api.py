import os
import logging
import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Any, List

from scheduler import Scheduler
from cluster_orchestrator import ClusterOrchestrator
from database import Database
from pubsub_client import PubSubClient
from mig_manager import MigManager
from fake_mig_manager import FakeMigManager

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = os.getenv("PROJECT_ID", "neat-airport-407301")
DB_PATH = os.getenv("DB_PATH", "scheduler-zen.sqlite")
PZ_ENV = os.getenv("PZ_ENV", "local")
# Cluster configurations
CLUSTER_CONFIGS: Dict[str, List[str]] = {
    '8xa100-40gb': ['asia-northeast1', 'asia-northeast-3', 'asia-southeast1',
                    'europe-west4', 'me-west1',
                    'us-central1', 'us-west1', 'us-west3', 'us-west4'],
}


# Define the JobRequest model, that will be used to create new jobs
class JobRequest(BaseModel):
    workflow: str
    args: Dict[str, Any]
    keep_alive: bool
    cluster: str


def init_scheduler():
    """Set up components needed for and initialize the scheduler."""
    db = Database(DB_PATH)
    pubsub = PubSubClient(PROJECT_ID)

    if PZ_ENV == 'local':
        logger.info("Using FakeMigManager for local environment")
        mig_manager = FakeMigManager(PROJECT_ID)
    else:
        logger.info("Using real MigManager for non-local environment")
        mig_manager = MigManager(PROJECT_ID)

    cluster_orchestrator = ClusterOrchestrator(PROJECT_ID, CLUSTER_CONFIGS, mig_manager)
    return Scheduler(db, pubsub, cluster_orchestrator)


scheduler = init_scheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load the ML model
    logger.info("Starting scheduler")
    asyncio.create_task(scheduler.start())
    yield
    logger.info("Stopping scheduler")
    await scheduler.stop()

# Create the FastAPI application with the lifespan context manager
app = FastAPI(lifespan=lifespan)


@app.post("/jobs")
async def create_job(job: JobRequest):
    """
    Create a new job.

    Args:
        job (JobRequest): The job details.

    Returns:
        dict: A dictionary containing the job_id of the added job.

    Raises:
        HTTPException: If there's an error adding the job.
    """
    try:
        job_id = await scheduler.add_job(job.dict())
        logger.info(f"Added new job with ID: {job_id}")
        return {"job_id": job_id}
    except Exception as e:
        logger.error(f"Error adding job: {str(e)}")
        raise HTTPException(status_code=500, detail="Error adding job")


@app.post("/jobs/{job_id}/stop")
async def stop_job(job_id: str):
    """
    Stop a running job.

    Args:
        job_id (str): The ID of the job to stop.

    Returns:
        dict: A dictionary indicating the job was stopped.

    Raises:
        HTTPException: If the job is not found or not running.
    """
    success = await scheduler.stop_job(job_id)
    if not success:
        logger.warning(f"Failed to stop job with ID: {job_id}")
        raise HTTPException(status_code=404, detail="Job not found or not running")
    logger.info(f"Stopped job with ID: {job_id}")
    return {"status": "stopped"}


@app.get("/status")
async def get_status():
    """
    Get the current status of the scheduler.

    Returns:
        dict: The current status of the scheduler.
    """
    try:
        status = await scheduler.get_status()
        logger.info("Retrieved scheduler status")
        return status
    except Exception as e:
        logger.error(f"Error retrieving scheduler status: {str(e)}")
        raise HTTPException(status_code=500, detail="Error retrieving status")

if __name__ == "__main__":
    import uvicorn
    logger.info(f"Starting API server in {PZ_ENV} environment")
    uvicorn.run(app, host="0.0.0.0", port=8000)
