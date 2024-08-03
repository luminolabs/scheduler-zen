import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Any

from app.config_manager import config
from app.utils import setup_logger
from app.scheduler import Scheduler
from app.cluster_orchestrator import ClusterOrchestrator
from app.database import Database
from app.pubsub_client import PubSubClient
from app.mig_manager import MigManager
from app.fake_mig_manager import FakeMigManager

# Set up logging
logger = setup_logger(__name__)


# Define the JobRequest model, that will be used to create new jobs
class JobRequest(BaseModel):
    job_id: str
    workflow: str
    args: Dict[str, Any]
    keep_alive: bool
    cluster: str


def init_scheduler():
    """Set up components needed for and initialize the scheduler."""
    db = Database(config.database_url)
    pubsub = PubSubClient(config.gcp_project)

    # Initialize the cluster orchestrator
    cluster_config = {k: config.gpu_regions[v] for k, v in config.mig_clusters.items()}

    # The FakeMigManager simulates VMs and MIGs for testing
    if config.use_fake_mig_manager:
        logger.info("Using FakeMigManager for local environment")
        mig_manager = FakeMigManager(config.gcp_project, config.heartbeat_topic, config.start_job_subscription)
    else:
        logger.info("Using real MigManager for non-local environment")
        mig_manager = MigManager(config.gcp_project)

    # Initialize the cluster orchestrator with max scale limits
    cluster_orchestrator = ClusterOrchestrator(config.gcp_project, cluster_config, mig_manager, config.max_scale_limits)
    return Scheduler(db, pubsub, cluster_orchestrator)


scheduler = init_scheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Application startup
    logger.info("Connecting to the database")
    await scheduler.db.connect()
    logger.info("Starting scheduler")
    asyncio.create_task(scheduler.start())
    if config.use_fake_mig_manager:
        logger.info("Starting fake mig manager")
        asyncio.create_task(scheduler.cluster_orchestrator.mig_manager.start())
    yield
    # Application shutdown
    logger.info("Stopping scheduler")
    await scheduler.stop()
    logger.info("Disconnecting from the database")
    await scheduler.db.close()
    if config.use_fake_mig_manager:
        logger.info("Stopping fake mig manager")
        await scheduler.cluster_orchestrator.mig_manager.stop()

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
        return {"job_id": job_id, "status": "new"}
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
    return {"status": "stopping"}


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
    logger.info(f"Starting API server")
    uvicorn.run(app, host="0.0.0.0", port=8000)
