import asyncio
import json
from contextlib import asynccontextmanager
from typing import Dict, Any, List

from fastapi import FastAPI, HTTPException

from app.core.config_manager import config
from app.core.database import Database
from app.core.schemas import CreateJobRequestGCP, ListUserJobsRequest, CreateJobRequestLUM
from app.core.utils import setup_logger
from app.gcp.cluster_orchestrator import ClusterOrchestrator
from app.gcp.fake_mig_client import FakeMigClient
from app.gcp.mig_client import MigClient
from app.gcp.pubsub_client import PubSubClient
from app.gcp.scheduler import Scheduler as GCPScheduler
from app.lum.job_manager_client import JobManagerClient
from app.lum.scheduler import Scheduler as LUMScheduler
from app.tasks.artifacts_sync import start_artifacts_sync_task

# Set up logging
logger = setup_logger(__name__)


def init_gcp_scheduler(db: Database):
    """Set up components needed for and initialize the scheduler."""
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
    return GCPScheduler(db, pubsub, cluster_orchestrator, mig_client)


def init_lum_scheduler(db: Database):
    """Set up components needed for and initialize the LUM scheduler."""
    # Load the JobManager contract ABI
    with open(config.lum_job_manager_abi_path) as f:
        job_manager_abi = json.loads(json.load(f)['result'])
    # Initialize the JobManager client
    job_manager_client = JobManagerClient(
        rpc_url=config.lum_rpc_url + config.alchemy_api_key,
        contract_address=config.lum_contract_address,
        abi=job_manager_abi,
        account_address=config.lum_account_address,
        account_private_key=config.lum_account_private_key
    )
    # Initialize the LUM scheduler
    return LUMScheduler(db, job_manager_client)


# Initialize the database and schedulers
db = Database(config.database_url)
gcp_scheduler = init_gcp_scheduler(db)
lum_scheduler = init_lum_scheduler(db)


async def raise_if_job_exists(job_id: str, user_id: str):
    """
    Raise an HTTPException if a job with the given ID already exists.

    Args:
        job_id (str): The job ID to check.
        user_id (str): The user ID associated with the job.
    """
    existing_job = await db.get_job(job_id, user_id)
    if existing_job:
        raise HTTPException(status_code=422, detail=f"Job with ID '{job_id}' already exists")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager. This is used to manage the application startup and shutdown.
    """
    # Application startup
    logger.info("Connecting to the database")
    await db.connect()
    await db.create_tables()
    logger.info("Starting schedulers")
    asyncio.create_task(gcp_scheduler.start())
    asyncio.create_task(lum_scheduler.start())
    logger.info("Starting artifacts sync task")
    start_artifacts_sync_task(db)
    yield
    # Application shutdown
    logger.info("Stopping schedulers")
    await gcp_scheduler.stop()
    await lum_scheduler.stop()
    logger.info("Disconnecting from the database")
    await db.close()

# Create the FastAPI application with the lifespan context manager
app = FastAPI(lifespan=lifespan)


@app.post("/jobs/gcp")
async def create_job_gcp(job: CreateJobRequestGCP) -> Dict[str, Any]:
    """
    Create a new job.

    Args:
        job (CreateJobRequestGCP): The job details.

    Returns:
        dict: A dictionary containing the job_id of the added job.
    """
    # Check if cluster exists
    if not gcp_scheduler.cluster_orchestrator.cluster_exists(job.cluster):
        raise HTTPException(status_code=422, detail=f"Cluster '{job.cluster}' does not exist")
    # Check if job_id already exists
    await raise_if_job_exists(job.job_id, job.user_id)
    # Add the job to the scheduler
    job_id = await gcp_scheduler.add_job(job.dict())
    logger.info(f"Added new job with ID: {job_id}")
    return {"job_id": job_id, "status": "new"}


@app.post("/jobs/lum")
async def create_job_lum(job: CreateJobRequestLUM) -> Dict[str, Any]:
    """
    Create a new job.

    Args:
        job (CreateJobRequestLUM): The job details.

    Returns:
        dict: A dictionary containing the job_id of the added job.
    """
    # Check if job_id already exists
    await raise_if_job_exists(job.job_id, job.user_id)
    # Add the job to the scheduler
    job_id = await lum_scheduler.add_job(job.dict())
    logger.info(f"Added new job with ID: {job_id}")
    return {"job_id": job_id, "status": "new"}


@app.post("/jobs/gcp/stop/{job_id}/{user_id}")
async def stop_job(job_id: str, user_id: str) -> Dict[str, Any]:
    """
    Stop a running job.

    Args:
        job_id (str): The ID of the job to stop.
        user_id (str): The user ID associated with the job.

    Returns:
        dict: A dictionary indicating the job was stopped.
    """
    # Check if job_id is a GCP job
    if not db.is_gcp_job(job_id, user_id):
        raise HTTPException(status_code=422, detail=f"This job ID is not a GCP job: {job_id}")
    # Attempt to stop the job
    success = await gcp_scheduler.stop_job(job_id, user_id)
    if success:
        logger.info(f"Stopped job with ID: {job_id}")
        return {"status": "stopping"}
    # Job not found or not running
    logger.warning(f"Failed to stop job with ID: {job_id}")
    raise HTTPException(status_code=404, detail="Job not found or not running")


@app.post("/jobs/get_by_user_and_ids")
async def get_jobs_by_user_and_ids(request: ListUserJobsRequest) -> List[Dict[str, Any]]:
    """
    Get jobs for a specific user with the given job IDs.

    This is primarily used by the Lumino API to refresh job details for a given user.
    TODO: In the future, let's use webhooks/MQ to notify the Customer API of job updates.

    Args:
        request (ListUserJobsRequest): The request containing user_id and job_ids.

    Returns:
        list: A list of jobs.
    """
    jobs = await db.get_jobs_by_user_and_ids(request.user_id, request.job_ids)
    logger.info(f"Retrieved {len(jobs)} jobs for user {request.user_id}")
    return jobs


if __name__ == "__main__":
    import uvicorn
    logger.info(f"Starting API server")
    uvicorn.run(app, host="0.0.0.0", port=5200)
