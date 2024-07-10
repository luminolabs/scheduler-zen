# main.py

import logging
from typing import List, Optional
from datetime import datetime

from fastapi import FastAPI, HTTPException, Depends
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel

from scheduler import Scheduler
from models import Job, JobStatus, Helm

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")
scheduler: Optional[Scheduler] = None


class JobSubmission(BaseModel):
    """Pydantic model for job submission data."""
    model: str
    dataset: str
    hyperparameters: dict


class JobQuery(BaseModel):
    """Pydantic model for job query parameters."""
    job_id: str
    status: Optional[JobStatus]
    start_time: Optional[datetime]
    end_time: Optional[datetime]


async def get_scheduler() -> Scheduler:
    """
    Get or create the global Scheduler instance.

    Returns:
        Scheduler: The global Scheduler instance.
    """
    global scheduler
    if scheduler is None:
        logger.info("Initializing scheduler")
        scheduler = Scheduler()
        await scheduler.start()
    return scheduler


@app.on_event("shutdown")
async def shutdown_event():
    """Shutdown event to stop the scheduler when the app is shutting down."""
    global scheduler
    if scheduler:
        logger.info("Stopping scheduler")
        await scheduler.stop()


@app.post("/jobs", response_model=Job)
async def create_job(
        job_submission: JobSubmission,
        token: str = Depends(oauth2_scheme)
) -> Job:
    """
    Create a new job.

    Args:
        job_submission (JobSubmission): The job details.
        token (str): Authentication token.

    Returns:
        Job: The created job.
    """
    scheduler = await get_scheduler()
    logger.info(f"Creating new job: {job_submission}")
    job = await scheduler.add_job(job_submission.dict())
    return job


@app.get("/jobs/{job_id}", response_model=Job)
async def get_job(job_id: str, token: str = Depends(oauth2_scheme)) -> Job:
    """
    Get details of a specific job.

    Args:
        job_id (str): The ID of the job to retrieve.
        token (str): Authentication token.

    Returns:
        Job: The requested job details.

    Raises:
        HTTPException: If the job is not found.
    """
    scheduler = await get_scheduler()
    job = await scheduler.get_job(job_id)
    if job is None:
        logger.warning(f"Job not found: {job_id}")
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@app.get("/jobs", response_model=List[Job])
async def list_jobs(
        status: Optional[JobStatus] = None,
        token: str = Depends(oauth2_scheme)
) -> List[Job]:
    """
    List all jobs, optionally filtered by status.

    Args:
        status (Optional[JobStatus]): Filter jobs by this status.
        token (str): Authentication token.

    Returns:
        List[Job]: List of jobs matching the criteria.
    """
    scheduler = await get_scheduler()
    logger.info(f"Listing jobs with status filter: {status}")
    jobs = await scheduler.list_jobs(status)
    return jobs


@app.post("/jobs/{job_id}/stop")
async def stop_job(job_id: str, token: str = Depends(oauth2_scheme)):
    """
    Stop a running job.

    Args:
        job_id (str): The ID of the job to stop.
        token (str): Authentication token.

    Returns:
        dict: A message indicating success.

    Raises:
        HTTPException: If the job is not found or already completed.
    """
    scheduler = await get_scheduler()
    logger.info(f"Attempting to stop job: {job_id}")
    success = await scheduler.stop_job(job_id)
    if not success:
        logger.warning(f"Failed to stop job: {job_id}")
        raise HTTPException(
            status_code=404,
            detail="Job not found or already completed"
        )
    return {"message": "Job stop signal sent successfully"}


@app.get("/status", response_model=Helm)
async def get_status(token: str = Depends(oauth2_scheme)) -> Helm:
    """
    Get the overall status of the scheduler.

    Args:
        token (str): Authentication token.

    Returns:
        Helm: The current status of the scheduler.
    """
    scheduler = await get_scheduler()
    logger.info("Fetching scheduler status")
    return await scheduler.get_status()


if __name__ == "__main__":
    import uvicorn
    logger.info("Starting the application")
    uvicorn.run(app, host="0.0.0.0", port=8000)
