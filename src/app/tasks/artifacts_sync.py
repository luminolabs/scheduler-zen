import asyncio

from google.cloud import storage

from app.core.artifacts_client import pull_artifacts_meta_from_gcs
from app.core.config_manager import config
from app.core.database import Database
from app.core.utils import setup_logger, NON_TERMINAL_JOB_STATUSES

logger = setup_logger(__name__)

async def sync_artifacts_for_lum_jobs(db: Database):
    """
    Synchronize artifacts for LUM provider jobs that are not in terminal status.
    """
    logger.info("Starting artifacts sync for LUM jobs")

    # Get all LUM jobs that are not in terminal status
    lum_jobs = await db.get_jobs_by_status_lum(NON_TERMINAL_JOB_STATUSES)

    # Initialize GCS client
    gcs_client = storage.Client(project=config.gcp_project)

    # Process each job
    for job in lum_jobs:
        try:
            # Pull artifacts meta from GCS
            artifacts_meta = await pull_artifacts_meta_from_gcs(job['job_id'], job['user_id'], db, gcs_client)

            if artifacts_meta:
                # Update the jobs_artifacts table
                await db.update_job_artifacts(job['job_id'], job['user_id'], artifacts_meta)
                logger.info(f"Updated artifacts for job {job['job_id']}")
            else:
                logger.warning(f"No artifacts found for job {job['job_id']}")

        except Exception as e:
            logger.error(f"Error processing artifacts for job {job['job_id']}: {str(e)}")

    logger.info("Completed artifacts sync for LUM jobs")

async def run_artifacts_sync(db: Database):
    """
    Run the artifacts sync task periodically.
    """
    while True:
        await sync_artifacts_for_lum_jobs(db)
        # Wait for 10 seconds before the next sync
        await asyncio.sleep(10)

# Function to start the background task
def start_artifacts_sync_task(db: Database):
    """
    Start the artifacts sync background task.
    """
    asyncio.create_task(run_artifacts_sync(db))
    logger.info("Started artifacts sync background task")