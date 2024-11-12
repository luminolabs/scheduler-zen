import asyncio

from app.core.artifacts_client import pull_artifacts_meta_from_gcs_task
from app.core.database import Database
from app.core.utils import setup_logger, NON_TERMINAL_JOB_STATUSES

logger = setup_logger(__name__)


async def sync_job_artifacts(db: Database):
    """
    Synchronize artifacts.
    """
    logger.info("Starting artifacts sync")

    try:
        # Get all jobs that are not in terminal status or are recently completed jobs
        lum_jobs = await db.get_jobs_by_status_lum(NON_TERMINAL_JOB_STATUSES)
        gcp_jobs = await db.get_jobs_by_status_gcp(NON_TERMINAL_JOB_STATUSES)
        recently_completed_jobs = await db.get_recently_completed_jobs()
        all_jobs = lum_jobs + gcp_jobs + recently_completed_jobs

        # Pull artifacts in parallel
        results = await asyncio.gather(
            *[pull_artifacts_meta_from_gcs_task(job['job_id'], job['user_id'], db) for job in all_jobs])

        # Update artifacts in DB in parallel
        await asyncio.gather(*[db.update_job_artifacts(*result) for result in results if result])
    except Exception as e:
        logger.error(f"Error in artifacts sync: {str(e)}")

    logger.info("Completed artifacts sync")