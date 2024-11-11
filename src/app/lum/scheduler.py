import asyncio
from typing import Dict, Any

from app.core.config_manager import config
from app.core.database import Database
from app.core.utils import (
    JOB_STATUS_NEW, JOB_STATUS_RUNNING,
    setup_logger, JOB_STATUS_WAIT_FOR_VM
)
from app.lum.job_manager_client import JobManagerClient

logger = setup_logger(__name__)

class Scheduler:
    """Manages the scheduling and monitoring of LUM jobs."""

    def __init__(self, db: Database, job_manager_client: JobManagerClient):
        """
        Initialize the LUMScheduler.

        Args:
            db (Database): The database instance for job tracking.
            job_manager_client (JobManagerClient): The JobManagerClient for interacting with the blockchain.
        """
        self.db = db
        self.job_manager_client = job_manager_client
        self.running = False
        logger.info("LUM Scheduler initialized")

    async def start(self) -> None:
        """Start the scheduler to manage jobs."""
        self.running = True
        if not await self.job_manager_client.web3.is_connected():
            raise ConnectionError("Unable to connect to the Ethereum network.")
        logger.info("LUM Scheduler started")
        await asyncio.gather(
            self._monitor_jobs()
        )

    async def stop(self) -> None:
        """Stop the scheduler."""
        self.running = False
        logger.info("LUM Scheduler stopped")

    async def add_job(self, job_data: Dict[str, Any]) -> str:
        """
        Add a new job to the system.

        Args:
            job_data (Dict[str, Any]): The job data including workflow and args.

        Returns:
            str: The ID of the newly added job.
        """
        # Wrap the job creation in a transaction so that we can roll back if the blockchain transaction fails
        async with self.db.pool.acquire() as conn:
            async with conn.transaction():
                # Add job to the database
                job_id = await self.db.add_job_lum(job_data, conn=conn)
                # Create job on the blockchain
                tx_hash = await self.job_manager_client.create_job(job_data['args'])
                # Update job with transaction hash
                await self.db.update_job_lum(job_id, job_data['user_id'], tx_hash=tx_hash, conn=conn)
                # Log the job creation and return the job ID
                logger.info(f"Added new LUM job with ID: {job_id}; "
                            f"tx_hash: {tx_hash}; status: {JOB_STATUS_NEW}")
                return job_id

    async def _monitor_jobs(self) -> None:
        """Monitor and update the status of LUM jobs."""
        logger.info("Starting LUM job monitoring")
        while self.running:
            try:
                # Get all jobs that are not in a final state
                active_jobs = await self.db.get_jobs_by_status_lum([JOB_STATUS_NEW, JOB_STATUS_WAIT_FOR_VM, JOB_STATUS_RUNNING])
                # Monitor the status of each job
                for job in active_jobs:
                    new_status = await self.job_manager_client.get_job_status(job['lum']['lum_id'])
                    if new_status != job['status']:
                        # Update the job status in the database
                        await self.db.update_job_lum(job['job_id'], job['user_id'], status=new_status)
                        logger.info(f"Updated LUM job {job['job_id']} status from {job['status']} to {new_status}")
                await asyncio.sleep(config.lum_job_monitor_interval_s)  # Wait before the next monitoring cycle
            except Exception as e:
                # Log the error and continue monitoring;
                # hopefully it's a transient issue, or it affects only some jobs
                logger.error(f"Error monitoring LUM jobs: {str(e)}")
                await asyncio.sleep(config.lum_job_monitor_interval_s)
