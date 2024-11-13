import asyncio
from typing import Dict, Any, Optional, Tuple

from google.api_core import retry_async
from web3.exceptions import TransactionNotFound

from app.core.database import Database
from app.core.utils import (
    JOB_STATUS_NEW, JOB_STATUS_RUNNING,
    setup_logger, JOB_STATUS_WAIT_FOR_VM
)
from app.lum.job_manager_client import JobManagerClient

logger = setup_logger(__name__)

# Configure the retry decorator for transaction receipt checks
retry_config_tx_receipt = retry_async.AsyncRetry(
    attempts=60,                    # Maximum number of retry attempts
    delay=1,                        # Initial delay between retries (in seconds)
    exponential_backoff=False,      # Don't use exponential backoff
    exceptions={                    # Specify which exceptions to retry
        TransactionNotFound: True,
    }
)


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
        """Start the scheduler and verify blockchain connection."""
        if not await self.job_manager_client.web3.is_connected():
            raise ConnectionError("Unable to connect to the Ethereum network.")
        self.running = True
        logger.info("LUM Scheduler started")

    async def stop(self) -> None:
        """Stop the scheduler."""
        self.running = False
        logger.info("LUM Scheduler stopped")

    async def run_cycle(self) -> None:
        """Execute one complete cycle of LUM scheduler tasks."""
        if not self.running:
            return

        try:
            logger.info("Starting LUM scheduler cycle")
            # Monitor jobs for status updates
            await self._update_job_statuses()
            # Process any pending transaction receipts
            await self._process_pending_receipts()
            logger.info("Completed LUM scheduler cycle")
        except Exception as e:
            logger.error(f"Error in LUM scheduler cycle: {str(e)}")

    async def add_job(self, job_data: Dict[str, Any]) -> None:
        """
        Add a new job to the system.

        Args:
            job_data (Dict[str, Any]): The job data including workflow and args.
        """
        # Wrap job creation in a transaction so we can roll back if blockchain transaction fails
        async with self.db.transaction() as conn:
            # Add job to database
            await self.db.add_job_lum(conn, job_data)
            # Create job on blockchain
            tx_hash = await self.job_manager_client.create_job(job_data['args'])
            # Update job with transaction hash
            await self.db.update_job_lum(
                conn,
                job_data['job_id'],
                job_data['user_id'],
                tx_hash=tx_hash
            )

        logger.info(
            f"Added LUM job id: {job_data['job_id']} "
            f"for user id: {job_data['user_id']}, "
            f"job data: {job_data}"
        )

    async def _update_job_statuses(self) -> None:
        """Monitor and update the status of LUM jobs."""
        # Get all non-final state jobs that have transaction hashes
        active_jobs = await self.db.get_jobs_by_status_lum([
            JOB_STATUS_NEW,
            JOB_STATUS_WAIT_FOR_VM,
            JOB_STATUS_RUNNING
        ])
        # Process each job
        await asyncio.gather(*[self._update_job_status_single(job) for job in active_jobs])

    async def _update_job_status_single(self, job: Dict[str, Any]) -> None:
        """
        Process a single LUM job.

        Args:
            job (Dict[str, Any]): The job to process.
        """
        # Get current status from blockchain
        new_status = await self.job_manager_client.get_job_status(
            job['lum']['lum_id']
        )
        # Update status if changed
        if new_status != job['status']:
            async with self.db.transaction() as conn:
                await self.db.update_job_lum(
                    conn,
                    job['job_id'],
                    job['user_id'],
                    status=new_status
                )
            logger.info(
                f"Updated LUM job {job['job_id']} status "
                f"from {job['status']} to {new_status}"
            )

    @retry_config_tx_receipt
    async def _get_tx_receipt(self, tx_hash: str) -> Optional[Tuple[str, Dict[str, Any]]]:
        """
        Wait for a transaction receipt with timeout.

        Args:
            tx_hash (str): The transaction hash
        Returns:
            Optional[Tuple[str, Dict[str, Any]]]: The transaction hash and receipt if found, None otherwise
        """
        receipt = await self.job_manager_client.web3.eth.get_transaction_receipt(tx_hash)
        if receipt:
            return tx_hash, receipt
        return None

    def _get_lum_id_from_receipt(self, receipt: Dict[str, Any]) -> Optional[int]:
        """
        Extract the LUM ID from a transaction receipt.

        The LUM ID is the protocol-specific job ID.

        Args:
            receipt (Dict[str, Any]): The transaction receipt
        Returns:
            Optional[int]: The LUM ID if found, None otherwise
        """
        event_signature_hash = self.job_manager_client.event_signature_hashes.get('JobCreated')
        if not event_signature_hash:
            logger.error("JobCreated event signature not found")
            return None

        for log in receipt['logs']:
            if log['topics'][0].hex() == event_signature_hash:
                # Extract the job ID from the log data (second topic)
                return int(log['topics'][1].hex(), 16)
        return None

    async def _process_pending_receipts(self) -> None:
        """
        Process all pending receipts for NEW jobs.
        """
        # Get all NEW jobs that have a transaction hash but no LUM ID
        jobs = await self.db.get_pending_lum_receipts()
        pending_jobs = [job for job in jobs
                        if job['lum']['tx_hash'] and not job['lum']['lum_id']]

        if not pending_jobs:
            return

        logger.info(f"Processing {len(pending_jobs)} pending receipts")

        # Map tx_hash to job dict
        tx_hash_to_job = {job['lum']['tx_hash']: job for job in pending_jobs}

        # Process receipts in parallel
        results = await asyncio.gather(*[self._get_tx_receipt(job['lum']['tx_hash']) for job in pending_jobs])

        # Wait for all receipt checks to complete
        for result in results:
            # Skip if no result, we logged an error in _get_tx_receipt()
            if not result:
                continue

            # Pull out tx_hash and receipt
            tx_hash, receipt = result
            # Get the job from the mapping
            job = tx_hash_to_job[tx_hash]
            # Extract the LUM ID from the receipt
            lum_id = self._get_lum_id_from_receipt(receipt)
            if lum_id:
                async with self.db.transaction() as conn:
                    await self.db.update_job_lum(conn, job['job_id'], job['user_id'], lum_id=lum_id)
                logger.info(f"Updated job {job['job_id']} with LUM ID {lum_id}")
            else:
                logger.error(f"Failed to extract LUM ID from receipt for job {job['job_id']}")
