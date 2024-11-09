import asyncio
from typing import Dict, Any, Optional

from web3.exceptions import TransactionNotFound

from app.core.database import Database
from app.core.utils import setup_logger
from app.lum.job_manager_client import JobManagerClient

logger = setup_logger(__name__)

async def get_receipt_with_timeout(job_manager_client: JobManagerClient, tx_hash: str,
                                   timeout: int = 120, poll_interval: int = 2) -> Optional[Dict[str, Any]]:
    """
    Wait for a transaction receipt with timeout.

    Args:
        job_manager_client (JobManagerClient): The JobManagerClient instance
        tx_hash (str): The transaction hash
        timeout (int): Maximum time to wait in seconds
        poll_interval (int): Time between polls in seconds
    Returns:
        Optional[Dict[str, Any]]: The transaction receipt or None if not found
    """
    for _ in range(timeout // poll_interval):
        try:
            receipt = await job_manager_client.web3.eth.get_transaction_receipt(tx_hash)
            if receipt:
                return receipt
        except TransactionNotFound:
            pass
        await asyncio.sleep(poll_interval)
    return None

async def extract_lum_id_from_receipt(job_manager_client: JobManagerClient,
                                      receipt: Dict[str, Any]) -> Optional[int]:
    """
    Extract the LUM ID from a transaction receipt.

    The LUM ID is the protocol-specific job ID.

    Args:
        job_manager_client (JobManagerClient): The JobManagerClient instance
        receipt (Dict[str, Any]): The transaction receipt
    Returns:
        Optional[int]: The LUM ID if found, None otherwise
    """
    event_signature_hash = job_manager_client.event_signature_hashes.get('JobCreated')
    if not event_signature_hash:
        logger.error("JobCreated event signature not found")
        return None

    for log in receipt['logs']:
        if log['topics'][0].hex() == event_signature_hash:
            # Extract the job ID from the log data (second topic)
            return int(log['topics'][1].hex(), 16)
    return None

async def process_pending_receipts(db: Database,
                                   job_manager_client: JobManagerClient) -> None:
    """
    Process all pending receipts for NEW jobs.

    Args:
        db (Database): The database instance
        job_manager_client (JobManagerClient): The JobManagerClient instance
    """
    # Get all NEW jobs that have a transaction hash but no LUM ID
    jobs = await db.get_pending_lum_receipts()
    pending_jobs = [job for job in jobs
                    if job['lum']['tx_hash'] and not job['lum']['lum_id']]

    if not pending_jobs:
        return

    logger.info(f"Processing {len(pending_jobs)} pending receipts")

    # Process receipts in parallel
    tasks = []
    for job in pending_jobs:
        task = get_receipt_with_timeout(job_manager_client, job['lum']['tx_hash'])
        tasks.append((job, task))

    # Wait for all receipt checks to complete
    for job, task in tasks:
        try:
            receipt = await task
            if receipt:
                lum_id = await extract_lum_id_from_receipt(job_manager_client, receipt)
                if lum_id:
                    await db.update_job_lum(job['job_id'], job['user_id'], lum_id=lum_id)
                    logger.info(f"Updated job {job['job_id']} with LUM ID {lum_id}")
        except Exception as e:
            logger.error(f"Error processing receipt for job {job['job_id']}: {str(e)}")

async def run_receipt_sync(db: Database, job_manager_client: JobManagerClient):
    """
    Run the receipt sync task periodically.
    """
    while True:
        try:
            await process_pending_receipts(db, job_manager_client)
        except Exception as e:
            logger.error(f"Error in receipt sync task: {str(e)}")
        await asyncio.sleep(10)  # Run every 10 seconds

def start_receipt_sync_task(db: Database, job_manager_client: JobManagerClient):
    """
    Start the receipt sync background task.
    """
    asyncio.create_task(run_receipt_sync(db, job_manager_client))
    logger.info("Started receipt sync background task")
