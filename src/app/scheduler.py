import asyncio
import json
import logging
from typing import Any, Dict

from database import Database
from pubsub_client import PubSubClient
from cluster_orchestrator import ClusterOrchestrator

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class Scheduler:
    """Manages the scheduling of jobs in the system."""

    def __init__(self, db: Database, pubsub: PubSubClient, cluster_orchestrator: ClusterOrchestrator):
        """
        Initialize the Scheduler.

        Args:
            db (Database): The database instance for job tracking.
            pubsub (PubSubClient): The PubSub client for messaging.
            cluster_orchestrator (ClusterOrchestrator): The cluster orchestrator.
        """
        self.db = db
        self.pubsub = pubsub
        self.cluster_orchestrator = cluster_orchestrator
        self.running = False
        logger.info("Scheduler initialized")

    async def start(self) -> None:
        """Start the scheduler to manage jobs."""
        self.running = True
        await self.db.create_tables()  # Ensure tables are created
        logger.info("Scheduler started")
        await asyncio.gather(
            self._schedule_jobs(),
            self._monitor_jobs(),
            self._listen_for_heartbeats()
        )

    async def stop(self) -> None:
        """Stop the scheduler."""
        self.running = False
        logger.info("Scheduler stopped")

    async def add_job(self, job_data: Dict[str, Any]) -> str:
        """
        Add a new job to the system.

        Args:
            job_data (Dict[str, Any]): The job data including workflow, args, keep_alive, and cluster.

        Returns:
            str: The ID of the newly added job.
        """
        job_id = await self.db.add_job(job_data)
        logger.info(f"Added new job with ID: {job_id}")
        return job_id

    async def _schedule_jobs(self) -> None:
        """Schedule new jobs and scale the cluster accordingly."""
        logger.info("Starting job scheduling")
        while self.running:
            new_jobs = await self.db.get_jobs_by_status('NEW')
            for job in new_jobs:
                await self.pubsub.publish_job('pipeline-zen-jobs-start', job)
                await self.cluster_orchestrator.scale_cluster(job['cluster'], scale_amount=1)
                await self.db.update_job(job['job_id'], 'PENDING')
                logger.info(f"Scheduled job id: {job['job_id']}")
            await asyncio.sleep(10)  # Check for new jobs every 10 seconds

    async def _monitor_jobs(self) -> None:
        """Monitor the running jobs to ensure they are progressing."""
        logger.info("Starting job monitoring")
        while self.running:
            running_jobs = await self.db.get_jobs_by_status('RUNNING')
            for job in running_jobs:
                # TODO: Implement job heartbeat monitoring
                pass
            await asyncio.sleep(60)  # Monitor jobs every minute

    async def _listen_for_heartbeats(self) -> None:
        """Listen for job heartbeats to update their status."""
        logger.info("Listening for job heartbeats")

        async def heartbeat_callback(message_data: str) -> None:
            data = json.loads(message_data)
            job_id = data['job_id']
            status = data['status']
            await self.db.update_job(job_id, status)
            logger.info(f"Updated job id: {job_id} with status: {status}")

        await self.pubsub.listen_for_heartbeats('pipeline-zen-jobs-heartbeats-scheduler', heartbeat_callback)

    async def stop_job(self, job_id: str) -> bool:
        """
        Stop a running job.

        Args:
            job_id (str): The ID of the job to stop.

        Returns:
            bool: True if the job was stopped, False otherwise.
        """
        job = await self.db.get_job(job_id)
        if job and job['status'] == 'RUNNING':
            await self.pubsub.publish_stop_signal('pipeline-zen-jobs-stop', job_id)
            await self.db.update_job(job_id, 'STOPPING')
            logger.info(f"Stopped job id: {job_id}")
            return True
        logger.warning(f"Job id: {job_id} not running or does not exist")
        return False

    async def get_status(self) -> Dict[str, Any]:
        """
        Get the status of the scheduler, including cluster and job statuses.

        Returns:
            Dict[str, Any]: A dictionary with the scheduler status.
        """
        cluster_status = await self.cluster_orchestrator.get_cluster_status()
        running_jobs = await self.db.get_jobs_by_status('RUNNING')
        pending_jobs = await self.db.get_jobs_by_status('PENDING')
        status = {
            'cluster_status': cluster_status,
            'running_jobs': len(running_jobs),
            'pending_jobs': len(pending_jobs)
        }
        logger.info(f"Scheduler status: {status}")
        return status
