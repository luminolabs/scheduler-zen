import asyncio
import json
import logging
from typing import Any, Dict

from database import Database
from pubsub_client import PubSubClient
from cluster_orchestrator import ClusterOrchestrator

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class Scheduler:
    """Manages the scheduling of jobs in the system."""

    def __init__(
            self,
            db: Database,
            pubsub: PubSubClient,
            cluster_orchestrator: ClusterOrchestrator
    ) -> None:
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
        self.cluster_status: Dict[str, Dict[str, Any]] = {}
        logger.info("Scheduler initialized")

    async def start(self) -> None:
        """Start the scheduler to manage jobs."""
        self.running = True
        await self.db.create_tables()  # Ensure tables are created
        logger.info("Scheduler started")
        await asyncio.gather(
            self._schedule_jobs(),
            self._listen_for_heartbeats(),
            self._monitor_and_scale_clusters()
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
        """Schedule new jobs and update their status."""
        logger.info("Starting job scheduling")
        while self.running:
            new_jobs = await self.db.get_jobs_by_status('NEW')
            for job in new_jobs:
                await self.pubsub.publish_start_signal('pipeline-zen-jobs-start', job)
                await self.db.update_job(job['job_id'], 'PENDING')
                logger.info(f"Scheduled job id: {job['job_id']} for cluster: {job['cluster']}")
            await asyncio.sleep(5)

    async def _listen_for_heartbeats(self) -> None:
        """Listen for job heartbeats to update their status."""
        logger.info("Listening for job heartbeats")

        async def heartbeat_callback(message_data: str) -> None:
            """
            Process heartbeat messages and update job status.

            Args:
                message_data (str): JSON-encoded heartbeat data.
            """
            data = json.loads(message_data)
            job_id = data['job_id']
            status = data['status']
            vm_name = data['vm_name']
            await self.db.update_job(job_id, status, vm_name)
            logger.info(f"Updated job id: {job_id} with status: {status} and VM name: {vm_name}")

        await self.pubsub.listen_for_heartbeats('pipeline-zen-jobs-heartbeats-scheduler',
                                                heartbeat_callback)

    async def _monitor_and_scale_clusters(self) -> None:
        """Monitor cluster status and scale as necessary."""
        logger.info("Starting cluster monitoring and scaling")
        while self.running:
            # Update cluster status
            self.cluster_status = await self.cluster_orchestrator.update_status()
            logger.info(f"Updated cluster status: {self.cluster_status}")
            # Get information needed for scaling
            pending_jobs = await self._get_pending_jobs_by_cluster()
            # Scale clusters
            await self.cluster_orchestrator.scale_clusters(pending_jobs)
            await asyncio.sleep(10)

    async def _get_pending_jobs_by_cluster(self) -> Dict[str, int]:
        """
        Get the count of pending jobs for each cluster.

        Returns:
            Dict[str, int]: A dictionary mapping cluster names to pending job counts.
        """
        pending_jobs = await self.db.get_pending_jobs()
        pending_jobs_by_cluster = {}
        for job in pending_jobs:
            cluster = job['cluster']
            pending_jobs_by_cluster[cluster] = pending_jobs_by_cluster.get(cluster, 0) + 1
        return pending_jobs_by_cluster

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
        running_jobs = await self.db.get_jobs_by_status('RUNNING')
        pending_jobs = await self.db.get_jobs_by_status('PENDING')
        status = {
            'cluster_status': self.cluster_status,
            'running_jobs': len(running_jobs),
            'pending_jobs': len(pending_jobs)
        }
        logger.info(f"Scheduler status: {status}")
        return status
