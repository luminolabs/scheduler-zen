import asyncio
import json
from datetime import datetime
from typing import Any, Dict, List

from app.config_manager import config
from app.utils import (
    JOB_STATUS_RUNNING, JOB_STATUS_PENDING, JOB_STATUS_COMPLETED,
    JOB_STATUS_FAILED, setup_logger, JOB_STATUS_NEW, get_region_from_vm_name, is_new_job_status_valid,
    JOB_STATUS_STOPPING
)
from app.database import Database
from app.pubsub_client import PubSubClient
from app.cluster_orchestrator import ClusterOrchestrator

logger = setup_logger(__name__)

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
        logger.info("Scheduler initialized")

    async def start(self) -> None:
        """Start the scheduler to manage jobs."""
        self.running = True
        await self.db.create_tables()  # Ensure tables are created
        logger.info("Scheduler started")
        asyncio.create_task(self.pubsub.start())
        await asyncio.gather(
            self._schedule_jobs(),
            self._listen_for_heartbeats(),
            self._monitor_and_scale_clusters()
        )

    async def stop(self) -> None:
        """Stop the scheduler."""
        self.running = False
        await self.pubsub.stop()
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
        # Log the activity
        activity_description = f"Added new job with ID: {job_id}; status: {JOB_STATUS_NEW}"
        await self.db.log_activity(activity_description)
        logger.info(activity_description)
        return job_id

    async def _schedule_jobs(self) -> None:
        """Schedule new jobs and update their status."""
        logger.info("Starting job scheduling")
        while self.running:
            new_jobs = await self.db.get_jobs_by_status(JOB_STATUS_NEW)
            for job in new_jobs:
                # Add job_id to args for the training workflow to pick up
                job['args']['job_id'] = job['job_id']
                # Publish start signal to Pub/Sub
                await self.pubsub.publish_start_signal('pipeline-zen-jobs-start', job)
                # Update job status to PENDING in the database
                await self.db.update_job(job['job_id'], JOB_STATUS_PENDING)
                # Log the activity
                activity_description = f"Job '{job['job_id']}' status changed from {JOB_STATUS_NEW} to {JOB_STATUS_PENDING}"
                await self.db.log_activity(activity_description)
                logger.info(activity_description)
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
            new_status = data['status']
            vm_name = data['vm_name']
            region = get_region_from_vm_name(vm_name)

            job = await self.db.get_job(job_id)
            if not job:
                logger.info(f"Ignoring heartbeat for non-existent job id: {job_id}")
                return

            old_status = job['status']
            if not is_new_job_status_valid(old_status, new_status):
                return

            # Update job status and timestamp in the database
            await self.db.update_job(job_id, new_status, vm_name, region)

            # Log the activity only for specific status changes
            if (old_status != new_status and
                    (new_status != JOB_STATUS_RUNNING or old_status == JOB_STATUS_PENDING)):
                activity_description = f"Job '{job_id}' status changed from {old_status} to {new_status}; region: {region}; VM name: {vm_name}"
                await self.db.log_activity(activity_description)
                logger.info(activity_description)

        self.pubsub.heartbeat_callback = heartbeat_callback
        await self.pubsub.listen_for_heartbeats(config.heartbeat_subscription)

    async def _monitor_and_scale_clusters(self) -> None:
        """Monitor cluster status and scale as necessary."""
        logger.info("Starting cluster monitoring and scaling")
        while self.running:
            # Get information needed for scaling
            pending_job_counts = await self._get_pending_jobs_by_cluster()
            # Scale clusters
            await self.cluster_orchestrator.scale_clusters(pending_job_counts)
            await asyncio.sleep(10)

    async def _get_pending_jobs_by_cluster(self) -> Dict[str, int]:
        """
        Get the count of pending jobs for each cluster.

        ex. {'cluster1': 2, 'cluster2': 1}

        Returns:
            Dict[str, int]: A dictionary mapping cluster names to pending job counts.
        """
        pending_jobs = await self.db.get_jobs_by_status(JOB_STATUS_PENDING)
        mapping = {}
        for job in pending_jobs:
            cluster = job['cluster']
            mapping[cluster] = mapping.get(cluster, 0) + 1
        return mapping

    async def stop_job(self, job_id: str) -> bool:
        """
        Stop a running job.

        Args:
            job_id (str): The ID of the job to stop.

        Returns:
            bool: True if the job was stopped, False otherwise.
        """
        job = await self.db.get_job(job_id)
        if job and job['status'] == JOB_STATUS_RUNNING:
            await self.pubsub.publish_stop_signal('pipeline-zen-jobs-stop', job_id)
            await self.db.update_job(job_id, JOB_STATUS_STOPPING)
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
        status = await self.cluster_orchestrator.get_status()

        # Add completed and failed job counts
        completed_jobs = await self.db.get_jobs_by_status(JOB_STATUS_COMPLETED)
        failed_jobs = await self.db.get_jobs_by_status(JOB_STATUS_FAILED)
        status["overall_summary"]["completed_jobs"] = len(completed_jobs)
        status["overall_summary"]["failed_jobs"] = len(failed_jobs)
        status["overall_summary"]["total_jobs"] += len(completed_jobs) + len(failed_jobs)

        # Add timestamp
        status["timestamp"] = datetime.utcnow().isoformat() + "Z"

        # Add recent activities
        status["recent_activities"] = await self.get_recent_activities()

        logger.info(f"Scheduler status: {status}")
        return status

    async def get_recent_activities(self) -> List[Dict[str, str]]:
        """
        Get recent activities from the system.

        Returns:
            List[Dict[str, str]]: A list of recent activities.
        """
        activities = await self.db.get_recent_activities(limit=10)

        return [
            {
                "timestamp": activity["timestamp"].isoformat() + "Z",
                "activity": activity["description"]
            }
            for activity in activities
        ]
