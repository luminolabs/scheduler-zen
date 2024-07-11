import asyncio
import json
import logging
from typing import Any, Dict

from cluster_orchestrator import ClusterOrchestrator
from database import Database
from pubsub_client import PubSubClient

logging.basicConfig(level=logging.INFO)


class Scheduler:
    """
    Manages the scheduling of jobs in the system.
    """

    def __init__(self, db: Database, pubsub: PubSubClient, cluster_orchestrator: ClusterOrchestrator):
        """
        Initializes the Scheduler.

        Args:
            db (Database): The database instance for job tracking.
            pubsub (PubSubClient): The PubSub client for messaging.
            cluster_orchestrator (ClusterOrchestrator): The cluster orchestrator.
        """
        self.db = db
        self.pubsub = pubsub
        self.cluster_orchestrator = cluster_orchestrator
        self.running = False
        logging.info("Scheduler initialized")

    async def start(self) -> None:
        """
        Starts the scheduler to manage jobs.
        """
        self.running = True
        logging.info("Scheduler started")
        await asyncio.gather(
            self._schedule_jobs(),
            self._monitor_jobs(),
            self._listen_for_heartbeats()
        )

    async def stop(self) -> None:
        """
        Stops the scheduler.
        """
        self.running = False
        logging.info("Scheduler stopped")

    async def _schedule_jobs(self) -> None:
        """
        Schedules new jobs and scales the cluster accordingly.
        """
        logging.info("Starting job scheduling")
        while self.running:
            new_jobs = self.db.get_jobs_by_status('NEW')
            for job in new_jobs:
                await self.pubsub.publish_job('pipeline-zen-jobs', job['data'])
                await self.cluster_orchestrator.scale_cluster(job['data']['gpu_config'], scale_amount=1)
                self.db.update_job(job['id'], 'PENDING')
                logging.info("Scheduled job id: %s", job['id'])
            await asyncio.sleep(10)  # Check for new jobs every 10 seconds

    async def _monitor_jobs(self) -> None:
        """
        Monitors the running jobs to ensure they are progressing.
        """
        logging.info("Starting job monitoring")
        while self.running:
            running_jobs = self.db.get_jobs_by_status('RUNNING')
            for job in running_jobs:
                # TODO: Implement job heartbeat monitoring
                pass
            await asyncio.sleep(60)  # Monitor jobs every minute

    async def _listen_for_heartbeats(self) -> None:
        """
        Listens for job heartbeats to update their status.
        """
        logging.info("Listening for job heartbeats")

        async def heartbeat_callback(message_data: str) -> None:
            data = json.loads(message_data)
            job_id = data['job_id']
            status = data['status']
            self.db.update_job(job_id, status)
            logging.info("Updated job id: %s with status: %s", job_id, status)

        await self.pubsub.listen_for_heartbeats('pipeline-zen-jobs-heartbeats', heartbeat_callback)

    async def stop_job(self, job_id: str) -> bool:
        """
        Stops a running job.

        Args:
            job_id (str): The ID of the job to stop.

        Returns:
            bool: True if the job was stopped, False otherwise.
        """
        job = self.db.get_job(job_id)
        if job and job['status'] == 'RUNNING':
            await self.pubsub.publish_stop_signal('pipeline-zen-jobs-stop', job_id)
            self.db.update_job(job_id, 'STOPPING')
            logging.info("Stopped job id: %s", job_id)
            return True
        logging.warning("Job id: %s not running or does not exist", job_id)
        return False

    async def get_status(self) -> Dict[str, Any]:
        """
        Gets the status of the scheduler, including cluster and job statuses.

        Returns:
            Dict[str, Any]: A dictionary with the scheduler status.
        """
        cluster_status = await self.cluster_orchestrator.get_cluster_status()
        running_jobs = self.db.get_jobs_by_status('RUNNING')
        pending_jobs = self.db.get_jobs_by_status('PENDING')
        status = {
            'cluster_status': cluster_status,
            'running_jobs': len(running_jobs),
            'pending_jobs': len(pending_jobs)
        }
        logging.info("Scheduler status: %s", status)
        return status
