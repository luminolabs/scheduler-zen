import asyncio
import json
from typing import Any, Dict

from app.core.config_manager import config
from app.core.database import Database
from app.core.utils import (
    JOB_STATUS_NEW,
    JOB_STATUS_WAIT_FOR_VM, JOB_STATUS_FOUND_VM, JOB_STATUS_DETACHED_VM,
    JOB_STATUS_RUNNING, JOB_STATUS_STOPPING,
    setup_logger, is_new_job_status_valid, JOB_STATUS_STOPPED
)
from app.gcp.cluster_orchestrator import ClusterOrchestrator
from app.gcp.mig_client import MigClient
from app.gcp.pubsub_client import PubSubClient
from app.gcp.utils import get_region_from_vm_name

logger = setup_logger(__name__)


class Scheduler:
    """Manages the scheduling of jobs in the system."""

    def __init__(
            self,
            db: Database,
            pubsub: PubSubClient,
            cluster_orchestrator: ClusterOrchestrator,
            mig_client: MigClient
    ) -> None:
        """
        Initialize the Scheduler.

        Args:
            db (Database): The database instance for job tracking.
            pubsub (PubSubClient): The PubSub client for messaging.
            cluster_orchestrator (ClusterOrchestrator): The cluster orchestrator.
            mig_client (MigClient): The MIG client for managing MIGs.
        """
        self.db = db
        self.pubsub = pubsub
        self.cluster_orchestrator = cluster_orchestrator
        self.mig_client = mig_client
        self.running = False
        logger.info("Scheduler initialized")

    async def start(self) -> None:
        """Start the scheduler to manage jobs."""
        self.running = True
        logger.info("Scheduler started")
        asyncio.create_task(self.pubsub.start())
        await asyncio.gather(
            self._schedule_jobs(),
            self._listen_for_heartbeats(),
            self._monitor_and_scale_clusters(),
            self._monitor_and_detach_vms()
        )

    async def stop(self) -> None:
        """Stop the scheduler."""
        self.running = False
        await self.pubsub.stop()
        logger.info("Scheduler stopped")

    async def add_job(self, job_data: Dict[str, Any]) -> None:
        """
        Add a new job to the system.

        Args:
            job_data (Dict[str, Any]): The job data.
        """
        async with self.db.transaction as conn:
            await self.db.add_job_gcp(conn, job_data)
        logger.info(f"Added job id: {job_data['job_id']} for user id: {job_data['user_id']}, job data: {job_data}")

    async def stop_job(self, job_id: str, user_id: str) -> bool:
        """
        Stop a running job.

        Args:
            job_id (str): The ID of the job to stop.
            user_id (str): The ID of the user who owns the job.

        Returns:
            bool: True if the job was stopped, False otherwise.
        """
        job = await self.db.get_job(job_id, user_id)
        if job and job['status'] == JOB_STATUS_RUNNING:
            async with self.db.transaction as conn:
                await self.db.update_job_gcp(conn, job_id, user_id, JOB_STATUS_STOPPING)
                await self.pubsub.publish_stop_signal(config.job_stop_topic, job_id)
            logger.info(f"Stopped job id: {job_id} for user id: {user_id}")
            return True
        logger.warning(f"Job id: {job_id} for user id: {user_id} not running or does not exist")
        return False

    async def _schedule_jobs(self) -> None:
        """Schedule new jobs and update their status."""
        logger.info("Starting job scheduling")
        while self.running:
            new_jobs = await self.db.get_jobs_by_status_gcp(JOB_STATUS_NEW)
            for job in new_jobs:
                # Add job_id, user_id, and num_gpus to args for the training workflow to pick up
                # That's because the training workflow only picks up `args`; not the entire job object
                job['args']['job_id'] = job['job_id']
                job['args']['user_id'] = job['user_id']
                #  `num_gpus` is only needed for the `torchtunewrapper` workflow
                if job['workflow'] == 'torchtunewrapper':
                    job['args']['num_gpus'] = job['gcp']['cluster'].split('x')[0]  # Extract number of GPUs from cluster name
                # Move `override_env` to the top level of the job data
                if 'override_env' in job['args']:
                    job['override_env'] = job['args'].pop('override_env')

                async with self.db.transaction as conn:
                    # Update job status to WAIT_FOR_VM
                    await self.db.update_job_gcp(conn, job['job_id'], job['user_id'], JOB_STATUS_WAIT_FOR_VM)
                    # Publish start signal to Pub/Sub
                    await self.pubsub.publish_start_signal(config.job_start_topic, job)

                # Log the activity
                logger.info(f"Job '{job['job_id']}' status changed from {JOB_STATUS_NEW} to {JOB_STATUS_WAIT_FOR_VM}")
            await asyncio.sleep(10)

    async def _monitor_and_scale_clusters(self) -> None:
        """Monitor cluster status and scale as necessary."""
        logger.info("Starting cluster monitoring and scaling")
        while self.running:
            # Scale clusters
            logger.info("_monitor_and_scale_clusters: start")
            await self.cluster_orchestrator.scale_clusters()
            logger.info("_monitor_and_scale_clusters: end")
            await asyncio.sleep(17)

    async def _monitor_and_detach_vms(self):
        """Monitor VMs that started running and detach them from the MIG."""

        async def _detach_vm_and_update_job_status(job):
            """Detach a VM from the MIG and update job status in the database."""
            async with self.db.transaction as conn:
                await self.db.update_job_gcp(conn, job['job_id'], job['user_id'], JOB_STATUS_DETACHED_VM)
                await self.mig_client.detach_vm(job['gcp']['vm_name'], job['job_id'])

        logger.info("Starting VM monitoring and detaching")
        while self.running:
            # Get a list of VMs to detach
            jobs = await self.db.get_jobs_by_status_gcp(JOB_STATUS_FOUND_VM)

            logger.info("_monitor_and_detach_vms: start")
            logger.info(f"Detaching VMs: "
                        f"{[job['gcp']['vm_name'] for job in jobs]} for jobs:"
                        f" {[job['job_id'] for job in jobs]}")

            # Detach VMs from the MIG and update job status in the database
            await asyncio.gather(*[_detach_vm_and_update_job_status(job) for job in jobs])

            logger.info('_monitor_and_detach_vms: end')
            await asyncio.sleep(7)

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
            user_id = data.get('user_id', "0")
            new_status = data['status']
            vm_name = data['vm_name']
            region = get_region_from_vm_name(vm_name)

            # Ignore heartbeats for workflow statuses
            # We'll come back to this later:
            # https://linear.app/luminoai/issue/LUM-640/mechanism-to-stream-job-logs-and-metrics
            if new_status.startswith('wf-'):
                return

            job = await self.db.get_job(job_id, user_id)
            if not job:
                logger.info(f"Ignoring heartbeat for non-existent job id: {job_id}")
                return

            old_status = job['status']
            if not is_new_job_status_valid(old_status, new_status):
                return

            # If new status is `STOPPED`, then update the MIG client cache
            if new_status == JOB_STATUS_STOPPED:
                self.mig_client.remove_vm_from_cache(region, vm_name)

            # Update job status and timestamp in the database
            async with self.db.transaction as conn:
                await self.db.update_job_gcp(conn, job_id, user_id, new_status, vm_name, region)

            logger.info(f"Job '{job_id}' status changed from {old_status} to {new_status}; region: {region}; VM name: {vm_name}")

        self.pubsub.heartbeat_callback = heartbeat_callback
        await self.pubsub.listen_for_heartbeats(config.heartbeat_subscription)
