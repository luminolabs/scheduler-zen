import asyncio
import json
from typing import Any, Dict

from app.config_manager import config
from app.mig_client import MigClient
from app.utils import (
    JOB_STATUS_NEW,
    JOB_STATUS_WAIT_FOR_VM, JOB_STATUS_FOUND_VM, JOB_STATUS_DETACHED_VM,
    JOB_STATUS_RUNNING, JOB_STATUS_STOPPING,
    setup_logger, get_region_from_vm_name, is_new_job_status_valid, JOB_STATUS_STOPPED
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
        await self.db.create_tables()  # Ensure tables are created
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
        logger.info(f"Added new job with ID: {job_id}; status: {JOB_STATUS_NEW}")
        return job_id

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

    async def _schedule_jobs(self) -> None:
        """Schedule new jobs and update their status."""
        logger.info("Starting job scheduling")
        while self.running:
            new_jobs = await self.db.get_jobs_by_status(JOB_STATUS_NEW)
            for job in new_jobs:
                # Add job_id and num_gpus to args for the training workflow to pick up
                # That's because the training workflow only picks up `args`; not the entire job object
                job['args']['job_id'] = job['job_id']
                #  `num_gpus` is only needed for the `torchtunewrapper` workflow
                if job['workflow'] == 'torchtunewrapper':
                    job['args']['num_gpus'] = job['cluster'].split('x')[0]  # Extract number of GPUs from cluster name
                # Update job status to WAIT_FOR_VM
                await self.db.update_job(job['job_id'], JOB_STATUS_WAIT_FOR_VM)
                # Publish start signal to Pub/Sub
                await self.pubsub.publish_start_signal('pipeline-zen-jobs-start', job)
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
        logger.info("Starting VM monitoring and detaching")
        while self.running:
            # Get a list of VMs to detach
            jobs = await self.db.get_jobs_by_status(JOB_STATUS_FOUND_VM)
            # Detach VMs in parallel and update their status in the database
            logger.info("_monitor_and_detach_vms: start")
            logger.info(f"Detaching VMs: "
                        f"{[job['vm_name'] for job in jobs]} for jobs:"
                        f" {[job['job_id'] for job in jobs]}")
            await asyncio.gather(*[self.mig_client.detach_vm(job['vm_name'], job['job_id']) for job in jobs])
            await asyncio.gather(*[self.db.update_job(job['job_id'], JOB_STATUS_DETACHED_VM) for job in jobs])
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

            # If new status is `STOPPED`, then update the MIG client cache
            if new_status == JOB_STATUS_STOPPED:
                self.mig_client.remove_vm_from_cache(region, vm_name)

            # Update job status and timestamp in the database
            await self.db.update_job(job_id, new_status, vm_name, region)
            logger.info(f"Job '{job_id}' status changed from {old_status} to {new_status}; region: {region}; VM name: {vm_name}")

        self.pubsub.heartbeat_callback = heartbeat_callback
        await self.pubsub.listen_for_heartbeats(config.heartbeat_subscription)
