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
        """Start scheduler and async tasks for the scheduler."""
        logger.info("Starting PubSub client")
        await self.pubsub.start()
        logger.info("PubSub client started")
        self.running = True
        logger.info("Scheduler started")

    async def stop(self) -> None:
        """Stop the scheduler and async tasks for the scheduler."""
        await self.pubsub.stop()
        logger.info("PubSub client stopped")
        self.running = False
        logger.info("Scheduler stopped")

    async def run_cycle(self) -> None:
        """Execute one complete cycle of scheduler tasks sequentially."""
        if not self.running:
            return

        try:
            logger.info("Starting scheduler cycle")
            # Schedule new jobs
            await self._schedule_jobs()
            # Monitor and detach VMs
            await self._monitor_and_detach_vms()
            # Process heartbeats
            await self._process_heartbeats()
            # Monitor and scale clusters
            await self._monitor_and_scale_clusters()
            logger.info("Completed scheduler cycle")
        except Exception as e:
            logger.error(f"Error in scheduler cycle: {str(e)}")

    async def add_job(self, job_data: Dict[str, Any]) -> None:
        """
        Add a new job to the system.

        Args:
            job_data (Dict[str, Any]): The job data.
        """
        async with self.db.transaction() as conn:
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
            async with self.db.transaction() as conn:
                await self.db.update_job_gcp(conn, job_id, user_id, JOB_STATUS_STOPPING)
                await self.pubsub.publish_stop_signal(config.job_stop_topic, job_id)
            logger.info(f"Stopped job id: {job_id} for user id: {user_id}")
            return True
        logger.warning(f"Job id: {job_id} for user id: {user_id} not running or does not exist")
        return False

    async def _schedule_jobs(self) -> None:
        """Schedule new jobs and update their status."""
        logger.info("Starting job scheduling")
        new_jobs = await self.db.get_jobs_by_status_gcp(JOB_STATUS_NEW)
        for job in new_jobs:
            # Add job_id, user_id, and num_gpus to args for the training workflow to pick up
            job['args']['job_id'] = job['job_id']
            job['args']['user_id'] = job['user_id']
            if job['workflow'] == 'torchtunewrapper':
                job['args']['num_gpus'] = job['gcp']['cluster'].split('x')[0]

            # Move override_env to top level
            if 'override_env' in job['args']:
                job['override_env'] = job['args'].pop('override_env')

            # Publish job start signal to PubSub and update job status in DB
            async with self.db.transaction() as conn:
                await self.db.update_job_gcp(conn, job['job_id'], job['user_id'], JOB_STATUS_WAIT_FOR_VM)
                await self.pubsub.publish_start_signal(config.job_start_topic, job)

            logger.info(f"Job '{job['job_id']}' status changed from {JOB_STATUS_NEW} to {JOB_STATUS_WAIT_FOR_VM}")

    async def _process_heartbeats(self) -> None:
        """Process any queued heartbeats."""
        logger.info("Starting processing heartbeats")
        while not self.pubsub.heartbeats_queue.empty():
            message = await self.pubsub.heartbeats_queue.get()
            await self._handle_heartbeat(message.data)
            message.ack()
        logger.info("Completed processing heartbeats")

    async def _monitor_and_scale_clusters(self) -> None:
        """Monitor cluster status and scale as necessary."""
        logger.info("Starting cluster monitoring and scaling")
        await self.cluster_orchestrator.scale_clusters()
        logger.info("Completed cluster monitoring and scaling")

    async def _monitor_and_detach_vms(self) -> None:
        """Monitor VMs that started running and detach them from the MIG."""
        logger.info("Starting VM monitoring and detaching")
        jobs = await self.db.get_jobs_by_status_gcp(JOB_STATUS_FOUND_VM)

        for job in jobs:
            try:
                async with self.db.transaction() as conn:
                    await self.db.update_job_gcp(
                        conn, job['job_id'], job['user_id'], JOB_STATUS_DETACHED_VM)
                    await self.mig_client.detach_vm(job['gcp']['vm_name'], job['job_id'])
                logger.info(f"Detached VM {job['gcp']['vm_name']} for job {job['job_id']}")
            except Exception as e:
                logger.error(f"Error detaching VM for job {job['job_id']}: {str(e)}")

    async def _handle_heartbeat(self, message_data: str) -> None:
        """
        Process a single heartbeat message and update job status.

        Args:
            message_data (str): JSON-encoded heartbeat data.
        """
        data = json.loads(message_data)
        job_id = data['job_id']
        user_id = data.get('user_id', "0")
        new_status = data['status']
        vm_name = data['vm_name']
        region = get_region_from_vm_name(vm_name)

        # Skip workflow status updates, we don't care about them yet
        if new_status.startswith('wf-'):
            return

        # Validation
        job = await self.db.get_job(job_id, user_id)
        if not job:
            logger.info(f"Ignoring heartbeat for non-existent job id: {job_id}")
            return
        old_status = job['status']
        if not is_new_job_status_valid(old_status, new_status):
            logger.info(f"Ignoring heartbeat for invalid status for job id: {job_id}; "
                        f"old status: {old_status}; new status: {new_status}")
            return

        # If the job is stopped, remove the VM from in-memory cache
        if new_status == JOB_STATUS_STOPPED:
            self.mig_client.remove_vm_from_cache(region, vm_name)

        # Everything is good, update the job status
        async with self.db.transaction() as conn:
            await self.db.update_job_gcp(conn, job_id, user_id, new_status, vm_name, region)

        logger.info(f"Job '{job_id}' status changed from {old_status} to {new_status}; "
                    f"region: {region}; VM name: {vm_name}")
