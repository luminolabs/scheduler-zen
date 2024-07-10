# scheduler.py

import asyncio
import logging
from typing import Dict, List, Optional
import time

from models import Job, JobStatus, Helm, ComputeInstance
from mig_manager import MIGManager
from pubsub_client import PubSubClient
from database import Database

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

GCP_PROJECT_ID = "neat-airport-407301"


class Scheduler:
    """
    Scheduler class responsible for managing jobs, VMs, and MIGs.
    """

    def __init__(self):
        """Initialize the Scheduler with its dependencies."""
        self.mig_manager = MIGManager(project_id=GCP_PROJECT_ID)
        self.pubsub_client = PubSubClient(project_id=GCP_PROJECT_ID)
        self.db = Database()
        self.running = False
        self.vm_statuses: Dict[str, ComputeInstance] = {}

    async def start(self):
        """Start the scheduler and its background tasks."""
        logger.info("Starting scheduler")
        self.running = True
        await self.db.create_tables()
        await asyncio.create_task(self._main_loop())
        await asyncio.create_task(self._listen_for_vm_updates())

    async def stop(self):
        """Stop the scheduler and its background tasks."""
        logger.info("Stopping scheduler")
        self.running = False

    async def add_job(self, job_data: dict) -> Job:
        """
        Add a new job to the system.

        Args:
            job_data (dict): Job details including model, dataset, and hyperparameters.

        Returns:
            Job: The created job object.
        """
        logger.info(f"Adding new job: {job_data}")
        job = Job(status=JobStatus.NEW, **job_data)
        job_id = await self.db.add_job(job)
        return await self.db.get_job(job_id)

    async def get_job(self, job_id: str) -> Optional[Job]:
        """
        Retrieve a job by its ID.

        Args:
            job_id (str): The ID of the job to retrieve.

        Returns:
            Optional[Job]: The job if found, None otherwise.
        """
        logger.info(f"Retrieving job: {job_id}")
        return await self.db.get_job(job_id)

    async def list_jobs(self, status: Optional[JobStatus] = None) -> List[Job]:
        """
        List jobs, optionally filtered by status.

        Args:
            status (Optional[JobStatus]): Filter jobs by this status.

        Returns:
            List[Job]: List of jobs matching the criteria.
        """
        logger.info(f"Listing jobs with status filter: {status}")
        return await self.db.get_jobs(status)

    async def stop_job(self, job_id: str) -> bool:
        """
        Stop a running job.

        Args:
            job_id (str): The ID of the job to stop.

        Returns:
            bool: True if the stop signal was sent successfully, False otherwise.
        """
        logger.info(f"Attempting to stop job: {job_id}")
        job = await self.db.get_job(job_id)
        if job and job.status == JobStatus.RUNNING:
            await self.pubsub_client.publish_stop_job_signal(job_id)
            return True
        return False

    async def get_status(self) -> Helm:
        """
        Get the current status of the scheduler.

        Returns:
            Helm: Current status including running jobs, VM statuses, and MIG sizes.
        """
        logger.info("Fetching scheduler status")
        running_jobs = await self.db.get_jobs(JobStatus.RUNNING)
        pending_jobs = await self.db.get_jobs(JobStatus.PENDING)
        mig_sizes = {region: await self.mig_manager.get_mig_size(region)
                     for region in self.mig_manager.regions}
        return Helm(
            running_jobs=running_jobs,
            pending_jobs=pending_jobs,
            vm_statuses=self.vm_statuses,
            mig_sizes=mig_sizes
        )

    async def _main_loop(self):
        """Main loop of the scheduler, running periodic tasks."""
        while self.running:
            await self._process_new_jobs()
            await self._monitor_jobs()
            await self._monitor_idle_vms()
            await asyncio.sleep(60)  # Run loop every minute

    async def _process_new_jobs(self):
        """Process newly added jobs."""
        new_jobs = await self.db.get_jobs(JobStatus.NEW)
        for job in new_jobs:
            logger.info(f"Processing new job: {job.id}")
            await self.pubsub_client.publish_start_job_signal(job)
            await self.mig_manager.scale_up_all_regions()
            await self.db.update_job(job.id, status=JobStatus.PENDING)

    async def _monitor_jobs(self):
        """Monitor and update status of running jobs."""
        running_jobs = await self.db.get_jobs(JobStatus.RUNNING)
        for job in running_jobs:
            status = self.vm_statuses.get(job.vm_name)
            if status and status.job_id == job.id and status.status == JobStatus.COMPLETED:
                logger.info(f"Job completed: {job.id}")
                await self.db.update_job(job.id, status=JobStatus.COMPLETED)

    async def _monitor_idle_vms(self):
        """Monitor and clean up idle VMs."""
        current_time = time.time()
        for vm_name, status in list(self.vm_statuses.items()):
            if status.job_id is None and current_time - status.created_at > 300:  # 5 minutes
                logger.info(f"Deleting idle VM: {vm_name}")
                await self.mig_manager.delete_vm(status.zone, vm_name)
                del self.vm_statuses[vm_name]

    async def _listen_for_vm_updates(self):
        """Listen for VM status updates from Pub/Sub."""
        while self.running:
            message = await self.pubsub_client.listen_for_vm_status()
            if message:
                await self._handle_vm_update(message)

    async def _handle_vm_update(self, message: dict):
        """
        Handle incoming VM status updates.

        Args:
            message (dict): The status update message from a VM.
        """
        vm_name = message['vm_name']
        logger.info(f"Received VM update: {vm_name}")
        if vm_name not in self.vm_statuses:
            self.vm_statuses[vm_name] = ComputeInstance(
                vm_name=vm_name,
                region=message['region'],
                job_id=message.get('job_id'),
                start_time=time.time()
            )
        else:
            self.vm_statuses[vm_name].job_id = message.get('job_id')
            self.vm_statuses[vm_name].status = message.get('status')

        if message.get('status') == JobStatus.RUNNING:
            await self.db.update_job(message['job_id'], status=JobStatus.RUNNING, vm_name=vm_name)
        elif message.get('status') == JobStatus.COMPLETED:
            await self.db.update_job(message['job_id'], status=JobStatus.COMPLETED)
            # Note: VM self-deletes after job completion
