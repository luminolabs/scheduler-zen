import asyncio
import json
import random
from typing import Dict, List, Tuple, Optional
import queue

from google.cloud import pubsub_v1

from app.config_manager import config


class FakeMigManager:
    """
    A fake Managed Instance Group (MIG) manager for simulating GCP MIG behavior.

    This class provides methods to simulate MIG operations such as scaling,
    job assignment, and VM lifecycle management.
    """

    def __init__(self, project_id: str, heartbeat_topic: str, start_job_subscription: str):
        """
        Initialize the FakeMigManager.

        Args:
            project_id (str): The GCP project ID.
            heartbeat_topic (str): The Pub/Sub topic for heartbeat messages.
            start_job_subscription (str): The Pub/Sub subscription for new job messages.
        """
        self.project_id = project_id
        self.heartbeat_topic = heartbeat_topic
        self.start_job_subscription = start_job_subscription
        self.migs: Dict[str, Dict[str, List[str]]] = {}  # region -> mig_name -> list of VM names
        self.publisher = pubsub_v1.PublisherClient()
        self.subscriber = pubsub_v1.SubscriberClient()
        self.heartbeat_topic_path = self.publisher.topic_path(self.project_id, self.heartbeat_topic)
        self.start_job_subscription_path = self.subscriber.subscription_path(
            self.project_id, self.start_job_subscription
        )
        self.active_vms: Dict[str, asyncio.Task] = {}  # VM name -> simulation task
        self.job_queue: asyncio.Queue = asyncio.Queue()
        self.message_queue = queue.Queue()
        self.running = False
        self.main_task: Optional[asyncio.Task] = None
        self.gpu_regions = config.gpu_regions  # Store the GPU regions from config

    async def start(self) -> None:
        self.running = True
        print("FakeMigManager started")
        await asyncio.gather(
            self.listen_for_new_jobs(),
            self.process_jobs(),
            self.process_messages()
        )

    async def stop(self) -> None:
        """Stop the FakeMigManager and wait for it to finish."""
        self.running = False
        print("FakeMigManager stopped")

    async def listen_for_new_jobs(self) -> None:
        """Listen for new job messages on the Pub/Sub subscription."""
        def callback(message: pubsub_v1.subscriber.message.Message) -> None:
            self.message_queue.put(message)

        self.subscriber.subscribe(
            self.start_job_subscription_path, callback=callback
        )
        print(f"Listening for messages on {self.start_job_subscription_path}")

    async def process_messages(self) -> None:
        """Process messages from the message queue."""
        while self.running:
            try:
                message = self.message_queue.get(block=False)
                await self.handle_new_job(message)
            except queue.Empty:
                await asyncio.sleep(0.1)

    async def handle_new_job(self, message: pubsub_v1.subscriber.message.Message) -> None:
        """
        Handle a new job message.

        Args:
            message (pubsub_v1.subscriber.message.Message): The Pub/Sub message containing job data.
        """
        data = json.loads(message.data.decode('utf-8'))
        await self.job_queue.put(data)
        print(f"Received new job: {data['job_id']}")
        message.ack()

    async def process_jobs(self) -> None:
        """Process jobs from the job queue."""
        while self.running:
            try:
                job = await asyncio.wait_for(self.job_queue.get(), timeout=1)
                await self.assign_job_to_vm(job)
            except asyncio.TimeoutError:
                continue

    async def assign_job_to_vm(self, job: Dict[str, str]) -> None:
        """
        Assign a job to a VM, scaling up the MIG if necessary.

        Args:
            job (Dict[str, str]): The job data containing job_id and cluster.
        """
        job_id = job['job_id']
        cluster = job['cluster']
        region = self.get_region_for_cluster(cluster)
        mig_name = f"pipeline-zen-jobs-{cluster}-{region}"

        if region not in self.migs or mig_name not in self.migs[region] or not self.migs[region][mig_name]:
            print(f"No available VMs for job {job_id}. Scaling up MIG.")
            await self.scale_mig(region, mig_name, 1)

        vm_name = self.migs[region][mig_name][0]
        task = asyncio.create_task(self.simulate_vm(job_id, vm_name, region, mig_name))
        self.active_vms[vm_name] = task

    def get_region_for_cluster(self, cluster: str) -> str:
        """
        Get a random region for the given cluster.

        Args:
            cluster (str): The cluster name.

        Returns:
            str: A randomly selected region for the cluster.
        """
        available_regions = self.gpu_regions[cluster]
        return random.choice(available_regions)

    async def scale_mig(self, region: str, mig_name: str, target_size: int) -> None:
        """
        Scale a MIG to the target size.

        Args:
            region (str): The region of the MIG.
            mig_name (str): The name of the MIG.
            target_size (int): The desired size of the MIG.
        """
        if region not in self.migs:
            self.migs[region] = {}
        if mig_name not in self.migs[region]:
            self.migs[region][mig_name] = []

        current_size = len(self.migs[region][mig_name])
        if target_size > current_size:
            # Scale up
            for i in range(current_size, target_size):
                vm_name = f"{mig_name}-{i:04d}"
                self.migs[region][mig_name].append(vm_name)
                print(f"Added new VM: {vm_name} in region {region}")
        elif target_size < current_size:
            # Scale down
            removed_vms = self.migs[region][mig_name][target_size:]
            self.migs[region][mig_name] = self.migs[region][mig_name][:target_size]
            for vm in removed_vms:
                if vm in self.active_vms:
                    self.active_vms[vm].cancel()
                    del self.active_vms[vm]
            print(f"Removed VMs: {removed_vms} from region {region}")

        print(f"Scaled MIG {mig_name} in region {region} to {target_size} instances")

    async def get_target_and_running_vm_counts(self, region: str, mig_name: str) -> Tuple[int, int]:
        """
        Get the target size and number of running VMs for a MIG.

        Args:
            region (str): The region of the MIG.
            mig_name (str): The name of the MIG.

        Returns:
            Tuple[int, int]: A tuple containing (target_size, running_vm_count).
        """
        if region not in self.migs or mig_name not in self.migs[region]:
            return 0, 0
        target_size = len(self.migs[region][mig_name])
        running_vms = sum(1 for vm in self.migs[region][mig_name] if vm in self.active_vms)
        return target_size, running_vms

    async def simulate_vm(self, job_id: str, vm_name: str, region: str, mig_name: str) -> None:
        """
        Simulate the lifecycle of a VM running a job.

        Args:
            job_id (str): The ID of the job running on the VM.
            vm_name (str): The name of the VM.
            region (str): The region of the VM.
            mig_name (str): The name of the MIG the VM belongs to.
        """
        # Simulate VM startup time
        await asyncio.sleep(random.uniform(5, 15))

        # Send a few RUNNING heartbeats
        for _ in range(random.randint(3, 7)):
            await self.send_heartbeat(job_id, vm_name, "RUNNING")
            await asyncio.sleep(random.uniform(2, 5))

        # Send a COMPLETED heartbeat
        await self.send_heartbeat(job_id, vm_name, "COMPLETED")

        # Simulate VM deletion and MIG scale down
        await self.delete_vm(region, mig_name, vm_name)

    async def send_heartbeat(self, job_id: str, vm_name: str, status: str) -> None:
        """
        Send a heartbeat message to the Pub/Sub topic.

        Args:
            job_id (str): The ID of the job.
            vm_name (str): The name of the VM.
            status (str): The status of the job (e.g., "RUNNING", "COMPLETED").
        """
        message = {
            "job_id": job_id,
            "status": status,
            "vm_name": vm_name
        }
        data = json.dumps(message).encode("utf-8")
        future = self.publisher.publish(self.heartbeat_topic_path, data)
        await asyncio.to_thread(future.result)
        print(f"Sent heartbeat: {message}")

    async def delete_vm(self, region: str, mig_name: str, vm_name: str) -> None:
        """
        Delete a VM from the MIG and scale down by 1.

        Args:
            region (str): The region of the MIG.
            mig_name (str): The name of the MIG.
            vm_name (str): The name of the VM to delete.
        """
        self.migs[region][mig_name].remove(vm_name)
        if vm_name in self.active_vms:
            del self.active_vms[vm_name]
        print(f"Deleted VM {vm_name} from MIG {mig_name} in region {region}")

        # Simulate MIG auto-scaling down by 1
        new_size = len(self.migs[region][mig_name])
        print(f"MIG {mig_name} in region {region} auto-scaled down to {new_size} instances")
