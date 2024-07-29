import asyncio
import json
import random
from google.cloud import pubsub_v1
from typing import List, Dict, Tuple
import queue
from app.config_manager import config


class FakeMigManager:
    def __init__(self, project_id: str, heartbeat_topic: str, start_job_subscription: str):
        self.project_id = project_id
        self.heartbeat_topic = heartbeat_topic
        self.start_job_subscription = start_job_subscription
        self.migs: Dict[str, Dict[str, List[str]]] = {}  # region -> mig_name -> list of VM names
        self.publisher = pubsub_v1.PublisherClient()
        self.subscriber = pubsub_v1.SubscriberClient()
        self.heartbeat_topic_path = self.publisher.topic_path(self.project_id, self.heartbeat_topic)
        self.start_job_subscription_path = self.subscriber.subscription_path(self.project_id, self.start_job_subscription)
        self.active_vms: Dict[str, asyncio.Task] = {}  # VM name -> simulation task
        self.job_queue: asyncio.Queue = asyncio.Queue()
        self.message_queue = queue.Queue()
        self.running = False
        self.main_task = None
        self.gpu_regions = config.gpu_regions  # Store the GPU regions from config

    def start(self):
        """Start the FakeMigManager in a non-blocking manner."""
        if self.main_task is None or self.main_task.done():
            self.running = True
            self.main_task = asyncio.create_task(self._run())
            print("FakeMigManager started")
        else:
            print("FakeMigManager is already running")

    async def stop(self):
        """Stop the FakeMigManager and wait for it to finish."""
        if self.main_task and not self.main_task.done():
            self.running = False
            await self.main_task
            print("FakeMigManager stopped")
        else:
            print("FakeMigManager is not running")

    async def _run(self):
        """Main loop of the FakeMigManager."""
        try:
            await asyncio.gather(
                self.listen_for_new_jobs(),
                self.process_jobs(),
                self.process_messages()
            )
        except Exception as e:
            print(f"Error in FakeMigManager main loop: {e}")
        finally:
            self.running = False

    async def listen_for_new_jobs(self):
        def callback(message):
            self.message_queue.put(message)

        streaming_pull_future = self.subscriber.subscribe(self.start_job_subscription_path, callback=callback)
        print(f"Listening for messages on {self.start_job_subscription_path}")

        with self.subscriber:
            try:
                while self.running:
                    await asyncio.sleep(1)  # Allow for stopping the loop
            except Exception as e:
                print(f"Error in listen_for_new_jobs: {e}")
            finally:
                streaming_pull_future.cancel()
                print(f"Stopped listening for messages on {self.start_job_subscription_path}")

    async def process_messages(self):
        while self.running:
            try:
                message = self.message_queue.get(block=False)
                await self.handle_new_job(message)
            except queue.Empty:
                await asyncio.sleep(0.1)

    async def handle_new_job(self, message):
        try:
            data = json.loads(message.data.decode('utf-8'))
            await self.job_queue.put(data)
            print(f"Received new job: {data['job_id']}")
            message.ack()
        except Exception as e:
            print(f"Error processing message: {e}")
            message.nack()

    async def process_jobs(self):
        while self.running:
            try:
                job = await asyncio.wait_for(self.job_queue.get(), timeout=1.0)
                await self.assign_job_to_vm(job)
            except asyncio.TimeoutError:
                continue

    async def assign_job_to_vm(self, job):
        job_id = job['job_id']
        cluster = job['cluster']
        region = self.get_region_for_cluster(cluster)
        mig_name = f"pipeline-zen-jobs-{cluster}-{region}"

        if region not in self.migs or mig_name not in self.migs[region] or not self.migs[region][mig_name]:
            print(f"No available VMs for job {job_id}. Scaling up MIG.")
            await self.scale_mig(region, mig_name, 1)

        if self.migs[region][mig_name]:
            vm_name = self.migs[region][mig_name][0]
            task = asyncio.create_task(self.simulate_vm(job_id, vm_name, region, mig_name))
            self.active_vms[vm_name] = task
        else:
            print(f"Failed to assign job {job_id} to a VM.")

    def get_region_for_cluster(self, cluster):
        if cluster in self.gpu_regions:
            available_regions = self.gpu_regions[cluster]
            return random.choice(available_regions)
        else:
            print(f"Warning: No regions found for cluster {cluster}. Using default region.")
            return "default-region"

    async def scale_mig(self, region: str, mig_name: str, target_size: int):
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
        if region not in self.migs or mig_name not in self.migs[region]:
            return 0, 0
        target_size = len(self.migs[region][mig_name])
        running_vms = sum(1 for vm in self.migs[region][mig_name] if vm in self.active_vms)
        return target_size, running_vms

    async def simulate_vm(self, job_id: str, vm_name: str, region: str, mig_name: str):
        try:
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
        except asyncio.CancelledError:
            print(f"VM simulation for {vm_name} was cancelled")

    async def send_heartbeat(self, job_id: str, vm_name: str, status: str):
        message = {
            "job_id": job_id,
            "status": status,
            "vm_name": vm_name
        }
        data = json.dumps(message).encode("utf-8")
        future = self.publisher.publish(self.heartbeat_topic_path, data)
        await asyncio.to_thread(future.result)
        print(f"Sent heartbeat: {message}")

    async def delete_vm(self, region: str, mig_name: str, vm_name: str):
        """Delete a VM from the MIG and scale down by 1."""
        if region in self.migs and mig_name in self.migs[region]:
            if vm_name in self.migs[region][mig_name]:
                self.migs[region][mig_name].remove(vm_name)
                if vm_name in self.active_vms:
                    del self.active_vms[vm_name]
                print(f"Deleted VM {vm_name} from MIG {mig_name} in region {region}")

                # Simulate MIG auto-scaling down by 1
                new_size = len(self.migs[region][mig_name])
                print(f"MIG {mig_name} in region {region} auto-scaled down to {new_size} instances")
            else:
                print(f"Warning: VM {vm_name} not found in MIG {mig_name} in region {region}")
        else:
            print(f"Warning: MIG {mig_name} in region {region} not found")
