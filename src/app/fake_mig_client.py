import asyncio
import json
import random
from typing import Dict, Optional, Any
import queue

from google.cloud import pubsub_v1

from app.config_manager import config
from app.database import Database
from app.utils import setup_logger, JOB_STATUS_RUNNING, JOB_STATUS_COMPLETED, JOB_STATUS_FAILED

logger = setup_logger(__name__)

class FakeMigClient:
    def __init__(self, project_id: str, heartbeat_topic: str, start_job_subscription: str, db: Database):
        self.project_id = project_id
        self.heartbeat_topic = heartbeat_topic
        self.start_job_subscription = start_job_subscription
        self.db = db
        self.migs: Dict[str, Dict[str, Dict[str, Any]]] = {}  # region -> mig_name -> MIG info
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
        self.gpu_regions = config.gpu_regions
        logger.info("FakeMigManager initialized")

    async def start(self) -> None:
        self.running = True
        logger.info("FakeMigManager started")
        await asyncio.gather(
            self.listen_for_new_jobs(),
            self.process_jobs(),
            self.process_messages(),
            self.simulate_mig_autoscaling()
        )

    async def stop(self) -> None:
        self.running = False
        logger.info("FakeMigManager stopped")
        for task in self.active_vms.values():
            task.cancel()
        await asyncio.gather(*self.active_vms.values(), return_exceptions=True)

    async def listen_for_new_jobs(self) -> None:
        def callback(message: pubsub_v1.subscriber.message.Message) -> None:
            self.message_queue.put(message)

        self.subscriber.subscribe(
            self.start_job_subscription_path, callback=callback
        )
        logger.info(f"Listening for messages on {self.start_job_subscription_path}")

    async def process_messages(self) -> None:
        while self.running:
            try:
                message = self.message_queue.get(block=False)
                await self.handle_new_job(message)
            except queue.Empty:
                await asyncio.sleep(0.1)

    async def handle_new_job(self, message: pubsub_v1.subscriber.message.Message) -> None:
        data = json.loads(message.data.decode('utf-8'))
        await self.job_queue.put(data)
        logger.info(f"Received new job: {data['job_id']}")
        message.ack()

    async def process_jobs(self) -> None:
        while self.running:
            try:
                job = await asyncio.wait_for(self.job_queue.get(), timeout=1)
                await self.assign_job_to_vm(job)
            except asyncio.TimeoutError:
                continue

    async def assign_job_to_vm(self, job: Dict[str, str]) -> None:
        job_id = job['job_id']
        cluster = job['cluster']
        region = await self.get_region_for_cluster(cluster)
        mig_name = f"pipeline-zen-jobs-{cluster}-{region}"

        if region not in self.migs or mig_name not in self.migs[region] or not self.migs[region][mig_name]['instances']:
            logger.info(f"No available VMs for job {job_id}. Scaling up MIG.")
            await self.scale_mig(region, mig_name, 1)

        vm_name = self.migs[region][mig_name]['instances'][0]
        task = asyncio.create_task(self.simulate_vm(job_id, vm_name, region, mig_name))
        self.active_vms[vm_name] = task

    async def get_region_for_cluster(self, cluster: str) -> str:
        await asyncio.sleep(0.1)  # Simulate API latency
        available_regions = self.gpu_regions.get(cluster, [])
        if not available_regions:
            raise ValueError(f"No regions available for cluster {cluster}")
        return random.choice(available_regions)

    async def scale_mig(self, region: str, mig_name: str, target_size: int) -> None:
        if region not in self.migs:
            self.migs[region] = {}
        if mig_name not in self.migs[region]:
            self.migs[region][mig_name] = {'instances': [], 'target_size': 0}

        current_size = len(self.migs[region][mig_name]['instances'])
        self.migs[region][mig_name]['target_size'] = target_size

        if target_size > current_size:
            # Scale up
            for i in range(current_size, target_size):
                vm_name = f"{mig_name}-{i:04d}"
                self.migs[region][mig_name]['instances'].append(vm_name)
                logger.info(f"Added new VM: {vm_name} in region {region}")
        elif target_size < current_size:
            # Scale down
            removed_vms = self.migs[region][mig_name]['instances'][target_size:]
            self.migs[region][mig_name]['instances'] = self.migs[region][mig_name]['instances'][:target_size]
            for vm in removed_vms:
                if vm in self.active_vms:
                    self.active_vms[vm].cancel()
                    del self.active_vms[vm]
            logger.info(f"Removed VMs: {removed_vms} from region {region}")

        logger.info(f"Scaled MIG {mig_name} in region {region} to {target_size} instances")

    async def get_target_size(self, region: str, mig_name: str) -> int:
        if region not in self.migs or mig_name not in self.migs[region]:
            return 0
        return self.migs[region][mig_name]['target_size']

    async def get_mig_status(self, region: str, mig_name: str) -> Dict[str, Any]:
        if region not in self.migs or mig_name not in self.migs[region]:
            return {
                "region": region,
                "mig_name": mig_name,
                "target_size": 0,
                "current_size": 0,
            }
        mig_info = self.migs[region][mig_name]
        return {
            "region": region,
            "mig_name": mig_name,
            "target_size": mig_info['target_size'],
            "current_size": len(mig_info['instances']),
        }

    async def simulate_vm(self, job_id: str, vm_name: str, region: str, mig_name: str) -> None:
        # Simulate VM startup time
        await asyncio.sleep(random.uniform(15, 25))

        # Send a few RUNNING heartbeats
        for _ in range(random.randint(5, 8)):
            await self.send_heartbeat(job_id, vm_name, JOB_STATUS_RUNNING)
            await asyncio.sleep(3)

        # Simulate job completion or failure
        final_status = random.choices([JOB_STATUS_COMPLETED, JOB_STATUS_FAILED], weights=[0.9, 0.1])[0]
        await self.send_heartbeat(job_id, vm_name, final_status)

        # Simulate VM deletion and MIG scale down
        await self.delete_vm(region, mig_name, vm_name)

    async def send_heartbeat(self, job_id: str, vm_name: str, status: str) -> None:
        message = {
            "job_id": job_id,
            "status": status,
            "vm_name": vm_name
        }
        data = json.dumps(message).encode("utf-8")
        future = self.publisher.publish(self.heartbeat_topic_path, data)
        await asyncio.to_thread(future.result)
        logger.info(f"Sent heartbeat: {message}")

    async def delete_vm(self, region: str, mig_name: str, vm_name: str) -> None:
        if region in self.migs and mig_name in self.migs[region]:
            self.migs[region][mig_name]['instances'].remove(vm_name)
        if vm_name in self.active_vms:
            del self.active_vms[vm_name]
        logger.info(f"Deleted VM {vm_name} from MIG {mig_name} in region {region}")

        # Simulate MIG auto-scaling down by 1
        new_size = len(self.migs[region][mig_name]['instances'])
        self.migs[region][mig_name]['target_size'] = new_size
        logger.info(f"MIG {mig_name} in region {region} auto-scaled down to {new_size} instances")

    async def simulate_mig_autoscaling(self) -> None:
        while self.running:
            for region in self.migs:
                for mig_name in self.migs[region]:
                    current_size = len(self.migs[region][mig_name]['instances'])
                    target_size = self.migs[region][mig_name]['target_size']
                    if current_size < target_size:
                        # Simulate gradual scale up
                        new_size = min(current_size + 1, target_size)
                        await self.scale_mig(region, mig_name, new_size)
                    elif current_size > target_size and not any(vm.startswith(f"{mig_name}-") for vm in self.active_vms):
                        # Simulate gradual scale down if no active jobs
                        new_size = max(current_size - 1, target_size)
                        await self.scale_mig(region, mig_name, new_size)
            await asyncio.sleep(20)  # Check every minute

    async def detach_vm(self, vm_name: str, region: str) -> None:
        mig_name = "-".join(vm_name.split("-")[:-1])
        if region in self.migs and mig_name in self.migs[region]:
            if vm_name in self.migs[region][mig_name]['instances']:
                self.migs[region][mig_name]['instances'].remove(vm_name)
                logger.info(f"Detached VM {vm_name} from MIG {mig_name} in region {region}")
            else:
                logger.warning(f"VM {vm_name} not found in MIG {mig_name} in region {region}")
        else:
            logger.warning(f"MIG {mig_name} not found in region {region}")
