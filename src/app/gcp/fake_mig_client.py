import json
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from time import sleep
from typing import Dict, Optional

from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.subscriber.message import Message

from app.core.config_manager import config
from app.core.utils import setup_logger, JOB_STATUS_FOUND_VM, JOB_STATUS_RUNNING, JOB_STATUS_COMPLETED
from app.gcp.utils import get_mig_name_from_cluster_and_region, get_region_from_vm_name

logger = setup_logger(__name__)


class FakeMigClient:
    """
    A mock implementation of the GCP MIG client for local development.
    Simulates MIG behavior and VM lifecycle events without actual GCP resources.
    """

    def __init__(self):
        """Initialize the fake MIG client."""
        # Dict to store MIG states: region -> {mig_name -> target_size}
        self.migs: Dict[str, Dict[str, int]] = {}
        # Dict to store active VMs and their jobs: region -> {vm_name -> job_id}
        self.active_vms: Dict[str, Dict[str, Optional[str]]] = {}
        # Counter for generating VM names
        self.vm_counter = 0

        # PubSub clients
        self.publisher = pubsub_v1.PublisherClient()
        self.subscriber = pubsub_v1.SubscriberClient()

        # Set up job start message handling
        self.project_id = config.gcp_project
        self.start_subscription = self.subscriber.subscription_path(
            self.project_id,
            config.job_start_subscription
        )
        self.heartbeat_topic = self.publisher.topic_path(
            self.project_id,
            config.heartbeat_topic
        )

        # Message queue and processing flag
        self.message_queue = Queue()
        self.running = False
        self.executor = ThreadPoolExecutor(max_workers=1)

        logger.info("FakeMigClient initialized")

    def initialize_mig(self, cluster: str, region: str) -> None:
        """
        Initialize a MIG for a cluster in a region.

        Args:
            cluster (str): The cluster name
            region (str): The region name
        """
        mig_name = get_mig_name_from_cluster_and_region(cluster, region)
        if region not in self.migs:
            self.migs[region] = {}
            self.active_vms[region] = {}
        self.migs[region][mig_name] = 0
        logger.info(f"Initialized MIG {mig_name} in region {region}")

    async def start(self) -> None:
        """Start listening for job start messages."""
        self.running = True

        # Start the subscriber in a separate thread
        def start_subscriber():
            self.subscriber.subscribe(
                self.start_subscription,
                self._handle_start_message,
                flow_control=pubsub_v1.types.FlowControl(max_messages=1)
            )

        self.executor.submit(start_subscriber)
        logger.info("Started listening for job start messages")

    async def stop(self) -> None:
        """Stop listening for job start messages and clean up."""
        self.running = False
        self.subscriber.close()
        self.executor.shutdown(wait=True)
        logger.info("Stopped listening for job start messages")

    def _handle_start_message(self, message: Message) -> None:
        """
        Handle incoming job start messages.

        Args:
            message (Message): The PubSub message containing job details
        """
        if not self.running:
            return

        try:
            data = json.loads(message.data.decode('utf-8'))
            job_id = data['job_id']
            cluster = data['gcp']['cluster']

            # Find an available VM in any region for this cluster
            for region in self.migs.keys():
                mig_name = get_mig_name_from_cluster_and_region(cluster, region)
                for vm_name, assigned_job in self.active_vms[region].items():
                    if vm_name.startswith(mig_name) and assigned_job is None:
                        # Assign job to VM
                        self.active_vms[region][vm_name] = job_id
                        # Start VM lifecycle simulation
                        self._simulate_vm_lifecycle(vm_name, job_id)
                        message.ack()
                        logger.info(f"Assigned job {job_id} to VM {vm_name}")
                        return

            # If we're here, we probably deleted the job record in the DB manually
            # or did something else manually that caused the job to be lost
            # Ack, so that we get rid of the message, but log a warning
            logger.warning(f"No available VM found for job {job_id}")
            message.ack()

        except Exception as e:
            # Something happened, ack the message and log an error,
            # so that we don't get stuck on a broken message
            logger.error(f"Error handling start message: {str(e)}")
            message.ack()

    async def set_target_size(self, region: str, mig_name: str, target_size: int) -> None:
        """
        Set the target size for a MIG and create/delete VMs accordingly.

        Args:
            region (str): The region of the MIG
            mig_name (str): The name of the MIG
            target_size (int): The new target size for the MIG
        """
        current_size = len([vm for vm in self.active_vms[region].keys()
                            if vm.startswith(mig_name)])

        # Scale up: Create new VMs
        while current_size < target_size:
            vm_name = f"{mig_name}-{str(self.vm_counter).zfill(4)}"
            self.active_vms[region][vm_name] = None  # No job assigned yet
            self.vm_counter += 1
            current_size += 1
            logger.info(f"Created VM {vm_name} in region {region}")

        # Scale down: Remove unassigned VMs
        if current_size > target_size:
            vms_to_remove = []
            for vm_name, job_id in self.active_vms[region].items():
                if current_size <= target_size:
                    break
                if vm_name.startswith(mig_name) and job_id is None:
                    vms_to_remove.append(vm_name)
                    current_size -= 1

            for vm_name in vms_to_remove:
                del self.active_vms[region][vm_name]
                logger.info(f"Removed VM {vm_name} from region {region}")

        # Update target size
        self.migs[region][mig_name] = target_size
        logger.info(f"Set target size to {target_size} for MIG {mig_name} in region {region}")

    async def get_current_target_size(self, region: str, mig_name: str) -> int:
        """
        Get the current target size for a MIG.

        Args:
            region (str): The region of the MIG
            mig_name (str): The name of the MIG

        Returns:
            int: The current target size
        """
        return self.migs[region].get(mig_name, 0)

    async def detach_vm(self, vm_name: str, job_id: str) -> None:
        """
        Simulate detaching a VM from its MIG.

        Args:
            vm_name (str): The name of the VM
            job_id (str): The ID of the job running on the VM
        """
        logger.info(f"Simulating detach of VM {vm_name} for job {job_id}")
        region = get_region_from_vm_name(vm_name)
        if region and vm_name in self.active_vms.get(region, {}):
            self.active_vms[region][vm_name] = job_id

    def remove_vm_from_cache(self, region: str, vm_name: str) -> None:
        """
        Remove a VM from the active VMs cache.

        Args:
            region (str): The region of the VM
            vm_name (str): The name of the VM
        """
        if vm_name in self.active_vms[region]:
            del self.active_vms[region][vm_name]
            logger.info(f"Removed VM {vm_name} from cache in region {region}")

    async def get_instance_zone(self, region: str, vm_name: str) -> Optional[str]:
        """
        Get the zone for a VM instance (simulated).

        Args:
            region (str): The region of the VM
            vm_name (str): The name of the VM

        Returns:
            str: The simulated zone (region + "-a")
        """
        if vm_name in self.active_vms.get(region, {}):
            return f"{region}-a"
        return None

    def _simulate_vm_lifecycle(self, vm_name: str, job_id: str) -> None:
        """
        Simulate the lifecycle of a VM running a job.
        Sends heartbeat messages for found_vm, running, and completed states.

        Args:
            vm_name (str): The name of the VM
            job_id (str): The ID of the job running on the VM
        """
        status_sequence = [
            (JOB_STATUS_FOUND_VM, 10),
            (JOB_STATUS_RUNNING, 20),
            (JOB_STATUS_COMPLETED, 20)
        ]

        for status, delay in status_sequence:
            # Wait for the specified delay
            sleep(delay)

            # Create and publish the heartbeat message
            message = {
                "job_id": job_id,
                "status": status,
                "vm_name": vm_name
            }
            self.publisher.publish(
                self.heartbeat_topic,
                json.dumps(message).encode("utf-8")
            )
            logger.info(f"Published heartbeat for job {job_id} with status {status}")
