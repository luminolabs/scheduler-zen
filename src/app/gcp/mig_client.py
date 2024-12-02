import asyncio
from typing import Optional

from google.api_core.exceptions import NotFound, BadRequest
from google.cloud import compute_v1

from app.core.config_manager import config
from app.core.utils import setup_logger, AsyncRetry
from app.gcp.fake_mig_client import FakeMigClient
from app.gcp.fake_mig_client_pipeline import FakeMigClientWithPipeline
from app.gcp.utils import get_region_from_vm_name, get_mig_name_from_vm_name

# Set up logging
logger = setup_logger(__name__)

# Configure the retry decorator
retry_config = AsyncRetry(
    attempts=5,
    delay=0.1,
    deadline=10,
    exceptions=(Exception,)
)


# noinspection PyTypeChecker
# ^ added for compute_v1 function calls
class MigClient:
    """Interfaces with the Google Cloud Managed Instance Groups (MIGs)."""

    def __init__(self, project_id: str):
        """
        Initialize the MigManager with a project ID.

        Args:
            project_id (str): The Google Cloud project ID.
        """
        self.project_id = project_id
        self.client = compute_v1.RegionInstanceGroupManagersClient()
        self.instances_client = compute_v1.InstancesClient()
        self.semaphore = asyncio.Semaphore(config.mig_api_rate_limit)
        self.instance_zone_cache = {}  # Cache for instance zone lookups
        logger.info(f"MigClient initialized with project_id: {project_id}")

    def remove_vm_from_cache(self, region: str, vm_name: str) -> None:
        """
        Remove a VM instance from the zone cache.

        Args:
            region (str): The region of the VM.
            vm_name (str): The name of the VM instance.
        """
        cache_key = f'{region}/{vm_name}'
        if cache_key in self.instance_zone_cache:
            del self.instance_zone_cache[cache_key]

    async def get_instance_zone(self, region: str, vm_name: str) -> Optional[str]:
        """
        Get the zone of a VM instance.

        Args:
            region (str): The region of the VM.
            vm_name (str): The name of the VM instance.
        Returns:
            str: The zone of the VM instance if found, None otherwise.
        """
        logger.info(f"Getting zone for VM {vm_name} in region {region}")
        cache_key = f'{region}/{vm_name}'
        if cache_key in self.instance_zone_cache:
            zone_name = self.instance_zone_cache[cache_key]
            logger.info(f"Cache hit zone {zone_name} for VM {vm_name} in region {region}")
            return zone_name
        async with self.semaphore:
            request = compute_v1.AggregatedListInstancesRequest(
                project=self.project_id,
                filter=f'name={vm_name}'
            )
            aggregated_list = await asyncio.to_thread(self.instances_client.aggregated_list, request)
            for zone, response in aggregated_list:
                if response.instances:
                    for instance in response.instances:
                        if instance.name == vm_name and zone.startswith(f'zones/{region}'):
                            zone_name = zone.split('/')[-1]
                            self.instance_zone_cache[cache_key] = zone_name
                            logger.info(f"Found zone {zone_name} for VM {vm_name} in region {region}")
                            return zone_name
            logger.warning(f"Zone not found for VM {vm_name} in region {region}")
            return None

    @retry_config
    async def set_target_size(self, region: str, mig_name: str, target_size: int) -> None:
        """
        Scale a specified MIG to a new size.

        Args:
            region (str): The region of the MIG.
            mig_name (str): The name of the MIG.
            target_size (int): The new size of the MIG.
        """
        async with self.semaphore:
            # Create the request to resize the MIG
            request = compute_v1.ResizeRegionInstanceGroupManagerRequest(
                project=self.project_id,
                region=region,
                instance_group_manager=mig_name,
                size=target_size
            )
            # Execute the request
            operation = await asyncio.to_thread(self.client.resize, request)
            await asyncio.to_thread(operation.result)
            logger.info(f"MIG {mig_name}: resized to {target_size}")

    @retry_config
    async def get_current_target_size(self, region: str, mig_name: str) -> int:
        """
        Get the current target size of a specified MIG.

        Args:
            region (str): The region of the MIG.
            mig_name (str): The name of the MIG.

        Returns:
            int: The target size of the MIG.
        """
        async with self.semaphore:
            # Create the request to get the MIG information
            request = compute_v1.GetRegionInstanceGroupManagerRequest(
                project=self.project_id,
                region=region,
                instance_group_manager=mig_name
            )
            try:
                # Execute the request
                response = await asyncio.to_thread(self.client.get, request)
                logger.info(f"MIG {mig_name}: current target size: {response.target_size}")
                return response.target_size
            except NotFound:
                # MIG not found is managed elsewhere; let's return 0 here
                return 0

    @retry_config
    async def detach_vm(self, vm_name: str, job_id: str) -> None:
        """
        Detach a VM from its regional MIG.

        Args:
            vm_name (str): The name of the VM to detach.
            job_id (str): The ID of the job associated with the VM.
        """
        async with self.semaphore:
            # Get the MIG name and region from the VM name
            mig_name = get_mig_name_from_vm_name(vm_name)
            region = get_region_from_vm_name(vm_name)
            zone = await self.get_instance_zone(region, vm_name)
            # Create the request to detach the instance from the regional MIG
            request = compute_v1.AbandonInstancesRegionInstanceGroupManagerRequest(
                project=self.project_id,
                region=region,
                instance_group_manager=mig_name,
                region_instance_group_managers_abandon_instances_request_resource=compute_v1.RegionInstanceGroupManagersAbandonInstancesRequest(
                    instances=[f"zones/{zone}/instances/{vm_name}"]
                )
            )
            # Execute the request
            logger.info(f"MIG {mig_name}: detaching VM {vm_name} for job {job_id}")
            try:
                operation = await asyncio.to_thread(self.client.abandon_instances, request)
                await asyncio.to_thread(operation.result)
                logger.info(f"MIG {mig_name}: detached VM {vm_name} for job {job_id}")
            except BadRequest:
                # This is okay, the VM is already detached, or not found anymore
                logger.warning(f"Coudn't detach VM {vm_name} for job {job_id}")


# Not placing this in utils to avoid circular imports
MigClientType = MigClient | FakeMigClient | FakeMigClientWithPipeline