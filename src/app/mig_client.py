import asyncio

from google.api_core.exceptions import NotFound
from google.cloud import compute_v1
from google.api_core import retry_async

from app.config_manager import config
from app.utils import setup_logger, get_mig_name_from_vm_name, get_region_from_vm_name

# Set up logging
logger = setup_logger(__name__)


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
        logger.info(f"MigClient initialized with project_id: {project_id}")

    @retry_async.AsyncRetry()
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

    @retry_async.AsyncRetry()
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

    @retry_async.AsyncRetry()
    async def detach_vm(self, vm_name: str) -> None:
        """
        Detach a VM from its regional MIG.

        Args:
            vm_name (str): The name of the VM to detach.
        """
        async with self.semaphore:
            # Get the MIG name and region from the VM name
            mig_name = get_mig_name_from_vm_name(vm_name)
            region = get_region_from_vm_name(vm_name)
            # Create the request to detach the instance from the regional MIG
            request = compute_v1.AbandonInstancesRegionInstanceGroupManagerRequest(
                project=self.project_id,
                region=region,
                instance_group_manager=mig_name,
                region_instance_group_managers_abandon_instances_request_resource=compute_v1.RegionInstanceGroupManagersAbandonInstancesRequest(
                    instances=[vm_name]
                )
            )
            # Execute the request
            operation = await asyncio.to_thread(self.client.abandon_instances, request)
            await asyncio.to_thread(operation.result)
            logger.info(f"MIG {mig_name}: detached VM {vm_name}")
