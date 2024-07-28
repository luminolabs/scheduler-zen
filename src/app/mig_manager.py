import asyncio
from typing import List, Tuple
from google.cloud import compute_v1
from google.api_core import retry_async

from app.utils import INSTANCE_STATUS_RUNNING, setup_logger

# Set up logging
logger = setup_logger(__name__)


class MigManager:
    """Manages the Google Cloud Managed Instance Groups (MIGs)."""

    def __init__(self, project_id: str):
        """
        Initialize the MigManager with a project ID.

        Args:
            project_id (str): The Google Cloud project ID.
        """
        self.project_id = project_id
        self.client = compute_v1.RegionInstanceGroupManagersClient()
        self.semaphore = asyncio.Semaphore(10)  # Limit to 10 concurrent requests
        logger.info(f"MigManager initialized with project_id: {project_id}")

    @retry_async.AsyncRetry()
    async def scale_mig(self, region: str, mig_name: str, target_size: int) -> None:
        """
        Scale a specified MIG to a new size.

        Args:
            region (str): The region of the MIG.
            mig_name (str): The name of the MIG.
            target_size (int): The new size of the MIG.
        """
        async with self.semaphore:
            request = compute_v1.ResizeRegionInstanceGroupManagerRequest(
                project=self.project_id,
                region=region,
                instance_group_manager=mig_name,
                size=target_size
            )
            operation = await asyncio.to_thread(self.client.resize, request)

            logger.info(f"MIG: {mig_name}: Scaled to new size: {target_size}")
            await asyncio.to_thread(operation.result)

    @retry_async.AsyncRetry()
    async def get_target_and_running_vm_counts(self, region: str, mig_name: str) -> Tuple[int, int]:
        """
        Get the target size and list of VMs for a specified MIG.

        Args:
            region (str): The region of the MIG.
            mig_name (str): The name of the MIG.

        Returns:
            Tuple[int, List[str]]: The target size and list of VM names.
        """
        async with self.semaphore:
            # Get target size
            size_request = compute_v1.GetRegionInstanceGroupManagerRequest(
                project=self.project_id,
                region=region,
                instance_group_manager=mig_name
            )
            size_response = await asyncio.to_thread(self.client.get, size_request)
            target_size = size_response.target_size

            # Get list of running VMs
            list_request = compute_v1.ListManagedInstancesRegionInstanceGroupManagersRequest(
                project=self.project_id,
                region=region,
                instance_group_manager=mig_name
            )
            list_response = await asyncio.to_thread(self.client.list_managed_instances, list_request)
            running_vm_list = [instance.instance.split('/')[-1] for instance in list_response.managed_instances
                               if instance.instance_status == INSTANCE_STATUS_RUNNING]

            logger.info(f"MIG: {mig_name}: Got target size: {target_size}, running VMs: {len(running_vm_list)}")
            return target_size, len(running_vm_list)
