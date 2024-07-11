import logging
from google.cloud import compute_v1
import asyncio
from typing import List

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


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
        logger.info(f"MigManager initialized with project_id: {project_id}")

    async def scale_mig(self, region: str, mig_name: str, target_size: int) -> None:
        """
        Scale a specified MIG to a new size.

        Args:
            region (str): The region of the MIG.
            mig_name (str): The name of the MIG.
            target_size (int): The new size of the MIG.
        """
        logger.info(f"Scaling MIG: {mig_name}, region: {region} to new size: {target_size}")
        request = compute_v1.ResizeRegionInstanceGroupManagerRequest(
            project=self.project_id,
            region=region,
            instance_group_manager=mig_name,
            size=target_size
        )
        operation = await asyncio.to_thread(self.client.resize, request)
        await asyncio.to_thread(operation.result)

    async def list_vms_in_mig(self, region: str, mig_name: str) -> List[str]:
        """
        List the VMs in a specified MIG.

        Args:
            region (str): The region of the MIG.
            mig_name (str): The name of the MIG.

        Returns:
            List[str]: A list of VM names.
        """
        logger.info(f"Listing VMs in MIG: {mig_name}, region: {region}")
        request = compute_v1.ListManagedInstancesRegionInstanceGroupManagersRequest(
            project=self.project_id,
            region=region,
            instance_group_manager=mig_name
        )
        response = await asyncio.to_thread(self.client.list_managed_instances, request)
        return [instance.instance.split('/')[-1] for instance in response.managed_instances]

    async def delete_vm(self, region: str, mig_name: str, instance_name: str) -> None:
        """
        Delete a specific VM from a MIG.

        Args:
            region (str): The region of the MIG.
            mig_name (str): The name of the MIG.
            instance_name (str): The name of the instance to delete.
        """
        logger.info(f"Deleting VM: {instance_name} from MIG: {mig_name}, region: {region}")
        request = compute_v1.DeleteInstancesRegionInstanceGroupManagerRequest(
            project=self.project_id,
            region=region,
            instance_group_manager=mig_name,
            region_instance_group_managers_delete_instances_request_body=compute_v1.RegionInstanceGroupManagersDeleteInstancesRequest(
                instances=[instance_name]
            )
        )
        operation = await asyncio.to_thread(self.client.delete_instances, request)
        await asyncio.to_thread(operation.result)
