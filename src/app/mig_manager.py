# mig_manager.py

import asyncio
import logging
from typing import List, Dict

from google.cloud import compute_v1

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_MIG_NAME_PREFIX = "pipeline-zen-jobs"


class MIGManager:
    """
    Manages interactions with Google Cloud Platform Managed Instance Groups (MIGs).
    """

    def __init__(self, project_id: str, machine_type: str,
                 regions_zones: Dict[str: List[str]],
                 mig_name_prefix: str = DEFAULT_MIG_NAME_PREFIX):
        """
        Initialize the MIGManager with the given project, regions, MIG name prefix, and machine type.

        Args:
            project_id (str): The GCP project ID
            machine_type (str): The machine type for the MIG instances, ex. "8xa100-40gb"
            regions_zones (Dict[str, List[str]]): A dictionary mapping available region and zones
            mig_name_prefix (str): The prefix for the MIG name, ex. "pipeline-zen-jobs"
        """
        self.region_instance_group_managers_client = compute_v1.RegionInstanceGroupManagersClient()
        self.project_id = project_id
        self.machine_type = machine_type
        self.regions_zones = regions_zones
        self.mig_name_prefix = mig_name_prefix
        # Get just the regions from the regions_zones dictionary
        self.regions = list(regions_zones.keys())

    def _get_mig_name(self, region: str) -> str:
        """
        Generate the name of the MIG for a given region.

        Args:
            region (str): The GCP region of the MIG

        Returns:
            str: The name of the MIG
        """
        return f"{self.mig_name_prefix}-{self.machine_type}-{region}"

    async def scale_up_all_regions(self):
        """Scale up MIGs in all regions by one instance."""
        for region in self.regions:
            await self.scale_up(region, 1)

    async def scale_up(self, region: str, count: int):
        """
        Scale up a MIG in a specific region by a given count.

        Args:
            region (str): The GCP region of the MIG
            count (int): The number of instances to add
        """
        current_size = await self.get_mig_size(region)
        target_size = current_size + count
        await self.set_mig_size(region, target_size)

    async def set_mig_size(self, region: str, target_size: int):
        """
        Set the size of a MIG in a specific region.

        Args:
            region (str): The GCP region of the MIG
            target_size (int): The new size of the MIG
        """
        logger.info(f"Setting MIG size in {region} to {target_size}")
        request = compute_v1.SetTargetSizeRegionInstanceGroupManagerRequest(
            project=self.project_id,
            region=region,
            instance_group_manager=self._get_mig_name(region),
            size=target_size
        )
        operation = await asyncio.to_thread(
            self.region_instance_group_managers_client.set_target_size,
            request=request
        )
        await asyncio.to_thread(operation.result)  # Wait for the operation to complete

    async def get_mig_size(self, region: str) -> int:
        """
        Get the current size of a MIG in a specific region.

        Args:
            region (str): The GCP region of the MIG

        Returns:
            int: The current size of the MIG
        """
        logger.info(f"Getting MIG size in {region}")
        # noinspection PyTypeChecker
        request = compute_v1.GetRegionInstanceGroupManagerRequest(
            project=self.project_id,
            region=region,
            instance_group_manager=self._get_mig_name(region),
        )
        response = await asyncio.to_thread(
            self.region_instance_group_managers_client.get,
            request=request
        )
        return response.target_size

    async def delete_vm(self, region: str, instance_name: str):
        """
        Delete a specific VM from a MIG in a given region.

        Args:
            region (str): The GCP region of the MIG
            instance_name (str): The name of the instance to delete
        """
        logger.info(f"Deleting VM {instance_name} in region {region}")
        # noinspection PyTypeChecker
        request = compute_v1.DeleteInstancesRegionInstanceGroupManagerRequest(
            project=self.project_id,
            region=region,
            instance_group_manager=self._get_mig_name(region),
            region_instance_group_managers_delete_instances_request_body=compute_v1.RegionInstanceGroupManagersDeleteInstancesRequest(
                instances=[f"zones/{region}/instances/{instance_name}"]
            )
        )
        operation = await asyncio.to_thread(
            self.region_instance_group_managers_client.delete_instances,
            request=request
        )
        await asyncio.to_thread(operation.result)  # Wait for the operation to complete
