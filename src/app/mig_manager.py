import logging

from google.cloud import compute_v1
import asyncio
from typing import List

logging.basicConfig(level=logging.INFO)


class MigManager:
    """
    Manages the Google Cloud Managed Instance Groups (MIGs).
    """

    def __init__(self, project_id: str):
        """
        Initializes the MigManager with a project ID.

        Args:
            project_id (str): The Google Cloud project ID.
        """
        self.project_id = project_id
        self.client = compute_v1.RegionInstanceGroupManagersClient()
        logging.info("MigManager initialized with project_id: %s", project_id)

    async def scale_mig(self, region: str, mig_name: str, target_size: int) -> None:
        """
        Scales a specified MIG to a new size.

        Args:
            region (str): The region of the MIG.
            mig_name (str): The name of the MIG.
            target_size (int): The new size of the MIG.
        """
        logging.info("Scaling MIG: %s, region: %s to new size: %d", mig_name, region, target_size)
        # noinspection PyTypeChecker
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
        Lists the VMs in a specified MIG.

        Args:
            region (str): The region of the MIG.
            mig_name (str): The name of the MIG.

        Returns:
            List[str]: A list of VM names.
        """
        logging.info("Listing VMs in MIG: %s, region: %s", mig_name, region)
        # noinspection PyTypeChecker
        request = compute_v1.ListManagedInstancesRegionInstanceGroupManagersRequest(
            project=self.project_id,
            region=region,
            instance_group_manager=mig_name
        )
        response = await asyncio.to_thread(self.client.list_managed_instances, request)
        return [instance.instance.split('/')[-1] for instance in response.managed_instances]

    async def delete_vm(self, region: str, mig_name: str, instance_name: str):
        # noinspection PyTypeChecker
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
