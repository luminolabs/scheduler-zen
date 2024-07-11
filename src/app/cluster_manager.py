import logging
from typing import Dict, List
from mig_manager import MigManager

logging.basicConfig(level=logging.INFO)


class ClusterManager:
    """
    Manages the cluster of machines for a given project and GPU configuration.
    """

    def __init__(self, project_id: str, regions: List[str], gpu_config: str):
        """
        Initializes the ClusterManager with project ID, regions, and GPU configuration.

        Args:
            project_id (str): The Google Cloud project ID.
            regions (List[str]): A list of regions where the cluster is located.
            gpu_config (str): The GPU configuration for the cluster.
        """
        self.project_id = project_id
        self.regions = regions
        self.gpu_config = gpu_config
        self.mig_manager = MigManager(project_id)
        logging.info("ClusterManager initialized with project_id: %s, regions: %s, gpu_config: %s",
                     project_id, regions, gpu_config)

    async def scale_all_regions(self, scale_amount: int) -> None:
        """
        Scales all regions by a specified amount.

        Args:
            scale_amount (int): The amount to scale the cluster by.
        """
        logging.info("Scaling all regions by: %d", scale_amount)
        for region in self.regions:
            mig_name = f"pipeline-zen-jobs-{self.gpu_config}-{region}"
            current_size = len(await self.mig_manager.list_vms_in_mig(region, mig_name))
            new_size = max(0, current_size + scale_amount)
            await self.mig_manager.scale_mig(region, mig_name, new_size)
            logging.info("Scaled region: %s, new size: %d", region, new_size)

    async def get_status(self) -> Dict[str, int]:
        """
        Gets the status of the cluster.

        Returns:
            Dict[str, int]: A dictionary with the region as the key and the number of VMs as the value.
        """
        status = {}
        logging.info("Getting status for all regions")
        for region in self.regions:
            mig_name = f"pipeline-zen-jobs-{self.gpu_config}-{region}"
            vms = await self.mig_manager.list_vms_in_mig(region, mig_name)
            status[region] = len(vms)
            logging.info("Region: %s, VM count: %d", region, len(vms))
        return status
