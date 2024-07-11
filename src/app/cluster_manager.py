import logging
from typing import Dict, List
from mig_manager import MigManager

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ClusterManager:
    """Manages the cluster of machines for a given project and cluster configuration."""

    def __init__(self, project_id: str, regions: List[str], cluster: str, mig_manager: MigManager):
        """
        Initialize the ClusterManager with project ID, regions, cluster configuration, and MIG manager.

        Args:
            project_id (str): The Google Cloud project ID.
            regions (List[str]): A list of regions where the cluster is located.
            cluster (str): The cluster configuration.
            mig_manager (MigManager): The MIG manager instance (can be real or fake).
        """
        self.project_id = project_id
        self.regions = regions
        self.cluster = cluster
        self.mig_manager = mig_manager
        logger.info(f"ClusterManager initialized with project_id: {project_id}, regions: {regions}, cluster: {cluster}")

    async def scale_all_regions(self, scale_amount: int) -> None:
        """
        Scale all regions by a specified amount.

        Args:
            scale_amount (int): The amount to scale the cluster by.
        """
        logger.info(f"Scaling all regions by: {scale_amount}")
        for region in self.regions:
            mig_name = f"pipeline-zen-jobs-{self.cluster}-{region}"
            current_size = len(await self.mig_manager.list_vms_in_mig(region, mig_name))
            new_size = max(0, current_size + scale_amount)
            await self.mig_manager.scale_mig(region, mig_name, new_size)
            logger.info(f"Scaled region: {region}, new size: {new_size}")

    async def get_status(self) -> Dict[str, int]:
        """
        Get the status of the cluster.

        Returns:
            Dict[str, int]: A dictionary with the region as the key and the number of VMs as the value.
        """
        status = {}
        logger.info("Getting status for all regions")
        for region in self.regions:
            mig_name = f"pipeline-zen-jobs-{self.cluster}-{region}"
            vms = await self.mig_manager.list_vms_in_mig(region, mig_name)
            status[region] = len(vms)
            logger.info(f"Region: {region}, VM count: {len(vms)}")
        return status
