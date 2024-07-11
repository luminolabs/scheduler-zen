import logging
from typing import Dict, List
from cluster_manager import ClusterManager
from mig_manager import MigManager

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ClusterOrchestrator:
    """Orchestrates operations within a cluster."""

    def __init__(self, project_id: str, cluster_configs: Dict[str, List[str]], mig_manager: MigManager):
        """
        Initialize the ClusterOrchestrator.

        Args:
            project_id (str): The Google Cloud project ID.
            cluster_configs (Dict[str, List[str]]): A dictionary with cluster configuration as the key and list of regions as the value.
            mig_manager (MigManager): The MIG manager instance (can be real or fake).
        """
        self.project_id = project_id
        self.cluster_managers = {
            cluster: ClusterManager(project_id, regions, cluster, mig_manager)
            for cluster, regions in cluster_configs.items()
        }
        logger.info(f"ClusterOrchestrator initialized with project_id: {project_id}, cluster_configs: {cluster_configs}")

    async def scale_cluster(self, cluster: str, scale_amount: int) -> None:
        """
        Scale the cluster for a given cluster configuration.

        Args:
            cluster (str): The cluster configuration to scale.
            scale_amount (int): The amount to scale the cluster by.
        """
        if cluster not in self.cluster_managers:
            logger.error(f"Unknown cluster configuration: {cluster}")
            raise ValueError(f"Unknown cluster configuration: {cluster}")
        logger.info(f"Scaling cluster for cluster config: {cluster} by amount: {scale_amount}")
        await self.cluster_managers[cluster].scale_all_regions(scale_amount)

    async def get_cluster_status(self) -> Dict[str, Dict[str, int]]:
        """
        Get the status of the entire cluster.

        Returns:
            Dict[str, Dict[str, int]]: A dictionary with cluster configuration as the key and region status as the value.
        """
        status = {}
        logger.info("Getting status for the entire cluster")
        for cluster, manager in self.cluster_managers.items():
            status[cluster] = await manager.get_status()
            logger.info(f"Status for cluster config: {cluster} - {status[cluster]}")
        return status
