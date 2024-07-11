import logging
from typing import Dict, List
from cluster_manager import ClusterManager

logging.basicConfig(level=logging.INFO)


class ClusterOrchestrator:
    """
    Orchestrates the operations within a cluster.
    """

    def __init__(self, project_id: str, gpu_configs: Dict[str, List[str]]):
        """
        Initializes the ClusterOrchestrator.

        Args:
            project_id (str): The Google Cloud project ID.
            gpu_configs (Dict[str, List[str]]): A dictionary with GPU configuration as the key and list of regions as the value.
        """
        self.project_id = project_id
        self.cluster_managers = {
            gpu_config: ClusterManager(project_id, regions, gpu_config)
            for gpu_config, regions in gpu_configs.items()
        }
        logging.info("ClusterOrchestrator initialized with project_id: %s, gpu_configs: %s", project_id, gpu_configs)

    async def scale_cluster(self, gpu_config: str, scale_amount: int) -> None:
        """
        Scales the cluster for a given GPU configuration.

        Args:
            gpu_config (str): The GPU configuration to scale.
            scale_amount (int): The amount to scale the cluster by.
        """
        if gpu_config not in self.cluster_managers:
            logging.error("Unknown GPU configuration: %s", gpu_config)
            raise ValueError(f"Unknown GPU configuration: {gpu_config}")
        logging.info("Scaling cluster for GPU config: %s by amount: %d", gpu_config, scale_amount)
        await self.cluster_managers[gpu_config].scale_all_regions(scale_amount)

    async def get_cluster_status(self) -> Dict[str, Dict[str, int]]:
        """
        Gets the status of the entire cluster.

        Returns:
            Dict[str, Dict[str, int]]: A dictionary with GPU configuration as the key and region status as the value.
        """
        status = {}
        logging.info("Getting status for the entire cluster")
        for gpu_config, manager in self.cluster_managers.items():
            status[gpu_config] = await manager.get_status()
            logging.info("Status for GPU config: %s - %s", gpu_config, status[gpu_config])
        return status
