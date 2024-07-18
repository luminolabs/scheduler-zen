import asyncio
import logging
from typing import Dict, List, Tuple

from cluster_manager import ClusterManager
from mig_manager import MigManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ClusterOrchestrator:
    """Orchestrates operations across multiple clusters and regions."""

    def __init__(
            self,
            project_id: str,
            cluster_configs: Dict[str, List[str]],
            mig_manager: MigManager,
            max_scale_limits: Dict[str, int]
    ):
        """
        Args:
            project_id (str): The Google Cloud project ID.
            cluster_configs (Dict[str, List[str]]): A dictionary mapping cluster names to lists of regions.
            mig_manager (MigManager): The MIG manager responsible for interacting with the MIG APIs.
            max_scale_limits (Dict[str, int]): A dictionary mapping cluster names to max scale limits.
        """
        self.project_id = project_id
        self.cluster_managers = {
            cluster: ClusterManager(
                project_id,
                regions,
                cluster,
                mig_manager,
                max_scale_limits.get(cluster, float('inf'))
            )
            for cluster, regions in cluster_configs.items()
        }
        self.mig_manager = mig_manager
        logger.info(f"ClusterOrchestrator initialized with project_id: {project_id}, "
                    f"cluster_configs: {cluster_configs}")

    async def update_status(self) -> Dict[str, Dict[str, Tuple[int, int]]]:
        """
        Update and return the status of all clusters.

        Returns:
            Dict[str, Dict[str, Tuple[int, int]]]: A dictionary mapping cluster names to their status 
            (region to (target size, current VM count)).
        """
        status = {}
        for cluster, manager in self.cluster_managers.items():
            try:
                cluster_status = await manager.get_status()
                status[cluster] = cluster_status
            except Exception as e:
                logger.error(f"Error updating status for cluster {cluster}: {e}")
                status[cluster] = {}
        return status

    async def scale_clusters(self, pending_jobs: Dict[str, int], running_jobs: Dict[str, Dict[str, int]]) -> None:
        """
        Scale all clusters based on running VMs and pending jobs.

        Args:
            pending_jobs (Dict[str, int]): Dictionary of pending job counts per cluster.
            running_jobs (Dict[str, Dict[str, int]]): Dictionary of running job counts per cluster and region
        """
        scaling_tasks = []
        for cluster, manager in self.cluster_managers.items():
            cluster_pending_jobs_count = pending_jobs.get(cluster, 0)
            cluster_running_jobs_count = running_jobs.get(cluster, {})
            scaling_tasks.append(manager.scale_all_regions(cluster_pending_jobs_count, cluster_running_jobs_count))

        await asyncio.gather(*scaling_tasks)
