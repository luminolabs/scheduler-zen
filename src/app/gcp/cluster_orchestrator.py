import asyncio
from typing import Dict, List, Union

from app.core.database import Database
from app.core.utils import setup_logger
from app.gcp.cluster_manager import ClusterManager
from app.gcp.fake_mig_client import FakeMigClient
from app.gcp.mig_client import MigClient

logger = setup_logger(__name__)

class ClusterOrchestrator:
    """Orchestrates operations across multiple clusters and regions."""

    def __init__(
            self,
            project_id: str,
            cluster_configs: Dict[str, List[str]],
            mig_client: Union[MigClient, FakeMigClient],
            max_scale_limits: Dict[str, int],
            db: Database
    ):
        """
        Initialize the ClusterOrchestrator.

        Args:
            project_id (str): The Google Cloud project ID.
            cluster_configs (Dict[str, List[str]]): A dictionary mapping cluster names to lists of regions.
            mig_client (Union[MigClient, FakeMigClient]): The MIG client instance responsible for controlling MIGs.
            max_scale_limits (Dict[str, int]): A dictionary mapping cluster names to max scale limits.
            db (Database): The database instance for querying job information.
        """
        self.project_id = project_id
        # Create a ClusterManager object for each cluster
        self.cluster_managers = {
            cluster: ClusterManager(
                project_id,
                regions,
                cluster,
                max_scale_limits.get(cluster, float('inf')),
                mig_client,
                db
            )
            for cluster, regions in cluster_configs.items()
        }
        logger.info(f"ClusterOrchestrator initialized with project_id: {project_id}, "
                    f"cluster_configs: {cluster_configs}")

        # Initialize MIGs for local development
        if isinstance(mig_client, FakeMigClient):
            for cluster, regions in cluster_configs.items():
                for region in regions:
                    mig_client.initialize_mig(cluster, region)

    async def scale_clusters(self) -> None:
        """
        Scale all clusters based on pending jobs.
        """
        scaling_tasks = []
        for cluster, cluster_manager in self.cluster_managers.items():
            scaling_tasks.append(cluster_manager.scale_all_regions())
        await asyncio.gather(*scaling_tasks)

    def cluster_exists(self, cluster: str) -> bool:
        """
        Check if the specified cluster exists.

        Args:
            cluster (str): The name of the cluster.
        Returns:
            bool: True if the cluster exists, False otherwise
        """
        return cluster in self.cluster_managers