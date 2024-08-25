import asyncio
from typing import Dict, List, Any, Union
from app.utils import setup_logger
from app.cluster_manager import ClusterManager
from app.mig_manager import MigManager
from app.fake_mig_manager import FakeMigManager
from app.database import Database

logger = setup_logger(__name__)

class ClusterOrchestrator:
    """Orchestrates operations across multiple clusters and regions."""

    def __init__(
            self,
            project_id: str,
            cluster_configs: Dict[str, List[str]],
            mig_manager: Union[MigManager, FakeMigManager],
            max_scale_limits: Dict[str, int],
            db: Database
    ):
        """
        Initialize the ClusterOrchestrator.

        Args:
            project_id (str): The Google Cloud project ID.
            cluster_configs (Dict[str, List[str]]): A dictionary mapping cluster names to lists of regions.
            mig_manager (Union[MigManager, FakeMigManager]): The MIG manager instance responsible for scaling VMs.
            max_scale_limits (Dict[str, int]): A dictionary mapping cluster names to max scale limits.
            db (Database): The database instance for querying job information.
        """
        self.project_id = project_id
        self.cluster_managers = {
            cluster: ClusterManager(
                project_id,
                regions,
                cluster,
                mig_manager,
                max_scale_limits.get(cluster, float('inf')),
                db
            )
            for cluster, regions in cluster_configs.items()
        }
        self.mig_manager = mig_manager
        self.db = db
        logger.info(f"ClusterOrchestrator initialized with project_id: {project_id}, "
                    f"cluster_configs: {cluster_configs}")

    async def get_status(self) -> Dict[str, Any]:
        """
        Update and return the status of all clusters.

        Returns:
            Dict[str, Any]: A dictionary containing the overall status and status of each cluster.
        """
        status = {
            "overall_summary": {
                "total_jobs": 0,
                "running_jobs": 0,
                "pending_jobs": 0,
                "total_clusters": len(self.cluster_managers),
                "total_running_vms": 0,
                "total_target_vms": 0
            },
            "clusters": []
        }

        for cluster, manager in self.cluster_managers.items():
            cluster_status = await manager.get_status()
            status["clusters"].append(cluster_status)

            # Update overall summary
            status["overall_summary"]["running_jobs"] += cluster_status["cluster_summary"]["jobs"]["running"]
            status["overall_summary"]["pending_jobs"] += cluster_status["cluster_summary"]["jobs"]["pending"]
            status["overall_summary"]["total_running_vms"] += cluster_status["cluster_summary"]["total_running_vms"]
            status["overall_summary"]["total_target_vms"] += cluster_status["cluster_summary"]["total_target_vms"]

        status["overall_summary"]["total_jobs"] = (
                status["overall_summary"]["running_jobs"] +
                status["overall_summary"]["pending_jobs"]
        )

        logger.info(f"Updated status: {status}")
        return status

    async def scale_clusters(self, pending_jobs: Dict[str, int], running_jobs: Dict[str, Dict[str, int]]) -> None:
        """
        Scale all clusters based on running VMs and pending jobs.

        Args:
            pending_jobs (Dict[str, int]): Pending job counts per cluster.
            running_jobs (Dict[str, Dict[str, int]]): Running job counts per cluster and region
        """
        scaling_tasks = []
        for cluster, manager in self.cluster_managers.items():
            cluster_pending_jobs_count = pending_jobs.get(cluster, 0)
            cluster_running_jobs_count = running_jobs.get(cluster, {})
            scaling_tasks.append(manager.scale_all_regions(cluster_pending_jobs_count, cluster_running_jobs_count))

        await asyncio.gather(*scaling_tasks)

        # Log the activity
        logger.info(f"Scaled clusters based on pending jobs: {pending_jobs}")

    def cluster_exists(self, cluster: str) -> bool:
        """
        Check if the specified cluster exists.

        Args:
            cluster (str): The name of the cluster.
        Returns:
            bool: True if the cluster exists, False otherwise
        """
        return cluster in self.cluster_managers
