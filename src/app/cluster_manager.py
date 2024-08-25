import asyncio
from typing import Dict, List, Any

from app.database import Database
from app.utils import setup_logger
from app.mig_manager import MigManager

# Set up logging
logger = setup_logger(__name__)


class ClusterManager:
    """Manages operations for a specific cluster across multiple regions."""

    def __init__(
            self,
            project_id: str,
            regions: List[str],
            cluster: str,
            mig_manager: MigManager,
            max_scale_limit: int,
            database: Database
    ):
        """
        Initialize the ClusterManager.

        Args:
            project_id (str): The Google Cloud project ID.
            regions (List[str]): List of regions for this cluster.
            cluster (str): The name of the cluster.
            mig_manager (MigManager): The MIG manager instance.
            max_scale_limit (int): The maximum scale limit for the cluster.
            database (Database): The database instance for querying job information.
        """
        self.project_id = project_id
        self.regions = regions
        self.cluster = cluster
        self.mig_manager = mig_manager
        self.max_scale_limit = max_scale_limit
        self.database = database
        logger.info(f"ClusterManager initialized with project_id: {project_id}, regions: {regions}, "
                    f"cluster: {cluster}, max_scale_limit: {max_scale_limit}")

    def _get_mig_name(self, region: str) -> str:
        """
        Get the MIG name for a given region and cluster.

        Args:
            region (str): The region of the MIG.

        Returns:
            str: The name of the MIG.
        """
        return f"pipeline-zen-jobs-{self.cluster}-{region}"

    async def get_status(self) -> Dict[str, Any]:
        """
        Get the status of all regions in the cluster.

        Returns:
            Dict[str, Any]: A dictionary containing the cluster status.
        """
        status = {
            "name": self.cluster,
            "cluster_summary": {
                "total_regions": len(self.regions),
                "active_regions": 0,
                "total_running_vms": 0,
                "total_target_vms": 0,
                "jobs": {
                    "running": 0,
                    "pending": 0
                }
            },
            "regions": []
        }

        for region in self.regions:
            region_status = await self._get_region_status(region)
            status["regions"].append(region_status)

            if region_status["current_size"] > 0:
                status["cluster_summary"]["active_regions"] += 1

            status["cluster_summary"]["total_running_vms"] += region_status["current_size"]
            status["cluster_summary"]["total_target_vms"] += region_status["target_size"]
            status["cluster_summary"]["jobs"]["running"] += region_status["jobs"]["running"]
            status["cluster_summary"]["jobs"]["pending"] += region_status["jobs"]["pending"]

        return status

    async def _get_region_status(self, region: str) -> Dict[str, Any]:
        """
        Get the status of a specific region.

        Args:
            region (str): The region to get the status for.

        Returns:
            Dict[str, Any]: A dictionary containing the region status.
        """
        mig_name = self._get_mig_name(region)
        jobs = await self._get_job_counts(region)
        mig_status = await self.mig_manager.get_mig_status(region, mig_name)

        return {
            **mig_status,
            **{"jobs": jobs}
        }

    async def _get_job_counts(self, region: str) -> Dict[str, int]:
        """
        Get the count of running and pending jobs for a specific region.

        Args:
            region (str): The region to get job counts for.

        Returns:
            Dict[str, int]: A dictionary containing the count of running and pending jobs.
        """
        return await self.database.get_job_counts(self.cluster, region)

    async def scale_all_regions(self, pending_jobs_count: int) -> None:
        """
        Scale all regions based on pending jobs.

        Args:
            pending_jobs_count (int): Number of pending jobs.
        """
        logger.info(f"Scaling all regions for cluster: {self.cluster}.")
        tasks = []
        for region in self.regions:
            tasks.append(self._scale_region(region, pending_jobs_count))
        await asyncio.gather(*tasks)

    async def _scale_region(self, region: str, pending_jobs_count: int) -> None:
        """
        Scale a specific region based on pending jobs.

        Args:
            region (str): The region to scale.
            pending_jobs_count (int): Number of pending jobs.
        """
        mig_name = self._get_mig_name(region)
        try:
            current_target_size, running_vm_count = \
                await self.mig_manager.get_target_and_running_vm_counts(region, mig_name)

            # How scaling works:
            # Scale up if there are pending jobs and not all running VMs are utilized
            # Scale down if there are no pending jobs and there are idle VMs
            # Always limit the target size to the max scale limit
            new_target_size = min(running_vm_count + pending_jobs_count, self.max_scale_limit)

            if new_target_size != current_target_size:
                await self.mig_manager.scale_mig(region, mig_name, new_target_size)
                logger.info(f"Scaled MIG: {mig_name}: Region: {region}: {current_target_size} --> {new_target_size}")
            else:
                logger.info(f"No scaling needed for MIG: {mig_name}, current target size: {current_target_size}")
        except Exception as e:
            error_message = f"Error scaling region {region}: {str(e)}"
            await self.database.log_activity(error_message)
            logger.error(error_message)
