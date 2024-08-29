import asyncio
from datetime import timedelta
from typing import Dict, List, Any

from google.api_core.exceptions import NotFound

from app.config_manager import config
from app.database import Database
from app.utils import setup_logger, JOB_STATUS_RUNNING, JOB_STATUS_PENDING
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
            db: Database
    ):
        """
        Initialize the ClusterManager.

        Args:
            project_id (str): The Google Cloud project ID.
            regions (List[str]): List of regions for this cluster.
            cluster (str): The name of the cluster.
            mig_manager (MigManager): The MIG manager instance.
            max_scale_limit (int): The maximum scale limit for the cluster.
            db (Database): The database instance for querying job information.
        """
        self.project_id = project_id
        self.regions = regions
        self.cluster = cluster
        self.mig_manager = mig_manager
        self.max_scale_limit = max_scale_limit
        self.db = db
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

    async def scale_all_regions(self,
                                pending_jobs_count: int,
                                running_jobs_count_per_region: Dict[str, int]) -> None:
        """
        Scale all regions based on running VMs and pending jobs.

        Args:
            pending_jobs_count (int): Number of pending jobs.
            running_jobs_count_per_region (Dict[str, int]): Number of running jobs per region.
        """
        logger.info(f"Scaling all regions for cluster: {self.cluster}.")
        tasks = []
        for region in self.regions:
            region_running_jobs_count = running_jobs_count_per_region.get(region, 0)
            tasks.append(self._scale_region(region, pending_jobs_count, region_running_jobs_count))
        await asyncio.gather(*tasks)

    async def _scale_region(self, region: str, pending_jobs_count: int, running_jobs_count: int) -> None:
        """
        Scale a specific region based on running VMs and pending jobs.

        Args:
            region (str): The region to scale.
            pending_jobs_count (int): Number of pending jobs in the region.
            running_jobs_count (int): Number of running jobs in the region.
        """
        mig_name = self._get_mig_name(region)
        try:
            current_target_size, running_vm_count = \
                await self.mig_manager.get_target_and_running_vm_counts(region, mig_name)

            # Check if there are any jobs running for less than the threshold time
            recent_jobs = await self.db.get_recent_running_jobs(
                self.cluster, region, timedelta(minutes=config.mig_recent_job_threshold))
            has_recent_jobs = len(recent_jobs) > 0

            # How scaling works:
            # - We target to whichever is higher:
            #   - the number running VMs;
            #     because we never want the scheduler to delete running VMs; VMs delete themselves when jobs finish
            #   - or the number of (running jobs + pending jobs)
            # - We also limit the target size to the max_scale_limit, so we don't surpass our quotas
            new_target_size = min(
                max(
                    running_vm_count,
                    running_jobs_count + pending_jobs_count
                ),
                self.max_scale_limit
            )

            # Prevent scale down if there are recent jobs
            # The GCP MIG API, which controls which VMs are deleted, is not aware of our jobs running on the VMs
            # and can delete VMs with running jobs if the VM is too new.
            # To prevent this, we skip scale down if there are jobs that have been running for less than the threshold time.
            if has_recent_jobs and new_target_size < current_target_size:
                logger.info(f"Skipping scale down for MIG: {mig_name} in region: {region} due to recent jobs")
                return

            if new_target_size != current_target_size:
                await self.mig_manager.scale_mig(region, mig_name, new_target_size)
                logger.info(f"Scaled MIG: {mig_name}: Region: {region}: {current_target_size} -> {new_target_size}")
            else:
                logger.info(f"No scaling needed for MIG: {mig_name}, current target size: {current_target_size}")
        except NotFound:
            logger.error(f"MIG: {mig_name}: Region: {region}: Not found - removing region from cluster.")
            self.regions.remove(region)

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
                    "pending": await self.db.get_job_count(JOB_STATUS_PENDING, self.cluster)
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
        running_job_count = await self.db.get_job_count(JOB_STATUS_RUNNING, self.cluster, region)
        mig_status = await self.mig_manager.get_mig_status(region, mig_name)

        return {
            **mig_status,
            **{"jobs": {"running": running_job_count}}
        }
