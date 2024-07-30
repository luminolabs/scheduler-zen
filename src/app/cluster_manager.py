import asyncio
from typing import Dict, List, Tuple

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
            max_scale_limit: int
    ):
        self.project_id = project_id
        self.regions = regions
        self.cluster = cluster
        self.mig_manager = mig_manager
        self.max_scale_limit = max_scale_limit
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
            running_jobs_count_per_region (Dict[str, int]): Dictionary of running job counts per region.
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
            pending_jobs_count (int): Number of pending jobs.
            running_jobs_count (int): Number of running jobs in the region.
        """
        mig_name = self._get_mig_name(region)
        try:
            current_target_size, running_vm_count = \
                await self.mig_manager.get_target_and_running_vm_counts(region, mig_name)

            # How scaling works:
            # Scale up if there are pending jobs and not all running VMs are utilized
            # Scale down if there are no pending jobs and there are idle VMs
            # Always limit the target size to the max scale limit
            new_target_size = min(max(running_vm_count, running_jobs_count + pending_jobs_count), self.max_scale_limit)

            if new_target_size != current_target_size:
                await self.mig_manager.scale_mig(region, mig_name, new_target_size)
            else:
                logger.info(f"No scaling needed for MIG: {mig_name}, current target size: {current_target_size}")
        except Exception:
            pass

    async def get_status(self) -> Dict[str, Tuple[int, int]]:
        """
        Get the status of all regions in the cluster.

        Returns:
            Dict[str, Tuple[int, int]]: A dictionary mapping regions to their (target size, current VM count).
        """
        tasks = [self._get_region_status(region) for region in self.regions]
        results = await asyncio.gather(*tasks)
        return dict(zip(self.regions, results))

    async def _get_region_status(self, region: str) -> Tuple[int, int]:
        """
        Get the status of a specific region.

        Args:
            region (str): The region to get the status for.

        Returns:
            Tuple[int, int]: The target size and current VM count of the MIG in the region.
        """
        mig_name = self._get_mig_name(region)
        try:
            target_size, running_vm_count = await self.mig_manager.get_target_and_running_vm_counts(region, mig_name)
            return target_size, running_vm_count
        except Exception:
            return 0, 0  # Return default values if there's an error
