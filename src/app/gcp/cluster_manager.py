import asyncio
from typing import List

from google.api_core.exceptions import NotFound

from app.core.database import Database
from app.core.utils import setup_logger, JOB_STATUS_WAIT_FOR_VM, JOB_STATUS_FOUND_VM
from app.gcp.mig_client import MigClient
from app.gcp.utils import get_mig_name_from_cluster_and_region

# Set up logging
logger = setup_logger(__name__)


class ClusterManager:
    """Manages operations for a specific cluster across multiple regions."""

    def __init__(
            self,
            project_id: str,
            regions: List[str],
            cluster: str,
            max_scale_limit: int,
            mig_client: MigClient,
            db: Database
    ):
        """
        Initialize the ClusterManager.

        Args:
            project_id (str): The Google Cloud project ID.
            regions (List[str]): List of regions for this cluster.
            cluster (str): The name of the cluster.
            max_scale_limit (int): The maximum scale limit for the cluster.
            mig_client (MigClient): The MIG client instance.
            db (Database): The database instance for querying job information.
        """
        self.project_id = project_id
        self.regions = regions
        self.cluster = cluster
        self.max_scale_limit = max_scale_limit
        self.mig_client = mig_client
        self.db = db
        logger.info(f"ClusterManager initialized with project_id: {project_id}, regions: {regions}, "
                    f"cluster: {cluster}, max_scale_limit: {max_scale_limit}")

    async def scale_all_regions(self) -> None:
        """
        Scale all regions based on pending jobs.
        """
        # Get the total number of pending jobs for the cluster
        pending_jobs_count = len(await self.db.get_jobs_by_status_gcp(JOB_STATUS_WAIT_FOR_VM, self.cluster))
        # Scale all regions in parallel
        logger.info(f"Cluster: {self.cluster}: Scaling all regions: Pending jobs count: {pending_jobs_count}")
        tasks = []
        for region in self.regions:
            tasks.append(self._scale_region(region, pending_jobs_count))
        await asyncio.gather(*tasks)

    async def _scale_region(self, region: str, pending_jobs_count: int) -> None:
        """
        Scale a specific region based on pending jobs.

        Args:
            region (str): The region to scale.
            pending_jobs_count (int): Number of pending jobs in the cluster.
        """
        log_prefix = f"Cluster: {self.cluster}: Region: {region}:"
        mig_name = get_mig_name_from_cluster_and_region(self.cluster, region)
        try:
            # Get the current target size
            current_target_size = await self.mig_client.get_current_target_size(region, mig_name)
            # Calculate new target size
            new_target_size = min(pending_jobs_count, self.max_scale_limit)
            # Scale down is not allowed when we are in the process of detaching VMs;
            # `FOUND_VM` status indicates that the VM is being detached
            # We do this so that we don't scale down while a VM is running a job
            can_scale_down = len(await self.db.get_jobs_by_status_gcp(JOB_STATUS_FOUND_VM, self.cluster, region)) == 0
            # Scale the MIG if needed:
            # - If new target size is greater than current target size
            # - If new target size is less than current target size and scaling down is allowed
            # - If new target size is equal to current target size, no scaling needed
            if new_target_size < current_target_size and not can_scale_down:
                logger.info(f"{log_prefix} Scaling down not allowed due to `FOUND_VM` jobs.")
            elif new_target_size == current_target_size:
                logger.info(f"{log_prefix} No scaling needed.")
            else:
                await self.mig_client.set_target_size(region, mig_name, new_target_size)
                logger.info(f"{log_prefix} Scaled MIG: {mig_name} to target size: {new_target_size}")
        except NotFound:
            # If MIG is not found,
            # remove region from cluster so that it's not considered for scaling in the future
            if region not in self.regions:
                # Region has already been removed by another task
                pass
            logger.error(f"{log_prefix} Not found - removing region from cluster.")
            self.regions.remove(region)
