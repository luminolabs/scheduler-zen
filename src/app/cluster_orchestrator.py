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

    async def scale_clusters(self, pending_jobs: Dict[str, int]) -> None:
        """
        Scale all clusters based on running VMs and pending jobs.

        Args:
            pending_jobs (Dict[str, int]): Dictionary of pending job counts per cluster.
        """
        scaling_tasks = []
        for cluster, manager in self.cluster_managers.items():
            cluster_pending_jobs = pending_jobs.get(cluster, 0)
            scaling_tasks.append(manager.scale_all_regions(cluster_pending_jobs))

        await asyncio.gather(*scaling_tasks)

    def _group_vms_by_cluster(self, vm_names: List[str]) -> Dict[str, List[str]]:
        """
        Group VM names by their respective clusters.

        Args:
            vm_names (List[str]): List of VM names.

        Returns:
            Dict[str, List[str]]: Dictionary mapping cluster names to lists of VM names.
        """
        grouped_vms = {}
        for vm_name in vm_names:
            cluster = self._get_cluster_from_vm_name(vm_name)
            if cluster:
                grouped_vms.setdefault(cluster, []).append(vm_name)
        return grouped_vms

    def _get_cluster_from_vm_name(self, vm_name: str) -> str:
        """
        Extract the cluster name from a VM name.

        Args:
            vm_name (str): The name of the VM.

        Returns:
            str: The extracted cluster name, or an empty string if not found.
        """
        # Assuming VM names are in the format: pipeline-zen-jobs-{cluster}-{region}-{instance}
        parts = vm_name.split('-')
        if len(parts) >= 4:
            cluster_name = '-'.join(parts[3:-3])  # Join all parts between 'jobs' and the last two parts
            if cluster_name in self.cluster_managers:
                return cluster_name
        logger.warning(f"Could not extract valid cluster name from VM: {vm_name}")
        return ""
