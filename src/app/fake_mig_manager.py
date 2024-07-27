import logging
from typing import Dict, Tuple
import random

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class FakeMigManager:
    """Simulates the Managed Instance Group (MIG) manager for testing purposes."""

    def __init__(self, project_id: str):
        """
        Initialize the FakeMigManager with a project ID.

        Args:
            project_id (str): The Google Cloud project ID.
        """
        self.project_id = project_id
        self.migs: Dict[str, Dict[str, Dict[str, int]]] = {}  # region -> mig_name -> {'target_size': int, 'running_vms': int}
        logger.info(f"FakeMigManager initialized with project_id: {project_id}")

    async def scale_mig(self, region: str, mig_name: str, target_size: int) -> None:
        """
        Scale a specified MIG to a new size.

        Args:
            region (str): The region of the MIG.
            mig_name (str): The name of the MIG.
            target_size (int): The new target size of the MIG.
        """
        if region not in self.migs:
            self.migs[region] = {}
        if mig_name not in self.migs[region]:
            self.migs[region][mig_name] = {'target_size': 0, 'running_vms': 0}

        self.migs[region][mig_name]['target_size'] = target_size
        self.migs[region][mig_name]['running_vms'] = max(0, min(target_size, self.migs[region][mig_name]['running_vms'] + random.randint(-2, 2)))

        logger.info(f"Scaled MIG: {mig_name}, region: {region} to new target size: {target_size}. "
                    f"Current running VMs: {self.migs[region][mig_name]['running_vms']}")

    async def get_target_and_running_vm_counts(self, region: str, mig_name: str) -> Tuple[int, int]:
        """
        Get the target size and number of running VMs for a specified MIG.

        Args:
            region (str): The region of the MIG.
            mig_name (str): The name of the MIG.

        Returns:
            Tuple[int, int]: The target size and number of running VMs.
        """
        if region not in self.migs or mig_name not in self.migs[region]:
            # If the MIG doesn't exist, create it with random values
            if region not in self.migs:
                self.migs[region] = {}
            self.migs[region][mig_name] = {
                'target_size': 0,
                'running_vms': 0
            }

        mig_info = self.migs[region][mig_name]
        logger.info(f"Getting info for MIG: {mig_name}, region: {region}. "
                    f"Target size: {mig_info['target_size']}, Running VMs: {mig_info['running_vms']}")
        return mig_info['target_size'], mig_info['running_vms']
