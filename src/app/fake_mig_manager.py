import logging
from typing import List, Dict

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
        self.migs: Dict[str, Dict[str, List[str]]] = {}  # region -> mig_name -> list of VM names
        logger.info(f"FakeMigManager initialized with project_id: {project_id}")

    async def list_vms_in_mig(self, region: str, mig_name: str) -> List[str]:
        """
        List the VMs in a specified MIG.

        Args:
            region (str): The region of the MIG.
            mig_name (str): The name of the MIG.

        Returns:
            List[str]: A list of VM names.
        """
        logger.info(f"Listing VMs in MIG: {mig_name}, region: {region}")
        return self.migs.get(region, {}).get(mig_name, [])

    async def scale_mig(self, region: str, mig_name: str, new_size: int) -> None:
        """
        Scale a specified MIG to a new size.

        Args:
            region (str): The region of the MIG.
            mig_name (str): The name of the MIG.
            new_size (int): The new size of the MIG.
        """
        logger.info(f"Scaling MIG: {mig_name}, region: {region} to new size: {new_size}")
        if region not in self.migs:
            self.migs[region] = {}
        if mig_name not in self.migs[region]:
            self.migs[region][mig_name] = []

        current_size = len(self.migs[region][mig_name])
        if new_size > current_size:
            # Add VMs
            for i in range(current_size, new_size):
                self.migs[region][mig_name].append(f"{mig_name}-vm-{i}")
        elif new_size < current_size:
            # Remove VMs
            self.migs[region][mig_name] = self.migs[region][mig_name][:new_size]

    async def delete_vm(self, region: str, mig_name: str, instance_name: str) -> None:
        """
        Delete a specific VM from a MIG.

        Args:
            region (str): The region of the MIG.
            mig_name (str): The name of the MIG.
            instance_name (str): The name of the instance to delete.
        """
        logger.info(f"Deleting VM: {instance_name} from MIG: {mig_name}, region: {region}")
        if region in self.migs and mig_name in self.migs[region]:
            if instance_name in self.migs[region][mig_name]:
                self.migs[region][mig_name].remove(instance_name)
