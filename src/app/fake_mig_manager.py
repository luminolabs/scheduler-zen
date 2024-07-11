import logging
from typing import List

logging.basicConfig(level=logging.INFO)


class FakeMigManager:
    """
    Simulates the Managed Instance Group (MIG) manager.
    """

    def __init__(self, project_id: str):
        """
        Initializes the FakeMigManager with a project ID.

        Args:
            project_id (str): The Google Cloud project ID.
        """
        self.project_id = project_id
        self.vms = {}
        logging.info("FakeMigManager initialized with project_id: %s", project_id)

    async def list_vms_in_mig(self, region: str, mig_name: str) -> List[str]:
        """
        Lists the VMs in a specified MIG.

        Args:
            region (str): The region of the MIG.
            mig_name (str): The name of the MIG.

        Returns:
            List[str]: A list of VM names.
        """
        logging.info("Listing VMs in MIG: %s, region: %s", mig_name, region)
        return self.vms.get((region, mig_name), [])

    async def scale_mig(self, region: str, mig_name: str, new_size: int) -> None:
        """
        Scales a specified MIG to a new size.

        Args:
            region (str): The region of the MIG.
            mig_name (str): The name of the MIG.
            new_size (int): The new size of the MIG.
        """
        logging.info("Scaling MIG: %s, region: %s to new size: %d", mig_name, region, new_size)
        self.vms[(region, mig_name)] = [f"vm-{i}" for i in range(new_size)]
