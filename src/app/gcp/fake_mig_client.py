from typing import Optional, Dict

from app.core.utils import setup_logger
from app.gcp.utils import get_mig_name_from_cluster_and_region

logger = setup_logger(__name__)

class FakeMigClient:
    """
    Simulates the behavior of MigClient for local development.
    NOTE:
        This is still a work in progress and will be expanded upon in future iterations.
        Currently, it only simulates setting target sizes for MIGs and creating/deleting VMs,
        but we need to add more functionality to fully simulate the behavior of the VMs and MIGs on GCP,
        such as emitting job heartbeats, consuming Pub/Sub messages, and handling VM detachments.
    """

    def __init__(self):
        self.migs: Dict[str, Dict[str, int]] = {}  # {region: {mig_name: target_size}}
        self.vms: Dict[str, Dict[str, str]] = {}  # {region: {vm_name: zone}}
        self.vm_counter = 0

    def remove_vm_from_cache(self, region: str, vm_name: str) -> None:
        """Remove a VM instance from the simulated cache."""
        if region in self.vms and vm_name in self.vms[region]:
            del self.vms[region][vm_name]
            logger.info(f"Removed VM {vm_name} from cache for region {region}")

    async def get_instance_zone(self, region: str, vm_name: str) -> Optional[str]:
        """Get the simulated zone of a VM instance."""
        return self.vms.get(region, {}).get(vm_name)

    async def set_target_size(self, region: str, mig_name: str, target_size: int) -> None:
        """Simulate setting the target size of a MIG."""
        if region not in self.migs:
            self.migs[region] = {}
        self.migs[region][mig_name] = target_size
        logger.info(f"Set target size of MIG {mig_name} in region {region} to {target_size}")

        # Simulate VM creation or deletion based on the new target size
        current_vms = len([vm for vm in self.vms.get(region, {}) if vm.startswith(mig_name)])
        if target_size > current_vms:
            # Create new VMs
            for _ in range(target_size - current_vms):
                self.vm_counter += 1
                new_vm_name = f"{mig_name}-{self.vm_counter:04d}"
                if region not in self.vms:
                    self.vms[region] = {}
                self.vms[region][new_vm_name] = f"{region}-a"  # Simulate a zone
                logger.info(f"Created new VM {new_vm_name} in region {region}")
        elif target_size < current_vms:
            # Remove excess VMs
            vms_to_remove = sorted([vm for vm in self.vms.get(region, {}) if vm.startswith(mig_name)])[:current_vms - target_size]
            for vm in vms_to_remove:
                del self.vms[region][vm]
                logger.info(f"Removed VM {vm} from region {region}")

    async def get_current_target_size(self, region: str, mig_name: str) -> int:
        """Get the current simulated target size of a MIG."""
        return self.migs.get(region, {}).get(mig_name, 0)

    async def detach_vm(self, vm_name: str, job_id: str) -> None:
        """Simulate detaching a VM from its regional MIG."""
        for region, vms in self.vms.items():
            if vm_name in vms:
                logger.info(f"Simulated detaching VM {vm_name} for job {job_id} in region {region}")
                # In a real scenario, we might want to keep track of detached VMs separately
                break
        else:
            logger.warning(f"Attempted to detach non-existent VM {vm_name} for job {job_id}")

    # Helper method to initialize MIGs for testing
    def initialize_mig(self, cluster: str, region: str, initial_size: int = 0):
        """Initialize a MIG for testing purposes."""
        mig_name = get_mig_name_from_cluster_and_region(cluster, region)
        if region not in self.migs:
            self.migs[region] = {}
        self.migs[region][mig_name] = initial_size
        logger.info(f"Initialized MIG {mig_name} in region {region} with size {initial_size}")

        # Create initial VMs
        if region not in self.vms:
            self.vms[region] = {}
        for i in range(initial_size):
            self.vm_counter += 1
            vm_name = f"{mig_name}-{self.vm_counter:04d}"
            self.vms[region][vm_name] = f"{region}-a"
            logger.info(f"Created initial VM {vm_name} in region {region}")