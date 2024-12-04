import asyncio
import os
import subprocess
from typing import Dict, Optional

from app.core.utils import setup_logger
from app.gcp.utils import get_mig_name_from_cluster_and_region, get_region_from_vm_name

logger = setup_logger(__name__)


class FakeMigClientWithPipeline:
    """
    A mock implementation of the GCP MIG client that runs pipeline processes locally.
    Instead of Docker containers, it executes the startup script directly.
    """

    def __init__(self):
        """Initialize the fake MIG client."""
        # Dict to store MIG states: region -> {mig_name -> target_size}
        self.migs: Dict[str, Dict[str, int]] = {}
        # Dict to store active processes and their jobs: region -> {vm_name -> job_id}
        self.active_processes: Dict[str, Dict[str, Optional[str]]] = {}
        # Dict to store process start tasks: vm_name -> asyncio.Task
        self.process_start_tasks: Dict[str, asyncio.Task] = {}
        # Counter for generating VM names
        self.vm_counter = 0
        # Store process objects to track and manage them
        self.processes: Dict[str, subprocess.Popen] = {}
        logger.info("FakeMigClientWithPipeline initialized")

    def initialize_mig(self, cluster: str, region: str) -> None:
        """
        Initialize a MIG for a cluster in a region.

        Args:
            cluster (str): The cluster name
            region (str): The region name
        """
        mig_name = get_mig_name_from_cluster_and_region(cluster, region)
        if region not in self.migs:
            self.migs[region] = {}
            self.active_processes[region] = {}
        self.migs[region][mig_name] = 0
        logger.info(f"Initialized MIG {mig_name} in region {region}")

    async def set_target_size(self, region: str, mig_name: str, target_size: int) -> None:
        """
        Set the target size for a MIG and create/delete processes accordingly.

        Args:
            region (str): The region of the MIG
            mig_name (str): The name of the MIG
            target_size (int): The new target size for the MIG
        """
        current_size = len([vm for vm in self.active_processes[region].keys()
                            if vm.startswith(mig_name)])

        # Scale up: Create new processes
        while current_size < target_size:
            vm_name = f"{mig_name}-{str(self.vm_counter).zfill(4)}"
            self.active_processes[region][vm_name] = None  # No job assigned yet

            # Schedule process start after 10 seconds
            self.process_start_tasks[vm_name] = asyncio.create_task(
                self._start_process_delayed(vm_name)
            )

            self.vm_counter += 1
            current_size += 1
            logger.info(f"Created process slot for VM {vm_name} in region {region}")

        # Scale down: Remove unassigned processes
        if current_size > target_size:
            vms_to_remove = []
            for vm_name, job_id in self.active_processes[region].items():
                if current_size <= target_size:
                    break
                if vm_name.startswith(mig_name) and job_id is None:
                    vms_to_remove.append(vm_name)
                    current_size -= 1

            for vm_name in vms_to_remove:
                # Cancel any pending process start
                if vm_name in self.process_start_tasks:
                    self.process_start_tasks[vm_name].cancel()
                    del self.process_start_tasks[vm_name]

                # Kill process if it was already started
                self._terminate_process(vm_name)

                del self.active_processes[region][vm_name]
                logger.info(f"Removed process slot for VM {vm_name} from region {region}")

        # Update target size
        self.migs[region][mig_name] = target_size
        logger.info(f"Set target size to {target_size} for MIG {mig_name} in region {region}")

    async def _start_process_delayed(self, vm_name: str) -> None:
        """
        Start a process after a delay.

        Args:
            vm_name (str): The name of the VM
        """
        try:
            # Wait 10 seconds before starting process
            await asyncio.sleep(10)

            # Set up environment variables
            env = os.environ.copy()
            env['VM_NAME'] = vm_name

            # Start the process
            process = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: subprocess.Popen(
                    ['./scripts/mig-runtime/startup-script.sh'],
                    env=env,
                    cwd='../pipeline-zen'
                )
            )

            # Store the process object
            self.processes[vm_name] = process
            logger.info(f"Started process for VM {vm_name}")

        except asyncio.CancelledError:
            logger.info(f"Process start cancelled for VM {vm_name}")
        except subprocess.SubprocessError as e:
            logger.error(f"Failed to start process for VM {vm_name}: {str(e)}")
        finally:
            # Clean up the task reference
            if vm_name in self.process_start_tasks:
                del self.process_start_tasks[vm_name]

    def _terminate_process(self, vm_name: str) -> None:
        """
        Terminate a running process.

        Args:
            vm_name (str): The name of the VM/process
        """
        if vm_name in self.processes:
            try:
                process = self.processes[vm_name]
                process.terminate()
                try:
                    process.wait(timeout=5)  # Wait up to 5 seconds for graceful termination
                except subprocess.TimeoutExpired:
                    process.kill()  # Force kill if it doesn't terminate gracefully
                logger.info(f"Terminated process for VM {vm_name}")
            except Exception as e:
                logger.error(f"Error terminating process for VM {vm_name}: {str(e)}")
            finally:
                del self.processes[vm_name]

    async def get_current_target_size(self, region: str, mig_name: str) -> int:
        """
        Get the current target size for a MIG.

        Args:
            region (str): The region of the MIG
            mig_name (str): The name of the MIG

        Returns:
            int: The current target size
        """
        return self.migs[region].get(mig_name, 0)

    async def detach_vm(self, vm_name: str, job_id: str) -> None:
        """
        Assign a job ID to a process.

        Args:
            vm_name (str): The name of the VM
            job_id (str): The ID of the job to run
        """
        region = get_region_from_vm_name(vm_name)
        if region and vm_name in self.active_processes.get(region, {}):
            # Just assign the job ID - process is already running
            self.active_processes[region][vm_name] = job_id
            logger.info(f"Assigned job {job_id} to VM {vm_name}")

    def remove_vm_from_cache(self, region: str, vm_name: str) -> None:
        """
        Remove a VM from the active processes cache and terminate its process.

        Args:
            region (str): The region of the VM
            vm_name (str): The name of the VM
        """
        if vm_name in self.active_processes[region]:
            # Cancel any pending process start
            if vm_name in self.process_start_tasks:
                self.process_start_tasks[vm_name].cancel()
                del self.process_start_tasks[vm_name]

            # Terminate the process
            self._terminate_process(vm_name)

            # Remove from active processes
            del self.active_processes[region][vm_name]
            logger.info(f"Removed VM {vm_name} from cache in region {region}")

    async def get_instance_zone(self, region: str, vm_name: str) -> Optional[str]:
        """
        Get the zone for a VM instance (simulated).

        Args:
            region (str): The region of the VM
            vm_name (str): The name of the VM

        Returns:
            str: The simulated zone (region + "-a")
        """
        if vm_name in self.active_processes.get(region, {}):
            return f"{region}-a"
        return None
