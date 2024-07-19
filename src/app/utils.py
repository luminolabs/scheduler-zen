# This file contains utility functions that are used across the application.
from typing import Optional


def get_region_from_vm_name(vm_name: Optional[str]) -> Optional[str]:
    """
    Get the region from a VM name.

    Args:
        vm_name (str): The name of the VM.

    Returns:
        str: The region of the VM.
    """
    return '-'.join(vm_name.split('-')[-3:-1]) if vm_name else None
