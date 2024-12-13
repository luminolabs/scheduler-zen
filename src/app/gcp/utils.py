from typing import Optional


def get_region_from_vm_name(vm_name: Optional[str]) -> Optional[str]:
    """
    Get the region from a VM name.

    Args:
        vm_name (str): The name of the VM.

    Returns:
        str: The region of the VM.
    """
    return '-'.join(vm_name.split('-')[-4:-2]) if vm_name else None


def get_mig_name_from_vm_name(vm_name: Optional[str]) -> Optional[str]:
    """
    Get the MIG name from a VM name.

    Args:
        vm_name (str): The name of the VM.

    Returns:
        str: The MIG name of the VM.
    """
    return '-'.join(vm_name.split('-')[:-2]) + '-mig' if vm_name else None


def get_mig_name_from_cluster_and_region(cluster: str, region: str) -> str:
    """
    Get the MIG name from a cluster and region.

    Args:
        cluster (str): The name of the cluster.
        region (str): The region of the MIG.

    Returns:
        str: The MIG name of the cluster and region.
    """
    return f"pipeline-zen-jobs-{cluster}-{region}-mig"
