from typing import Optional

# VM status constants
INSTANCE_STATUS_RUNNING = 'RUNNING'

# Job status constants
JOB_STATUS_NEW = 'NEW'
JOB_STATUS_PENDING = 'PENDING'
JOB_STATUS_RUNNING = 'RUNNING'
JOB_STATUS_STOPPING = 'STOPPING'
JOB_STATUS_STOPPED = 'STOPPED'
JOB_STATUS_COMPLETED = 'COMPLETED'
JOB_STATUS_FAILED = 'FAILED'

# Ordered list of job statuses
HEARTBEAT_ORDERED_JOB_STATUSES = [JOB_STATUS_NEW, JOB_STATUS_PENDING, JOB_STATUS_RUNNING,
                                  JOB_STATUS_STOPPING, JOB_STATUS_STOPPED,
                                  JOB_STATUS_COMPLETED, JOB_STATUS_FAILED]


def is_new_job_status_valid(old_status: str, new_status: str) -> bool:
    """
    Checks if the new job status is valid. We allow same old and new status so that we can process heartbeats.

    Args:
        old_status: old job status
        new_status: new job status
    Returns:
        bool: True if the new job status is valid, False otherwise
    """
    return HEARTBEAT_ORDERED_JOB_STATUSES.index(new_status) >= HEARTBEAT_ORDERED_JOB_STATUSES.index(old_status)


def get_region_from_vm_name(vm_name: Optional[str]) -> Optional[str]:
    """
    Get the region from a VM name.

    Args:
        vm_name (str): The name of the VM.

    Returns:
        str: The region of the VM.
    """
    return '-'.join(vm_name.split('-')[-3:-1]) if vm_name else None
