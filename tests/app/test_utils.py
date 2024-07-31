from app.utils import (
    is_new_job_status_valid,
    get_region_from_vm_name,
    HEARTBEAT_ORDERED_JOB_STATUSES,
    JOB_STATUS_NEW,
    JOB_STATUS_RUNNING,
    JOB_STATUS_COMPLETED,
)


def test_is_new_job_status_valid():
    """
    Test the is_new_job_status_valid function for various job status transitions.

    This test checks if the function correctly validates job status transitions
    according to the defined order in HEARTBEAT_ORDERED_JOB_STATUSES.
    """
    # Test valid transitions
    assert is_new_job_status_valid(JOB_STATUS_NEW, JOB_STATUS_RUNNING) is True
    assert is_new_job_status_valid(JOB_STATUS_RUNNING, JOB_STATUS_COMPLETED) is True
    assert is_new_job_status_valid(JOB_STATUS_NEW, JOB_STATUS_NEW) is True  # Same status should be valid

    # Test invalid transitions
    assert is_new_job_status_valid(JOB_STATUS_COMPLETED, JOB_STATUS_RUNNING) is False
    assert is_new_job_status_valid(JOB_STATUS_RUNNING, JOB_STATUS_NEW) is False


def test_get_region_from_vm_name():
    """
    Test the get_region_from_vm_name function for extracting regions from VM names.

    This test verifies if the function correctly extracts the region from
    various VM name formats and handles edge cases like None input.
    """
    # Test valid VM names
    assert get_region_from_vm_name("pipeline-zen-jobs-4xa100-40gb-us-west1-abcd") == "us-west1"
    assert get_region_from_vm_name("pipeline-zen-jobs-4xa100-40gb-us-central1-xyz0") == "us-central1"

    # Test edge case: None input
    assert get_region_from_vm_name(None) is None


def test_heartbeat_ordered_job_statuses():
    """
    Test the HEARTBEAT_ORDERED_JOB_STATUSES constant for correct order of job statuses.

    This test ensures that the predefined order of job statuses matches the expected order.
    """
    expected_order = [
        'NEW', 'PENDING', 'RUNNING', 'STOPPING', 'STOPPED', 'COMPLETED', 'FAILED'
    ]
    assert HEARTBEAT_ORDERED_JOB_STATUSES == expected_order
