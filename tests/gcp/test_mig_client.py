import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from google.api_core.exceptions import BadRequest
from google.cloud import compute_v1

from app.core.exceptions import DetachVMError
from app.gcp.mig_client import MigClient


@pytest.fixture
def mock_instances_client():
    """Create a mock InstancesClient."""
    mock = AsyncMock(spec=compute_v1.InstancesClient)
    return mock


@pytest.fixture
def mock_mig_client():
    """Create a mock RegionInstanceGroupManagersClient."""
    mock = AsyncMock(spec=compute_v1.RegionInstanceGroupManagersClient)
    return mock


@pytest.fixture
async def mig_client(mock_instances_client, mock_mig_client):
    """Create a MigClient instance with mocked dependencies."""
    with patch('app.gcp.mig_client.compute_v1.InstancesClient', return_value=mock_instances_client), \
            patch('app.gcp.mig_client.compute_v1.RegionInstanceGroupManagersClient', return_value=mock_mig_client), \
            patch('app.gcp.mig_client.asyncio.to_thread') as mock_to_thread:
        client = MigClient("test-project")
        client.client = mock_mig_client
        client.instances_client = mock_instances_client

        # Configure mock_to_thread to pass through the result
        async def async_pass_through(func, *args, **kwargs):
            if asyncio.iscoroutine(func):
                return await func
            if callable(func):
                return func(*args, **kwargs)
            return func
        mock_to_thread.side_effect = async_pass_through

        yield client


@pytest.mark.asyncio
async def test_set_target_size(mig_client):
    """Test setting MIG target size."""
    # Set up test data
    region = "us-central1"
    mig_name = "test-mig"
    target_size = 3

    # Mock operation result
    operation_result = MagicMock()
    mock_operation = MagicMock()
    mock_operation.result = operation_result
    mig_client.client.resize.return_value = mock_operation

    # Set target size
    await mig_client.set_target_size(region, mig_name, target_size)

    # Verify API call
    # noinspection PyTypeChecker
    expected_request = compute_v1.ResizeRegionInstanceGroupManagerRequest(
        project="test-project",
        region=region,
        instance_group_manager=mig_name,
        size=target_size
    )
    mig_client.client.resize.assert_called_once_with(expected_request)
    # Verify operation result was called
    assert operation_result.called


@pytest.mark.asyncio
async def test_detach_vm(mig_client):
    """Test detaching a VM from its MIG."""
    # Set up test data
    vm_name = "pipeline-zen-jobs-4xa100-40gb-us-central1-vm-0001"
    job_id = "test-job"
    region = "us-central1"
    zone = "us-central1-a"
    mig_name = "pipeline-zen-jobs-4xa100-40gb-us-central1-mig"

    # Mock get_instance_zone call
    mig_client.get_instance_zone = AsyncMock(return_value=zone)

    # Mock operation result
    operation_result = MagicMock()
    mock_operation = MagicMock()
    mock_operation.result = operation_result
    mig_client.client.abandon_instances.return_value = mock_operation

    # Detach VM
    await mig_client.detach_vm(vm_name, job_id)

    # Verify API calls
    mig_client.get_instance_zone.assert_called_once_with(region, vm_name)

    # noinspection PyTypeChecker
    expected_request = compute_v1.AbandonInstancesRegionInstanceGroupManagerRequest(
        project="test-project",
        region=region,
        instance_group_manager=mig_name,
        region_instance_group_managers_abandon_instances_request_resource=compute_v1.RegionInstanceGroupManagersAbandonInstancesRequest(
            instances=[f"zones/{zone}/instances/{vm_name}"]
        )
    )
    mig_client.client.abandon_instances.assert_called_once_with(expected_request)
    # Verify operation result was called
    assert operation_result.called

@pytest.mark.asyncio
async def test_detach_vm_error_handling(mig_client):
    """Test error handling when detaching a VM fails."""
    # Set up test data
    vm_name = "pipeline-zen-jobs-4xa100-40gb-us-central1-vm-0001"
    job_id = "test-job"
    region = "us-central1"
    zone = "us-central1-a"
    mig_name = "pipeline-zen-jobs-4xa100-40gb-us-central1-mig"

    # Mock get_instance_zone call
    mig_client.get_instance_zone = AsyncMock(return_value=zone)

    # Mock abandon_instances to raise a BadRequest error
    error_message = "Failed to detach VM"
    mock_error = BadRequest(message=error_message)
    mig_client.client.abandon_instances.side_effect = mock_error

    # Attempt to detach VM and verify error handling
    with pytest.raises(DetachVMError) as exc_info:
        await mig_client.detach_vm(vm_name, job_id)

    # Verify correct error was raised and propagated
    assert str(exc_info.value) == f"Couldn't detach VM {vm_name} for job {job_id}"
    assert exc_info.value.__cause__ == mock_error

    # Verify API calls were made correctly before error
    mig_client.get_instance_zone.assert_called_once_with(region, vm_name)
    expected_request = compute_v1.AbandonInstancesRegionInstanceGroupManagerRequest(
        project="test-project",
        region=region,
        instance_group_manager=mig_name,
        region_instance_group_managers_abandon_instances_request_resource=compute_v1.RegionInstanceGroupManagersAbandonInstancesRequest(
            instances=[f"zones/{zone}/instances/{vm_name}"]
        )
    )
    mig_client.client.abandon_instances.assert_called_once_with(expected_request)
