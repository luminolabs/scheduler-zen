import json
from contextlib import asynccontextmanager
from unittest import mock
from unittest.mock import AsyncMock, MagicMock, call

import pytest

from app.core.exceptions import DetachVMError
from app.core.utils import (
    JOB_STATUS_NEW, JOB_STATUS_WAIT_FOR_VM, JOB_STATUS_RUNNING, JOB_STATUS_STOPPED, JOB_STATUS_DETACHED_VM
)
from app.gcp.scheduler import Scheduler


class MockTransaction:
    """Mock transaction context manager"""
    def __init__(self):
        self.conn = AsyncMock()

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


@pytest.fixture
def mock_pubsub():
    """Create a mock PubSub client."""
    client = AsyncMock()
    client.publish_start_signal = AsyncMock()
    client.publish_stop_signal = AsyncMock()
    client.get_next_message = MagicMock()
    return client


@pytest.fixture
def mock_cluster_orchestrator():
    """Create a mock ClusterOrchestrator."""
    orchestrator = AsyncMock()
    orchestrator.cluster_exists.return_value = True
    orchestrator.scale_clusters = AsyncMock()
    return orchestrator


@pytest.fixture
def mock_mig_client():
    """Create a mock MIG client."""
    client = AsyncMock()
    client.detach_vm = AsyncMock()
    client.remove_vm_from_cache = MagicMock()
    return client


@pytest.fixture
def mock_db():
    """Create a mock database with transaction support."""
    db = AsyncMock()

    # Mock the transaction context manager
    @asynccontextmanager
    async def transaction():
        mock_transaction = MockTransaction()
        yield mock_transaction.conn

    db.transaction = transaction
    return db


@pytest.fixture
def scheduler(mock_db, mock_pubsub, mock_cluster_orchestrator, mock_mig_client):
    """Create a Scheduler instance with mocked dependencies."""
    return Scheduler(mock_db, mock_pubsub, mock_cluster_orchestrator, mock_mig_client)


@pytest.fixture
def sample_job_data():
    """Sample job data for testing."""
    return {
        "job_id": "test-job-123",
        "user_id": "test-user-456",
        "workflow": "test-workflow",
        "args": {"param1": "value1", "param2": "value2"},
        "gpu_type": "a100-40gb",
        "num_gpus": 4,
        "keep_alive": False,
        "cluster": "4xa100-40gb"
    }


@pytest.mark.asyncio
async def test_add_job(scheduler, mock_db, sample_job_data):
    """Test adding a new job."""
    await scheduler.add_job(sample_job_data)

    mock_db.add_job_gcp.assert_awaited_once()
    call_args = mock_db.add_job_gcp.await_args[0]
    assert call_args[1] == sample_job_data


@pytest.mark.asyncio
async def test_stop_job_success(scheduler, mock_db, mock_pubsub):
    """Test successfully stopping a running job."""
    # Mock a running job
    mock_db.get_job.return_value = {
        "job_id": "test-job",
        "user_id": "test-user",
        "status": JOB_STATUS_RUNNING
    }

    # Stop the job
    result = await scheduler.stop_job("test-job", "test-user")

    # Verify results
    assert result is True
    mock_db.update_job_gcp.assert_awaited_once()
    mock_pubsub.publish_stop_signal.assert_awaited_once()


@pytest.mark.asyncio
async def test_stop_job_not_running(scheduler, mock_db, mock_pubsub):
    """Test attempting to stop a job that isn't running."""
    # Mock a non-running job
    mock_db.get_job.return_value = {
        "job_id": "test-job",
        "user_id": "test-user",
        "status": JOB_STATUS_NEW
    }

    # Attempt to stop the job
    result = await scheduler.stop_job("test-job", "test-user")

    # Verify results
    assert result is False
    mock_db.update_job_gcp.assert_not_awaited()
    mock_pubsub.publish_stop_signal.assert_not_awaited()


@pytest.mark.asyncio
async def test_schedule_jobs(scheduler, mock_db, mock_pubsub):
    """Test scheduling new jobs."""
    # Mock new jobs
    mock_db.get_jobs_by_status_gcp.return_value = [{
        "job_id": "test-job",
        "user_id": "test-user",
        "workflow": "test-workflow",
        "args": {},
        "gcp": {"cluster": "test-cluster"}
    }]

    # Run the scheduling cycle
    await scheduler._schedule_jobs()

    # Verify results
    mock_db.update_job_gcp.assert_awaited_once()
    mock_pubsub.publish_start_signal.assert_awaited_once()


@pytest.mark.asyncio
async def test_monitor_and_detach_vms(scheduler, mock_db, mock_mig_client):
    """Test monitoring and detaching VMs."""
    # Mock jobs with FOUND_VM status
    mock_db.get_jobs_by_status_gcp.return_value = [{
        "job_id": "test-job",
        "user_id": "test-user",
        "gcp": {"vm_name": "test-vm"}
    }]

    # Run the monitoring cycle
    await scheduler._monitor_and_detach_vms()

    # Verify results
    mock_db.update_job_gcp.assert_awaited_once()
    mock_mig_client.detach_vm.assert_awaited_once_with("test-vm", "test-job")


@pytest.mark.asyncio
async def test_monitor_and_detach_vms_handles_detach_error(scheduler, mock_db, mock_mig_client):
    """Test error handling when VM detachment fails."""
    # Mock a job in FOUND_VM status
    found_vm_job = {
        "job_id": "test-job",
        "user_id": "test-user",
        "gcp": {"vm_name": "test-vm"}
    }
    mock_db.get_jobs_by_status_gcp.return_value = [found_vm_job]

    # Make detach_vm raise DetachVMError
    mock_mig_client.detach_vm.side_effect = DetachVMError("Failed to detach VM")

    # Run the monitoring cycle
    await scheduler._monitor_and_detach_vms()

    # Verify the job status was reset to NEW
    mock_db.update_job_gcp.assert_has_awaits([
        # First call trying to update to DETACHED_VM (will fail)
        mock.call(mock.ANY, "test-job", "test-user", "DETACHED_VM"),
        # Second call resetting to NEW
        mock.call(mock.ANY, "test-job", "test-user", "NEW")
    ])

    # Verify detach was attempted
    mock_mig_client.detach_vm.assert_awaited_once_with("test-vm", "test-job")


@pytest.mark.asyncio
async def test_handle_heartbeat(scheduler, mock_db):
    """Test handling a heartbeat message."""
    # Create sample heartbeat data
    heartbeat_data = {
        "job_id": "test-job",
        "user_id": "test-user",
        "status": JOB_STATUS_RUNNING,
        "vm_name": "test-vm-name-us-central1-0001"  # Valid VM name format
    }

    # Mock current job status
    mock_db.get_job.return_value = {
        "job_id": "test-job",
        "user_id": "test-user",
        "status": JOB_STATUS_WAIT_FOR_VM
    }

    # Handle the heartbeat
    await scheduler._handle_heartbeat(json.dumps(heartbeat_data).encode())

    # Verify database was updated
    mock_db.update_job_gcp.assert_awaited_once()


@pytest.mark.asyncio
async def test_run_cycle(scheduler):
    """Test running a complete scheduler cycle."""
    # Set up component mocks
    scheduler._schedule_jobs = AsyncMock()
    scheduler._monitor_and_detach_vms = AsyncMock()
    scheduler._process_heartbeats = AsyncMock()
    scheduler._monitor_and_scale_clusters = AsyncMock()

    # Set scheduler as running
    scheduler.running = True

    # Run a cycle
    await scheduler.run_cycle()

    # Verify all components were called
    scheduler._schedule_jobs.assert_awaited_once()
    scheduler._monitor_and_detach_vms.assert_awaited_once()
    scheduler._process_heartbeats.assert_awaited_once()
    scheduler._monitor_and_scale_clusters.assert_awaited_once()


@pytest.mark.asyncio
async def test_run_cycle_with_error(scheduler):
    """Test running a cycle when a component raises an error."""
    # Mock the component methods
    scheduler._schedule_jobs = AsyncMock()
    scheduler._monitor_and_detach_vms = AsyncMock()
    scheduler._process_heartbeats = AsyncMock()
    scheduler._monitor_and_scale_clusters = AsyncMock(side_effect=Exception("Test error"))

    # Set scheduler as running
    scheduler.running = True

    # Run a cycle
    await scheduler.run_cycle()

    # Verify all components were called despite the error
    scheduler._schedule_jobs.assert_awaited_once()
    scheduler._monitor_and_detach_vms.assert_awaited_once()
    scheduler._process_heartbeats.assert_awaited_once()


@pytest.mark.asyncio
async def test_process_heartbeats(scheduler, mock_pubsub):
    """Test processing heartbeat messages."""
    # Create sample messages with valid VM names
    message1 = MagicMock()
    message1.data = json.dumps({
        "job_id": "test-job-1",
        "status": JOB_STATUS_RUNNING,
        "vm_name": "test-vm-name-us-central1-0001"
    }).encode()

    message2 = MagicMock()
    message2.data = json.dumps({
        "job_id": "test-job-2",
        "status": JOB_STATUS_STOPPED,
        "vm_name": "test-vm-name-us-central1-0002"
    }).encode()

    # Mock getting messages from queue
    mock_pubsub.get_next_message.side_effect = [message1, message2, None]

    # Mock handle_heartbeat to avoid transaction issues
    scheduler._handle_heartbeat = AsyncMock()

    # Process heartbeats
    await scheduler._process_heartbeats()

    # Verify messages were processed
    assert scheduler._handle_heartbeat.await_count == 2
    message1.ack.assert_called_once()
    message2.ack.assert_called_once()


@pytest.mark.asyncio
async def test_start_stop(scheduler, mock_pubsub):
    """Test starting and stopping the scheduler."""
    # Test start
    await scheduler.start()
    assert scheduler.running is True
    mock_pubsub.start.assert_awaited_once()

    # Test stop
    await scheduler.stop()
    assert scheduler.running is False
    mock_pubsub.stop.assert_awaited_once()