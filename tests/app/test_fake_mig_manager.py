from collections.abc import Coroutine

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
import json

from app.fake_mig_manager import FakeMigManager
from app.config_manager import config


@pytest.fixture
def fake_mig_manager():
    """Fixture to create a FakeMigManager instance for testing."""
    return FakeMigManager("test-project", "test-heartbeat-topic", "test-start-job-subscription")


@pytest.mark.asyncio
async def test_init(fake_mig_manager):
    """Test the initialization of FakeMigManager."""
    assert fake_mig_manager.project_id == "test-project"
    assert fake_mig_manager.heartbeat_topic == "test-heartbeat-topic"
    assert fake_mig_manager.start_job_subscription == "test-start-job-subscription"
    assert isinstance(fake_mig_manager.migs, dict)
    assert isinstance(fake_mig_manager.active_vms, dict)
    assert isinstance(fake_mig_manager.job_queue, asyncio.Queue)


@pytest.mark.asyncio
async def test_start_stop(fake_mig_manager):
    """Test the start and stop methods of FakeMigManager."""
    with patch.object(fake_mig_manager, 'listen_for_new_jobs', AsyncMock()), \
            patch.object(fake_mig_manager, 'process_jobs', AsyncMock()), \
            patch.object(fake_mig_manager, 'process_messages', AsyncMock()):

        await fake_mig_manager.start()
        assert fake_mig_manager.running is True

        await fake_mig_manager.stop()
        assert fake_mig_manager.running is False


@pytest.mark.asyncio
async def test_handle_new_job(fake_mig_manager):
    """Test the handle_new_job method of FakeMigManager."""
    message = MagicMock()
    message.data = json.dumps({"job_id": "test-job"}).encode('utf-8')

    await fake_mig_manager.handle_new_job(message)

    assert fake_mig_manager.job_queue.qsize() == 1
    job = await fake_mig_manager.job_queue.get()
    assert job == {"job_id": "test-job"}
    message.ack.assert_called_once()


@pytest.mark.asyncio
async def test_scale_mig(fake_mig_manager):
    """Test the scale_mig method of FakeMigManager."""
    region = "test-region"
    mig_name = "pipeline-zen-jobs-test-cluster-test-region"

    # Test scaling up
    await fake_mig_manager.scale_mig(region, mig_name, 2)
    assert region in fake_mig_manager.migs
    assert mig_name in fake_mig_manager.migs[region]
    assert len(fake_mig_manager.migs[region][mig_name]) == 2

    # Test scaling down
    await fake_mig_manager.scale_mig(region, mig_name, 1)
    assert len(fake_mig_manager.migs[region][mig_name]) == 1


@pytest.mark.asyncio
async def test_assign_job_to_vm(fake_mig_manager):
    """Test the assign_job_to_vm method of FakeMigManager."""
    job = {"job_id": "test-job", "cluster": "test-cluster"}

    with patch.object(fake_mig_manager, 'get_region_for_cluster', return_value="test-region"), \
            patch.object(fake_mig_manager, 'simulate_vm', AsyncMock()) as mock_simulate, \
            patch.object(asyncio, 'create_task') as mock_create_task:

        # Scenario: Scaling is needed
        fake_mig_manager.migs = {}
        fake_mig_manager.active_vms = {}

        await fake_mig_manager.assign_job_to_vm(job)

        # Check if migs is properly updated
        assert "test-region" in fake_mig_manager.migs
        assert "pipeline-zen-jobs-test-cluster-test-region" in fake_mig_manager.migs["test-region"]
        assert len(fake_mig_manager.migs["test-region"]["pipeline-zen-jobs-test-cluster-test-region"]) > 0

        mock_create_task.assert_called_once()
        create_task_args = mock_create_task.call_args[0][0]
        assert isinstance(create_task_args, Coroutine)

        # Call the coroutine to simulate the task execution
        await create_task_args

        mock_simulate.assert_awaited_once()
        assert len(fake_mig_manager.active_vms) == 1

    # Print the migs and active_vms for debugging
    print(f"migs: {fake_mig_manager.migs}")
    print(f"active_vms: {fake_mig_manager.active_vms}")


def test_get_region_for_cluster(fake_mig_manager):
    """Test the get_region_for_cluster method of FakeMigManager."""
    config.gpu_regions = {"test-cluster": ["region1", "region2"]}
    fake_mig_manager.gpu_regions = config.gpu_regions

    region = fake_mig_manager.get_region_for_cluster("test-cluster")
    assert region in ["region1", "region2"]


@pytest.mark.asyncio
async def test_get_target_and_running_vm_counts(fake_mig_manager):
    """Test the get_target_and_running_vm_counts method of FakeMigManager."""
    fake_mig_manager.migs = {"test-region": {"test-mig": ["vm1", "vm2"]}}
    fake_mig_manager.active_vms = {"vm1": AsyncMock()}

    target, running = await fake_mig_manager.get_target_and_running_vm_counts("test-region", "test-mig")
    assert target == 2
    assert running == 1


@pytest.mark.asyncio
async def test_simulate_vm(fake_mig_manager):
    """Test the simulate_vm method of FakeMigManager."""
    with patch.object(fake_mig_manager, 'send_heartbeat', AsyncMock()) as mock_heartbeat, \
            patch.object(fake_mig_manager, 'delete_vm', AsyncMock()) as mock_delete, \
            patch('asyncio.sleep', AsyncMock()):

        await fake_mig_manager.simulate_vm("test-job", "test-vm", "test-region", "test-mig")

        assert mock_heartbeat.await_count >= 4  # At least 3 RUNNING and 1 COMPLETED
        mock_delete.assert_awaited_once_with("test-region", "test-mig", "test-vm")


@pytest.mark.asyncio
async def test_send_heartbeat(fake_mig_manager):
    """Test the send_heartbeat method of FakeMigManager."""
    with patch.object(fake_mig_manager.publisher, 'publish', return_value=MagicMock()) as mock_publish:
        await fake_mig_manager.send_heartbeat("test-job", "test-vm", "RUNNING")

        mock_publish.assert_called_once()
        args, _ = mock_publish.call_args
        assert args[0] == fake_mig_manager.heartbeat_topic_path
        message = json.loads(args[1].decode('utf-8'))
        assert message == {"job_id": "test-job", "status": "RUNNING", "vm_name": "test-vm"}


@pytest.mark.asyncio
async def test_delete_vm(fake_mig_manager):
    """Test the delete_vm method of FakeMigManager."""
    fake_mig_manager.migs = {"test-region": {"test-mig": ["test-vm"]}}
    fake_mig_manager.active_vms = {"test-vm": AsyncMock()}

    await fake_mig_manager.delete_vm("test-region", "test-mig", "test-vm")

    assert "test-vm" not in fake_mig_manager.migs["test-region"]["test-mig"]
    assert "test-vm" not in fake_mig_manager.active_vms
