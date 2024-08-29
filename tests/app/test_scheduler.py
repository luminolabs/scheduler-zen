import json
import asyncio

import pytest
from unittest.mock import AsyncMock, patch

from app.scheduler import Scheduler
from app.database import Database
from app.pubsub_client import PubSubClient
from app.cluster_orchestrator import ClusterOrchestrator
from app.utils import (
    JOB_STATUS_NEW,
    JOB_STATUS_PENDING,
    JOB_STATUS_RUNNING,
    JOB_STATUS_STOPPING
)


@pytest.fixture
def mock_db():
    """Fixture to create a mock Database instance."""
    return AsyncMock(spec=Database)


@pytest.fixture
def mock_pubsub():
    """Fixture to create a mock PubSubClient instance."""
    return AsyncMock(spec=PubSubClient)


@pytest.fixture
def mock_cluster_orchestrator():
    """Fixture to create a mock ClusterOrchestrator instance."""
    return AsyncMock(spec=ClusterOrchestrator)


@pytest.fixture
def scheduler(mock_db, mock_pubsub, mock_cluster_orchestrator):
    """Fixture to create a Scheduler instance with mock dependencies."""
    return Scheduler(mock_db, mock_pubsub, mock_cluster_orchestrator)


@pytest.mark.asyncio
async def test_init(scheduler, mock_db, mock_pubsub, mock_cluster_orchestrator):
    """Test the initialization of Scheduler."""
    assert scheduler.db == mock_db
    assert scheduler.pubsub == mock_pubsub
    assert scheduler.cluster_orchestrator == mock_cluster_orchestrator
    assert scheduler.running is False
    assert isinstance(scheduler.cluster_status, dict)


@pytest.mark.asyncio
async def test_start_stop(scheduler):
    """Test the start and stop methods of Scheduler."""
    with patch.object(scheduler, '_schedule_jobs', AsyncMock()) as mock_schedule, \
            patch.object(scheduler, '_listen_for_heartbeats', AsyncMock()) as mock_listen, \
            patch.object(scheduler, '_monitor_and_scale_clusters', AsyncMock()) as mock_monitor:

        await scheduler.start()
        assert scheduler.running is True
        scheduler.db.create_tables.assert_called_once()
        scheduler.pubsub.start.assert_called_once()

        mock_schedule.assert_called_once()
        mock_listen.assert_called_once()
        mock_monitor.assert_called_once()

        await scheduler.stop()
        assert scheduler.running is False
        scheduler.pubsub.stop.assert_called_once()


@pytest.mark.asyncio
async def test_add_job(scheduler):
    """Test the add_job method of Scheduler."""
    job_data = {
        "workflow": "test_workflow",
        "args": {"arg1": "value1"},
        "keep_alive": False,
        "cluster": "test_cluster"
    }
    scheduler.db.add_job.return_value = "test_job_id"

    job_id = await scheduler.add_job(job_data)

    assert job_id == "test_job_id"
    scheduler.db.add_job.assert_called_once_with(job_data)


@pytest.mark.asyncio
async def test_schedule_jobs(scheduler):
    """Test the _schedule_jobs method of Scheduler."""
    scheduler.running = True
    mock_jobs = [
        {
            "job_id": "job1",
            "args": {},
            "cluster": "test_cluster",
            "workflow": "test_workflow",
            "keep_alive": False
        }
    ]
    scheduler.db.get_jobs_by_status.side_effect = [
        mock_jobs,  # First call
        []  # Second call to exit the while loop
    ]

    async def stop_after_one_iteration():
        await asyncio.sleep(0.1)
        scheduler.running = False

    await asyncio.gather(
        scheduler._schedule_jobs(),
        stop_after_one_iteration()
    )

    scheduler.db.get_jobs_by_status.assert_called_with(JOB_STATUS_NEW)
    scheduler.pubsub.publish_start_signal.assert_called_once_with('pipeline-zen-jobs-start', mock_jobs[0])
    scheduler.db.update_job.assert_called_once_with("job1", JOB_STATUS_PENDING)


@pytest.mark.asyncio
async def test_listen_for_heartbeats(scheduler):
    """Test the _listen_for_heartbeats method of Scheduler."""
    scheduler.running = True
    mock_message = json.dumps({
        "job_id": "job1",
        "status": "RUNNING",
        "vm_name": "vm1"
    }).encode('utf-8')

    # Mock the get_job method to return a valid job
    scheduler.db.get_job.return_value = {
        "job_id": "job1",
        "status": JOB_STATUS_PENDING
    }

    async def mock_listen_for_heartbeats(subscription):
        # Simulate PubSub behavior by calling the heartbeat_callback directly
        await scheduler.pubsub.heartbeat_callback(mock_message)
        scheduler.running = False  # Stop the loop after first iteration

    # Replace the listen_for_heartbeats method with our mock
    scheduler.pubsub.listen_for_heartbeats = mock_listen_for_heartbeats

    await scheduler._listen_for_heartbeats()

    # Check that update_job was called with the correct arguments
    scheduler.db.update_job.assert_called_once_with("job1", "RUNNING", "vm1", '')
    scheduler.db.get_job.assert_called_once_with("job1")


@pytest.mark.asyncio
async def test_monitor_and_scale_clusters(scheduler):
    """Test the _monitor_and_scale_clusters method of Scheduler."""
    scheduler.running = True
    scheduler.cluster_orchestrator.get_status.return_value = {"cluster1": {"region1": (5, 3)}}
    scheduler.db.get_jobs_by_status.side_effect = [
        [{"cluster": "cluster1"}],  # Pending jobs
        [{"cluster": "cluster1", "region": "region1"}]  # Running jobs
    ]

    async def stop_after_one_iteration():
        await asyncio.sleep(0.1)
        scheduler.running = False

    await asyncio.gather(
        scheduler._monitor_and_scale_clusters(),
        stop_after_one_iteration()
    )

    scheduler.cluster_orchestrator.get_status.assert_called_once()
    scheduler.cluster_orchestrator.scale_clusters.assert_called_once_with(
        {"cluster1": 1},
        {"cluster1": {"region1": 1}}
    )


@pytest.mark.asyncio
async def test_stop_job(scheduler):
    """Test the stop_job method of Scheduler."""
    # Test stopping a running job
    scheduler.db.get_job.return_value = {"status": JOB_STATUS_RUNNING}

    result = await scheduler.stop_job("job1")

    assert result is True
    scheduler.pubsub.publish_stop_signal.assert_called_once_with('pipeline-zen-jobs-stop', 'job1')
    scheduler.db.update_job.assert_called_once_with('job1', JOB_STATUS_STOPPING)

    # Test stopping a non-existent job
    scheduler.db.get_job.return_value = None
    result = await scheduler.stop_job("job2")
    assert result is False


@pytest.mark.asyncio
async def test_get_status(scheduler):
    """Test the get_status method of Scheduler."""
    scheduler.cluster_status = {"cluster1": {"region1": (5, 3)}}
    scheduler.db.get_jobs_by_status.side_effect = [
        [{"job_id": "job1"}, {"job_id": "job2"}],  # Running jobs
        [{"job_id": "job3"}]  # Pending jobs
    ]

    status = await scheduler.get_status()

    assert status == {
        'cluster_status': {"cluster1": {"region1": (5, 3)}},
        'running_jobs': 2,
        'pending_jobs': 1
    }
    scheduler.db.get_jobs_by_status.assert_any_call(JOB_STATUS_RUNNING)
    scheduler.db.get_jobs_by_status.assert_any_call(JOB_STATUS_PENDING)
