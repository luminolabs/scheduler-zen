import asyncio
from unittest.mock import patch, AsyncMock

import pytest

from app.tasks.artifacts_sync import sync_job_artifacts


@pytest.fixture
def sample_job():
    """Sample job fixture for tests."""
    return {
        "job_id": "test-job-123",
        "user_id": "test-user-456"
    }


@pytest.fixture
def sample_jobs():
    """Sample list of jobs fixture for tests."""
    return [
        {"job_id": "job1", "user_id": "user1"},
        {"job_id": "job2", "user_id": "user2"},
        {"job_id": "job3", "user_id": "user3"}
    ]


@pytest.mark.asyncio
async def test_sync_job_artifacts_success(mock_db, sample_jobs):
    """Test successful synchronization of job artifacts."""
    # Mock DB responses
    mock_db.get_jobs_by_status_lum.return_value = [sample_jobs[0]]
    mock_db.get_jobs_by_status_gcp.return_value = [sample_jobs[1]]
    mock_db.get_recently_completed_jobs.return_value = [sample_jobs[2]]

    # Mock artifacts pulling results
    mock_results = [
        ("job1", "user1", {"metric1": "value1"}),
        ("job2", "user2", {"metric2": "value2"}),
        ("job3", "user3", {"metric3": "value3"})
    ]

    # Mock the artifacts client function
    with patch('app.tasks.artifacts_sync.pull_artifacts_meta_from_gcs_task',
               AsyncMock(side_effect=mock_results)):
        await sync_job_artifacts(mock_db)

    # Verify DB calls
    mock_db.get_jobs_by_status_lum.assert_called_once()
    mock_db.get_jobs_by_status_gcp.assert_called_once()
    mock_db.get_recently_completed_jobs.assert_called_once()

    # Verify artifacts were updated for each job
    assert mock_db.update_job_artifacts.call_count == 3
    mock_db.update_job_artifacts.assert_any_call("job1", "user1", {"metric1": "value1"})
    mock_db.update_job_artifacts.assert_any_call("job2", "user2", {"metric2": "value2"})
    mock_db.update_job_artifacts.assert_any_call("job3", "user3", {"metric3": "value3"})


@pytest.mark.asyncio
async def test_sync_job_artifacts_with_none_results(mock_db, sample_jobs):
    """Test synchronization when some artifact pulls return None."""
    # Mock DB responses
    mock_db.get_jobs_by_status_lum.return_value = [sample_jobs[0]]
    mock_db.get_jobs_by_status_gcp.return_value = [sample_jobs[1]]
    mock_db.get_recently_completed_jobs.return_value = [sample_jobs[2]]

    # Mock results with some None values
    mock_results = [
        None,  # Failed pull
        ("job2", "user2", {"metric2": "value2"}),
        None   # Failed pull
    ]

    # Mock the artifacts client function
    with patch('app.tasks.artifacts_sync.pull_artifacts_meta_from_gcs_task',
               AsyncMock(side_effect=mock_results)):
        await sync_job_artifacts(mock_db)

    # Verify only successful pulls were updated
    assert mock_db.update_job_artifacts.call_count == 1
    mock_db.update_job_artifacts.assert_called_once_with(
        "job2", "user2", {"metric2": "value2"}
    )


@pytest.mark.asyncio
async def test_sync_job_artifacts_db_error(mock_db):
    """Test handling of database errors during synchronization."""
    # Mock DB to raise an exception
    mock_db.get_jobs_by_status_lum.side_effect = Exception("DB Error")

    # Run the sync and verify it handles the error gracefully
    await sync_job_artifacts(mock_db)

    # Verify no artifact updates were attempted
    mock_db.update_job_artifacts.assert_not_called()


@pytest.mark.asyncio
async def test_sync_job_artifacts_parallel_execution(mock_db, sample_jobs):
    """Test that artifact pulling is executed in parallel."""
    # Mock DB responses
    mock_db.get_jobs_by_status_lum.return_value = sample_jobs
    mock_db.get_jobs_by_status_gcp.return_value = []
    mock_db.get_recently_completed_jobs.return_value = []

    # Create a mock for artifact pulling that includes a delay
    async def mock_pull_with_delay(*args):
        await asyncio.sleep(0.1)  # Small delay to simulate work
        return (args[0], args[1], {"metric": "value"})

    # Mock the artifacts client function
    with patch('app.tasks.artifacts_sync.pull_artifacts_meta_from_gcs_task',
               side_effect=mock_pull_with_delay) as mock_pull:
        # Start timing
        start_time = asyncio.get_event_loop().time()

        # Run the sync
        await sync_job_artifacts(mock_db)

        # Calculate total execution time
        execution_time = asyncio.get_event_loop().time() - start_time

    # Verify all jobs were processed
    assert mock_pull.call_count == 3

    # Verify execution time is less than sequential execution would take
    # Sequential would be at least 0.3s (3 * 0.1s)
    # Parallel should be closer to 0.1s plus minimal overhead
    assert execution_time < 0.3


@pytest.mark.asyncio
async def test_sync_job_artifacts_empty_jobs(mock_db):
    """Test synchronization when there are no jobs to process."""
    # Mock DB to return empty lists
    mock_db.get_jobs_by_status_lum.return_value = []
    mock_db.get_jobs_by_status_gcp.return_value = []
    mock_db.get_recently_completed_jobs.return_value = []

    # Mock the artifacts client function
    with patch('app.tasks.artifacts_sync.pull_artifacts_meta_from_gcs_task') as mock_pull:
        await sync_job_artifacts(mock_db)

    # Verify no artifact pulls were attempted
    mock_pull.assert_not_called()

    # Verify no updates were attempted
    mock_db.update_job_artifacts.assert_not_called()


@pytest.mark.asyncio
async def test_sync_job_artifacts_update_error(mock_db, sample_jobs):
    """Test handling of errors during artifact updates."""
    # Mock DB responses
    mock_db.get_jobs_by_status_lum.return_value = sample_jobs
    mock_db.get_jobs_by_status_gcp.return_value = []
    mock_db.get_recently_completed_jobs.return_value = []

    # Mock update_job_artifacts to raise an exception
    mock_db.update_job_artifacts.side_effect = Exception("Update Error")

    # Mock successful artifact pulls
    mock_results = [
        ("job1", "user1", {"metric": "value1"}),
        ("job2", "user2", {"metric": "value2"}),
        ("job3", "user3", {"metric": "value3"})
    ]

    # Mock the artifacts client function
    with patch('app.tasks.artifacts_sync.pull_artifacts_meta_from_gcs_task',
               AsyncMock(side_effect=mock_results)):
        # Verify the function handles the error gracefully
        await sync_job_artifacts(mock_db)

    # Verify update was attempted for all jobs despite errors
    assert mock_db.update_job_artifacts.call_count == 3
