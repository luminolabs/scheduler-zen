from unittest.mock import AsyncMock, call

import pytest

from app.gcp.cluster_manager import ClusterManager


@pytest.fixture
def mock_mig_client():
    """Create a mock MIG client."""
    client = AsyncMock()
    # Set a default return value for get_current_target_size
    client.get_current_target_size.return_value = 0
    client.set_target_size = AsyncMock()
    return client


@pytest.fixture
def mock_db():
    """Create a mock database."""
    db = AsyncMock()
    db.get_jobs_by_status_gcp = AsyncMock()
    # Set default return value for FOUND_VM status check
    db.get_jobs_by_status_gcp.return_value = []
    return db


@pytest.fixture
def cluster_manager(mock_mig_client, mock_db):
    """Create a ClusterManager instance with mocked dependencies."""
    return ClusterManager(
        project_id="test-project",
        regions=["region1", "region2"],
        cluster="test-cluster",
        max_scale_limit=4,
        mig_client=mock_mig_client,
        db=mock_db
    )


@pytest.mark.asyncio
async def test_scale_all_regions_no_pending_jobs(cluster_manager, mock_db):
    """Test scaling when there are no pending jobs."""
    # Setup
    mock_db.get_jobs_by_status_gcp.side_effect = [
        [],  # First call for WAIT_FOR_VM
        [],  # Second call for FOUND_VM region1
        []   # Third call for FOUND_VM region2
    ]
    cluster_manager.mig_client.get_current_target_size.return_value = 1

    # Execute
    await cluster_manager.scale_all_regions()

    # Verify
    assert cluster_manager.mig_client.set_target_size.call_count == 2  # One for each region
    expected_calls = [
        call("region1", "pipeline-zen-jobs-test-cluster-region1", 0),
        call("region2", "pipeline-zen-jobs-test-cluster-region2", 0)
    ]
    cluster_manager.mig_client.set_target_size.assert_has_calls(expected_calls, any_order=True)


@pytest.mark.asyncio
async def test_scale_all_regions_with_pending_jobs(cluster_manager, mock_db):
    """Test scaling when there are pending jobs."""
    # Setup
    mock_db.get_jobs_by_status_gcp.side_effect = [
        [{"job_id": "1"}, {"job_id": "2"}, {"job_id": "3"}],  # WAIT_FOR_VM jobs
        [],  # FOUND_VM jobs for region1
        []   # FOUND_VM jobs for region2
    ]
    cluster_manager.mig_client.get_current_target_size.return_value = 1

    # Execute
    await cluster_manager.scale_all_regions()

    # Verify scaling was called for each region with correct size
    expected_calls = [
        call("region1", "pipeline-zen-jobs-test-cluster-region1", 3),
        call("region2", "pipeline-zen-jobs-test-cluster-region2", 3)
    ]
    cluster_manager.mig_client.set_target_size.assert_has_calls(expected_calls, any_order=True)


@pytest.mark.asyncio
async def test_scale_all_regions_respects_max_limit(cluster_manager, mock_db):
    """Test that scaling respects the maximum scale limit."""
    # Setup - Create more pending jobs than the max limit
    mock_db.get_jobs_by_status_gcp.side_effect = [
        [{"job_id": str(i)} for i in range(10)],  # 10 jobs > max_scale_limit of 4
        [],  # FOUND_VM jobs for region1
        []   # FOUND_VM jobs for region2
    ]
    cluster_manager.mig_client.get_current_target_size.return_value = 1

    # Execute
    await cluster_manager.scale_all_regions()

    # Verify target size is capped at max_scale_limit
    expected_calls = [
        call("region1", "pipeline-zen-jobs-test-cluster-region1", 4),
        call("region2", "pipeline-zen-jobs-test-cluster-region2", 4)
    ]
    cluster_manager.mig_client.set_target_size.assert_has_calls(expected_calls, any_order=True)
