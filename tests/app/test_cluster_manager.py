import pytest
from unittest.mock import AsyncMock, MagicMock

from app.cluster_manager import ClusterManager
from app.mig_manager import MigManager


@pytest.fixture
def mock_mig_manager():
    """
    Fixture to create a mock MigManager.

    Returns:
        MagicMock: A mock object simulating MigManager behavior.
    """
    return MagicMock(spec=MigManager)


@pytest.fixture
def cluster_manager(mock_mig_manager):
    """
    Fixture to create a ClusterManager instance for testing.

    Args:
        mock_mig_manager (MagicMock): A mock MigManager instance.

    Returns:
        ClusterManager: An instance of ClusterManager for testing.
    """
    return ClusterManager(
        project_id="test-project",
        regions=["region1", "region2"],
        cluster="test-cluster",
        mig_manager=mock_mig_manager,
        max_scale_limit=10
    )


def test_init(cluster_manager):
    """
    Test the initialization of ClusterManager.

    Verifies that the ClusterManager is correctly initialized
    with the provided parameters.
    """
    assert cluster_manager.project_id == "test-project"
    assert cluster_manager.regions == ["region1", "region2"]
    assert cluster_manager.cluster == "test-cluster"
    assert cluster_manager.max_scale_limit == 10


def test_get_mig_name(cluster_manager):
    """
    Test the _get_mig_name method.

    Verifies that the method correctly generates MIG names
    based on the cluster and region.
    """
    mig_name = cluster_manager._get_mig_name("region1")
    assert mig_name == "pipeline-zen-jobs-test-cluster-region1"


@pytest.mark.asyncio
async def test_scale_all_regions(cluster_manager):
    """
    Test the scale_all_regions method.

    Verifies that the method correctly calls _scale_region
    for each region with the appropriate parameters.
    """
    cluster_manager._scale_region = AsyncMock()
    pending_jobs_count = 5
    running_jobs_count_per_region = {"region1": 2, "region2": 3}

    await cluster_manager.scale_all_regions(
        pending_jobs_count, running_jobs_count_per_region
    )

    cluster_manager._scale_region.assert_any_call("region1", 5, 2)
    cluster_manager._scale_region.assert_any_call("region2", 5, 3)
    assert cluster_manager._scale_region.call_count == 2


@pytest.mark.asyncio
async def test_scale_region(cluster_manager, mock_mig_manager):
    """
    Test the _scale_region method.

    Verifies that the method correctly calculates the new target size
    and calls the MigManager to scale the MIG accordingly.
    """
    mock_mig_manager.get_target_and_running_vm_counts.return_value = (5, 3)
    cluster_manager.mig_manager = mock_mig_manager

    await cluster_manager._scale_region("region1", 4, 2)

    mock_mig_manager.get_target_and_running_vm_counts.assert_called_once_with(
        "region1", "pipeline-zen-jobs-test-cluster-region1"
    )
    mock_mig_manager.scale_mig.assert_called_once_with(
        "region1", "pipeline-zen-jobs-test-cluster-region1", 6
    )


@pytest.mark.asyncio
async def test_get_status(cluster_manager, mock_mig_manager):
    """
    Test the get_status method.

    Verifies that the method correctly retrieves and returns
    the status for all regions in the cluster.
    """
    mock_mig_manager.get_target_and_running_vm_counts.side_effect = [
        (5, 3),
        (7, 6)
    ]
    cluster_manager.mig_manager = mock_mig_manager

    status = await cluster_manager.get_status()

    assert status == {
        "region1": (5, 3),
        "region2": (7, 6)
    }
    assert mock_mig_manager.get_target_and_running_vm_counts.call_count == 2
