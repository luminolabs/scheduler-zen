"""
This module contains unit tests for the ClusterOrchestrator class.

It uses pytest for testing and mocks the necessary dependencies
to isolate the ClusterOrchestrator behavior.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock

from app.cluster_orchestrator import ClusterOrchestrator
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
def cluster_orchestrator(mock_mig_manager):
    """
    Fixture to create a ClusterOrchestrator instance for testing.

    Args:
        mock_mig_manager (MagicMock): A mock MigManager instance.

    Returns:
        ClusterOrchestrator: An instance of ClusterOrchestrator for testing.
    """
    cluster_configs = {
        "cluster1": ["region1", "region2"],
        "cluster2": ["region3"]
    }
    max_scale_limits = {
        "cluster1": 10,
        "cluster2": 5
    }
    return ClusterOrchestrator(
        project_id="test-project",
        cluster_configs=cluster_configs,
        mig_manager=mock_mig_manager,
        max_scale_limits=max_scale_limits
    )


def test_init(cluster_orchestrator):
    """
    Test the initialization of ClusterOrchestrator.

    Verifies that the ClusterOrchestrator is correctly initialized
    with the provided parameters.
    """
    assert cluster_orchestrator.project_id == "test-project"
    assert len(cluster_orchestrator.cluster_managers) == 2
    assert "cluster1" in cluster_orchestrator.cluster_managers
    assert "cluster2" in cluster_orchestrator.cluster_managers


@pytest.mark.asyncio
async def test_update_status(cluster_orchestrator):
    """
    Test the update_status method.

    Verifies that the method correctly aggregates status information
    from all cluster managers.
    """
    # Mock the get_status method of each cluster manager
    cluster_orchestrator.cluster_managers["cluster1"].get_status = AsyncMock(
        return_value={
            "region1": (5, 3),
            "region2": (7, 6)
        }
    )
    cluster_orchestrator.cluster_managers["cluster2"].get_status = AsyncMock(
        return_value={
            "region3": (4, 2)
        }
    )

    status = await cluster_orchestrator.update_status()

    assert status == {
        "cluster1": {
            "region1": (5, 3),
            "region2": (7, 6)
        },
        "cluster2": {
            "region3": (4, 2)
        }
    }


@pytest.mark.asyncio
async def test_scale_clusters(cluster_orchestrator):
    """
    Test the scale_clusters method.

    Verifies that the method correctly calls scale_all_regions
    on each cluster manager with the appropriate parameters.
    """
    # Mock the scale_all_regions method of each cluster manager
    cluster_orchestrator.cluster_managers["cluster1"].scale_all_regions = AsyncMock()
    cluster_orchestrator.cluster_managers["cluster2"].scale_all_regions = AsyncMock()

    pending_jobs = {
        "cluster1": 5,
        "cluster2": 3
    }
    running_jobs = {
        "cluster1": {
            "region1": 2,
            "region2": 3
        },
        "cluster2": {
            "region3": 1
        }
    }

    await cluster_orchestrator.scale_clusters(pending_jobs, running_jobs)

    cluster_orchestrator.cluster_managers["cluster1"].scale_all_regions.assert_called_once_with(
        5, {"region1": 2, "region2": 3}
    )
    cluster_orchestrator.cluster_managers["cluster2"].scale_all_regions.assert_called_once_with(
        3, {"region3": 1}
    )


@pytest.mark.asyncio
async def test_scale_clusters_with_missing_data(cluster_orchestrator):
    """
    Test the scale_clusters method with missing data for some clusters.

    Verifies that the method handles cases where some clusters have no
    pending or running jobs data.
    """
    # Mock the scale_all_regions method of each cluster manager
    cluster_orchestrator.cluster_managers["cluster1"].scale_all_regions = AsyncMock()
    cluster_orchestrator.cluster_managers["cluster2"].scale_all_regions = AsyncMock()

    pending_jobs = {
        "cluster1": 5
    }
    running_jobs = {
        "cluster2": {
            "region3": 1
        }
    }

    await cluster_orchestrator.scale_clusters(pending_jobs, running_jobs)

    cluster_orchestrator.cluster_managers["cluster1"].scale_all_regions.assert_called_once_with(
        5, {}
    )
    cluster_orchestrator.cluster_managers["cluster2"].scale_all_regions.assert_called_once_with(
        0, {"region3": 1}
    )
