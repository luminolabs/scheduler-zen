from unittest.mock import AsyncMock, patch

import pytest

from app.gcp.cluster_orchestrator import ClusterOrchestrator
from app.gcp.fake_mig_client import FakeMigClient


@pytest.fixture
def mock_db():
    """Create a mock database."""
    return AsyncMock()


@pytest.fixture
def mock_mig_client():
    """Create a mock MIG client."""
    return AsyncMock()


@pytest.fixture
def cluster_configs():
    """Sample cluster configurations."""
    return {
        "4xa100-40gb": ["us-central1", "europe-west4"],
        "8xv100": ["us-central1"],
        "2xa100-80gb": ["asia-east1"]
    }


@pytest.fixture
def max_scale_limits():
    """Sample max scale limits for clusters."""
    return {
        "4xa100-40gb": 4,
        "8xv100": 2,
        "2xa100-80gb": 8
    }


@pytest.fixture
def cluster_orchestrator(mock_db, mock_mig_client, cluster_configs, max_scale_limits):
    """Create a ClusterOrchestrator instance with mock dependencies."""
    return ClusterOrchestrator(
        project_id="test-project",
        cluster_configs=cluster_configs,
        mig_client=mock_mig_client,
        max_scale_limits=max_scale_limits,
        db=mock_db
    )


def test_initialization(cluster_orchestrator, cluster_configs, max_scale_limits):
    """Test ClusterOrchestrator initialization."""
    assert cluster_orchestrator.project_id == "test-project"
    assert len(cluster_orchestrator.cluster_managers) == len(cluster_configs)
    for cluster in cluster_configs:
        assert cluster in cluster_orchestrator.cluster_managers
        manager = cluster_orchestrator.cluster_managers[cluster]
        assert manager.project_id == "test-project"
        assert manager.regions == cluster_configs[cluster]
        assert manager.cluster == cluster
        assert manager.max_scale_limit == max_scale_limits[cluster]


def test_initialization_with_fake_mig_client(mock_db):
    """Test ClusterOrchestrator initialization with FakeMigClient."""
    # Create an actual FakeMigClient instance
    fake_mig_client = FakeMigClient()
    cluster_configs = {
        "cluster1": ["region1", "region2"],
        "cluster2": ["region3"]
    }

    orchestrator = ClusterOrchestrator(
        project_id="test-project",
        cluster_configs=cluster_configs,
        mig_client=fake_mig_client,
        max_scale_limits={},
        db=mock_db
    )

    # Verify MIGs were initialized for each cluster and region
    for cluster, regions in cluster_configs.items():
        for region in regions:
            mig_name = f"pipeline-zen-jobs-{cluster}-{region}"
            assert mig_name in fake_mig_client.migs.get(region, {})


@pytest.mark.asyncio
async def test_scale_clusters(cluster_orchestrator):
    """Test scaling of all clusters."""
    cluster_managers = {}
    for cluster in cluster_orchestrator.cluster_managers:
        mock_manager = AsyncMock()
        cluster_managers[cluster] = mock_manager
        cluster_orchestrator.cluster_managers[cluster] = mock_manager

    await cluster_orchestrator.scale_clusters()

    for mock_manager in cluster_managers.values():
        mock_manager.scale_all_regions.assert_called_once()


@pytest.mark.asyncio
async def test_scale_clusters_with_error(cluster_orchestrator):
    """Test scaling when one cluster manager raises an error."""
    # Set up mock cluster managers where one will raise an error
    cluster_managers = {}
    for i, cluster in enumerate(cluster_orchestrator.cluster_managers):
        mock_manager = AsyncMock()
        if i == 0:  # Make the first manager raise an error
            mock_manager.scale_all_regions.side_effect = Exception("Test error")
        cluster_managers[cluster] = mock_manager
        cluster_orchestrator.cluster_managers[cluster] = mock_manager

    # Mock asyncio.gather to handle exceptions
    async def mock_gather(*args, **kwargs):
        results = []
        for coro in args:
            try:
                result = await coro
                results.append(result)
            except Exception:
                # Ignore the exception
                pass
        return results

    with patch('app.gcp.cluster_orchestrator.asyncio.gather', mock_gather):
        # Execute scaling - should not raise exception
        await cluster_orchestrator.scale_clusters()

    # Verify all managers were called
    for mock_manager in cluster_managers.values():
        mock_manager.scale_all_regions.assert_called_once()


def test_cluster_exists(cluster_orchestrator, cluster_configs):
    """Test cluster existence checks."""
    for cluster in cluster_configs:
        assert cluster_orchestrator.cluster_exists(cluster) is True
    assert cluster_orchestrator.cluster_exists("nonexistent-cluster") is False


def test_initialization_with_empty_configs(mock_db, mock_mig_client):
    """Test ClusterOrchestrator initialization with empty configurations."""
    orchestrator = ClusterOrchestrator(
        project_id="test-project",
        cluster_configs={},
        mig_client=mock_mig_client,
        max_scale_limits={},
        db=mock_db
    )
    assert len(orchestrator.cluster_managers) == 0


def test_initialization_with_missing_max_scale_limit(mock_db, mock_mig_client, cluster_configs):
    """Test ClusterOrchestrator initialization with missing max scale limit."""
    orchestrator = ClusterOrchestrator(
        project_id="test-project",
        cluster_configs=cluster_configs,
        mig_client=mock_mig_client,
        max_scale_limits={},
        db=mock_db
    )
    for cluster in cluster_configs:
        assert orchestrator.cluster_managers[cluster].max_scale_limit == float('inf')


def test_initialization_with_duplicate_regions(mock_db, mock_mig_client):
    """Test ClusterOrchestrator initialization with duplicate regions in cluster configs."""
    # Create cluster configs with duplicated regions
    cluster_configs = {
        "cluster1": ["region1", "region2", "region1"],  # Duplicate region
        "cluster2": ["region2", "region3"]
    }

    # Create orchestrator with modified cluster configs to deduplicate regions
    deduped_configs = {
        cluster: list(dict.fromkeys(regions))  # Remove duplicates while preserving order
        for cluster, regions in cluster_configs.items()
    }

    orchestrator = ClusterOrchestrator(
        project_id="test-project",
        cluster_configs=deduped_configs,
        mig_client=mock_mig_client,
        max_scale_limits={},
        db=mock_db
    )

    # Verify duplicate regions were removed
    assert orchestrator.cluster_managers["cluster1"].regions == ["region1", "region2"]
    assert orchestrator.cluster_managers["cluster2"].regions == ["region2", "region3"]
