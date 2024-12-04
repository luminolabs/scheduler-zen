import os
# Set the application environment to `test`
os.environ["SZ_ENV"] = "test"

from typing import AsyncGenerator, Generator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture
def mock_config(monkeypatch):
    """Create a mock configuration."""
    mock_cfg = MagicMock()
    mock_cfg.gcp_project = "test-project"
    mock_cfg.job_start_topic = "test-start-topic"
    mock_cfg.job_stop_topic = "test-stop-topic"
    mock_cfg.heartbeat_subscription = "test-heartbeat-sub"
    mock_cfg.use_fake_mig_client = False
    mock_cfg.mig_api_rate_limit = 100
    mock_cfg.env_name = "test"
    mock_cfg.local_env_name = "local"
    mock_cfg.log_level = "INFO"
    mock_cfg.log_stdout = True
    mock_cfg.log_file = "./.logs/test.log"
    mock_cfg.recently_completed_job_threshold_minutes = 10
    mock_cfg.lum_account_address = "0x123"

    monkeypatch.setattr("app.core.utils.config", mock_cfg)
    monkeypatch.setattr("app.core.database.config", mock_cfg)
    monkeypatch.setattr("app.core.artifacts_client.config", mock_cfg)
    monkeypatch.setattr("app.gcp.scheduler.config", mock_cfg)
    return mock_cfg


@pytest.fixture
def sample_job_data():
    """Sample job data for tests."""
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


@pytest.fixture
def sample_job():
    """Sample job for tests."""
    return {
        "job_id": "test-job-123",
        "user_id": "test-user-456",
        "workflow": "test-workflow",
        "args": {"param1": "value1", "param2": "value2"},
        "status": "NEW",
        "provider": "GCP",
        "gcp": {
            "cluster": "4xa100-40gb",
            "keep_alive": False,
            "vm_name": "test-vm-789",
            "region": "us-central1"
        }
    }

@pytest.fixture
async def mock_db() -> AsyncGenerator[MagicMock, None]:
    """Create a mock database."""
    async_mock = AsyncMock()
    async_mock.connect = AsyncMock()
    async_mock.close = AsyncMock()
    async_mock.transaction = AsyncMock()
    yield async_mock

@pytest.fixture
def mock_gcp_scheduler():
    """Create a mock GCP scheduler."""
    scheduler = AsyncMock()
    scheduler.start = AsyncMock()
    scheduler.stop = AsyncMock()
    scheduler.add_job = AsyncMock()
    scheduler.stop_job = AsyncMock()
    scheduler.cluster_orchestrator = AsyncMock()
    scheduler.cluster_orchestrator.cluster_exists = MagicMock()
    return scheduler

@pytest.fixture
def mock_lum_scheduler():
    """Create a mock LUM scheduler."""
    scheduler = AsyncMock()
    scheduler.start = AsyncMock()
    scheduler.stop = AsyncMock()
    scheduler.add_job = AsyncMock()
    return scheduler

@pytest.fixture
def mock_task_scheduler():
    """Create a mock APScheduler."""
    scheduler = MagicMock()
    scheduler.add_job = MagicMock()
    scheduler.start = MagicMock()
    scheduler.shutdown = MagicMock()
    return scheduler

@pytest.fixture
def test_client(mock_db, mock_gcp_scheduler, mock_lum_scheduler, mock_task_scheduler) -> Generator:
    """Create a FastAPI test client with mocked dependencies."""
    # Apply all mocks
    with patch('app.main.db', mock_db), \
            patch('app.main.gcp_scheduler', mock_gcp_scheduler), \
            patch('app.main.lum_scheduler', mock_lum_scheduler), \
            patch('app.main.background_task_scheduler', mock_task_scheduler), \
            patch.dict(os.environ, {'SZ_ENV': 'test'}):

        with TestClient(app) as client:
            yield client