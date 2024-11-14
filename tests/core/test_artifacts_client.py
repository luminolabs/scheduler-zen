import json
from unittest.mock import patch

import pytest

from app.core.artifacts_client import pull_artifacts_meta_from_gcs_task
from app.core.utils import PROVIDER_GCP, PROVIDER_LUM


@pytest.fixture
def mock_gcs_client():
    """Mock for GCS client functions."""
    with patch('app.core.artifacts_client.read_gcs_file') as mock:
        yield mock


@pytest.fixture
def sample_artifacts_meta():
    """Sample artifacts metadata."""
    return {
        "metrics": {
            "loss": 0.123,
            "accuracy": 0.987
        },
        "checkpoints": [
            "checkpoint_1.pt",
            "checkpoint_2.pt"
        ]
    }


@pytest.mark.asyncio
async def test_pull_artifacts_meta_gcp_job(mock_db, mock_gcs_client, sample_artifacts_meta):
    """Test pulling artifacts metadata for a GCP job."""
    # Setup
    job_id = "test-job-123"
    user_id = "test-user-456"
    region = "us-central1"

    # Mock database response for GCP job
    mock_db.get_job.return_value = {
        "provider": PROVIDER_GCP,
        "gcp": {"region": region}
    }

    # Mock GCS response
    mock_gcs_client.return_value = json.dumps(sample_artifacts_meta).encode('utf-8')

    # Execute
    result = await pull_artifacts_meta_from_gcs_task(job_id, user_id, mock_db)

    # Assert
    assert result is not None
    assert result[0] == job_id
    assert result[1] == user_id
    assert result[2] == sample_artifacts_meta

    # Verify GCS path construction
    mock_gcs_client.assert_called_once()
    call_args = mock_gcs_client.call_args[0]
    assert f"{user_id}/{job_id}/job-meta.json" in call_args


@pytest.mark.asyncio
async def test_pull_artifacts_meta_lum_job(mock_db, mock_gcs_client, sample_artifacts_meta, mock_config):
    """Test pulling artifacts metadata for a LUM job."""
    # Setup
    job_id = "test-job-123"
    user_id = "test-user-456"
    lum_id = 789

    # Mock database response for LUM job
    mock_db.get_job.return_value = {
        "provider": PROVIDER_LUM,
        "lum": {"lum_id": lum_id}
    }

    # Mock GCS response
    mock_gcs_client.return_value = json.dumps(sample_artifacts_meta).encode('utf-8')

    # Execute
    result = await pull_artifacts_meta_from_gcs_task(job_id, user_id, mock_db)

    # Assert
    assert result is not None
    assert result[0] == job_id
    assert result[1] == user_id
    assert result[2] == sample_artifacts_meta

    # Verify GCS path construction for LUM job
    mock_gcs_client.assert_called_once()
    call_args = mock_gcs_client.call_args[0]
    assert f"{mock_config.lum_account_address}/{lum_id}/job-meta.json" in call_args


@pytest.mark.asyncio
async def test_pull_artifacts_meta_no_job(mock_db, mock_gcs_client):
    """Test pulling artifacts metadata when job doesn't exist."""
    # Setup
    job_id = "nonexistent-job"
    user_id = "test-user"

    # Mock database to return None for nonexistent job
    mock_db.get_job.return_value = None

    # Execute & Assert
    with pytest.raises(TypeError):
        await pull_artifacts_meta_from_gcs_task(job_id, user_id, mock_db)

    # Verify GCS was not called
    mock_gcs_client.assert_not_called()


@pytest.mark.asyncio
async def test_pull_artifacts_meta_invalid_provider(mock_db, mock_gcs_client):
    """Test pulling artifacts metadata with invalid provider."""
    # Setup
    job_id = "test-job"
    user_id = "test-user"

    # Mock database to return job with invalid provider
    mock_db.get_job.return_value = {
        "provider": "INVALID_PROVIDER"
    }

    # Execute & Assert
    with pytest.raises(SystemError) as exc_info:
        await pull_artifacts_meta_from_gcs_task(job_id, user_id, mock_db)

    assert "Unknown provider" in str(exc_info.value)
    mock_gcs_client.assert_not_called()


@pytest.mark.asyncio
async def test_pull_artifacts_meta_gcs_not_found(mock_db, mock_gcs_client):
    """Test pulling artifacts metadata when GCS file is not found."""
    # Setup
    job_id = "test-job"
    user_id = "test-user"

    # Mock database response
    mock_db.get_job.return_value = {
        "provider": PROVIDER_GCP,
        "gcp": {"region": "us-central1"}
    }

    # Mock GCS to return None (file not found)
    mock_gcs_client.return_value = None

    # Execute
    result = await pull_artifacts_meta_from_gcs_task(job_id, user_id, mock_db)

    # Assert
    assert result is None
    mock_gcs_client.assert_called_once()
