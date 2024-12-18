from unittest.mock import AsyncMock, patch, MagicMock

import pytest
from aiohttp import ClientResponseError

from app.core.gcp_client import read_gcs_file, get_results_bucket


@pytest.mark.asyncio
async def test_read_gcs_file_success():
    """Test successful file read from GCS."""
    # Mock data
    expected_content = b"test content"
    bucket_name = "test-bucket"
    object_name = "test/object.txt"

    # Create mock storage client
    mock_storage = AsyncMock()
    mock_storage.download.return_value = expected_content

    # Test the function
    with patch('app.core.gcp_client.Storage', return_value=mock_storage):
        result = await read_gcs_file(bucket_name, object_name)

        # Verify results
        assert result == expected_content
        mock_storage.download.assert_called_once_with(bucket_name, object_name)
        mock_storage.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_read_gcs_file_not_found():
    """Test handling of 404 error when reading from GCS."""
    # Mock data
    bucket_name = "test-bucket"
    object_name = "test/object.txt"

    # Create mock storage client that raises 404
    mock_storage = AsyncMock()
    mock_storage.download.side_effect = ClientResponseError(
        request_info=MagicMock(),
        history=(),
        status=404
    )

    # Test the function with ignore_404=True
    with patch('app.core.gcp_client.Storage', return_value=mock_storage):
        result = await read_gcs_file(bucket_name, object_name, ignore_404=True)

        # Verify results
        assert result is None
        mock_storage.download.assert_called_once_with(bucket_name, object_name)
        mock_storage.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_read_gcs_file_not_found_raise():
    """Test that 404 error is raised when ignore_404=False."""
    # Mock data
    bucket_name = "test-bucket"
    object_name = "test/object.txt"

    # Create mock storage client that raises 404
    mock_storage = AsyncMock()
    error_404 = ClientResponseError(
        request_info=MagicMock(),
        history=(),
        status=404
    )
    mock_storage.download.side_effect = error_404

    # Test the function with ignore_404=False
    with patch('app.core.gcp_client.Storage', return_value=mock_storage):
        with pytest.raises(ClientResponseError) as exc_info:
            await read_gcs_file(bucket_name, object_name, ignore_404=False)

        # Verify the error was raised and cleanup occurred
        assert exc_info.value.status == 404
        mock_storage.download.assert_called_once_with(bucket_name, object_name)
        mock_storage.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_read_gcs_file_other_error():
    """Test handling of non-404 errors when reading from GCS."""
    # Mock data
    bucket_name = "test-bucket"
    object_name = "test/object.txt"

    # Create mock storage client that raises a general error
    mock_storage = AsyncMock()
    error_500 = ClientResponseError(
        request_info=MagicMock(),
        history=(),
        status=500
    )
    mock_storage.download.side_effect = error_500

    # Test the function
    with patch('app.core.gcp_client.Storage', return_value=mock_storage):
        with pytest.raises(ClientResponseError) as exc_info:
            await read_gcs_file(bucket_name, object_name)

        # Verify the error was raised and cleanup occurred
        assert exc_info.value.status == 500
        mock_storage.download.assert_called_once_with(bucket_name, object_name)
        mock_storage.close.assert_awaited_once()


def test_get_results_bucket_local_env(mock_config):
    """Test get_results_bucket when in local environment."""
    # Set up mock config for local environment
    mock_config.env_name = mock_config.local_env_name
    mock_config.local_env_name = "local"

    # Test the function
    result = get_results_bucket("us-central1")

    # Verify result
    assert result == "lum-test-pipeline-zen-jobs-local"


def test_get_results_bucket_us_region():
    """Test get_results_bucket with US region."""
    result = get_results_bucket("us-central1")
    assert result == "lum-test-pipeline-zen-jobs-us"


def test_get_results_bucket_europe_region():
    """Test get_results_bucket with Europe region."""
    result = get_results_bucket("europe-west1")
    assert result == "lum-test-pipeline-zen-jobs-europe"


def test_get_results_bucket_asia_region():
    """Test get_results_bucket with Asia region."""
    result = get_results_bucket("asia-east1")
    assert result == "lum-test-pipeline-zen-jobs-asia"


def test_get_results_bucket_me_region():
    """Test get_results_bucket with the Middle East region."""
    result = get_results_bucket("me-west1")
    assert result == "lum-test-pipeline-zen-jobs-me-west1"


def test_get_results_bucket_default_region():
    """Test get_results_bucket with default region."""
    result = get_results_bucket()
    assert result == "lum-test-pipeline-zen-jobs-us"
