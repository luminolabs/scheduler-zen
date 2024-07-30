"""
This module contains unit tests for the API endpoints of the Pipeline Zen Jobs Scheduler.

It uses pytest for testing and mocks the scheduler to isolate API behavior.
"""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch

from app.api import app

# Create a test client for our FastAPI app
client = TestClient(app)


@pytest.fixture
def mock_scheduler():
    """
    Fixture to mock the scheduler object used in the API.

    This allows us to control the behavior of the scheduler in our tests.
    """
    with patch('app.api.scheduler') as mock:
        yield mock


def test_create_job(mock_scheduler):
    """
    Test successful job creation through the API.

    This test verifies that a POST request to /jobs with valid data
    results in a successful job creation.
    """
    # Mock the add_job method to return a test job ID
    mock_scheduler.add_job = AsyncMock(return_value="test_job_id")

    # Make a POST request to create a job
    response = client.post(
        "/jobs",
        json={
            "job_id": "test_job",
            "workflow": "test_workflow",
            "args": {"arg1": "value1"},
            "keep_alive": False,
            "cluster": "test_cluster"
        }
    )

    # Assert that the response is as expected
    assert response.status_code == 200
    assert response.json() == {"job_id": "test_job_id"}

    # Verify that the add_job method was called
    mock_scheduler.add_job.assert_called_once()


def test_create_job_error(mock_scheduler):
    """
    Test error handling when job creation fails.

    This test verifies that when the scheduler's add_job method raises an exception,
    the API returns an appropriate error response.
    """
    # Mock add_job to raise an exception
    mock_scheduler.add_job = AsyncMock(side_effect=Exception("Error adding job"))

    # Make a POST request to create a job
    response = client.post(
        "/jobs",
        json={
            "job_id": "test_job",
            "workflow": "test_workflow",
            "args": {"arg1": "value1"},
            "keep_alive": False,
            "cluster": "test_cluster"
        }
    )

    # Assert that we get a 500 error with the correct message
    assert response.status_code == 500
    assert response.json() == {"detail": "Error adding job"}


def test_stop_job(mock_scheduler):
    """
    Test successful job stopping through the API.

    This test verifies that a POST request to /jobs/{job_id}/stop
    results in the job being stopped successfully.
    """
    # Mock stop_job to return True (successful stop)
    mock_scheduler.stop_job = AsyncMock(return_value=True)

    # Make a POST request to stop a job
    response = client.post("/jobs/test_job_id/stop")

    # Assert that the response is as expected
    assert response.status_code == 200
    assert response.json() == {"status": "stopped"}

    # Verify that stop_job was called with the correct job ID
    mock_scheduler.stop_job.assert_called_once_with("test_job_id")


def test_stop_job_not_found(mock_scheduler):
    """
    Test error handling when stopping a non-existent job.

    This test verifies that when trying to stop a job that doesn't exist,
    the API returns an appropriate error response.
    """
    # Mock stop_job to return False (job not found)
    mock_scheduler.stop_job = AsyncMock(return_value=False)

    # Make a POST request to stop a non-existent job
    response = client.post("/jobs/non_existent_job/stop")

    # Assert that we get a 404 error with the correct message
    assert response.status_code == 404
    assert response.json() == {"detail": "Job not found or not running"}


def test_get_status(mock_scheduler):
    """
    Test retrieval of scheduler status through the API.

    This test verifies that a GET request to /status returns the correct
    status information from the scheduler.
    """
    # Mock status data
    mock_status = {
        "cluster_status": {"cluster1": {"region1": [5, 3]}},  # Changed from tuple to list
        "running_jobs": 3,
        "pending_jobs": 2
    }
    # Mock get_status to return our test status
    mock_scheduler.get_status = AsyncMock(return_value=mock_status)

    # Make a GET request to the status endpoint
    response = client.get("/status")

    # Assert that the response is as expected
    assert response.status_code == 200
    assert response.json() == mock_status

    # Verify that get_status was called
    mock_scheduler.get_status.assert_called_once()
