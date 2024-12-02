import pytest

from app.main import app


@pytest.mark.asyncio
async def test_create_job_gcp_success(test_client, mock_db, mock_gcp_scheduler, sample_job_data):
    """Test successful GCP job creation."""
    # Set up mock cluster check
    mock_gcp_scheduler.cluster_orchestrator.cluster_exists.return_value = True
    # Set up mock job check
    mock_db.get_job.return_value = None

    # Make the request
    response = test_client.post("/jobs/gcp", json=sample_job_data)

    # Verify response
    assert response.status_code == 200
    assert response.json() == {"job_id": sample_job_data["job_id"], "status": "new"}

    # Verify scheduler was called
    mock_gcp_scheduler.add_job.assert_awaited_once_with(sample_job_data)


@pytest.mark.asyncio
async def test_create_job_gcp_duplicate(test_client, mock_db, mock_gcp_scheduler, sample_job_data):
    """Test creating a GCP job with duplicate ID."""
    # Set up mock cluster check
    mock_gcp_scheduler.cluster_orchestrator.cluster_exists.return_value = True
    # Set up mock job check to simulate existing job
    mock_db.get_job.return_value = {"job_id": sample_job_data["job_id"]}

    # Make the request
    response = test_client.post("/jobs/gcp", json=sample_job_data)

    # Verify response
    assert response.status_code == 422
    assert "already exists" in response.json()["detail"]

    # Verify scheduler was not called
    mock_gcp_scheduler.add_job.assert_not_awaited()


@pytest.mark.asyncio
async def test_stop_job_success(test_client, mock_db, mock_gcp_scheduler):
    """Test successful job stopping."""
    job_id = "test-job-123"
    user_id = "test-user-456"

    # Set up mock db to identify as GCP job
    mock_db.is_gcp_job.return_value = True
    # Set up mock scheduler to succeed
    mock_gcp_scheduler.stop_job.return_value = True

    # Make the request
    response = test_client.post(f"/jobs/gcp/stop/{job_id}/{user_id}")

    # Verify response
    assert response.status_code == 200
    assert response.json() == {"status": "stopping"}

    # Verify scheduler was called
    mock_gcp_scheduler.stop_job.assert_awaited_once_with(job_id, user_id)


@pytest.mark.asyncio
async def test_get_jobs_by_user_and_ids_success(test_client, mock_db):
    """Test successful retrieval of jobs by user and IDs."""
    test_jobs = [
        {"job_id": "job1", "status": "RUNNING"},
        {"job_id": "job2", "status": "COMPLETED"}
    ]
    request_data = {
        "user_id": "test-user",
        "job_ids": ["job1", "job2"]
    }

    # Set up mock db response
    mock_db.get_jobs_by_user_and_ids.return_value = test_jobs

    # Make the request
    response = test_client.post("/jobs/get_by_user_and_ids", json=request_data)

    # Verify response
    assert response.status_code == 200
    assert response.json() == test_jobs

    # Verify db was called correctly
    mock_db.get_jobs_by_user_and_ids.assert_awaited_once_with(
        request_data["user_id"], request_data["job_ids"]
    )


@pytest.mark.asyncio
async def test_create_job_lum_success(test_client, mock_db, mock_lum_scheduler, sample_job_data):
    """Test successful LUM job creation."""
    # Set up mock job check
    mock_db.get_job.return_value = None

    # Prepare request data (remove GCP specific fields)
    lum_job_data = {k: v for k, v in sample_job_data.items()
                    if k not in ['gpu_type', 'num_gpus', 'keep_alive', 'cluster']}

    # Make the request
    response = test_client.post("/jobs/lum", json=lum_job_data)

    # Verify response
    assert response.status_code == 200
    assert response.json() == {"job_id": lum_job_data["job_id"], "status": "new"}

    # Verify scheduler was called
    mock_lum_scheduler.add_job.assert_awaited_once_with(lum_job_data)


@pytest.mark.asyncio
async def test_lifespan(mock_db, mock_gcp_scheduler, mock_lum_scheduler, mock_task_scheduler):
    """Test application lifespan (startup and shutdown)."""
    # TODO: Implement this test
    pass
