import pytest
from pydantic import ValidationError

from app.core.schemas import (
    CreateJobRequestBase,
    CreateJobRequestGCP,
    CreateJobRequestLUM,
    ListUserJobsRequest
)


def test_create_job_request_base_valid():
    """Test CreateJobRequestBase with valid data."""
    data = {
        "job_id": "test-123",
        "workflow": "test-workflow",
        "args": {"param1": "value1"}
    }
    request = CreateJobRequestBase(**data)
    assert request.job_id == "test-123"
    assert request.user_id == "0"  # Default value
    assert request.workflow == "test-workflow"
    assert request.args == {"param1": "value1"}


def test_create_job_request_base_custom_user():
    """Test CreateJobRequestBase with custom user_id."""
    data = {
        "job_id": "test-123",
        "user_id": "user-456",
        "workflow": "test-workflow",
        "args": {"param1": "value1"}
    }
    request = CreateJobRequestBase(**data)
    assert request.user_id == "user-456"


def test_create_job_request_base_missing_required():
    """Test CreateJobRequestBase with missing required fields."""
    data = {
        "job_id": "test-123"
        # Missing workflow and args
    }
    with pytest.raises(ValidationError) as exc_info:
        CreateJobRequestBase(**data)
    errors = exc_info.value.errors()
    assert len(errors) == 2
    assert any(e["msg"] == "Field required" and e["loc"][0] == "workflow" for e in errors)
    assert any(e["msg"] == "Field required" and e["loc"][0] == "args" for e in errors)


def test_create_job_request_gcp_valid():
    """Test CreateJobRequestGCP with valid data."""
    data = {
        "job_id": "test-123",
        "workflow": "test-workflow",
        "args": {"param1": "value1"},
        "gpu_type": "a100-40gb",
        "num_gpus": 4,
        "keep_alive": True
    }
    request = CreateJobRequestGCP(**data)
    assert request.job_id == "test-123"
    assert request.gpu_type == "a100-40gb"
    assert request.num_gpus == 4
    assert request.keep_alive is True
    assert request.cluster == "4xa100-40gb"


def test_create_job_request_gcp_default_values():
    """Test CreateJobRequestGCP with default values."""
    data = {
        "job_id": "test-123",
        "workflow": "test-workflow",
        "args": {"param1": "value1"},
        "gpu_type": "a100-40gb"
    }
    request = CreateJobRequestGCP(**data)
    assert request.num_gpus == 1  # Default value
    assert request.keep_alive is False  # Default value
    assert request.cluster == "1xa100-40gb"


def test_create_job_request_gcp_missing_required():
    """Test CreateJobRequestGCP with missing required fields."""
    data = {
        "job_id": "test-123",
        "workflow": "test-workflow",
        "args": {"param1": "value1"}
        # Missing gpu_type
    }
    with pytest.raises(ValidationError) as exc_info:
        CreateJobRequestGCP(**data)
    errors = exc_info.value.errors()
    assert len(errors) == 1
    assert errors[0]["msg"] == "Field required"
    assert errors[0]["loc"][0] == "gpu_type"


def test_create_job_request_lum_valid():
    """Test CreateJobRequestLUM with valid data."""
    data = {
        "job_id": "test-123",
        "workflow": "test-workflow",
        "args": {"param1": "value1"}
    }
    request = CreateJobRequestLUM(**data)
    assert request.job_id == "test-123"
    assert request.workflow == "test-workflow"
    assert request.args == {"param1": "value1"}


def test_list_user_jobs_request_valid():
    """Test ListUserJobsRequest with valid data."""
    data = {
        "user_id": "user-123",
        "job_ids": ["job-1", "job-2", "job-3"]
    }
    request = ListUserJobsRequest(**data)
    assert request.user_id == "user-123"
    assert request.job_ids == ["job-1", "job-2", "job-3"]


def test_list_user_jobs_request_missing_required():
    """Test ListUserJobsRequest with missing required fields."""
    data = {
        "user_id": "user-123"
        # Missing job_ids
    }
    with pytest.raises(ValidationError) as exc_info:
        ListUserJobsRequest(**data)
    errors = exc_info.value.errors()
    assert len(errors) == 1
    assert errors[0]["msg"] == "Field required"
    assert errors[0]["loc"][0] == "job_ids"


def test_list_user_jobs_request_empty_job_ids():
    """Test ListUserJobsRequest with empty job_ids list."""
    data = {
        "user_id": "user-123",
        "job_ids": []
    }
    request = ListUserJobsRequest(**data)
    assert request.job_ids == []
