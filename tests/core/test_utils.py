import json
from datetime import datetime, timezone

import pytest

from app.core.utils import (
    is_new_job_status_valid,
    format_time,
    is_local_env,
    recursive_json_decode,
    JOB_STATUS_NEW,
    JOB_STATUS_WAIT_FOR_VM,
    JOB_STATUS_FOUND_VM,
    JOB_STATUS_RUNNING
)


def test_is_new_job_status_valid_same_status():
    """Test that same status is considered invalid (must progress forward)."""
    assert is_new_job_status_valid(JOB_STATUS_NEW, JOB_STATUS_NEW) is False
    assert is_new_job_status_valid(JOB_STATUS_RUNNING, JOB_STATUS_RUNNING) is False


def test_is_new_job_status_valid_forward_progression():
    """Test that moving forward in status is valid."""
    assert is_new_job_status_valid(JOB_STATUS_NEW, JOB_STATUS_WAIT_FOR_VM) is True
    assert is_new_job_status_valid(JOB_STATUS_WAIT_FOR_VM, JOB_STATUS_FOUND_VM) is True


def test_is_new_job_status_valid_backward_progression():
    """Test that moving backward in status is invalid."""
    assert is_new_job_status_valid(JOB_STATUS_WAIT_FOR_VM, JOB_STATUS_NEW) is False
    assert is_new_job_status_valid(JOB_STATUS_FOUND_VM, JOB_STATUS_WAIT_FOR_VM) is False


def test_is_new_job_status_valid_found_vm_to_running():
    """Test that moving from FOUND_VM to RUNNING is invalid."""
    assert is_new_job_status_valid(JOB_STATUS_FOUND_VM, JOB_STATUS_RUNNING) is False


def test_is_new_job_status_valid_invalid_status():
    """Test handling of invalid status values."""
    with pytest.raises(ValueError):
        is_new_job_status_valid("INVALID_STATUS", JOB_STATUS_NEW)
    with pytest.raises(ValueError):
        is_new_job_status_valid(JOB_STATUS_NEW, "INVALID_STATUS")


def test_format_time():
    """Test datetime formatting."""
    # Test with a specific datetime
    dt = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    assert format_time(dt) == "2024-01-01T12:00:00Z"

    # Test with None
    assert format_time(None) is None


def test_is_local_env(mock_config):
    """Test local environment detection."""
    # Test local environment
    mock_config.env_name = mock_config.local_env_name
    assert is_local_env() is True

    # Test non-local environment
    mock_config.env_name = "production"
    assert is_local_env() is False


def test_recursive_json_decode():
    """Test recursive JSON decoding."""
    # Test nested JSON string
    nested_json = json.dumps({
        "str_field": json.dumps({"inner": "value"}),
        "list_field": json.dumps([1, 2, 3]),
        "normal_field": "not_json"
    })

    expected = {
        "str_field": {"inner": "value"},
        "list_field": [1, 2, 3],
        "normal_field": "not_json"
    }

    assert recursive_json_decode(nested_json) == expected

    # Test with non-JSON string
    assert recursive_json_decode("not_json") == "not_json"

    # Test with already decoded dict
    decoded = {"key": "value"}
    assert recursive_json_decode(decoded) == decoded

    # Test with list containing JSON strings
    json_list = [json.dumps({"a": 1}), json.dumps({"b": 2})]
    expected_list = [{"a": 1}, {"b": 2}]
    assert recursive_json_decode(json_list) == expected_list

    # Test with None
    assert recursive_json_decode(None) is None


def test_recursive_json_decode_invalid_json():
    """Test recursive JSON decoding with invalid JSON strings."""
    # Invalid JSON should be returned as-is
    invalid_json = "{not valid json}"
    assert recursive_json_decode(invalid_json) == invalid_json

    # Test with nested invalid JSON
    nested_invalid = {
        "valid": json.dumps({"a": 1}),
        "invalid": "{not valid json}"
    }
    expected = {
        "valid": {"a": 1},
        "invalid": "{not valid json}"
    }
    assert recursive_json_decode(nested_invalid) == expected
