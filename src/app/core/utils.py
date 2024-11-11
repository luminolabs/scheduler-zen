import json
import logging
import os
import sys
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from typing import Any

from app.core.config_manager import config

# Compute providers
PROVIDER_GCP = 'GCP'
PROVIDER_LUM = 'LUM'

# VM statuses
INSTANCE_STATUS_RUNNING = 'RUNNING'

# Job statuses
JOB_STATUS_NEW = 'NEW'
JOB_STATUS_RUNNING = 'RUNNING'
JOB_STATUS_STOPPING = 'STOPPING'
JOB_STATUS_STOPPED = 'STOPPED'
JOB_STATUS_COMPLETED = 'COMPLETED'
JOB_STATUS_FAILED = 'FAILED'

# GCP specific job statuses
JOB_STATUS_WAIT_FOR_VM = 'WAIT_FOR_VM'
JOB_STATUS_FOUND_VM = 'FOUND_VM'
JOB_STATUS_DETACHED_VM = 'DETACHED_VM'

# Ordered list of job statuses
HEARTBEAT_ORDERED_JOB_STATUSES = [
    # Common statuses
    JOB_STATUS_NEW,
    # GCP specific statuses
    JOB_STATUS_WAIT_FOR_VM, JOB_STATUS_FOUND_VM, JOB_STATUS_DETACHED_VM,
    JOB_STATUS_STOPPING, JOB_STATUS_STOPPED,
    # Common statuses
    JOB_STATUS_RUNNING,
    JOB_STATUS_COMPLETED, JOB_STATUS_FAILED]

# Terminal job statuses
TERMINAL_JOB_STATUSES = [JOB_STATUS_STOPPED, JOB_STATUS_COMPLETED, JOB_STATUS_FAILED]
# Non-terminal job statuses (computed)
NON_TERMINAL_JOB_STATUSES = [status for status in HEARTBEAT_ORDERED_JOB_STATUSES
                             if status not in TERMINAL_JOB_STATUSES]


def is_new_job_status_valid(old_status: str, new_status: str) -> bool:
    """
    Checks if the new job status is valid. We allow same old and new status so that we can process heartbeats.

    Args:
        old_status: old job status
        new_status: new job status
    Returns:
        bool: True if the new job status is valid, False otherwise
    """
    # Don't go to `RUNNING` from `FOUND_VM`;
    # the `FOUND_VM` status is what triggers the VM detachment, and we don't want to miss it
    if new_status == JOB_STATUS_RUNNING and old_status == JOB_STATUS_FOUND_VM:
        return False
    return HEARTBEAT_ORDERED_JOB_STATUSES.index(new_status) > HEARTBEAT_ORDERED_JOB_STATUSES.index(old_status)


def setup_logger(name: str,
                 add_stdout: bool = True,
                 log_level: int = logging.INFO) -> logging.Logger:
    """
    Sets up a logger

    :param name: The name of the logger
    :param add_stdout: Whether to add the stdout logger or not
    :param log_level: The log level to log at, ex. `logging.INFO`
    :return: A logger instance
    """
    log_level = log_level or config.log_level
    log_format = logging.Formatter(f'{config.env_name} - %(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Log to stdout and to file
    os.makedirs(os.path.dirname(config.log_file), exist_ok=True)
    stdout_handler = logging.StreamHandler(sys.stdout)
    file_handler = TimedRotatingFileHandler(config.log_file, when="midnight", interval=1, backupCount=2)
    file_handler.suffix = "%Y%m%d"

    # Set the logger format
    stdout_handler.setFormatter(log_format)
    file_handler.setFormatter(log_format)

    # Configure logger
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    if add_stdout and config.log_stdout:
        logger.addHandler(stdout_handler)
    logger.addHandler(file_handler)
    return logger


def format_time(datetime_obj: datetime | None) -> str | None:
    """
    Format a datetime object to a string.
    """
    if not datetime_obj:
        return None
    return datetime_obj.strftime("%Y-%m-%dT%H:%M:%SZ")


def is_local_env() -> bool:
    """
    :return: True if the environment is local, False otherwise
    """
    return config.env_name == config.local_env_name


def recursive_json_decode(data: Any) -> Any:
    """
    Recursively decode JSON strings into Python objects.
    """
    if isinstance(data, str):
        try:
            return recursive_json_decode(json.loads(data))
        except json.JSONDecodeError:
            return data
    elif isinstance(data, dict):
        return {key: recursive_json_decode(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [recursive_json_decode(item) for item in data]
    else:
        return data