import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler
from typing import Optional

from app.config_manager import config

# VM status constants
INSTANCE_STATUS_RUNNING = 'RUNNING'

# Job status constants
JOB_STATUS_NEW = 'NEW'
JOB_STATUS_PENDING = 'PENDING'
JOB_STATUS_RUNNING = 'RUNNING'
JOB_STATUS_STOPPING = 'STOPPING'
JOB_STATUS_STOPPED = 'STOPPED'
JOB_STATUS_COMPLETED = 'COMPLETED'
JOB_STATUS_FAILED = 'FAILED'

# Ordered list of job statuses
HEARTBEAT_ORDERED_JOB_STATUSES = [JOB_STATUS_NEW, JOB_STATUS_PENDING, JOB_STATUS_RUNNING,
                                  JOB_STATUS_STOPPING, JOB_STATUS_STOPPED,
                                  JOB_STATUS_COMPLETED, JOB_STATUS_FAILED]


def is_new_job_status_valid(old_status: str, new_status: str) -> bool:
    """
    Checks if the new job status is valid. We allow same old and new status so that we can process heartbeats.

    Args:
        old_status: old job status
        new_status: new job status
    Returns:
        bool: True if the new job status is valid, False otherwise
    """
    return HEARTBEAT_ORDERED_JOB_STATUSES.index(new_status) >= HEARTBEAT_ORDERED_JOB_STATUSES.index(old_status)


def get_region_from_vm_name(vm_name: Optional[str]) -> Optional[str]:
    """
    Get the region from a VM name.

    Args:
        vm_name (str): The name of the VM.

    Returns:
        str: The region of the VM.
    """
    return '-'.join(vm_name.split('-')[-3:-1]) if vm_name else None


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
    log_format = logging.Formatter(f'{config.env_name} - %(asctime)s - %(message)s')

    # Log to stdout and to file
    os.makedirs(os.path.dirname(config.log_file), exist_ok=True)
    stdout_handler = logging.StreamHandler(sys.stdout)
    file_handler = TimedRotatingFileHandler(config.log_file, when="midnight", interval=1)
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
