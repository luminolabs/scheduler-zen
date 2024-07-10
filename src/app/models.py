# models.py

from enum import Enum
from typing import Dict, List, Optional
from pydantic import BaseModel, Field
from datetime import datetime


class JobStatus(str, Enum):
    """Job statuses."""
    NEW = "NEW"
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    STOPPED = "STOPPED"


class VMStatus(str, Enum):
    """VM statuses."""
    RUNNING = "RUNNING"
    STOPPED = "STOPPED"
    TERMINATED = "TERMINATED"


class JobOptions(BaseModel):
    """
    Job options for running a job.
    """
    workflow: str = Field(..., description="Name of the workflow to run")
    args: Dict[str, str] = Field(..., description="Arguments to pass to the workflow")
    keep_alive: bool = Field(..., description="Whether to keep the VM running after the job completes")


class Job(JobOptions):
    """
    Represents an accepted job.
    """
    id: str = Field(..., description="A unique identifier for the job")
    status: JobStatus = Field(default=JobStatus.NEW, description="Current status of the job")
    vm_name: Optional[str] = Field(None, description="Name of the VM running this job")
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Time when the job was created")
    updated_at: datetime = Field(default_factory=datetime.utcnow, description="Time when the job was last updated")

    class Config:
        """Pydantic model configuration."""
        allow_population_by_field_name = True


class ComputeInstance(BaseModel):
    """
    Represents a compute instance (VM) in the system.
    """
    name: str = Field(..., description="Name of the VM")
    zone: str = Field(..., description="GCP zone where the VM is running")
    job_id: Optional[str] = Field(None, description="ID of the job currently running on this VM")
    status: VMStatus = Field(..., description="Current status of the VM")
    created_at: float = Field(..., description="Timestamp when the VM was created")
    heartbeat_at: float = Field(..., description="Timestamp when the VM last sent a status update")


class Helm(BaseModel):
    """
    Represents the overall status of the scheduler.
    """
    running_jobs: List[Job] = Field(..., description="List of currently running jobs")
    pending_jobs: List[Job] = Field(..., description="List of pending jobs")
    vm_statuses: Dict[str, ComputeInstance] = Field(..., description="Status of all VMs in the system")
    mig_sizes: Dict[str, int] = Field(..., description="Current sizes of MIGs in each region")


class Message(BaseModel):
    """
    Represents a message sent through Pub/Sub.
    """
    message_type: str = Field(..., description="Type of the message (e.g., 'job', 'vm_update', 'stop_signal')")
    payload: Dict[str, any] = Field(..., description="Content of the message")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Time when the message was created")
