from typing import Dict, Any, List

from pydantic import BaseModel, computed_field

from app.core.config_manager import config


class CreateJobRequestBase(BaseModel):
    """
    Base request schema for creating a job.
    """
    job_id: str
    user_id: str = "0"  # Default to 0 for internal jobs; the Customer API will set this to the user ID
    workflow: str
    args: Dict[str, Any]


class CreateJobRequestGCP(CreateJobRequestBase):
    """
    Request schema for creating a job.
    """
    gpu_type: str  # ex: "a100-40gb"
    num_gpus: int = 1
    keep_alive: bool = False

    @computed_field
    def cluster(self) -> str:
        """
        Return the cluster name based on the GPU type and number of GPUs.
        Returns:
            str: The cluster name. ex: "4xa100-40gb" or "local" if using fake MIG manager.
        """
        return f"{self.num_gpus}x{self.gpu_type}"


class CreateJobRequestLUM(CreateJobRequestBase):
    pass


class ListUserJobsRequest(BaseModel):
    """
    Request schema for getting jobs for a specific user with the given job IDs.
    """
    user_id: str
    job_ids: List[str]
