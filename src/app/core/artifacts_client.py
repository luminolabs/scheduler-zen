import json
from typing import Optional, Tuple

from app.core.config_manager import config
from app.core.database import Database
from app.core.gcp_client import read_gcs_file, get_results_bucket
from app.core.utils import setup_logger, PROVIDER_GCP, PROVIDER_LUM

logger = setup_logger(__name__)


async def pull_artifacts_meta_from_gcs_task(
        job_id: str, user_id: str, db: Database) -> Optional[Tuple[str, str, dict]]:
    """
    Pull the artifacts meta from GCS using an async client.

    Note: This is supposed to be a task that is run in parallel,
        so we will return the inputs to the task as well.

    Args:
        job_id (str): The job ID.
        user_id (str): The user ID.
        db (Database): The database instance.
    Returns:
        Optional[Tuple[str, str, dict]]: The job ID, user ID, and the artifacts meta if found, None otherwise.
    """
    # Get job region from DB
    job = await db.get_job(job_id, user_id)

    # Construct the job-meta.json object location in GCS
    if job["provider"] == PROVIDER_GCP:
        object_name = f'{user_id}/{job_id}/job-meta.json'
        region = job['gcp']['region']
    elif job["provider"] == PROVIDER_LUM:
        object_name = f'{config.lum_account_address}/{job["lum"]["lum_id"]}/job-meta.json'
        region = 'us-central1'
    else:
        raise SystemError(f"Unknown provider: {job['provider']}")

    # If the job region is not set, there's no artifact to pull
    if not region:
        return None

    # Get the results bucket name based on the job region
    bucket_name = get_results_bucket(region)
    # Download and parse the job-meta.json object
    blob = await read_gcs_file(bucket_name, object_name, ignore_404=True)
    if not blob:
        return None
    result = json.loads(blob.decode('utf-8'))
    # Return the job ID, user ID along with the result
    # because we'll need to associate the result with the job
    # in the calling function
    return job_id, user_id, result
