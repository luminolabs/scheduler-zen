import json
from typing import Optional, Tuple

from gcloud.aio.storage import Storage

from app.core.config_manager import config
from app.core.database import Database
from app.core.utils import is_local_env, setup_logger, PROVIDER_GCP, PROVIDER_LUM

logger = setup_logger(__name__)

# The prefix for the storage buckets;
# the full bucket name will be the prefix + the multi-region
STORAGE_BUCKET_PREFIX = 'lum-pipeline-zen-jobs'


def get_results_bucket(region: Optional[str] = 'us-central1') -> str:
    """
    Get the results bucket name.

    We maintain buckets for the `us`, `asia`, and `europe` multi-regions.
    We have a regional bucket for `me-west1`, because Middle East doesn't
    have multi-region storage infrastructure on GCP.

    ex.
    - 'us-central1' -> 'pipeline-zen-jobs-us'
    - 'me-west1' -> 'pipeline-zen-jobs-me-west1'

    :return: The results bucket name
    """
    # If running locally, use the local dev bucket
    if is_local_env():
        return f'{STORAGE_BUCKET_PREFIX}-{config.local_env_name}'  # ie. 'pipeline-zen-jobs-local'

    # Get multi-region from the region
    # ie. 'us-central1' -> 'us'
    multi_region = region.split('-')[0]

    # Middle East doesn't have a multi-region storage configuration on GCP,
    # so we maintain a regional bucket for `me-west1`.
    if multi_region == 'me':
        return f'{STORAGE_BUCKET_PREFIX}-{region}'  # regional bucket; ie. 'pipeline-zen-jobs-me-west1'
    return f'{STORAGE_BUCKET_PREFIX}-{multi_region}'  # multi-region bucket; ie. 'pipeline-zen-jobs-us'


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

    # Initialize the async GCS client
    storage = Storage()

    # Construct the job-meta.json object location in GCS
    if job["provider"] == PROVIDER_GCP:
        object_name = f'{user_id}/{job_id}/job-meta.json'
        region = job['gcp']['region']
    elif job["provider"] == PROVIDER_LUM:
        object_name = f'{config.lum_account_address}/{job["lum"]["lum_id"]}/job-meta.json'
        region = 'us-central1'
    else:
        raise SystemError(f"Unknown provider: {job['provider']}")

    # Get the results bucket name based on the job region
    bucket_name = get_results_bucket(region)

    # Download and parse the job-meta.json object
    blob = await storage.download(bucket_name, object_name)
    if not blob:
        return None
    result = json.loads(blob.decode('utf-8'))

    # Close the storage client
    await storage.close()

    return job_id, user_id, result
