import json
from typing import Optional

from google.cloud.exceptions import NotFound
from google.cloud.storage import Client

from app.core.config_manager import config
from app.core.database import Database
from app.core.utils import is_local_env, setup_logger

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


async def pull_artifacts_meta_from_gcs(job_id: str, user_id: str, db: Database, gcs: Client) -> Optional[dict]:
    """
    Pull the artifacts meta from GCS.

    Args:
        job_id (str): The job ID.
        user_id (str): The user ID.
        db (Database): The database instance.
        gcs (Client): The GCS client.
    Returns:
        dict: The artifacts data
    """
    # Get job region from DB
    job = await db.get_job(job_id, user_id)
    region = job['gcp']['region'] if job['provider'] == 'gcp' else 'us-central1'
    # Construct the job-meta.json object location in GCS
    bucket_name = get_results_bucket(region)
    object_name = f'{user_id}/{job_id}/job-meta.json'
    # Read and return the job-meta.json object as a dict
    bucket = gcs.bucket(bucket_name)
    try:
        blob = bucket.blob(object_name)
        return json.loads(blob.download_as_bytes())
    except NotFound:
        logger.error(f"Blob not found: {object_name} in bucket {bucket_name}")
        return None