from typing import Optional

from aiohttp import ClientResponseError
from gcloud.aio.storage import Storage

from app.core.config_manager import config
from app.core.utils import is_local_env

# The prefix for the storage buckets;
# the full bucket name will be the prefix + the multi-region
STORAGE_BUCKET_PREFIX = 'lum-pipeline-zen-jobs'


async def read_gcs_file(bucket_name: str, object_name: str, ignore_404: bool = False) -> Optional[bytes]:
    """
    Read a file from GCS.

    Args:
        bucket_name (str): The name of the bucket.
        object_name (str): The name of the object.
        ignore_404 (bool): Whether to ignore 404 errors or not.

    Returns:
        Optional[bytes]: The file content.
    """
    try:
        storage_client = Storage()
        blob = await storage_client.download(bucket_name, object_name)
        await storage_client.close()
        return blob
    except ClientResponseError as e:
        if e.status == 404 and ignore_404:
            return None
        raise e


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
