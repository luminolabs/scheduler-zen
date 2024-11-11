import asyncio
import json
from typing import Callable, Dict, Any

from google.cloud import pubsub_v1

from app.core.utils import setup_logger

# Set up logging
logger = setup_logger(__name__)


class PubSubClient:
    """Manages Google Cloud Pub/Sub operations."""

    # Heartbeat callback function
    heartbeat_callback: Callable = None

    def __init__(self, project_id: str):
        """
        Initialize the PubSubClient with a project ID.

        Args:
            project_id (str): The Google Cloud project ID.
        """
        self.project_id = project_id
        self.publisher = pubsub_v1.PublisherClient()
        self.subscriber = pubsub_v1.SubscriberClient()
        self.heartbeats_queue = asyncio.Queue()
        self.running = False
        logger.info(f"PubSubClient initialized with project_id: {project_id}")

    async def start(self) -> None:
        """Start PubSub processes."""
        self.running = True
        logger.info("PubSub processes started")

    async def stop(self) -> None:
        """Stop PubSub processes."""
        self.running = False
        logger.info("PubSub processes stopped")

    async def publish_start_signal(self, topic_name: str, job_data: Dict[str, Any]) -> None:
        """
        Publish a job to a specified Pub/Sub topic.

        Args:
            topic_name (str): The Pub/Sub topic.
            job_data (dict): The data to publish.
        """
        logger.info(f"Publishing job to topic: {topic_name} with data: {job_data}")
        topic_path = self.publisher.topic_path(self.project_id, topic_name)
        data = json.dumps(job_data).encode("utf-8")
        future = self.publisher.publish(topic_path, data, cluster=job_data['gcp']['cluster'])
        await asyncio.to_thread(future.result)

    async def publish_stop_signal(self, topic_name: str, job_id: str) -> None:
        """
        Publish a stop signal for a job to a specified Pub/Sub topic.

        Args:
            topic_name (str): The Pub/Sub topic.
            job_id (str): The ID of the job to stop.
        """
        logger.info(f"Publishing stop signal to topic: {topic_name} for job_id: {job_id}")
        topic_path = self.publisher.topic_path(self.project_id, topic_name)
        data = json.dumps({"action": "stop", "job_id": job_id}).encode("utf-8")
        future = self.publisher.publish(topic_path, data)
        await asyncio.to_thread(future.result)
