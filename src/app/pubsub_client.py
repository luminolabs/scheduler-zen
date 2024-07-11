import asyncio
import json
import logging
from typing import Callable, Dict, Any

from google.cloud import pubsub_v1

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class PubSubClient:
    """Manages Google Cloud Pub/Sub operations."""

    def __init__(self, project_id: str):
        """
        Initialize the PubSubClient with a project ID.

        Args:
            project_id (str): The Google Cloud project ID.
        """
        self.project_id = project_id
        self.publisher = pubsub_v1.PublisherClient()
        self.subscriber = pubsub_v1.SubscriberClient()
        logger.info(f"PubSubClient initialized with project_id: {project_id}")

    async def publish_job(self, topic_name: str, job_data: Dict[str, Any]) -> None:
        """
        Publish a job to a specified Pub/Sub topic.

        Args:
            topic_name (str): The Pub/Sub topic.
            job_data (dict): The data to publish.
        """
        logger.info(f"Publishing job to topic: {topic_name} with data: {job_data}")
        topic_path = self.publisher.topic_path(self.project_id, topic_name)
        data = json.dumps(job_data).encode("utf-8")
        future = self.publisher.publish(topic_path, data, cluster=job_data['cluster'])
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

    async def listen_for_heartbeats(self, subscription_name: str, callback: Callable) -> None:
        """
        Listen for heartbeats on a specified Pub/Sub subscription.

        Args:
            subscription_name (str): The Pub/Sub subscription.
            callback (Callable[[str], None]): The callback function to handle incoming messages.
        """
        logger.info(f"Listening for heartbeats on subscription: {subscription_name}")
        subscription_path = self.subscriber.subscription_path(self.project_id, subscription_name)

        async def process_message(message):
            await callback(message.data)
            message.ack()

        def callback_wrapper(message):
            asyncio.create_task(process_message(message))

        streaming_pull_future = self.subscriber.subscribe(subscription_path, callback=callback_wrapper)
        with self.subscriber:
            try:
                await asyncio.to_thread(streaming_pull_future.result)
            except Exception as e:
                streaming_pull_future.cancel()
                logger.error(f"Listening for heartbeats failed: {e}")
