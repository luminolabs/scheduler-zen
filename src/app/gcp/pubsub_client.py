import asyncio
import json
from typing import Callable, Dict, Any

from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.subscriber.message import Message

from app.core.utils import setup_logger

# Set up logging
logger = setup_logger(__name__)


class PubSubClient:
    """Manages Google Cloud Pub/Sub operations."""

    def __init__(self, project_id: str, subscription: str) -> None:
        """
        Initialize the PubSubClient with a project ID.

        Args:
            project_id (str): The Google Cloud project ID.
            subscription (str): The name of the subscription.
        """
        self.project_id = project_id
        self.subscription = subscription
        self.publisher = pubsub_v1.PublisherClient()
        self.subscriber = pubsub_v1.SubscriberClient()
        self.heartbeats_queue = asyncio.Queue()
        self.running = False
        self.subscription_future = None
        logger.info(f"PubSubClient initialized with project_id: {project_id}")

    async def start(self) -> None:
        """
        Start PubSub processes and initialize the subscription.
        """
        self.running = True

        # Initialize the subscription path
        subscription_path = self.subscriber.subscription_path(
            self.project_id,
            self.subscription
        )

        # Define the callback for handling messages
        def callback(message: Message) -> None:
            """
            Handle incoming messages by adding them to the heartbeats queue.

            Args:
                message (Message): The Pub/Sub message to process.
            """
            if not self.running:
                return

            try:
                # Add the message to the queue for processing
                asyncio.run_coroutine_threadsafe(
                    self.heartbeats_queue.put(message),
                    asyncio.get_event_loop()
                )
                logger.debug(f"Added message to queue: {message.data}")
            except Exception as e:
                logger.error(f"Error processing message: {str(e)}")
                message.nack()  # Negative acknowledgment

        # Start the subscription with flow control settings
        flow_control = pubsub_v1.types.FlowControl(
            max_messages=100,  # Maximum number of unprocessed messages
        )

        # Start subscribing in a separate thread
        self.subscription_future = self.subscriber.subscribe(
            subscription_path,
            callback=callback,
            flow_control=flow_control
        )

        logger.info(f"Started subscription on {self.subscription}")

    async def stop(self) -> None:
        """Stop PubSub processes and clean up subscriptions."""
        self.running = False

        # Cancel the subscription if it exists
        if self.subscription_future:
            self.subscription_future.cancel()
            self.subscription_future = None

        # Close the subscriber clients
        self.subscriber.close()

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
