import asyncio
import json
from queue import Queue
from threading import Lock
from typing import Dict, Any, Optional

from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.subscriber.message import Message

from app.core.utils import setup_logger

logger = setup_logger(__name__)

class PubSubClient:
    def __init__(self, project_id: str, subscription: str) -> None:
        self.project_id = project_id
        self.subscription = subscription
        self.publisher = pubsub_v1.PublisherClient()
        self.subscriber = pubsub_v1.SubscriberClient()
        self.subscription_future = None

        # Create a queue to store incoming messages
        # This queue is used to communicate between the PubSub callback
        # which runs in a separate thread, and the main event loop.
        self.heartbeats_queue = Queue()
        self.queue_lock = Lock()

        self.running = False
        logger.info(f"PubSubClient initialized with project_id: {project_id}")

    def callback(self, message: Message) -> None:
        """
        Handle incoming messages by adding them to the heartbeats queue.
        This runs in a separate thread, not the main event loop, so we need to
        use a lock to ensure thread safety.
        """
        if not self.running:
            return

        try:
            with self.queue_lock:
                self.heartbeats_queue.put(message)
            logger.debug(f"Added message to queue: {message.data}")
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            message.nack()

    def get_next_message(self) -> Optional[Message]:
        """
        Get the next message from the queue if available.
        This runs in the main event loop.
        """
        try:
            with self.queue_lock:
                if not self.heartbeats_queue.empty():
                    return self.heartbeats_queue.get_nowait()
        except Exception as e:
            logger.error(f"Error getting message from queue: {str(e)}")
        return None

    async def start(self) -> None:
        self.running = True

        subscription_path = self.subscriber.subscription_path(
            self.project_id,
            self.subscription
        )

        flow_control = pubsub_v1.types.FlowControl(
            max_messages=100,
        )

        # Start the subscription, it will run in a separate thread
        self.subscription_future = self.subscriber.subscribe(
            subscription_path,
            callback=self.callback,
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
