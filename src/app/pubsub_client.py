# pubsub_client.py

import asyncio
import json
import logging
from typing import Optional

from google.cloud import pubsub_v1
from google.api_core import retry

from models import Job, Message


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PubSubClient:
    """
    Client for interacting with Google Cloud Pub/Sub.
    """

    def __init__(self, project_id: str,
                 start_signal_topic_name: str = 'pipeline-zen-start',
                 stop_signal_topic_name: str = 'pipeline-zen-stop',
                 vm_updates_subscription_name: str = 'scheduler-zen-updates'):
        """
        Initialize the PubSubClient.

        Args:
            project_id (str): The GCP project ID
            start_signal_topic_name (str): The name of the Pub/Sub topic for job sending start signals
            stop_signal_topic_name (str): The name of the Pub/Sub topic for job sending stop signals
            vm_updates_subscription_name (str): The name of the Pub/Sub subscription for receiving VM updates
        """

        self.project_id = project_id
        self.start_signal_topic_name = start_signal_topic_name
        self.stop_signal_topic_name = stop_signal_topic_name
        self.vm_updates_subscription_name = vm_updates_subscription_name

        self.publisher = pubsub_v1.PublisherClient()
        self.subscriber = pubsub_v1.SubscriberClient()

        self.start_signal_topic_path = self.publisher.topic_path(self.project_id, self.start_signal_topic_name)
        self.stop_signal_topic_path = self.publisher.topic_path(self.project_id, self.stop_signal_topic_name)
        self.vm_updates_subscription_path = self.subscriber.subscription_path(
            self.project_id, self.vm_updates_subscription_name
        )

    async def _publish_message(self, topic_path: str, message: Message) -> str:
        """
        Publish a message to the given Pub/Sub topic.

        Args:
            topic_path (str): The full path of the Pub/Sub topic.
            message (Message): The message to publish.

        Returns:
            str: The message ID of the published message.
        """
        try:
            data = json.dumps(message.model_dump()).encode("utf-8")
            future = self.publisher.publish(topic_path, data)
            message_id = await asyncio.to_thread(future.result)
            logger.info(f"Published message to {topic_path} with message ID: {message_id}")
            return message_id
        except Exception as e:
            logger.error(f"Error publishing message to {topic_path}: {e}")
            raise

    async def publish_start_job_signal(self, job: Job) -> str:
        """
        Publish a start signal for a job to the Pub/Sub topic.

        Args:
            job (Job): The job to publish.

        Returns:
            str: The message ID of the published message.
        """
        message = Message(message_type="start_signal", payload=job.model_dump())
        return await self._publish_message(self.start_signal_topic_path, message)

    async def publish_stop_job_signal(self, job_id: str) -> str:
        """
        Publish a stop signal for a job to the Pub/Sub topic.

        Args:
            job_id (str): The ID of the job to stop.

        Returns:
            str: The message ID of the published message.
        """
        message = Message(message_type="stop_signal", payload={"job_id": job_id})
        return await self._publish_message(self.stop_signal_topic_path, message)

    async def listen_for_vm_status(self) -> Optional[dict]:
        """
        Receive a VM status update message from the Pub/Sub subscription.

        Returns:
            Optional[dict]: The received message as a dictionary, or None if no message was received.
        """
        try:
            response = await asyncio.to_thread(
                self.subscriber.pull,
                request={
                    "subscription": self.vm_updates_subscription_path,
                    "max_messages": 1,
                },
                retry=retry.Retry(deadline=300),
            )

            if response.received_messages:
                message = response.received_messages[0]
                await asyncio.to_thread(
                    self.subscriber.acknowledge,
                    request={
                        "subscription": self.vm_updates_subscription_path,
                        "ack_ids": [message.ack_id],
                    }
                )
                data = json.loads(message.message.data.decode("utf-8"))
                logger.info(f"Received VM update: {data}")
                return data
            else:
                return None
        except Exception as e:
            logger.error(f"Error receiving VM update: {e}")
            return None
