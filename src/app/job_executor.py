# job_executor.py

import asyncio
import json
import logging
import os
import signal
import sys
from typing import Dict, Any

from google.cloud import pubsub_v1, storage
from google.api_core import retry

from models import JobStatus, Message


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class JobExecutor:
    """
    Responsible for executing fine-tuning jobs on a VM and reporting status updates.
    """

    def __init__(self):
        """Initialize the JobExecutor with necessary clients and configurations."""
        self.project_id = os.environ.get("PROJECT_ID")
        self.job_subscription_name = os.environ.get("JOB_SUBSCRIPTION")
        self.vm_updates_topic_name = os.environ.get("VM_UPDATES_TOPIC")
        self.stop_signal_subscription_name = os.environ.get("STOP_SIGNAL_SUBSCRIPTION")
        self.results_bucket_name = os.environ.get("RESULTS_BUCKET")

        self.subscriber = pubsub_v1.SubscriberClient()
        self.publisher = pubsub_v1.PublisherClient()
        self.storage_client = storage.Client()

        self.job_subscription_path = self.subscriber.subscription_path(
            self.project_id, self.job_subscription_name
        )
        self.vm_updates_topic_path = self.publisher.topic_path(
            self.project_id, self.vm_updates_topic_name
        )
        self.stop_signal_subscription_path = self.subscriber.subscription_path(
            self.project_id, self.stop_signal_subscription_name
        )

        self.current_job: Dict[str, Any] = None
        self.stop_requested = False

    async def start(self):
        """Start the JobExecutor to listen for and execute jobs."""
        logger.info("Starting JobExecutor")
        await self.send_status_update("STARTED")

        # Set up signal handlers
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, self.handle_shutdown)

        while True:
            try:
                await self.process_job()
            except Exception as e:
                logger.error(f"Error processing job: {e}")
                await asyncio.sleep(10)  # Wait before retrying

    def handle_shutdown(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}. Initiating shutdown...")
        self.stop_requested = True
        if self.current_job:
            asyncio.create_task(self.stop_current_job())

    async def process_job(self):
        """Process a single job from the job subscription."""
        response = await asyncio.to_thread(
            self.subscriber.pull,
            request={"subscription": self.job_subscription_path, "max_messages": 1},
            retry=retry.Retry(deadline=300),
        )

        for msg in response.received_messages:
            try:
                job_data = json.loads(msg.message.data.decode("utf-8"))
                self.current_job = job_data["payload"]

                logger.info(f"Received job: {self.current_job['id']}")
                await self.send_status_update(JobStatus.RUNNING)

                # Start listening for stop signals
                stop_signal_task = asyncio.create_task(self.listen_for_stop_signal())

                # Execute the job
                success = await self.execute_job()

                # Cancel the stop signal listener
                stop_signal_task.cancel()

                if success:
                    await self.send_status_update(JobStatus.COMPLETED)
                else:
                    await self.send_status_update(JobStatus.FAILED)

                # Acknowledge the message
                await asyncio.to_thread(
                    self.subscriber.acknowledge,
                    request={"subscription": self.job_subscription_path, "ack_ids": [msg.ack_id]},
                )

                self.current_job = None

                # After job completion, initiate VM shutdown
                await self.shutdown_vm()

            except Exception as e:
                logger.error(f"Error processing job message: {e}")
                await self.send_status_update(JobStatus.FAILED)

    async def execute_job(self) -> bool:
        """
        Execute the current job.

        Returns:
            bool: True if the job was successful, False otherwise.
        """
        try:
            # Simulate job execution
            logger.info(f"Executing job {self.current_job['id']}")
            await asyncio.sleep(60)  # Simulating work for 60 seconds

            if self.stop_requested:
                logger.info("Job execution stopped due to stop request")
                return False

            # Simulate uploading results
            await self.upload_results()

            return True
        except Exception as e:
            logger.error(f"Error executing job: {e}")
            return False

    async def upload_results(self):
        """Upload job results to the designated GCS bucket."""
        bucket = self.storage_client.bucket(self.results_bucket_name)
        blob = bucket.blob(f"results/{self.current_job['id']}")

        # Simulate result data
        result_data = json.dumps({
            "job_id": self.current_job['id'],
            "status": "completed",
            "metrics": {
                "accuracy": 0.95,
                "loss": 0.02
            }
        }).encode('utf-8')

        await asyncio.to_thread(blob.upload_from_string, result_data)
        logger.info(f"Uploaded results for job {self.current_job['id']}")

    async def send_status_update(self, status: JobStatus):
        """Send a status update to the VM updates topic."""
        message = Message(
            message_type="vm_update",
            payload={
                "vm_name": os.environ.get("VM_NAME"),
                "job_id": self.current_job['id'] if self.current_job else None,
                "status": status
            }
        )
        data = json.dumps(message.dict()).encode("utf-8")

        await asyncio.to_thread(
            self.publisher.publish,
            self.vm_updates_topic_path,
            data
        )
        logger.info(f"Sent status update: {status}")

    async def listen_for_stop_signal(self):
        """Listen for stop signals for the current job."""
        while self.current_job and not self.stop_requested:
            response = await asyncio.to_thread(
                self.subscriber.pull,
                request={"subscription": self.stop_signal_subscription_path, "max_messages": 1},
                retry=retry.Retry(deadline=300),
            )

            for msg in response.received_messages:
                stop_data = json.loads(msg.message.data.decode("utf-8"))
                if stop_data["payload"]["job_id"] == self.current_job['id']:
                    logger.info(f"Received stop signal for job {self.current_job['id']}")
                    self.stop_requested = True
                    break

            await asyncio.sleep(5)  # Check for stop signals every 5 seconds

    async def stop_current_job(self):
        """Stop the current job execution."""
        self.stop_requested = True
        logger.info(f"Stopping job {self.current_job['id']}")
        # Additional logic to stop the job execution could be added here

    async def shutdown_vm(self):
        """Initiate VM shutdown after job completion."""
        logger.info("Job completed. Initiating VM shutdown...")
        # In a real-world scenario, you would use the Compute Engine API to delete the instance
        # For this example, we'll just exit the script
        sys.exit(0)


if __name__ == "__main__":
    executor = JobExecutor()
    asyncio.run(executor.start())