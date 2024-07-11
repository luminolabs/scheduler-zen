import asyncio
import logging

from app.cluster_orchestrator import ClusterOrchestrator
from app.database import Database
from app.pubsub_client import PubSubClient
from scheduler import Scheduler

logging.basicConfig(level=logging.INFO)


async def main():
    """Main entry point for the application."""

    # Define the GPU configurations and regions where they are available.
    gpu_configs = {
        '8xa100-40gb': ['asia-northeast1', 'asia-northeast-3', 'asia-southeast1',
                        'europe-west4', 'me-west1',
                        'us-central1', 'us-west1', 'us-west3', 'us-west4'],
    }

    logging.info("Starting main function")

    # Initialize the database, PubSub client, and cluster orchestrator.
    db = Database('scheduler-zen.sqlite')
    pubsub = PubSubClient('neat-airport-407301')
    cluster_orchestrator = ClusterOrchestrator('neat-airport-407301', gpu_configs)

    # Create the tables in the database, if they don't exist.
    db.create_tables()

    # Start the scheduler.
    scheduler = Scheduler(db, pubsub, cluster_orchestrator)
    try:
        logging.info("Starting scheduler")
        await scheduler.start()
    except KeyboardInterrupt:
        logging.info("Stopping scheduler")
        await scheduler.stop()

if __name__ == "__main__":
    asyncio.run(main())
