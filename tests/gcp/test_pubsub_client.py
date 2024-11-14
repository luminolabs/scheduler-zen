import json
from unittest.mock import patch, MagicMock

import pytest
from google.cloud.pubsub_v1.subscriber.message import Message
from google.cloud.pubsub_v1.types import FlowControl

from app.gcp.pubsub_client import PubSubClient


@pytest.fixture
def mock_publisher():
    """Create a mock publisher client."""
    mock = MagicMock()
    mock.topic_path = MagicMock(return_value="projects/test-project/topics/test-topic")
    return mock


@pytest.fixture
def mock_subscriber():
    """Create a mock subscriber client."""
    mock = MagicMock()
    mock.subscription_path = MagicMock(
        return_value="projects/test-project/subscriptions/test-subscription"
    )
    return mock


@pytest.fixture
def pubsub_client(mock_publisher, mock_subscriber):
    """Create a PubSubClient instance with mocked dependencies."""
    with patch('app.gcp.pubsub_client.pubsub_v1.PublisherClient', return_value=mock_publisher), \
            patch('app.gcp.pubsub_client.pubsub_v1.SubscriberClient', return_value=mock_subscriber):
        client = PubSubClient("test-project", "test-subscription")
        return client


def test_initialization(pubsub_client, mock_publisher, mock_subscriber):
    """Test PubSubClient initialization."""
    assert pubsub_client.project_id == "test-project"
    assert pubsub_client.subscription == "test-subscription"
    assert pubsub_client.publisher == mock_publisher
    assert pubsub_client.subscriber == mock_subscriber
    assert pubsub_client.subscription_future is None
    assert not pubsub_client.running


def test_callback_when_not_running(pubsub_client):
    """Test callback behavior when client is not running."""
    message = MagicMock(spec=Message)
    pubsub_client.running = False
    pubsub_client.callback(message)
    assert pubsub_client.heartbeats_queue.empty()


def test_callback_when_running(pubsub_client):
    """Test callback behavior when client is running."""
    message = MagicMock(spec=Message)
    message.data = b'{"test": "data"}'
    pubsub_client.running = True
    pubsub_client.callback(message)
    assert not pubsub_client.heartbeats_queue.empty()
    assert pubsub_client.heartbeats_queue.get() == message


def test_get_next_message_empty_queue(pubsub_client):
    """Test get_next_message when queue is empty."""
    assert pubsub_client.get_next_message() is None


def test_get_next_message_with_message(pubsub_client):
    """Test get_next_message when queue has a message."""
    test_message = MagicMock(spec=Message)
    pubsub_client.heartbeats_queue.put(test_message)
    assert pubsub_client.get_next_message() == test_message


@pytest.mark.asyncio
async def test_start(pubsub_client):
    """Test start method."""
    await pubsub_client.start()

    assert pubsub_client.running is True
    pubsub_client.subscriber.subscription_path.assert_called_once_with(
        "test-project",
        "test-subscription"
    )
    pubsub_client.subscriber.subscribe.assert_called_once_with(
        pubsub_client.subscriber.subscription_path(),
        callback=pubsub_client.callback,
        flow_control=FlowControl(max_messages=100)
    )


@pytest.mark.asyncio
async def test_stop(pubsub_client):
    """Test stop method."""
    # Set up a mock subscription future
    mock_future = MagicMock()
    pubsub_client.subscription_future = mock_future
    pubsub_client.running = True

    await pubsub_client.stop()

    assert pubsub_client.running is False
    mock_future.cancel.assert_called_once()
    pubsub_client.subscriber.close.assert_called_once()
    assert pubsub_client.subscription_future is None


@pytest.mark.asyncio
async def test_publish_start_signal(pubsub_client):
    """Test publish_start_signal method."""
    topic_name = "test-topic"
    job_data = {
        "job_id": "test-job",
        "gcp": {"cluster": "test-cluster"}
    }

    # Mock the publish operation
    future = MagicMock()
    pubsub_client.publisher.publish.return_value = future

    await pubsub_client.publish_start_signal(topic_name, job_data)

    # Verify the publish call
    pubsub_client.publisher.topic_path.assert_called_once_with(
        "test-project",
        topic_name
    )
    pubsub_client.publisher.publish.assert_called_once_with(
        pubsub_client.publisher.topic_path(),
        json.dumps(job_data).encode("utf-8"),
        cluster=job_data['gcp']['cluster']
    )
    future.result.assert_called_once()


@pytest.mark.asyncio
async def test_publish_stop_signal(pubsub_client):
    """Test publish_stop_signal method."""
    topic_name = "test-topic"
    job_id = "test-job"

    # Mock the publish operation
    future = MagicMock()
    pubsub_client.publisher.publish.return_value = future

    await pubsub_client.publish_stop_signal(topic_name, job_id)

    # Verify the publish call
    pubsub_client.publisher.topic_path.assert_called_once_with(
        "test-project",
        topic_name
    )
    expected_data = json.dumps({"action": "stop", "job_id": job_id}).encode("utf-8")
    pubsub_client.publisher.publish.assert_called_once_with(
        pubsub_client.publisher.topic_path(),
        expected_data
    )
    future.result.assert_called_once()


@pytest.mark.asyncio
async def test_multiple_callbacks_thread_safety(pubsub_client):
    """Test thread safety of multiple concurrent callbacks."""
    messages = [MagicMock(spec=Message) for _ in range(5)]
    for msg in messages:
        msg.data = b'{"test": "data"}'

    pubsub_client.running = True

    # Simulate multiple concurrent callbacks
    for message in messages:
        pubsub_client.callback(message)

    # Verify all messages were added to queue
    assert pubsub_client.heartbeats_queue.qsize() == len(messages)

    # Verify messages can be retrieved in order
    retrieved_messages = []
    while not pubsub_client.heartbeats_queue.empty():
        retrieved_messages.append(pubsub_client.get_next_message())

    assert len(retrieved_messages) == len(messages)
    assert all(msg in retrieved_messages for msg in messages)
