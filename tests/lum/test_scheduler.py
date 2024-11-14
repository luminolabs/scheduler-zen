from unittest.mock import AsyncMock, MagicMock

import pytest
from web3.exceptions import Web3RPCError

from app.core.utils import (
    JOB_STATUS_NEW,
    JOB_STATUS_WAIT_FOR_VM,
    JOB_STATUS_RUNNING
)
from app.lum.scheduler import Scheduler, LUMReceiptProcessor, LUMJobStatusUpdater


@pytest.fixture
def mock_transaction():
    """Create a mock transaction context manager"""
    class MockTransaction:
        def __init__(self):
            self.conn = AsyncMock()

        async def __aenter__(self):
            return self.conn

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            pass
    return MockTransaction()


@pytest.fixture
def mock_job_manager_client():
    """Create a mock JobManagerClient"""
    client = AsyncMock()
    client.web3 = AsyncMock()
    client.web3.is_connected.return_value = True
    client.web3.eth = AsyncMock()
    client.create_job = AsyncMock(return_value="0xtxhash123")
    client.event_signature_hashes = {
        'JobCreated': '0x1234567890'  # Use a valid hex string
    }
    client.get_job_status = AsyncMock(return_value=JOB_STATUS_RUNNING)
    return client


@pytest.fixture
def mock_db():
    """Create a mock Database with proper transaction support"""
    class AsyncContextManagerMock:
        async def __aenter__(self):
            return AsyncMock()

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            pass

    db = AsyncMock()
    # Make transaction() return an async context manager
    db.transaction.return_value = AsyncContextManagerMock()

    # Set up other mock methods
    db.get_jobs_by_status_lum = AsyncMock(return_value=[])
    db.get_pending_lum_receipts = AsyncMock(return_value=[])
    db.add_job_lum = AsyncMock()
    db.update_job_lum = AsyncMock()
    return db


@pytest.fixture
def mock_receipt_processor(mock_job_manager_client):
    """Create a mock receipt processor"""
    processor = AsyncMock()
    processor.get_tx_receipt = AsyncMock(return_value=None)
    processor.get_lum_id_from_receipt = MagicMock(return_value=None)
    return processor


@pytest.fixture
def mock_status_updater():
    """Create a mock status updater"""
    updater = AsyncMock()
    updater.update_job_status_single = AsyncMock()
    return updater


@pytest.fixture
def scheduler(mock_db, mock_job_manager_client, mock_receipt_processor, mock_status_updater):
    """Create a Scheduler instance with mocked dependencies"""
    scheduler = Scheduler(mock_db, mock_job_manager_client)
    scheduler.receipt_processor = mock_receipt_processor
    scheduler.status_updater = mock_status_updater
    return scheduler


@pytest.fixture
def sample_job():
    """Create a sample job fixture"""
    return {
        "job_id": "test-job-123",
        "user_id": "test-user-456",
        "workflow": "test-workflow",
        "args": {"param1": "value1"},
        "status": JOB_STATUS_NEW,
        "lum": {
            "tx_hash": "0xtxhash123",
            "lum_id": None
        }
    }


@pytest.mark.asyncio
async def test_receipt_processor_get_tx_receipt_not_found():
    """Test handling of transaction receipt not found"""
    mock_job_manager_client = AsyncMock()
    error = Web3RPCError(message="Transaction not found")
    mock_job_manager_client.web3.eth.get_transaction_receipt.side_effect = error

    processor = LUMReceiptProcessor(mock_job_manager_client)

    with pytest.raises(Web3RPCError):
        await processor.get_tx_receipt("0xtxhash123")


def test_receipt_processor_get_lum_id_from_receipt_success():
    """Test successful LUM ID extraction from receipt"""
    mock_job_manager_client = MagicMock()
    # Event signature should be saved without '0x' prefix in the mock
    mock_job_manager_client.event_signature_hashes = {
        'JobCreated': '1234567890123456789012345678901234567890123456789012345678901234'  # 64 char hex
    }

    processor = LUMReceiptProcessor(mock_job_manager_client)

    # Create mock receipt with matching event signature
    mock_receipt = {
        'logs': [{
            'topics': [
                # First topic is event signature hash - bytes.fromhex() needs string without '0x' prefix
                bytes.fromhex('1234567890123456789012345678901234567890123456789012345678901234'),
                # Second topic is the LUM ID padded to 32 bytes
                bytes.fromhex('0000000000000000000000000000000000000000000000000000000000000123')
            ]
        }]
    }

    lum_id = processor.get_lum_id_from_receipt(mock_receipt)
    assert lum_id == 0x123  # The LUM ID we encoded above


def test_receipt_processor_get_lum_id_from_receipt_no_matching_event():
    """Test LUM ID extraction when no matching event is found"""
    mock_job_manager_client = MagicMock()
    mock_job_manager_client.event_signature_hashes = {
        'JobCreated': '0x1234567890'
    }

    processor = LUMReceiptProcessor(mock_job_manager_client)

    # Create mock receipt with properly formatted hex strings
    mock_receipt = {
        'logs': [{
            'topics': [
                bytes.fromhex('abcdef1234'),  # Different event signature
                bytes.fromhex('0123')  # LUM ID in hex
            ]
        }]
    }

    lum_id = processor.get_lum_id_from_receipt(mock_receipt)
    assert lum_id is None


@pytest.mark.asyncio
async def test_status_updater_update_job_status_change(mock_db, mock_job_manager_client):
    """Test job status update when status has changed"""
    # TODO: Implement this test
    pass


@pytest.mark.asyncio
async def test_scheduler_start_success(scheduler):
    """Test successful scheduler start"""
    await scheduler.start()
    assert scheduler.running is True
    scheduler.job_manager_client.web3.is_connected.assert_awaited_once()


@pytest.mark.asyncio
async def test_scheduler_start_connection_failure(scheduler):
    """Test scheduler start with connection failure"""
    scheduler.job_manager_client.web3.is_connected.return_value = False

    with pytest.raises(ConnectionError):
        await scheduler.start()
    assert scheduler.running is False


@pytest.mark.asyncio
async def test_scheduler_stop(scheduler):
    """Test scheduler stop"""
    scheduler.running = True
    await scheduler.stop()
    assert scheduler.running is False


@pytest.mark.asyncio
async def test_add_job_success(scheduler, sample_job):
    """Test successful job addition"""
    # TODO: Implement this test
    pass


@pytest.mark.asyncio
async def test_update_job_statuses(scheduler):
    """Test job status updates"""
    mock_jobs = [
        {"job_id": "job1", "user_id": "user1", "status": JOB_STATUS_NEW, "lum": {"lum_id": 1}},
        {"job_id": "job2", "user_id": "user2", "status": JOB_STATUS_WAIT_FOR_VM, "lum": {"lum_id": 2}}
    ]
    scheduler.db.get_jobs_by_status_lum.return_value = mock_jobs

    await scheduler._update_job_statuses()

    assert scheduler.db.get_jobs_by_status_lum.await_count == 1
    assert scheduler.db.get_jobs_by_status_lum.await_args[0][0] == [
        JOB_STATUS_NEW,
        JOB_STATUS_WAIT_FOR_VM,
        JOB_STATUS_RUNNING
    ]
    # Verify each job was processed
    assert scheduler.status_updater.update_job_status_single.await_count == 2


@pytest.mark.asyncio
async def test_process_pending_receipts(scheduler):
    """Test processing of pending receipts"""
    # TODO: Implement this test
    pass


@pytest.mark.asyncio
async def test_process_pending_receipts_no_pending_jobs(scheduler):
    """Test processing pending receipts when there are none"""
    scheduler.db.get_pending_lum_receipts.return_value = []

    await scheduler._process_pending_receipts()

    # Verify no processing was attempted
    scheduler.receipt_processor.get_tx_receipt.assert_not_awaited()
    scheduler.db.update_job_lum.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_cycle_success(scheduler):
    """Test successful scheduler cycle"""
    scheduler.running = True

    await scheduler.run_cycle()

    # Verify both update methods were called
    assert scheduler.db.get_jobs_by_status_lum.await_count == 1
    assert scheduler.db.get_pending_lum_receipts.await_count == 1


@pytest.mark.asyncio
async def test_run_cycle_not_running(scheduler):
    """Test scheduler cycle when not running"""
    scheduler.running = False

    await scheduler.run_cycle()

    # Verify no updates were attempted
    scheduler.db.get_jobs_by_status_lum.assert_not_awaited()
    scheduler.db.get_pending_lum_receipts.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_cycle_with_error(scheduler):
    """Test scheduler cycle error handling"""
    scheduler.running = True
    scheduler.db.get_jobs_by_status_lum.side_effect = Exception("Test error")

    # Should not raise exception
    await scheduler.run_cycle()

    # First method should have been called
    scheduler.db.get_jobs_by_status_lum.assert_awaited_once()
    # Second method should not have been called due to error
    scheduler.db.get_pending_lum_receipts.assert_not_awaited()
