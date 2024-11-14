import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from web3 import Web3

from app.core.utils import (
    JOB_STATUS_NEW, JOB_STATUS_WAIT_FOR_VM, JOB_STATUS_RUNNING,
    JOB_STATUS_COMPLETED, JOB_STATUS_FAILED
)
from app.lum.job_manager_client import JobManagerClient


@pytest.fixture
def sample_abi():
    """Sample ABI with events for testing."""
    return [
        {
            "type": "event",
            "name": "JobCreated",
            "inputs": [
                {"type": "uint256", "indexed": True, "name": "jobId"},
                {"type": "address", "indexed": True, "name": "creator"},
                {"type": "uint32", "indexed": False, "name": "epoch"}
            ]
        },
        {
            "type": "event",
            "name": "JobStatusUpdated",
            "inputs": [
                {"type": "uint256", "indexed": True, "name": "jobId"},
                {"type": "uint8", "indexed": False, "name": "newStatus"}
            ]
        }
    ]


@pytest.fixture
def mock_web3():
    """Create a mock Web3 instance."""
    mock = AsyncMock()
    mock.eth = AsyncMock()
    mock.eth.contract = MagicMock()
    mock.eth.get_transaction_count = AsyncMock(return_value=1)
    mock.to_wei = MagicMock(return_value=10000000000000000)  # 0.01 ETH in wei
    mock.to_checksum_address = Web3.to_checksum_address
    mock.keccak = Web3.keccak
    return mock


@pytest.fixture
def job_manager_client(mock_web3, sample_abi):
    """Create a JobManagerClient instance with mocked dependencies."""
    with patch('app.lum.job_manager_client.AsyncWeb3', return_value=mock_web3):
        client = JobManagerClient(
            rpc_url="https://example.com",
            contract_address="0x1234567890123456789012345678901234567890",
            abi=sample_abi,
            account_address="0x4118CFD00dD5e8CED96e0ff8061F56F2d155e83B",
            account_private_key="0xabcdef"
        )
        return client


@pytest.mark.asyncio
async def test_initialization(job_manager_client, sample_abi):
    """Test JobManagerClient initialization."""
    assert job_manager_client.contract_address == \
           Web3.to_checksum_address("0x1234567890123456789012345678901234567890")
    assert job_manager_client.account_address == \
           Web3.to_checksum_address("0x4118CFD00dD5e8CED96e0ff8061F56F2d155e83B")
    assert job_manager_client.account_private_key == "0xabcdef"

    # Verify event signatures were generated correctly
    expected_job_created_sig = Web3.keccak(
        text="JobCreated(uint256,address,uint32)").hex()
    expected_job_status_sig = Web3.keccak(
        text="JobStatusUpdated(uint256,uint8)").hex()

    assert job_manager_client.event_signature_hashes["JobCreated"] == expected_job_created_sig
    assert job_manager_client.event_signature_hashes["JobStatusUpdated"] == expected_job_status_sig


@pytest.mark.asyncio
async def test_get_job_status(job_manager_client):
    """Test getting a job's status."""
    # Set up mock return values for different status indices
    status_mappings = {
        0: JOB_STATUS_NEW,
        1: JOB_STATUS_WAIT_FOR_VM,
        2: JOB_STATUS_RUNNING,
        3: JOB_STATUS_COMPLETED,
        4: JOB_STATUS_FAILED
    }

    for idx, expected_status in status_mappings.items():
        # Configure mock to return specific status index
        job_manager_client.contract.functions.getJobStatus = MagicMock()
        job_manager_client.contract.functions.getJobStatus().call = AsyncMock(
            return_value=idx
        )

        # Test status retrieval
        status = await job_manager_client.get_job_status(123)
        assert status == expected_status

        # Verify the contract call
        job_manager_client.contract.functions.getJobStatus.assert_called_with(123)


@pytest.mark.asyncio
async def test_create_job_success(job_manager_client):
    """Test successful job creation."""
    # Mock transaction hash
    expected_tx_hash = "0x123abc"

    # Set up the mock chain of calls
    mock_tx = {"from": job_manager_client.account_address,
               "value": job_manager_client.web3.to_wei(0.01, "ether"),
               "gas": 2000000,
               "gasPrice": job_manager_client.web3.to_wei("50", "gwei"),
               "nonce": 1}

    job_manager_client.contract.functions.createJob = MagicMock()
    create_job_func = job_manager_client.contract.functions.createJob()
    create_job_func.build_transaction = AsyncMock(return_value=mock_tx)

    # Mock signing and sending transaction
    mock_signed_tx = MagicMock()
    job_manager_client.web3.eth.account.sign_transaction = MagicMock(
        return_value=mock_signed_tx
    )
    job_manager_client.web3.eth.send_raw_transaction = AsyncMock(
        return_value=bytes.fromhex(expected_tx_hash.replace("0x", ""))
    )

    # Test job creation
    job_args = {"param1": "value1", "param2": "value2"}
    tx_hash = await job_manager_client.create_job(job_args)

    # Verify the transaction hash
    assert tx_hash == expected_tx_hash

    # Verify all the method calls
    job_manager_client.contract.functions.createJob.assert_called_with(
        json.dumps(job_args)
    )
    create_job_func.build_transaction.assert_called_once()
    job_manager_client.web3.eth.account.sign_transaction.assert_called_with(
        mock_tx, job_manager_client.account_private_key
    )
    job_manager_client.web3.eth.send_raw_transaction.assert_called_with(
        mock_signed_tx.raw_transaction
    )


@pytest.mark.asyncio
async def test_create_job_retries_once_on_exception():
    """Test that create_job() is retried once when _get_create_job_tx_params raises an exception."""
    # TODO: Implement this test
    pass


def test_generate_event_signature_hashes(sample_abi):
    """Test event signature hash generation."""
    client = JobManagerClient(
        rpc_url="https://example.com",
        contract_address="0x1234567890123456789012345678901234567890",
        abi=sample_abi,
        account_address="0x4118CFD00dD5e8CED96e0ff8061F56F2d155e83B",
        account_private_key="0xabcdef"
    )

    # Verify generated hashes
    assert "JobCreated" in client.event_signature_hashes
    assert "JobStatusUpdated" in client.event_signature_hashes

    # Verify hash values
    job_created_hash = Web3.keccak(
        text="JobCreated(uint256,address,uint32)").hex()
    job_status_hash = Web3.keccak(
        text="JobStatusUpdated(uint256,uint8)").hex()

    assert client.event_signature_hashes["JobCreated"] == job_created_hash
    assert client.event_signature_hashes["JobStatusUpdated"] == job_status_hash


def test_generate_event_signature_hashes_empty_abi():
    """Test event signature hash generation with empty ABI."""
    client = JobManagerClient(
        rpc_url="https://example.com",
        contract_address="0x1234567890123456789012345678901234567890",
        abi=[],  # Empty ABI
        account_address="0x4118CFD00dD5e8CED96e0ff8061F56F2d155e83B",
        account_private_key="0xabcdef"
    )

    # Verify no hashes were generated
    assert len(client.event_signature_hashes) == 0


def test_generate_event_signature_hashes_no_events():
    """Test event signature hash generation with ABI containing no events."""
    abi_no_events = [
        {
            "type": "function",
            "name": "someFunction",
            "inputs": []
        }
    ]

    client = JobManagerClient(
        rpc_url="https://example.com",
        contract_address="0x1234567890123456789012345678901234567890",
        abi=abi_no_events,
        account_address="0x4118CFD00dD5e8CED96e0ff8061F56F2d155e83B",
        account_private_key="0xabcdef"
    )

    # Verify no hashes were generated
    assert len(client.event_signature_hashes) == 0
