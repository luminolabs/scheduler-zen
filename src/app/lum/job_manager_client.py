import asyncio
import json
from typing import Dict, Tuple

from hexbytes import HexBytes
from web3 import Web3, AsyncWeb3, WebSocketProvider
from web3.exceptions import TransactionNotFound

from app.core.utils import (
    JOB_STATUS_NEW, JOB_STATUS_WAIT_FOR_VM,
    JOB_STATUS_RUNNING,
    JOB_STATUS_COMPLETED, JOB_STATUS_FAILED,
    setup_logger
)

# Set up logging
logger = setup_logger(__name__)

# Job statuses, indexed by their integer values, as defined in the contract
# i.e. order of declaration is important
JOB_STATUSES = (
    JOB_STATUS_NEW, JOB_STATUS_WAIT_FOR_VM, JOB_STATUS_RUNNING,
    JOB_STATUS_COMPLETED, JOB_STATUS_FAILED
)

class JobManagerClient:
    """
    A client for interacting with the JobManager contract.
    """
    def __init__(self, rpc_url: str, contract_address: str, abi: dict,
                 account_address: str, account_private_key: str):
        """
        Initialize the JobManagerClient.

        Args:
            rpc_url (str): The Web3 RPC URL.
            contract_address (str): The contract address.
            abi (dict): The contract ABI.
            account_address (str): The account address.
            account_private_key (str): The account private key.
        """
        logger.info(f"Initializing JobManagerClient with contract at {contract_address}...")
        self.web3 = AsyncWeb3(WebSocketProvider(rpc_url))
        self.contract_address = Web3.to_checksum_address(contract_address)
        self.contract = self.web3.eth.contract(address=self.contract_address, abi=abi)
        self.event_signature_hashes = self._generate_event_signature_hashes(abi)
        self.account_address = Web3.to_checksum_address(account_address)
        self.account_private_key = account_private_key
        logger.info("JobManagerClient initialization complete.")

    def _generate_event_signature_hashes(self, abi: dict) -> Dict[str, str]:
        """
        Generate event signature hashes from the ABI.

        Example output: {'JobCreated': '0x1234abcd', 'JobCompleted': '0x5678efgh'}

        Args:
            abi: The contract ABI.
        Returns:
            dict: A dictionary of event names to their corresponding signature hashes.
        """
        logger.debug("Generating event signature hashes...")
        event_signature_hashes = {}
        # Iterate over the ABI items
        for item in abi:
            # Skip non-event items
            if item.get('type') != 'event':
                continue
            # Generate the event signature hash
            signature = f"{item['name']}({','.join([input['type'] for input in item['inputs']])})"
            event_signature_hashes[item['name']] = self.web3.keccak(text=signature).hex()
        logger.debug(f"Generated event signatures: {event_signature_hashes}")
        return event_signature_hashes

    async def get_job_status(self, lum_id: str) -> str:
        """
        Fetches the status of a job by its ID.

        Args:
            lum_id (str): The LUM job ID.
        Returns:
            str: The job status.
        """
        logger.info(f"Fetching job status for tx_hash: {lum_id}")
        # Fetch the job status index from the contract, e.g., 0, 1, 2, etc.
        status_idx = await self.contract.functions.getJobStatus(lum_id).call()
        # Map and return the status string, e.g., "NEW", "QUEUED", etc.
        status = JOB_STATUSES[status_idx]
        logger.info(f"Fetched status '{status}' for LUM job ID: {lum_id}")
        return status

    async def create_job(self, job_args: dict) -> Tuple[str, int]:
        """
        Creates a new job with the given details.

        Args:
            job_args (dict): The job details.
        Returns:
            str: The transaction hash of the job creation transaction, also known as the job ID.
        """
        logger.info(f"Creating a new job with args: {job_args}")
        # Build and sign the transaction
        job_args_json = json.dumps(job_args)
        tx = await self.contract.functions.createJob(job_args_json).build_transaction({
            'from': self.account_address,
            'value': self.web3.to_wei(0.01, 'ether'),
            'gas': 2000000,
            'gasPrice': self.web3.to_wei('50', 'gwei'),
            'nonce': await self.web3.eth.get_transaction_count(self.account_address),
        })
        signed_tx = self.web3.eth.account.sign_transaction(tx, self.account_private_key)
        # Send the signed transaction
        tx_hash = await self.web3.eth.send_raw_transaction(signed_tx.raw_transaction)
        logger.info(f"Transaction sent, tx_hash: {tx_hash.hex()}")
        # Wait for the transaction receipt and return the transaction hash
        receipt = await self.wait_for_receipt(tx_hash)
        # Decode the event logs to get the job ID
        event_signature_hash = self.event_signature_hashes.get('JobCreated')
        for log in receipt['logs']:
            if log['topics'][0].hex() == event_signature_hash:
                # Extract the job ID from the log data
                lum_id = int(log['topics'][1].hex(), 16)
                logger.info(f"Job created with job_id: {lum_id}, tx_hash: {tx_hash.hex()}")
                return tx_hash.hex(), lum_id
        # Raise an error if the job ID is not found in the transaction logs
        raise SystemError(f"Job creation failed, could not retrieve lum_id, tx_hash: {tx_hash.hex()}")

    async def wait_for_receipt(self, tx_hash: HexBytes, timeout: int = 120, poll_interval: int = 2) -> dict:
        """
        Wait for a transaction to be mined and return the receipt.

        Args:
            tx_hash (HexBytes): The transaction hash.
            timeout (int): The maximum time to wait in seconds.
            poll_interval (int): The interval to poll for the receipt in seconds.
        Returns:
            dict: The transaction receipt.
        """
        logger.info(f"Waiting for receipt, tx_hash: {tx_hash.hex()}")
        for attempt in range(timeout // poll_interval):
            try:
                receipt = await self.web3.eth.get_transaction_receipt(tx_hash)
                if receipt:
                    logger.info(f"Transaction receipt received, tx_hash: {tx_hash.hex()}")
                    return receipt
            except TransactionNotFound:
                logger.warning(f"Transaction not found yet, attempt {attempt + 1}, tx_hash: {tx_hash.hex()}")
            await asyncio.sleep(poll_interval)
        # Raise an error if the receipt is not found within the timeout
        raise TimeoutError(f"Transaction timed out, tx_hash: {tx_hash.hex()}")
