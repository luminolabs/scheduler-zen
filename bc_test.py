import asyncio
import json

from app.lum.job_manager_client import JobManagerClient


async def main():
    with open("./assets/abi.json") as f:
        abi = json.loads(json.load(f)["result"])
    client = JobManagerClient(
        rpc_url="wss://holesky.drpc.org",
        contract_address="0x76ebc106f195D8DA1bffC26390360a258E7dD8A4",
        abi=abi,
        account_address="0x4118CFD00dD5e8CED96e0ff8061F56F2d155e83B",
        account_private_key="611ae5ad48dcab478a57107c8dbd34a592ad6bf6660c8e42f3a75464905d91c3",
    )
    await client.web3.provider.connect()

    if not await client.web3.is_connected():
        raise ConnectionError("Unable to connect to the Ethereum network.")

    # Create a job
    tx_hash = await client.create_job(
        job_args={"description": "Sample Job"}
    )

    # Get job status
    status = await client.get_job_status(tx_hash)

    await client.web3.provider.disconnect()

# Run the example
asyncio.run(main())