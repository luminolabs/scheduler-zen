import asyncpg
import json
from typing import List, Dict, Any, Optional, Union

from app.utils import JOB_STATUS_NEW, setup_logger

# Set up logging
logger = setup_logger(__name__)


class Database:
    """Manages the PostgreSQL database for job tracking."""

    def __init__(self, connection_string: str):
        """
        Initialize the Database with a PostgreSQL connection string.

        Args:
            connection_string (str): The PostgreSQL connection string.
        """
        self.connection_string = connection_string
        self.pool = None
        logger.info(f"Database initialized with connection string: {connection_string}")

    async def connect(self):
        """Create a connection pool to the PostgreSQL database."""
        self.pool = await asyncpg.create_pool(self.connection_string)
        logger.info("Database connection pool created")

    async def close(self):
        """Close the database connection pool."""
        await self.pool.close()
        logger.info("Database connection pool closed")

    async def create_tables(self) -> None:
        """Create the necessary tables for job tracking."""
        async with self.pool.acquire() as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    workflow TEXT,
                    args JSONB,
                    keep_alive BOOLEAN,
                    cluster TEXT,
                    status TEXT,
                    vm_name TEXT,
                    region TEXT
                )
            ''')
        logger.info("Database tables created")

    async def add_job(self, job_data: Dict[str, Any]) -> str:
        """
        Add a new job to the database.

        Args:
            job_data (Dict[str, Any]): The job data including workflow, args, keep_alive, and cluster.

        Returns:
            str: The ID of the newly added job.
        """
        job_id = job_data.get('job_id') or self._generate_job_id()
        workflow = job_data['workflow']
        args = json.dumps(job_data['args'])
        keep_alive = job_data['keep_alive']
        cluster = job_data['cluster']
        status = JOB_STATUS_NEW

        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO jobs (id, workflow, args, keep_alive, cluster, status)
                VALUES ($1, $2, $3, $4, $5, $6)
            ''', job_id, workflow, args, keep_alive, cluster, status)

        logger.info(f"Added job with id: {job_id}, workflow: {workflow}, cluster: {cluster}")
        return job_id

    async def update_job(self, job_id: str, status: str,
                         vm_name: Optional[str] = None, region: Optional[str] = None) -> None:
        """
        Update the status of a job in the database.

        Args:
            job_id (str): The ID of the job.
            status (str): The new status of the job.
            vm_name (Optional[str]): The name of the VM assigned to the job.
            region (Optional[str]): The region of the VM assigned to the job
        """
        async with self.pool.acquire() as conn:
            await conn.execute('''
                UPDATE jobs
                SET status = $1, vm_name = $2, region = $3, updated_at = CURRENT_TIMESTAMP
                WHERE id = $4
            ''', status, vm_name, region, job_id)
        logger.info(f"Updated job id: {job_id} to status: {status}")

    async def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a job from the database by its ID.

        Args:
            job_id (str): The ID of the job.

        Returns:
            Optional[Dict[str, Any]]: A dictionary with job details if found, None otherwise.
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT * FROM jobs WHERE id = $1', job_id)
            logger.info(f"Retrieved job with id: {job_id}")
            return self._row_to_dict(row)

    async def get_jobs_by_status(self, statuses: Union[str, List[str]]) -> List[Dict[str, Any]]:
        """
        Retrieve all jobs with a given status or statuses.

        Args:
            statuses (Union[str, List[str]]): The status(es) to filter jobs by.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries with job details.
        """
        if not isinstance(statuses, list):
            statuses = [statuses]

        async with self.pool.acquire() as conn:
            rows = await conn.fetch('SELECT * FROM jobs WHERE status = ANY($1)', statuses)
            jobs = [self._row_to_dict(row) for row in rows]
        logger.info(f"Retrieved {len(jobs)} jobs with status: {statuses}")
        return jobs

    @staticmethod
    def _generate_job_id() -> str:
        """
        Generate a unique job ID.

        Returns:
            str: A unique job ID.
        """
        import uuid
        return str(uuid.uuid4())

    @staticmethod
    def _row_to_dict(row: Optional[asyncpg.Record]) -> Optional[Dict[str, Any]]:
        """
        Convert a database row to a dictionary.

        Args:
            row: A row from the database.
        Returns:
            dict: A dictionary representation of the row.
        """
        if row is None:
            return None
        return {
            'job_id': row['id'],
            'created_at': row['created_at'].isoformat(),
            'updated_at': row['updated_at'].isoformat(),
            'workflow': row['workflow'],
            'args': json.loads(row['args']),
            'keep_alive': row['keep_alive'],
            'cluster': row['cluster'],
            'status': row['status'],
            'vm_name': row['vm_name'],
            'region': row['region'],
        }