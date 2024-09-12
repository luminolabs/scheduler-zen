import asyncpg
import json
from typing import List, Dict, Any, Optional, Union

from app.utils import (
    JOB_STATUS_NEW, setup_logger
)

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
        """Create the necessary tables for job tracking and activity logging."""
        async with self.pool.acquire() as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    user_id VARCHAR DEFAULT '0' NOT NULL,
                    notes TEXT,
                    workflow TEXT,
                    args JSONB,
                    keep_alive BOOLEAN,
                    cluster TEXT,
                    status TEXT,
                    vm_name TEXT,
                    region TEXT,
                    CONSTRAINT jobs_pk PRIMARY KEY (id, user_id)
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS job_status_timestamps (
                    job_id TEXT PRIMARY KEY,
                    user_id VARCHAR DEFAULT '0' NOT NULL,
                    new_timestamp TIMESTAMP WITH TIME ZONE,
                    wait_for_vm_timestamp TIMESTAMP WITH TIME ZONE,
                    found_vm_timestamp TIMESTAMP WITH TIME ZONE,
                    detached_vm_timestamp TIMESTAMP WITH TIME ZONE,
                    running_timestamp TIMESTAMP WITH TIME ZONE,
                    stopping_timestamp TIMESTAMP WITH TIME ZONE,
                    stopped_timestamp TIMESTAMP WITH TIME ZONE,
                    completed_timestamp TIMESTAMP WITH TIME ZONE,
                    failed_timestamp TIMESTAMP WITH TIME ZONE,
                    CONSTRAINT jobs_pk PRIMARY KEY (job_id, user_id)
                )
            ''')
            logger.info("Database tables created")

    async def add_job(self, job_data: Dict[str, Any]) -> str:
        """
        Add a new job to the database and initialize its status timestamp.

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
        user_id = job_data.get('user_id')

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute('''
                    INSERT INTO jobs (id, workflow, args, keep_alive, cluster, status, user_id)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                ''', job_id, workflow, args, keep_alive, cluster, status, user_id)

                await conn.execute('''
                    INSERT INTO job_status_timestamps (job_id, new_timestamp, user_id)
                    VALUES ($1, CURRENT_TIMESTAMP, $2)
                ''', job_id, user_id)

        logger.info(f"Added job with id: {job_id}, workflow: {workflow}, cluster: {cluster}")
        return job_id

    async def update_job(self, job_id: str, status: str,
                         vm_name: Optional[str] = None, region: Optional[str] = None) -> None:
        """
        Update the status of a job in the database and update its status timestamp.

        Args:
            job_id (str): The ID of the job.
            status (str): The new status of the job.
            vm_name (Optional[str]): The name of the VM assigned to the job.
            region (Optional[str]): The region of the VM assigned to the job
        """
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                args = [status]
                sql = """
                    UPDATE jobs
                    SET status = $1, updated_at = CURRENT_TIMESTAMP
                """
                if vm_name:
                    args.append(vm_name)
                    sql += f', vm_name = ${len(args)}'
                if region:
                    args.append(region)
                    sql += f', region = ${len(args)}'
                args.append(job_id)
                sql += f' WHERE id = ${len(args)}'
                await conn.execute(sql, *args)

                status_column = f"{status.lower()}_timestamp"
                await conn.execute(f'''
                    UPDATE job_status_timestamps
                    SET {status_column} = CURRENT_TIMESTAMP
                    WHERE job_id = $1
                ''', job_id)

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

    async def get_jobs_by_status(self, statuses: Union[str, List[str]],
                                 cluster: Optional[str] = None, region: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Retrieve all jobs with a given status or statuses.

        Args:
            statuses (Union[str, List[str]]): The status(es) to filter jobs by.
            cluster (Optional[str]): The cluster to filter jobs by.
            region (Optional[str]): The region to filter jobs by.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries with job details.
        """
        if not isinstance(statuses, list):
            statuses = [statuses]

        sql = 'SELECT * FROM jobs WHERE status = ANY($1)'
        args = [statuses]
        if cluster:
            args.append(cluster)
            sql += f' AND cluster = ${len(args)}'
        if region:
            args.append(region)
            sql += f' AND region = ${len(args)}'

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql, *args)
            jobs = [self._row_to_dict(row) for row in rows]
        logger.info(f"Retrieved {len(jobs)} jobs with status: {statuses}, cluster: {cluster}, region: {region}")
        return jobs

    async def get_jobs_by_user_and_ids(self, user_id: str, job_ids: List[str]) -> List[Dict[str, Any]]:
        """
        Retrieve jobs for a specific user with the given job IDs.

        Args:
            user_id (str): The ID of the user.
            job_ids (List[str]): A list of job IDs to retrieve.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries with job details.
        """
        async with self.pool.acquire() as conn:
            query = """
                SELECT * FROM jobs 
                WHERE user_id = $1 AND id = ANY($2)
            """
            rows = await conn.fetch(query, user_id, job_ids)
            jobs = [self._row_to_dict(row) for row in rows]

        logger.info(f"Retrieved {len(jobs)} jobs for user {user_id}")
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
            'user_id': row['user_id'],
            'notes': row['notes'],
        }
