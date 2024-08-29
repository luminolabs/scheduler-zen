from datetime import timedelta

import asyncpg
import json
from typing import List, Dict, Any, Optional, Union

from app.utils import (
    JOB_STATUS_RUNNING, JOB_STATUS_PENDING, JOB_STATUS_COMPLETED,
    JOB_STATUS_FAILED, JOB_STATUS_NEW, JOB_STATUS_STOPPING,
    JOB_STATUS_STOPPED, setup_logger
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

    async def get_recent_activities(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Fetch recent activities from the database.

        Args:
            limit (int): Maximum number of activities to fetch.

        Returns:
            List[Dict[str, Any]]: A list of recent activities.
        """
        async with self.pool.acquire() as conn:
            query = """
                SELECT timestamp, description 
                FROM activities 
                ORDER BY timestamp DESC 
                LIMIT $1
            """
            rows = await conn.fetch(query, limit)
            return [dict(row) for row in rows]

    async def log_activity(self, description: str) -> None:
        """
        Log a new activity in the database.

        Args:
            description (str): Description of the activity.
        """
        async with self.pool.acquire() as conn:
            query = """
                INSERT INTO activities (description) 
                VALUES ($1)
            """
            await conn.execute(query, description)

    async def create_tables(self) -> None:
        """Create the necessary tables for job tracking and activity logging."""
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
                    region TEXT,
                    user_id TEXT,
                    notes TEXT
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS activities (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    description TEXT
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS job_status_timestamps (
                    job_id TEXT PRIMARY KEY,
                    new_timestamp TIMESTAMP WITH TIME ZONE,
                    pending_timestamp TIMESTAMP WITH TIME ZONE,
                    running_timestamp TIMESTAMP WITH TIME ZONE,
                    stopping_timestamp TIMESTAMP WITH TIME ZONE,
                    stopped_timestamp TIMESTAMP WITH TIME ZONE,
                    completed_timestamp TIMESTAMP WITH TIME ZONE,
                    failed_timestamp TIMESTAMP WITH TIME ZONE
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
                    INSERT INTO job_status_timestamps (job_id, new_timestamp)
                    VALUES ($1, CURRENT_TIMESTAMP)
                ''', job_id)

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
                await conn.execute('''
                    UPDATE jobs
                    SET status = $1, vm_name = $2, region = $3, updated_at = CURRENT_TIMESTAMP
                    WHERE id = $4
                ''', status, vm_name, region, job_id)

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

    async def get_job_count(self,
                            status: str,
                            cluster: str, region: Optional[str] = None) -> int:
        """
        Get the count of running and pending jobs for a specific cluster and region.

        Args:
            status (str): The status of the jobs to count.
            cluster (str): The cluster name.
            region (Optional[str]): The region name.

        Returns:
            Dict[str, int]: A dictionary containing the count of running and pending jobs.
        """
        async with self.pool.acquire() as conn:
            args = [cluster]
            query = """
                SELECT COUNT(*) 
                FROM jobs 
                WHERE cluster = $1
            """
            if region:
                args.append(region)
                query += " AND region = $2"
            args.append(status)
            query += f" AND status = ${len(args)}"
            result = await conn.fetch(query, *args)
            count = result[0]['count']

            logger.info(f"Running jobs for cluster {cluster}, region {region}: {count}")
            return count

    async def get_recent_running_jobs(self, cluster: str, region: str, within: timedelta) -> List[Dict[str, Any]]:
        """
        Get jobs that started running within the specified time period.

        Args:
            cluster (str): The cluster name.
            region (str): The region name.
            within (timedelta): The time period to check for recent jobs.

        Returns:
            List[Dict[str, Any]]: A list of recent running jobs.
        """
        async with self.pool.acquire() as conn:
            query = """
                SELECT * FROM jobs
                WHERE cluster = $1
                AND region = $2
                AND status = 'RUNNING'
                AND (
                    SELECT running_timestamp 
                    FROM job_status_timestamps 
                    WHERE job_id = jobs.id
                ) > NOW() - $3::interval
            """
            rows = await conn.fetch(query, cluster, region, within)
            return [dict(row) for row in rows]

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
