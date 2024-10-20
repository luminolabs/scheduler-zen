import json
from typing import List, Dict, Any, Optional, Union

import asyncpg

from app.core.utils import (
    JOB_STATUS_NEW, setup_logger, format_time, PROVIDER_GCP, PROVIDER_LUM
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
                    id VARCHAR NOT NULL,
                    user_id VARCHAR NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
                    workflow VARCHAR NOT NULL,
                    args JSONB DEFAULT '{}' NOT NULL,
                    status VARCHAR NOT NULL,
                    provider VARCHAR NOT NULL,
                    CONSTRAINT jobs_pk PRIMARY KEY (id, user_id)
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS jobs_gcp (
                    job_id VARCHAR NOT NULL,
                    user_id VARCHAR NOT NULL,
                    keep_alive BOOLEAN NOT NULL,
                    cluster VARCHAR,
                    vm_name VARCHAR,
                    region VARCHAR,
                    CONSTRAINT jobs_gcp_pk PRIMARY KEY (job_id, user_id)
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS public.jobs_lum (
                    job_id VARCHAR NOT NULL,
                    user_id VARCHAR NOT NULL,
                    tx_hash VARCHAR,
                    CONSTRAINT jobs_lum_pk PRIMARY KEY (job_id, user_id)
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS jobs_status_timestamps (
                    job_id VARCHAR NOT NULL,
                    user_id VARCHAR NOT NULL,
                    new_timestamp TIMESTAMP WITH TIME ZONE,
                    wait_for_vm_timestamp TIMESTAMP WITH TIME ZONE,
                    found_vm_timestamp TIMESTAMP WITH TIME ZONE,
                    detached_vm_timestamp TIMESTAMP WITH TIME ZONE,
                    running_timestamp TIMESTAMP WITH TIME ZONE,
                    stopping_timestamp TIMESTAMP WITH TIME ZONE,
                    stopped_timestamp TIMESTAMP WITH TIME ZONE,
                    completed_timestamp TIMESTAMP WITH TIME ZONE,
                    failed_timestamp TIMESTAMP WITH TIME ZONE,
                    CONSTRAINT jobs_status_timestamps_pk PRIMARY KEY (job_id, user_id)
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS public.jobs_artifacts (
                    job_id VARCHAR NOT NULL,
                    user_id VARCHAR NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
                    data jsonb DEFAULT '{}'  NOT NULL,
                    CONSTRAINT jobs_artifacts_pk PRIMARY KEY (job_id, user_id)
                )
            ''')
            logger.info("Database tables created")

    ### GCP Job Operations ###

    async def is_gcp_job(self, job_id: str, user_id: str) -> bool:
        """
        Check if a job is a GCP job.

        Args:
            job_id (str): The ID of the job.
            user_id (str): The ID of the user.

        Returns:
            bool: True if the job is a GCP job, False otherwise.
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('''
                SELECT provider FROM jobs
                WHERE id = $1 AND user_id = $2
            ''', job_id, user_id)
            return row['provider'] == PROVIDER_GCP if row else False

    async def add_job_gcp(self, job_data: Dict[str, Any]) -> str:
        """
        Add a new job to the database and initialize its status timestamp.

        Args:
            job_data (Dict[str, Any]): The job data including workflow, args, keep_alive, and cluster.

        Returns:
            str: The ID of the newly added job.
        """
        job_id = job_data['job_id']
        user_id = job_data['user_id']
        workflow = job_data['workflow']
        args = json.dumps(job_data['args'])
        keep_alive = job_data['keep_alive']
        cluster = job_data['cluster']
        status = JOB_STATUS_NEW

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Add the job to the main jobs table
                await conn.execute('''
                    INSERT INTO jobs (id, user_id, workflow, args, status, provider)
                    VALUES ($1, $2, $3, $4, $5, $6)
                ''', job_id, user_id, workflow, args, status, PROVIDER_GCP)
                # Add the job to the GCP jobs table
                await conn.execute('''
                    INSERT INTO jobs_gcp (job_id, user_id, keep_alive, cluster)
                    VALUES ($1, $2, $3, $4)
                ''', job_id, user_id, keep_alive, cluster)
                # Add the job status timestamp to the jobs timestamps table
                await conn.execute('''
                    INSERT INTO jobs_status_timestamps (job_id, user_id, new_timestamp)
                    VALUES ($1, $2, CURRENT_TIMESTAMP)
                ''', job_id, user_id)

        logger.info(f"Added {PROVIDER_GCP} job with id: {job_id}, user_id: {user_id}, "
                    f"workflow: {workflow}, cluster: {cluster}")
        return job_id

    async def update_job_gcp(self, job_id: str, user_id: str, status: str,
                             vm_name: Optional[str] = None, region: Optional[str] = None) -> None:
        """
        Update the status of a job in the database and update its status timestamp.

        Args:
            job_id (str): The ID of the job.
            user_id (str): The ID of the user.
            status (str): The new status of the job.
            vm_name (Optional[str]): The name of the VM assigned to the job.
            region (Optional[str]): The region of the VM assigned to the job
        """
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Update the job status and timestamp in the main jobs table
                await conn.execute('''
                    UPDATE jobs
                    SET status = $1, updated_at = CURRENT_TIMESTAMP
                    WHERE id = $2 AND user_id = $3
                ''', status, job_id, user_id)
                # Update the job details in the GCP jobs table
                sql = 'UPDATE jobs_gcp SET '
                args = []
                sql_parts = []
                if vm_name:
                    args.append(vm_name)
                    sql_parts.append(f'vm_name = ${len(args)}')
                if region:
                    args.append(region)
                    sql_parts.append(f'region = ${len(args)}')
                if sql_parts:
                    sql += ', '.join(sql_parts)
                    args.extend([job_id, user_id])
                    sql += f' WHERE job_id = ${len(args)-1} AND user_id = ${len(args)}'
                    await conn.execute(sql, *args)
                # Update the job status timestamp in the jobs timestamps table
                status_column = f"{status.lower()}_timestamp"
                await conn.execute(f'''
                    UPDATE jobs_status_timestamps
                    SET {status_column} = CURRENT_TIMESTAMP
                    WHERE job_id = $1 AND user_id = $2
                ''', job_id, user_id)

        logger.info(f"Updated job id: {job_id} for user {user_id} to status: {status}")

    async def get_jobs_by_status_gcp(self, statuses: Union[str, List[str]],
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
        # Convert single status to list for consistency
        if not isinstance(statuses, list):
            statuses = [statuses]
        # Build the SQL query
        sql = 'SELECT * FROM jobs j LEFT JOIN jobs_gcp g ON j.id = g.job_id WHERE provider = $1 AND j.status = ANY($2)'
        args = [PROVIDER_GCP, statuses]
        if cluster:
            args.append(cluster)
            sql += f' AND cluster = ${len(args)}'
        if region:
            args.append(region)
            sql += f' AND region = ${len(args)}'
        # Execute the query
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql, *args)
            # Convert rows to dictionaries
            jobs = [self._row_to_dict(row) for row in rows]
        logger.info(f"Retrieved {len(jobs)} jobs with status: {statuses}, cluster: {cluster}, region: {region}")
        return jobs

    ### LUM Job Operations ###

    async def add_job_lum(self, job_data: Dict[str, Any]) -> str:
        """
        Add a new job to the database and initialize its status timestamp.

        Args:
            job_data (Dict[str, Any]): The job data including workflow and tx_hash.

        Returns:
            str: The ID of the newly added job.
        """
        job_id = job_data['job_id']
        user_id = job_data['user_id']
        workflow = job_data['workflow']
        args = json.dumps(job_data['args'])
        status = JOB_STATUS_NEW

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Add the job to the main jobs table
                await conn.execute('''
                    INSERT INTO jobs (id, user_id, workflow, args, status, provider)
                    VALUES ($1, $2, $3, $4, $5, $6)
                ''', job_id, user_id, workflow, args, status, PROVIDER_LUM)
                # Add the job to the LUM jobs table
                await conn.execute('''
                    INSERT INTO jobs_lum (job_id, user_id)
                    VALUES ($1, $2)
                ''', job_id, user_id)
                # Add the job status timestamp to the jobs timestamps table
                await conn.execute('''
                    INSERT INTO jobs_status_timestamps (job_id, user_id, new_timestamp)
                    VALUES ($1, $2, CURRENT_TIMESTAMP)
                ''', job_id, user_id)

        logger.info(f"Added LUM job with id: {job_id}, user_id: {user_id}, workflow: {workflow}")
        return job_id

    async def update_job_lum(self, job_id: str, user_id: str,
                             status: Optional[str] = None,
                             tx_hash: Optional[str] = None) -> None:
        """
        Update the status of a job in the database and update its status timestamp.

        Args:
            job_id (str): The ID of the job.
            user_id (str): The ID of the user.
            status (str): The new status of the job.
            tx_hash (Optional[str]): The transaction hash of the job.
        """
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                if status:
                    # Update the job status and timestamp in the main jobs table
                    await conn.execute('''
                        UPDATE jobs
                        SET status = $1, updated_at = CURRENT_TIMESTAMP
                        WHERE id = $2 AND user_id = $3
                    ''', status, job_id, user_id)
                    # Update the job status timestamp in the jobs timestamps table
                    status_column = f"{status.lower()}_timestamp"
                    await conn.execute(f'''
                        UPDATE jobs_status_timestamps
                        SET {status_column} = CURRENT_TIMESTAMP
                        WHERE job_id = $1 AND user_id = $2
                    ''', job_id, user_id)
                # Update the job details in the LUM jobs table
                if tx_hash:
                    sql = 'UPDATE jobs_lum SET tx_hash = $1 WHERE job_id = $2 AND user_id = $3'
                    await conn.execute(sql, tx_hash, job_id, user_id)

    async def get_jobs_by_status_lum(self, statuses: Union[str, List[str]]) -> List[Dict[str, Any]]:
        """
        Retrieve all jobs with a given status or statuses.

        Args:
            statuses (Union[str, List[str]]): The status(es) to filter jobs by.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries with job details.
        """
        # Convert single status to list for consistency
        if not isinstance(statuses, list):
            statuses = [statuses]
        # Build the SQL query
        sql = '''
        SELECT * FROM jobs j 
        LEFT JOIN jobs_lum l ON j.id = l.job_id 
        WHERE provider = $1 AND l.tx_hash IS NOT NULL AND j.status = ANY($2)
        '''
        # Execute the query
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql, PROVIDER_LUM, statuses)
            # Convert rows to dictionaries
            jobs = [self._row_to_dict(row) for row in rows]
        logger.info(f"Retrieved {len(jobs)} jobs with status: {statuses}")
        return jobs

    ### Job Operations ###

    async def get_job(self, job_id: str, user_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a job from the database by its ID.

        Args:
            job_id (str): The ID of the job.
            user_id (str): The ID of the user.

        Returns:
            Optional[Dict[str, Any]]: A dictionary with job details if found, None otherwise.
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('''
            SELECT *, a.data as artifacts FROM jobs j
            LEFT JOIN jobs_gcp g ON j.id = g.job_id
            LEFT JOIN jobs_lum l ON j.id = l.job_id
            LEFT JOIN jobs_status_timestamps t ON j.id = t.job_id
            LEFT JOIN jobs_artifacts a ON j.id = a.job_id
            WHERE j.id = $1 AND j.user_id = $2
            ''', job_id, user_id)
            logger.info(f"Retrieved job with id: {job_id}")
            return self._row_to_dict(row)

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
                SELECT *, a.data as artifacts FROM jobs j
                LEFT JOIN jobs_gcp g ON j.id = g.job_id
                LEFT JOIN jobs_lum l ON j.id = l.job_id
                LEFT JOIN jobs_status_timestamps t ON j.id = t.job_id
                LEFT JOIN jobs_artifacts a ON j.id = a.job_id
                WHERE jobs_gcp.user_id = $1 AND id = ANY($2)
            """
            rows = await conn.fetch(query, user_id, job_ids)
            jobs = [self._row_to_dict(row) for row in rows]

        logger.info(f"Retrieved {len(jobs)} jobs for user {user_id}")
        return jobs

    async def update_job_artifacts(self, job_id: str, user_id: str, data: Dict[str, Any]) -> None:
        """
        Update the artifacts of a job in the database.

        Args:
            job_id (str): The ID of the job.
            user_id (str): The ID of the user.
            data (Dict[str, Any]): The new artifact data.
        """
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO jobs_artifacts (job_id, user_id, data)
                VALUES ($1, $2, $3)
                ON CONFLICT (job_id, user_id) DO UPDATE
                SET data = $3, updated_at = CURRENT_TIMESTAMP
            ''', job_id, user_id, json.dumps(data))
        logger.info(f"Updated artifacts for job id: {job_id} for user id: {user_id}, data: {data}")

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
            'user_id': row['user_id'],
            'created_at': format_time(row['created_at']),
            'updated_at': format_time(row['updated_at']),
            'workflow': row['workflow'],
            'args': json.loads(row['args']),
            'status': row['status'],
            'provider': row['provider'],
            'lum': {
                'tx_hash': row['tx_hash'],
            } if row['provider'] == 'LUM' else None,
            'gcp': {
                'keep_alive': row['keep_alive'],
                'cluster': row['cluster'],
                'status': row['status'],
                'vm_name': row['vm_name'],
                'region': row['region'],
            } if row['provider'] == 'GCP' else None,
            'timestamps': {
                'new': format_time(row.get('new_timestamp')),
                'wait_for_vm': format_time(row.get('wait_for_vm_timestamp')),
                'found_vm': format_time(row.get('found_vm_timestamp')),
                'detached_vm': format_time(row.get('detached_vm_timestamp')),
                'running': format_time(row.get('running_timestamp')),
                'stopping': format_time(row.get('stopping_timestamp')),
                'stopped': format_time(row.get('stopped_timestamp')),
                'completed': format_time(row.get('completed_timestamp')),
                'failed': format_time(row.get('failed_timestamp')),
            },
            'artifacts': row.get('artifacts')
        }
