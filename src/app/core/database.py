import json
from contextlib import asynccontextmanager
from typing import List, Dict, Any, Optional, Union, AsyncContextManager

import asyncpg

from app.core.config_manager import config
from app.core.utils import (
    JOB_STATUS_NEW, PROVIDER_GCP, PROVIDER_LUM, setup_logger, format_time, recursive_json_decode
)

# Set up logging
logger = setup_logger(__name__)

# Query to select and join jobs with all related tables
SELECT_AND_JOIN_JOBS_SQL = '''
SELECT *, j.user_id as user_id, a.data as artifacts FROM jobs j
LEFT JOIN jobs_gcp g ON j.id = g.job_id
LEFT JOIN jobs_lum l ON j.id = l.job_id
LEFT JOIN jobs_status_timestamps t ON j.id = t.job_id
LEFT JOIN jobs_artifacts a ON j.id = a.job_id
'''


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

    @asynccontextmanager
    async def transaction(self) -> AsyncContextManager[asyncpg.Connection]:
        """
        Manages database transactions when run alongside other domain operations.

        This is helpful when we need to run domain logic that involves multiple database operations.

        For example, when we need to make an external API call and update the database in the same operation;
        if the external API call fails, we want to roll back the database changes.
        """
        async with self.pool.acquire() as conn:
            transaction = conn.transaction()
            await transaction.start()
            try:
                # Provide the connection to the caller
                yield conn
                # Commit the transaction if successful
                await transaction.commit()
            except Exception:
                # Rollback the transaction if an exception occurs within the context
                await transaction.rollback()
                raise

    async def connect(self):
        """Create a connection pool to the PostgreSQL database."""
        self.pool = await asyncpg.create_pool(self.connection_string)
        logger.info("Database connection pool and transaction manager created")

    async def close(self):
        """Close the database connection pool."""
        await self.pool.close()
        logger.info("Database connection pool closed")

    ### GCP Job Operations ###

    ### READ ###

    async def is_gcp_job(self,
                         job_id: str, user_id: str) -> bool:
        """
        Check if a job is a GCP job.

        Args:
            job_id (str): The ID of the job.
            user_id (str): The ID of the user who owns the job.

        Returns:
            bool: True if the job is a GCP job, False otherwise.
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('''
                SELECT provider FROM jobs
                WHERE id = $1 AND user_id = $2
            ''', job_id, user_id)

        result = row['provider'] == PROVIDER_GCP if row else False
        logger.info(f"Checked if job id: {job_id}, user id: {user_id} is a GCP job: {result}")
        return result

    async def get_jobs_by_status_gcp(self,
                                     statuses: Union[str, List[str]],
                                     cluster: Optional[str] = None,
                                     region: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Retrieve all jobs with a given status or statuses, optionally filtered by cluster and region.

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
        sql = f'''
        {SELECT_AND_JOIN_JOBS_SQL}
        WHERE provider = $1 AND j.status = ANY($2)
        '''
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

        logger.info(f"Retrieved {len(jobs)} GCP jobs with status: {statuses}, cluster: {cluster}, region: {region}")
        return jobs

    ### WRITE - transaction is handled by the caller ###

    @staticmethod
    async def add_job_gcp(conn: asyncpg.Connection,
                          job_data: Dict[str, Any]) -> None:
        """
        Add a new GCP job to the database.

        Note: Transaction is handled by the caller.

        Args:
            conn (asyncpg.Connection): The database connection.
            job_data (Dict[str, Any]): The job data.
        """
        job_id = job_data['job_id']
        user_id = job_data['user_id']
        workflow = job_data['workflow']
        args = json.dumps(job_data['args'])
        keep_alive = job_data['keep_alive']
        cluster = job_data['cluster']
        status = JOB_STATUS_NEW

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
        # Add record to the jobs timestamps table
        await conn.execute('''
            INSERT INTO jobs_status_timestamps (job_id, user_id, new_timestamp)
            VALUES ($1, $2, CURRENT_TIMESTAMP)
        ''', job_id, user_id)

        logger.info(f"Added GCP job with job_data: {job_data}")

    @staticmethod
    async def update_job_gcp(conn: asyncpg.Connection,
                             job_id: str, user_id: str, status: str,
                             vm_name: Optional[str] = None, region: Optional[str] = None) -> None:
        """
        Update the status of a job in the database, optionally with VM name and region.

        Note: Transaction is handled by the caller.

        Args:
            conn (asyncpg.Connection): The database connection.
            job_id (str): The ID of the job.
            user_id (str): The ID of the user.
            status (str): The new status of the job.
            vm_name (Optional[str]): The name of the VM assigned to the job.
            region (Optional[str]): The region of the VM assigned to the job.
        """
        # Update the job status in the main jobs table
        await conn.execute('''
            UPDATE jobs
            SET status = $1, updated_at = CURRENT_TIMESTAMP
            WHERE id = $2 AND user_id = $3
        ''', status, job_id, user_id)

        # Create the SQL query to update the job in the GCP jobs table
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
            sql += f' WHERE job_id = ${len(args) - 1} AND user_id = ${len(args)}'
            # Update the job in the GCP jobs table
            await conn.execute(sql, *args)

        # Update the jobs timestamps table
        status_col = f"{status.lower()}_timestamp"
        await conn.execute(f'''
            UPDATE jobs_status_timestamps
            SET {status_col} = CURRENT_TIMESTAMP
            WHERE job_id = $1 AND user_id = $2
        ''', job_id, user_id)

        logger.info(f"Updated job id: {job_id}, user id: {user_id} "
                    f"with status: {status}, vm_name: {vm_name}, region: {region}")

    ### LUM Job Operations ###

    ### READ ###

    async def get_jobs_by_status_lum(self,
                                     statuses: Union[str, List[str]]) -> List[Dict[str, Any]]:
        """
        Retrieve all jobs with a given status or statuses, as long as they have a tx_hash.

        Args:
            statuses (Union[str, List[str]]): The status(es) to filter jobs by.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries with job details.
        """
        # Convert single status to list for consistency
        if not isinstance(statuses, list):
            statuses = [statuses]

        # Build the SQL query
        sql = f'''
        {SELECT_AND_JOIN_JOBS_SQL}
        WHERE provider = $1 AND l.tx_hash IS NOT NULL AND j.status = ANY($2)
        '''
        # Execute the query
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql, PROVIDER_LUM, statuses)
        # Convert rows to dictionaries
        jobs = [self._row_to_dict(row) for row in rows]

        logger.info(f"Retrieved {len(jobs)} LUM jobs with status: {statuses}")
        return jobs

    ### WRITE - transaction is handled by the caller ###

    @staticmethod
    async def add_job_lum(conn: asyncpg.Connection,
                          job_data: Dict[str, Any]) -> None:
        """
        Add a new job to the database and initialize its status timestamp.

        Note: Transaction is handled by the caller.

        Args:
            conn (asyncpg.Connection): The database connection.
            job_data (Dict[str, Any]): The job data including workflow and tx_hash.
        """
        job_id = job_data['job_id']
        user_id = job_data['user_id']
        workflow = job_data['workflow']
        args = json.dumps(job_data['args'])
        status = JOB_STATUS_NEW

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
        # Add record to the jobs timestamps table
        await conn.execute('''
            INSERT INTO jobs_status_timestamps (job_id, user_id, new_timestamp)
            VALUES ($1, $2, CURRENT_TIMESTAMP)
        ''', job_id, user_id)

        logger.info(f"Added LUM job with job_data: {job_data}")

    @staticmethod
    async def update_job_lum(conn: asyncpg.Connection,
                             job_id: str, user_id: str,
                             status: Optional[str] = None,
                             tx_hash: Optional[str] = None,
                             lum_id: Optional[int] = None) -> None:
        """
        Update the status of a job in the database, optionally updating its transaction hash and LUM ID.

        Note: Transaction is handled by the caller.

        Args:
            conn (asyncpg.Connection): The database connection.
            job_id (str): The ID of the job.
            user_id (str): The ID of the user.
            status (str): The new status of the job.
            tx_hash (Optional[str]): The transaction hash of the job.
            lum_id (Optional[int]): The protocol job ID.
        """
        if status:
            # Update the job status and timestamp in the main jobs table
            await conn.execute('''
                UPDATE jobs
                SET status = $1, updated_at = CURRENT_TIMESTAMP
                WHERE id = $2 AND user_id = $3
            ''', status, job_id, user_id)

            # Update the job status timestamp in the jobs timestamps table
            status_col = f"{status.lower()}_timestamp"
            await conn.execute(f'''
                UPDATE jobs_status_timestamps
                SET {status_col} = CURRENT_TIMESTAMP
                WHERE job_id = $1 AND user_id = $2
            ''', job_id, user_id)

        # Update the job details in the LUM jobs table
        if tx_hash:
            sql = 'UPDATE jobs_lum SET tx_hash = $1 WHERE job_id = $2 AND user_id = $3'
            await conn.execute(sql, tx_hash, job_id, user_id)
        if lum_id:
            sql = 'UPDATE jobs_lum SET lum_id = $1 WHERE job_id = $2 AND user_id = $3'
            await conn.execute(sql, lum_id, job_id, user_id)

        logger.info(f"Updated LUM job id: {job_id}, user id: {user_id} "
                    f"with status: {status}, tx_hash: {tx_hash}, lum_id: {lum_id}")

    ### Job Operations ###

    ### READ ###

    async def get_job(self,
                      job_id: str, user_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a job from the database by its ID and user ID.

        Args:
            job_id (str): The ID of the job.
            user_id (str): The ID of the user.

        Returns:
            Optional[Dict[str, Any]]: A dictionary with job details if found, None otherwise.
        """
        # Execute the query
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(f'''
                {SELECT_AND_JOIN_JOBS_SQL}
                WHERE j.id = $1 AND j.user_id = $2
            ''', job_id, user_id)

        result = self._row_to_dict(row)
        logger.info(f"Retrieved job id: {job_id}, user id: {user_id}: {result}")
        return result

    async def get_recently_completed_jobs(self) -> List[Dict[str, Any]]:
        """
        Retrieve all jobs that have completed in the last X minutes;
        this is useful for background tasks that need to process completed jobs.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries with job details.
        """
        # Get the threshold from the config
        threshold = config.recently_completed_job_threshold_minutes

        # Execute the query
        async with self.pool.acquire() as conn:
            query = f'''
            {SELECT_AND_JOIN_JOBS_SQL}
            WHERE j.status = 'COMPLETED' AND j.updated_at > NOW() - INTERVAL '{threshold} minutes'
            '''
            rows = await conn.fetch(query)
        # Convert rows to dictionaries
        jobs = [self._row_to_dict(row) for row in rows]

        logger.info(f"Retrieved {len(jobs)} recently completed jobs")
        return jobs

    async def get_jobs_by_user_and_ids(self, user_id: str, job_ids: List[str]) -> List[Dict[str, Any]]:
        """
        Retrieve jobs for a specific user and the given job IDs.

        Args:
            user_id (str): The ID of the user.
            job_ids (List[str]): A list of job IDs to retrieve.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries with job details.
        """
        # Execute the query
        async with self.pool.acquire() as conn:
            query = f'''
            {SELECT_AND_JOIN_JOBS_SQL}
            WHERE j.user_id = $1 AND j.id = ANY($2)
            '''
            rows = await conn.fetch(query, user_id, job_ids)
        # Convert rows to dictionaries
        jobs = [self._row_to_dict(row) for row in rows]

        logger.info(f"Retrieved {len(jobs)} jobs for user id: {user_id}, job ids: {job_ids}")
        return jobs

    @staticmethod
    async def update_job_artifacts(conn: asyncpg.Connection,
                                   job_id: str, user_id: str, data: Dict[str, Any]) -> None:
        """
        Update the artifacts of a job in the database.

        Note: Transaction is handled by the caller.

        Args:
            conn (asyncpg.Connection): The database connection.
            job_id (str): The ID of the job.
            user_id (str): The ID of the user.
            data (Dict[str, Any]): The new artifact data.
        """
        await conn.execute('''
            INSERT INTO jobs_artifacts (job_id, user_id, data)
            VALUES ($1, $2, $3)
            ON CONFLICT (job_id, user_id) DO UPDATE
            SET data = $3, updated_at = CURRENT_TIMESTAMP
        ''', job_id, user_id, json.dumps(data))

        logger.info(f"Updated job id: {job_id}, user id: {user_id} with new artifacts: {data}")

    async def get_pending_lum_receipts(self) -> List[Dict[str, Any]]:
        """
        Retrieve all LUM jobs that:
        - Have status = NEW
        - Have a tx_hash
        - Don't have a lum_id
        - Were updated more than 10 seconds ago (to allow time for the job to be mined)

        Returns:
            List[Dict[str, Any]]: A list of jobs needing receipt processing
        """
        # Execute the query
        async with self.pool.acquire() as conn:
            sql = f'''
            {SELECT_AND_JOIN_JOBS_SQL}
            WHERE provider = $1 
            AND j.status = $2 
            AND l.tx_hash IS NOT NULL 
            AND l.lum_id IS NULL
            AND j.updated_at < NOW() - INTERVAL '10 seconds'
            '''
            rows = await conn.fetch(sql, PROVIDER_LUM, JOB_STATUS_NEW)
        # Convert rows to dictionaries
        jobs = [self._row_to_dict(row) for row in rows]

        logger.info(f"Retrieved {len(jobs)} LUM jobs needing receipt processing")
        return jobs

    # HELPER METHODS #

    @staticmethod
    def _row_to_dict(row: Optional[asyncpg.Record]) -> Optional[Dict[str, Any]]:
        """
        Convert a database row to a dictionary.

        Args:
            row: A row from the database.
        Returns:
            dict: A dictionary representation of the row.
        """
        # Return None if the row is empty
        if row is None:
            return None

        # Convert the artifacts JSON string to a dictionary
        artifacts = recursive_json_decode(row.get('artifacts'))
        # Build and return the job dictionary
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
                'lum_id': row['lum_id'],
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
            'artifacts': artifacts,
        }
