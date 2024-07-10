# database.py

import asyncio
import json
import logging
from typing import List, Optional

import aiosqlite

from models import Job, JobStatus


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Database:
    """
    Handles database operations for storing and retrieving job information.
    """

    def __init__(self, db_name: str = "scheduler-zen.sqlite"):
        """
        Initialize the Database with the given database name.

        Args:
            db_name (str): Name of the SQLite database file.
        """
        self.db_name = db_name

    async def create_tables(self):
        """Create necessary tables if they don't exist."""
        async with aiosqlite.connect(self.db_name) as db:
            await db.execute('''
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT NOT NULL,
                    workflow TEXT NOT NULL,
                    args TEXT NOT NULL,
                    keep_alive INTEGER NOT NULL
                    vm_name TEXT,
                )
            ''')
            await db.commit()
        logger.info("Database tables created or verified.")

    async def add_job(self, job: Job) -> str:
        """
        Add a new job to the database.

        Args:
            job (Job): The job to add.

        Returns:
            str: The ID of the added job.
        """
        async with aiosqlite.connect(self.db_name) as db:
            await db.execute('''
                INSERT INTO jobs (id, status, workflow, args, keep_alive)
                VALUES (?, ?, ?, ?, ?)
            ''', (job.id, job.status.value, job.workflow, json.dumps(job.args), int(job.keep_alive)))
            await db.commit()
        logger.info(f"Added job {job.id} to the database.")
        return job.id

    async def get_job(self, job_id: str) -> Optional[Job]:
        """
        Retrieve a job by its ID.

        Args:
            job_id (str): The ID of the job to retrieve.

        Returns:
            Optional[Job]: The job if found, None otherwise.
        """
        async with aiosqlite.connect(self.db_name) as db:
            async with db.execute('SELECT * FROM jobs WHERE id = ?', (job_id,)) as cursor:
                row = await cursor.fetchone()
                if row:
                    logger.info(f"Retrieved job {job_id} from the database.")
                    return self._get_job_from_row(row)
        logger.info(f"Job {job_id} not found in the database.")
        return None

    async def update_job(self, job_id: str, status: JobStatus, vm_name: Optional[str] = None):
        """
        Update a job's status and optionally its VM name.

        Args:
            job_id (str): The ID of the job to update.
            status (JobStatus): The new status of the job.
            vm_name (Optional[str]): The name of the VM running the job, if applicable.
        """
        async with aiosqlite.connect(self.db_name) as db:
            await db.execute('''
                UPDATE jobs SET status = ?, vm_name = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (status.value, vm_name, job_id))
            await db.commit()
        log_message = f"Updated job {job_id} in the database. New status: {status}"
        if vm_name:
            log_message += f", VM name: {vm_name}"
        logger.info(log_message)

    async def get_jobs(self, status: Optional[JobStatus] = None) -> List[Job]:
        """
        Retrieve all jobs with a specific status.

        Args:
            status (JobStatus): The status to filter by.

        Returns:
            List[Job]: A list of jobs with the specified status.
        """
        sql, params = 'SELECT * FROM jobs', []
        if status:
            sql += ' WHERE status = ?'
            params += status.value
        jobs = []
        async with aiosqlite.connect(self.db_name) as db:
            async with db.execute(sql, params) as cursor:
                async for row in cursor:
                    jobs.append(self._get_job_from_row(row))
        log_message = f"Retrieved {len(jobs)} jobs"
        if status:
            log_message += f" with status {status}"
        log_message += " from the database."
        logger.info(log_message)
        return jobs

    @staticmethod
    def _get_job_from_row(row) -> Job:
        """
        Create a Job object from a database row.

        Args:
            row: The database row to create the Job from.
        Returns:
            Job: The Job object created from the row.
        """
        return Job(
            id=row[0],
            model=row[1],
            dataset=row[2],
            hyperparameters=eval(row[3]),
            status=JobStatus(row[4]),
            vm_name=row[5],
            created_at=row[6],
            updated_at=row[7]
        )


