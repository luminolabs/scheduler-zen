import aiosqlite
import json
from typing import List, Dict, Any, Optional, Union

from app.utils import JOB_STATUS_NEW, setup_logger

# Set up logging
logger = setup_logger(__name__)


class Database:
    """Manages the SQLite database for job tracking."""

    def __init__(self, db_path: str):
        """
        Initialize the Database with the path to the SQLite file.

        Args:
            db_path (str): The path to the SQLite database file.
        """
        self.db_path = db_path
        logger.info(f"Database initialized with db_path: {db_path}")

    async def create_tables(self) -> None:
        """Create the necessary tables for job tracking."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('''CREATE TABLE IF NOT EXISTS jobs (
                                id TEXT PRIMARY KEY,
                                created_at DATETIME,
                                updated_at DATETIME,
                                workflow TEXT,
                                args TEXT,
                                keep_alive INTEGER,
                                cluster TEXT,
                                status TEXT,
                                vm_name TEXT,
                                region TEXT)''')
            await db.commit()
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
        keep_alive = int(job_data['keep_alive'])
        cluster = job_data['cluster']
        status = JOB_STATUS_NEW

        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('INSERT INTO jobs (id, created_at, workflow, args, keep_alive, cluster, status) '
                             'VALUES (?, datetime(), ?, ?, ?, ?, ?)',
                             (job_id, workflow, args, keep_alive, cluster, status))
            await db.commit()

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
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('UPDATE jobs SET status = ?, vm_name = ?, region = ?, updated_at = datetime() WHERE id = ?',
                             (status, vm_name, region, job_id))
            await db.commit()
        logger.info(f"Updated job id: {job_id} to status: {status}")

    async def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a job from the database by its ID.

        Args:
            job_id (str): The ID of the job.

        Returns:
            Optional[Dict[str, Any]]: A dictionary with job details if found, None otherwise.
        """
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute('SELECT * FROM jobs WHERE id = ?', (job_id,)) as cursor:
                row = await cursor.fetchone()
                logger.info(f"Retrieved job with id: {job_id}")
                return self._row_to_dict(row)

    async def get_jobs_by_status(self, statuses: Union[str, List[str]]) -> List[Dict[str, Any]]:
        """
        Retrieve all jobs with a given status.

        Args:
            statuses (str): The status to filter jobs by.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries with job details.
        """
        # Ensure status is a list
        if not isinstance(statuses, list):
            statuses = [statuses]
        # Generate the required number of placeholders
        placeholders = ','.join('?' for _ in statuses)
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(f'SELECT * FROM jobs WHERE status IN ({placeholders})', [*statuses]) as cursor:
                rows = await cursor.fetchall()
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
        # This is a simple implementation. You might want to use a more robust method in production.
        import uuid
        return str(uuid.uuid4())

    @staticmethod
    def _row_to_dict(row: Optional[List]) -> Optional[Dict[str, Any]]:
        """
        Convert a database row to a dictionary.

        Args:
            row: A row from the database.
        Returns:
            dict: A dictionary representation of the row.
        """
        return {
            'job_id': row[0],
            'created_at': row[1],
            'updated_at': row[2],
            'workflow': row[3],
            'args': json.loads(row[4]),
            'keep_alive': bool(row[5]),
            'cluster': row[6],
            'status': row[7],
            'vm_name': row[8],
            'region': row[9],
        } if row else None