import aiosqlite
import json
import logging
from typing import List, Dict, Any, Optional

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


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
                                workflow TEXT,
                                args TEXT,
                                keep_alive INTEGER,
                                cluster TEXT,
                                status TEXT,
                                vm_name TEXT)''')
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
        status = 'NEW'

        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('INSERT INTO jobs (id, workflow, args, keep_alive, cluster, status) VALUES (?, ?, ?, ?, ?, ?)',
                             (job_id, workflow, args, keep_alive, cluster, status))
            await db.commit()

        logger.info(f"Added job with id: {job_id}, workflow: {workflow}, cluster: {cluster}")
        return job_id

    async def update_job(self, job_id: str, status: str, vm_name: Optional[str] = None) -> None:
        """
        Update the status of a job in the database.

        Args:
            job_id (str): The ID of the job.
            status (str): The new status of the job.
            vm_name (Optional[str]): The name of the VM assigned to the job.
        """
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('UPDATE jobs SET status = ?, vm_name = ? WHERE id = ?', (status, vm_name, job_id))
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
                if row:
                    return {
                        'job_id': row[0],
                        'workflow': row[1],
                        'args': json.loads(row[2]),
                        'keep_alive': bool(row[3]),
                        'cluster': row[4],
                        'status': row[5],
                        'vm_name': row[6]
                    }
        logger.info(f"Retrieved job with id: {job_id}")
        return None

    async def get_jobs_by_status(self, status: str) -> List[Dict[str, Any]]:
        """
        Retrieve all jobs with a given status.

        Args:
            status (str): The status to filter jobs by.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries with job details.
        """
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute('SELECT * FROM jobs WHERE status = ?', (status,)) as cursor:
                rows = await cursor.fetchall()
                jobs = [
                    {
                        'job_id': row[0],
                        'workflow': row[1],
                        'args': json.loads(row[2]),
                        'keep_alive': bool(row[3]),
                        'cluster': row[4],
                        'status': row[5],
                        'vm_name': row[6]
                    } for row in rows
                ]
        logger.info(f"Retrieved {len(jobs)} jobs with status: {status}")
        return jobs

    async def get_pending_jobs(self) -> List[Dict[str, Any]]:
        """
        Retrieve all pending jobs with their cluster information.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries containing job information.
        """
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute('SELECT id, cluster FROM jobs WHERE status = "PENDING"') as cursor:
                rows = await cursor.fetchall()
                jobs = [{'job_id': row[0], 'cluster': row[1]} for row in rows]
        logger.info(f"Retrieved {len(jobs)} pending jobs")
        return jobs

    async def get_pending_jobs_count(self) -> int:
        """
        Retrieve the count of pending jobs.

        Returns:
            int: The number of pending jobs.
        """
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute('SELECT COUNT(*) FROM jobs WHERE status = "PENDING"') as cursor:
                (count,) = await cursor.fetchone()
        logger.info(f"Retrieved pending jobs count: {count}")
        return count

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
