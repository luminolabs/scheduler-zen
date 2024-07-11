import logging
import sqlite3
from typing import Any, Dict, List

logging.basicConfig(level=logging.INFO)


class Database:
    """
    Manages the SQLite database for job tracking.
    """

    def __init__(self, db_path: str):
        """
        Initializes the Database with the path to the SQLite file.

        Args:
            db_path (str): The path to the SQLite database file.
        """
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        logging.info("Database initialized with db_path: %s", db_path)

    def create_tables(self) -> None:
        """
        Creates the necessary tables for job tracking.
        """
        logging.info("Creating tables in the database")
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS jobs (
                                id TEXT PRIMARY KEY,
                                status TEXT,
                                data TEXT)''')
        self.conn.commit()

    def add_job(self, job_id: str, status: str, data: str) -> None:
        """
        Adds a new job to the database.

        Args:
            job_id (str): The ID of the job.
            status (str): The status of the job.
            data (str): The data associated with the job.
        """
        logging.info("Adding job with id: %s, status: %s", job_id, status)
        self.cursor.execute('INSERT INTO jobs (id, status, data) VALUES (?, ?, ?)', (job_id, status, data))
        self.conn.commit()

    def update_job(self, job_id: str, status: str) -> None:
        """
        Updates the status of a job in the database.

        Args:
            job_id (str): The ID of the job.
            status (str): The new status of the job.
        """
        logging.info("Updating job id: %s to status: %s", job_id, status)
        self.cursor.execute('UPDATE jobs SET status = ? WHERE id = ?', (status, job_id))
        self.conn.commit()

    def get_job(self, job_id: str) -> Dict[str, Any]:
        """
        Retrieves a job from the database by its ID.

        Args:
            job_id (str): The ID of the job.

        Returns:
            Dict[str, Any]: A dictionary with job details.
        """
        logging.info("Getting job with id: %s", job_id)
        self.cursor.execute('SELECT * FROM jobs WHERE id = ?', (job_id,))
        row = self.cursor.fetchone()
        if row:
            return {'id': row[0], 'status': row[1], 'data': row[2]}
        return {}

    def get_jobs_by_status(self, status: str) -> List[Dict[str, Any]]:
        """
        Retrieves all jobs with a given status.

        Args:
            status (str): The status to filter jobs by.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries with job details.
        """
        logging.info("Getting jobs with status: %s", status)
        self.cursor.execute('SELECT * FROM jobs WHERE status = ?', (status,))
        rows = self.cursor.fetchall()
        return [{'id': row[0], 'status': row[1], 'data': row[2]} for row in rows]
