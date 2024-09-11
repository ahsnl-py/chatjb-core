from pymongo import MongoClient
from datetime import datetime
from bson.objectid import ObjectId

from service.common_service import CommonService

# Dictionary to map request statuses to numbers
STATUS_CODES = {
    "in_progress": 0,
    "success": 1,
    "complete": 1,  # Mapping 'complete' and 'success' to the same value
    "failed": -1,
    "canceled": -2
}

class JobTracker:
    def __init__(self, db_uri="mongodb://localhost:27017/", db_name="core_dev"):
        """
        Initialize the JobTracker with a MongoDB connection.
        :param db_uri: MongoDB connection string
        :param db_name: Name of the database
        """
        self.client = MongoClient(db_uri)
        self.db = self.client[db_name]
        self.jobs_collection = self.db.requests
        self.util = CommonService()

    def create_job(self, job_type, run_date, status="in_progress"):
        """
        Create a new job session and store it in the database.
        :param job_type: Type of the job
        :param status: Status of the job, defaults to 'in_progress'
        :return: Inserted job's session_id (which is the MongoDB inserted_id)
        """
        job = {
            'job_type': job_type,
            'date': run_date,
            'start_date': self.util.get_date_time(),
            'end_date': None,
            'status': STATUS_CODES.get(status, 0)  # Default to 'in_progress' if unknown status
        }
        
        # Insert the job into the MongoDB collection
        result = self.jobs_collection.insert_one(job)
        session_id = str(result.inserted_id)        
        print(f"Job session created with session_id: {session_id}")
        return session_id

    def get_job(self, workflowname, date, status=1):
        """
        Retrieve job session details from the database by session_id.
        :param session_id: Unique identifier for the session
        :return: Job details or None if not found
        """
        job = self.jobs_collection.find_one({
            "job_type": workflowname,
            "date": date,
            "status": status
        })
        
        if job:
            job['_id'] = str(job['_id'])  # Convert ObjectId to string
            return job
        else:
            print("Job session not found.")
            return None

    def update_job(self, session_id, status=None, end_date=None):
        """
        Update an existing job session in the database.
        :param session_id: Unique identifier for the session
        :param status: Updated status of the job
        :param end_date: Updated end date of the job (can be None)
        :return: True if the job was updated, False otherwise
        """
        updated_data = {}
        if status:
            updated_data['status'] = STATUS_CODES.get(status, updated_data.get('status', 0))
        if end_date:
            updated_data['end_date'] = end_date
        else:
            updated_data['end_date'] = self.util.get_date_time() if status == 'success' else None
        
        result = self.jobs_collection.update_one({"_id": ObjectId(session_id)}, {"$set": updated_data})
        if result.matched_count > 0:
            print(f"Job session {session_id} updated.")
            return True
        else:
            print(f"Job session {session_id} not found.")
            return False

    def delete_job(self, session_id):
        """
        Delete a job session from the database.
        :param session_id: Unique identifier for the session
        :return: True if the job was deleted, False otherwise
        """
        result = self.jobs_collection.delete_one({"_id": (ObjectId)})
        if result.deleted_count > 0:
            print(f"Job session {session_id} deleted.")
            return True
        else:
            print(f"Job session {session_id} not found.")
            return False

    def list_jobs(self):
        """
        List all job sessions stored in the database.
        :return: List of all job sessions
        """
        jobs = self.jobs_collection.find()
        job_list = []
        for job in jobs:
            job['_id'] = str(job['_id'])  # Convert ObjectId to string
            job_list.append(job)
        return job_list

