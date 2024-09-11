
from abc import ABC, abstractmethod
from typing import Callable
from service.exec_service import IExecutableService

from service.job_tracker import JobTracker

class WorkFlow(ABC):
    """
    Abstract workflow for common jobs
    """

    def __init__(self, conn_str, db_name="core_dev") -> None:
        self.tracker = JobTracker(conn_str, db_name)

    def run(self, workflow, date, service: IExecutableService):
        try:
            # job = self.tracker.get_job(workflow, date, 1)
            # if job:
            #     print(f"request {job['_id']} already completed with sucess.")
            #     return

            # sessionId = self.tracker.create_job(workflow, date)
            service.exec()
            # self.tracker.update_job(sessionId, "success")
        except ValueError as e:
            # self.tracker.update_job(sessionId, "failed")
            raise ValueError(f"Issue occur while runnig workflow: {workflow}. Error: {e}")