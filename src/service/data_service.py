

from abc import ABC, abstractmethod
from typing import Callable
import uuid
import os
import time
from service.api_service import ApiService
from service.common_service import CommonService

class IDataService(ABC):

    @abstractmethod
    def web_provider(self):
        """
        Abstract method to get data from extrenal source
        """

class DataService(IDataService):

    def __init__(self, isdebug=False) -> None:
        self.util = CommonService()
        self.isdebug = isdebug

    def web_provider(self, transformer: Callable[[str], object], **params):
        web_api = ApiService(params["base_url"])
        try:
            time.sleep(2)
            print('')
            print(f'Start request. GET /' + params["parameter_url"])
            html = web_api.get(params["parameter_url"])
            print('End request with sucess.')

            if not html: return None

            if self.isdebug:
                file = os.getcwd() + f'/data/raw/{self.util.get_date()}/{str(uuid.uuid4())}.txt'
                print('Response save for debugging.')
                self.util.save_html_to_file(html, file)

            transform_data = transformer(html)
            if (params.get('target_path', False)):
                self.util.save_pandas_to_file(transform_data, params["target_path"])
                return None
            else:
                return transform_data
        
        except ValueError as e:
            raise f'Issue while loading page request {params["parameter_url"]} with error {e}'
        