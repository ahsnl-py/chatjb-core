import re
from urllib.parse import urlencode, urlparse, parse_qs
from datetime import datetime
import os
import uuid
import pandas as pd

class CommonService:

    def get_guid(self):
        return str(uuid.uuid4())

    def get_date(self):
        current_date = datetime.now()
        return current_date.strftime("%Y-%m-%d")

    def get_date_time(self):
        utc_now = datetime.utcnow()
        return utc_now.strftime('%Y-%m-%d %H:%M:%S.%f')

    def save_html_to_file(self, html_content, file_name):
        """
        Save the HTML content to a specified file.
        
        :param html_content: The HTML content to be saved (string).
        :param file_name: The name of the file to save the content to.
        """
        try:
            if not os.path.exists(file_name):
                self.create_path_if_not_exists(file_name)

            with open(file_name, 'w', encoding='utf-8') as file:
                # Write the HTML content to the file
                file.write(html_content)
            print(f"HTML content successfully saved to {file_name}")
        except Exception as e:
            print(f"An error occurred while saving the HTML file: {e}")

    def get_files(self, directory_path):
        """
        Extracts all files from the specified directory.

        :param directory_path: The path of the directory to scan.
        :return: A list of files in the directory.
        """
        try:
            # List to hold file names
            files_list = []

            # Walk through the directory
            for root, dirs, files in os.walk(directory_path):
                for file in files:
                    # Append each file to the list
                    files_list.append(os.path.join(root, file))
            
            return files_list
        except Exception as e:
            print(f"An error occurred: {e}")
            return []

    def merge_csv_files_in_folder(self, folder_path):
        # List to hold data frames from all CSV files
        data_frames = []

        # Iterate over all files in the folder
        for file_name in os.listdir(folder_path):
            # Check if the file is a CSV file
            if file_name.endswith('.csv'):
                file_path = os.path.join(folder_path, file_name)
                df = pd.read_csv(file_path)
                data_frames.append(df)

        merged_df = pd.concat(data_frames, ignore_index=True)

        return merged_df

    def save_pandas_to_file(self, rows, file_path):
        mode='a'
        header= False
        if not os.path.exists(file_path): 
            self.create_path_if_not_exists(file_path)
            mode, header = 'w', True
        df = pd.DataFrame(rows)    
        df.to_csv(file_path, mode=mode, index=False, header=header)

    def create_path_if_not_exists(self, file_path):
        dirname = os.path.dirname(file_path)
        if not os.path.exists(dirname): 
            os.makedirs(dirname)

    def parse_url(self, url):
        # Parse the URL
        parsed_url = urlparse(url)
        # Check if the URL has both scheme and netloc (basic validation)
        if not all([parsed_url.scheme, parsed_url.netloc]):
            raise ValueError("Invalid url: "+ url)

        # Get the base URL (scheme, domain, and path)
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"
        # Get the query parameters as a dictionary
        params = parse_qs(parsed_url.query)
        query_string = urlencode({k: v[0] for k, v in params.items()}, doseq=True)

        return {
            'base_url': base_url,
            'parameter_url': "?" + query_string
        }

    def fix_spacing(self, text):
        if isinstance(text, str):  # Check if the input is a string to avoid errors
            return re.sub(r'([.,;:?!()])(?!\d)([^\s])', r'\1 \2', text)
        return text