
import os
import pandas as pd
from service.api_service import ApiService
from service.exec_service import IExecutableService
from service.common_service import CommonService
from service.data_service import DataService
from service.rag_service import RAG
from service.transform_service import IDTS, TransformationService
from service.db_service import DbService

class JobCzLoader(IExecutableService):
    def __init__(self, dbservice:DbService, apiService:ApiService) -> None:
        self.transformer = TransformationService()
        self.util = CommonService()
        self.dbservice = dbservice
        self.rag = RAG(dbservice=dbservice, api=apiService)
        self.service = DataService()
        self.root_path_job_list = os.getcwd() + f'/data/stage/{self.util.get_date()}/'
        self.root_path_job_details_bu = os.getcwd() + f'/data/details/{self.util.get_date()}/jobcz_details_bu_{self.util.get_guid()}.csv'
    
    def exec(self):
        print("Start executing workflow jobcz")
        self.task_load_jobcz_list(pagelimit=1);
        self.task_load_jobcz_details();
        self.task_split_and_embed_jobcz_details();
        print("End executing workflow jobcz")


    def task_load_jobcz_list(self, pagelimit=5):
        for i in range(0, pagelimit):
            page_no = i+1
            print(f"Start request page [{page_no}/{pagelimit}]")
            params = {
                'base_url': "https://www.jobs.cz/en",
                'parameter_url': f"?page={page_no}",
                'target_path': f'{self.root_path_job_list}/jobcz_list_{page_no}.csv'
            }
            self.service.web_provider(self.transformer.jobcz, **params )
            print("Complete page request with sucess")

    def task_load_jobcz_details(self):
        print("Start task: Load job offer details ... ")
        
        df_job_list = self.util.merge_csv_files_in_folder(self.root_path_job_list)
        content = []
        print(f"Found {len(df_job_list)} jobs for date " + self.util.get_date())
        for i in range(0, len(df_job_list)): 
            url = self.util.parse_url(df_job_list.iloc[i]['link'])
            params = {
                'jobid': df_job_list.iloc[i]['id'],
                'base_url': url['base_url'],
                'parameter_url': url['parameter_url'],
                'target_path': self.root_path_job_details_bu
            }
            content_detail = self.service.web_provider(self.transformer.jobcz_details, **params)
            if content_detail:
                content.extend(content_detail)
        print("End task: Load job offer complete with success ")

        try:
            print("Cleaning job details ... ")
            if len(content) == 0:
                content = pd.read_csv(self.root_path_job_details_bu)
            df_job_detail = pd.DataFrame(content)
            df = self.merge_job_list_to_details(df_job_list, df_job_detail)
            
            print("Saving job details to db")
            self.dbservice.put_all_jobcz_details(df.to_dict(orient='records'))
        except ValueError as e:
            print("Issue while saving data to db. " + str(e))


    def embedded_jobcz_details_hf(self):
        print("Begin vectorizing job details ... ")
        for job in self.dbservice.get_jobcz_details():
            job['job_offer_embedding_hf'] = self.rag.generate_embidding(job['job_offer'])
            self.dbservice.put_jobcz_details_embedding(job)
        print("Vector embedding complete with success")

    def embedded_jobcz_details_openai(self):
        print("Begin vectorizing job details ... ")
        for job in self.dbservice.get_jobcz_details():
            job['job_offer_embedded'] = self.rag.generate_embedding_openai(job['job_offer'])
            self.dbservice.put_jobcz_details_embedding(job)
        print("Vector embedding complete with success")

    def task_split_and_embed_jobcz_details(self):
        print("Vector searching content ... ")
        collectionName = "jobcz_details"
        content = []
        for detail in self.dbservice.get_collection(collectionName, False).find({}):
            content.append(detail['job_offer'])

        self.dbservice.get_collection(collectionName+"_embedded").delete_many({})
        self.rag.split_text_content('\n\n'.join(content), f'{collectionName}_embedded')

    def merge_job_list_to_details(self, job_list, job_details):
        # Remove duplicates in place
        job_details.drop_duplicates(subset=['id'], inplace=True)
        job_list.drop_duplicates(subset=['id'], inplace=True)

        # Transform introduction and job descriptions
        job_details['Intro_Transform'] = (
            job_details['introduction']
            .str.replace(r'^(Introduction|Apply)', '', regex=True)
            .str.strip()
        )
        job_details['JobDesc_Transform'] = (
            job_details['job_descriptions']
            .str.replace(r'^(Job offer)', '', regex=True)
            .str.strip()
        )

        # Filter job_details with unwanted text and invalid ids
        job_details = job_details[
            ~job_details['Intro_Transform'].str.contains('ApplySave offerSave', na=False) & 
            (job_details['id'] != 0)
        ]

        # Merge the job_list and job_details on 'id'
        df_merge = pd.merge(job_list, job_details, on='id', how='inner')

        # Apply spacing fix
        df_merge['Intro_Transform'] = df_merge['Intro_Transform'].apply(self.util.fix_spacing)
        df_merge['JobDesc_Transform'] = df_merge['JobDesc_Transform'].apply(self.util.fix_spacing)

        # Fill NaN values for salary
        df_merge['salary_l'].fillna("0", inplace=True)
        df_merge['salary_h'].fillna("0", inplace=True)

        # Create the job_offer field by concatenating the formatted text
        df_merge['job_offer'] = (
            "Job Title: " + df_merge['title'] + "\n\n" +
            "Job Details:\n" + df_merge['Intro_Transform'] + "\n" + df_merge['JobDesc_Transform'] + "\n\n" +
            "Salary: " + df_merge['salary_l'] + "-" + df_merge['salary_h'] + "\n\n" +
            "Link: " + df_merge['link']
        )

        # Rename column and return the required columns
        df_merge.rename(columns={'id': 'job_id'}, inplace=True)
        return df_merge[['job_id', 'job_offer']]

            


        
         