from abc import abstractmethod
import datetime
import re
from urllib.parse import urlparse
import bs4
from typing import List, Dict
from bs4 import BeautifulSoup

from service.common_service import CommonService

class IDTS:
    """
    Abstract class for all common Data Transformation Service
    """

    @abstractmethod
    def jobcz(self, data) -> List[Dict[str, object]]:
        """
        Transform the main webpage of jobcz.cz by pages
        """
        pass

    @abstractmethod
    def jobcz_details(self, data):
        """
        Transform the details of each job per page/link
        """
        pass

class TransformationService(IDTS):

    def __init__(self) -> None:
        self.util = CommonService()

    def jobcz(self, data) -> List[Dict[str, object]]:

        def contains_currency(text):
            czk_pattern = r'\b(\d+\.?\d*)\s?(Kč|Kc|CZK|EUR)\b' 
            match = re.search(czk_pattern, text, re.IGNORECASE)
            return bool(match)

        def get_job_added(job) -> str:
            if len(job) > 0:
                return job[0].get_text().strip()
            return "N/A"

        def get_salary(salary:str):
            cs = [''.join(re.findall(r'\d+', s.strip())) for s in salary.split('–‍')]
            if len(cs) >= 2:
                return [cs[0], cs[1]]
            else:
                return [cs[0], 0]

        soup = BeautifulSoup(data, 'html.parser')
        job_listings = soup.find_all('article', class_='SearchResultCard')
        rows = list()
        for job in job_listings:
            salary, wfh, two_week_res, other_details = 'N/A', 'N/A', 'N/A', 'N/A'
            for jb in job.find('div', class_='SearchResultCard__body').find_all('span'):
                jb = jb.text.strip()
                if contains_currency(jb): salary = jb
                elif "home" in jb: wfh = jb
                elif "2 weeks" in jb: two_week_res = jb
                else: other_details = jb

            job_rating = 'N/A'
            job_info = list()
            for jf in job.find('footer').find_all('li', class_='SearchResultCard__footerItem'):
                jf = jf.text.strip()
                if 'rating' in jf: job_rating = jf
                else: job_info.append(jf)

            collection_attr = {
                "id": int(job.find('a', class_='link-primary SearchResultCard__titleLink').get('data-jobad-id')),
                "load_date": self.util.get_date(),
                "title": job.find('h2', class_='SearchResultCard__title').text.strip(),
                "link": job.find('a', class_='link-primary SearchResultCard__titleLink').get('href'),
                "date_added": get_job_added(job.find_all(class_=re.compile("SearchResultCard__status--[a-z]+"))),
                "salary_l": get_salary(salary)[0] if salary != 'N/A' else salary,
                "salary_h": get_salary(salary)[1] if salary != 'N/A' else salary,
                "salary_currency": re.findall(r'[a-zA-Z]+', salary)[0] if salary != 'N/A' else salary,
                "company": job_info[0],
                "city": job_info[1].split('–')[0].strip(),
                "district": job_info[1].split('–')[1].strip() if len(job_info[1].split('–')) >= 2 else "N/A",
                "work_from_home": wfh,
                "response_period": two_week_res,
                "rating": job_rating,
                "other_details": other_details
            }
            rows.append(collection_attr)

        return rows

    def jobcz_details(self, data):
        soup = BeautifulSoup(data, 'html.parser')
        # Extract the Introduction from the div with class 'mb-1000'
        introduction = soup.find('div', class_='mb-1000')

        # Extract the Job Offer from the tag with custom attribute 'data-jobad="body"'
        job_offer = soup.find('div', attrs={'data-jobad': 'body'})
    
        meta_tag = soup.find('meta', attrs={'property': 'og:url'})
        match = None
        if meta_tag and 'content' in meta_tag.attrs:
            og_url = meta_tag['content']
            parsed_url = urlparse(og_url)
            match = re.search(r'/(\d+)/', parsed_url.path)

        return [{
            "id": int(match.group(1)) if match else 0,
            "introduction": introduction.get_text(strip=True) if introduction else '',
            "job_descriptions": job_offer.get_text(strip=True) if job_offer else ''
        }]




    