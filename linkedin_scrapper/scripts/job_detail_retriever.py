import sys
sys.path.append('../')
from qdrant_utils import QdrantUtils
from helpers import clean_job_postings
from datetime import datetime
import pandas as pd
import requests
import time
from urllib.parse import quote
from openai import OpenAI
from qdrant_client.http import models
from qdrant_client.http.models import PointStruct
from session_manager import SessionManager
import os
load_dotenv()


OPEN_API_KEY = os.getenv("OPEN_API_KEY")

class JobDetailRetriever(QdrantUtils):
    def __init__(self, sessions):
        super().__init__()
        self.error_count = 0
        self.job_details_link = "https://www.linkedin.com/voyager/api/jobs/jobPostings/{}?decorationId=com.linkedin.voyager.deco.jobs.web.shared.WebFullJobPosting-65"
        self.client = OpenAI(api_key=OPEN_API_KEY)
        self.sessions = sessions
        self.session_index = 0
        self.variable_paths = pd.read_csv('../json_paths/data_variables.csv')

        self.headers = [{
            'Authority': 'www.linkedin.com',
            'Method': 'GET',
            'Path': '/voyager/api/search/hits?decorationId=com.linkedin.voyager.deco.jserp.WebJobSearchHitWithSalary-25&count=25&filters=List(sortBy-%3EDD,resultType-%3EJOBS)&origin=JOB_SEARCH_PAGE_JOB_FILTER&q=jserpFilters&queryContext=List(primaryHitType-%3EJOBS,spellCorrectionEnabled-%3Etrue)&start=0&topNRequestedFlavors=List(HIDDEN_GEM,IN_NETWORK,SCHOOL_RECRUIT,COMPANY_RECRUIT,SALARY,JOB_SEEKER_QUALIFIED,PRE_SCREENING_QUESTIONS,SKILL_ASSESSMENTS,ACTIVELY_HIRING_COMPANY,TOP_APPLICANT)',
            'Scheme': 'https',
            'Accept': 'application/vnd.linkedin.normalized+json+2.1',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cookie': "; ".join([f"{key}={value}" for key, value in session.cookies.items()]),
            'Csrf-Token': session.cookies.get('JSESSIONID').strip('"'),
            # 'TE': 'Trailers',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
            # 'X-Li-Track': '{"clientVersion":"1.12.7990","mpVersion":"1.12.7990","osName":"web","timezoneOffset":-7,"timezone":"America/Los_Angeles","deviceFormFactor":"DESKTOP","mpName":"voyager-web","displayDensity":1,"displayWidth":1920,"displayHeight":1080}'
            'X-Li-Track': '{"clientVersion":"1.13.5589","mpVersion":"1.13.5589","osName":"web","timezoneOffset":-7,"timezone":"America/Los_Angeles","deviceFormFactor":"DESKTOP","mpName":"voyager-web","displayDensity":1,"displayWidth":360,"displayHeight":800}'
        } for session in self.sessions]

        # self.proxies = [{'http': f'http://{proxy}', 'https': f'http://{proxy}'} for proxy in []]


    def get_job_details(self, job_ids):
        job_details = {}
        for job_id in job_ids:
            error = False
            try:
                details = self.sessions[self.session_index].get(self.job_details_link.format(job_id), headers=self.headers[self.session_index])#, proxies=self.proxies[self.session_index], timeout=5)
            except requests.exceptions.Timeout:
                print('Timeout for job {}'.format(job_id))
                error = True
            if details.status_code != 200:
                job_details[job_id] = -1
                print('Status code {} for job {}\nText: {}'.format(details.status_code, job_id, details.text))
                error = True
            if error:
                self.error_count += 1
                if self.error_count > 20:
                    raise Exception('Too many errors')
            else:
                self.error_count = 0
                job_details[job_id] = details.json()
            self.session_index = (self.session_index + 1) % len(self.sessions)
            time.sleep(.3)
        return job_details

    def get_embedding(self,text, model="text-embedding-3-small"):
        return self.client.embeddings.create(input=text, model=model).data[0].embedding

    def retrieve_job_details(self):
        total_insert = 0
        offset = None  # start from the beginning

        while True:
            job_to_scrape, next_offset = self.qdrant.scroll(
                collection_name=self.COLLECTION_NAME,
                scroll_filter=models.Filter(
                    must=[
                        models.FieldCondition(
                            key="embedding",
                            match=models.MatchValue(value=False)
                        ),
                        models.FieldCondition(
                            key="enriched",
                            match=models.MatchValue(value=False)
                        ),
                    ]
                ),
                offset=offset,
                limit=100,
                with_payload=True,
            )
            # Stop when no more pages
            if next_offset is None:
                break

            ids_to_scrape = [result.id for result in job_to_scrape]
            details = self.get_job_details(ids_to_scrape)
            cleaned_details = clean_job_postings(details)
            timestamp_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            embedding_points = []
            for key in cleaned_details.keys():
                obj = cleaned_details[key]
                try:
                    if "companies" in obj:
                        obj['full_text'] = f'''{obj['companies']['name']} with sizing {obj['companies']['company_size']} based in {obj['companies']['city']}, {obj['companies']['state']}, {obj['companies']['country']}.
                                                Company Overview: {obj['companies']['description']}.
                                                They are hiring {obj.get('jobs', {}).get('formatted_experience_level', '')} {obj['jobs']['description']} {obj['jobs']['location']}
                                            '''
                        meta_data = {'job_id':key,
                                    'work_type': obj['jobs']['work_type'],
                                    'location': obj['jobs']['location'],
                                    'job_posting_url':obj['jobs']['job_posting_url'],
                                    'original_listed_time':obj['jobs']['original_listed_time'],
                                    'remote_allowed':obj.get('jobs', {}).get('remote_allowed', None),
                                    'expiry':obj['jobs']['expiry'],
                                    'formatted_experience_level':obj.get('jobs', {}).get('formatted_experience_level', None),
                                    'title':obj['jobs']['title'],
                                    'listed_time':obj['jobs']['listed_time'],
                                    'city':obj['companies']['city'],
                                    'country':obj['companies']['country'],
                                    'added_ts':timestamp_str,
                                    'industries':obj.get('industries', {}).get('industry_names', None),
                                    'embedding':True,
                                    'enriched':True,
                                    'full_text':obj['full_text']
                                    }
                        embedding_points.append(PointStruct(
                                            id=key,
                                            vector=self.get_embedding(obj['full_text']),
                                            payload=meta_data
                                        ))
                except:
                    print('Failed to get embedding for job {}'.format(key))
                    continue
            self.qdrant.upsert(collection_name=self.COLLECTION_NAME, points=embedding_points)
            print(f'INSERTED {len(embedding_points)} NEW VALUES')
            total_insert += len(embedding_points)
            offset = next_offset
        print(f'JOB ENDED: TOTAL INSERTED {total_insert} NEW VALUES')

if __name__ == '__main__':
    sessions = SessionManager('chrome')
    job_detail_retriever = JobDetailRetriever(sessions.sessions)
    job_detail_retriever.retrieve_job_details()
    