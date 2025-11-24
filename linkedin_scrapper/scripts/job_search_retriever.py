import sys
sys.path.append('../')
from qdrant_utils import QdrantUtils
from session_manager import SessionManager
from qdrant_client.http.models import PointStruct
from helpers import strip_val
from datetime import datetime
from tqdm import tqdm
import pandas as pd
from urllib.parse import quote


class JobSearchRetriever(QdrantUtils):
    def __init__(self, sessions):
        super().__init__()
        self.job_search_link = 'https://www.linkedin.com/voyager/api/voyagerJobsDashJobCards?decorationId=com.linkedin.voyager.dash.deco.jobs.search.JobSearchCardsCollection-187&count=100&q=jobSearch&query=(origin:JOB_SEARCH_PAGE_OTHER_ENTRY,keywords:{job_title},selectedFilters:(sortBy:List(DD)),spellCorrectionEnabled:true)&start={start}'
        self.sessions = sessions
        self.session_index = 0
        self.headers = [{
            'Authority': 'www.linkedin.com',
            'Method': 'GET',
            'Path': 'voyager/api/voyagerJobsDashJobCards?decorationId=com.linkedin.voyager.dash.deco.jobs.search.JobSearchCardsCollection-187&count=25&q=jobSearch&query=(origin:JOB_SEARCH_PAGE_OTHER_ENTRY,selectedFilters:(sortBy:List(DD)),spellCorrectionEnabled:true)&start=0',
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
        self.dummy_vector = [0.0] * 1536

    def get_jobs(self, job_title, start = 0):
        results = self.sessions[self.session_index].get(self.job_search_link.format(job_title = job_title, start = start), headers=self.headers[self.session_index])
        self.session_index = (self.session_index + 1) % len(self.sessions)

        if results.status_code != 200:
            raise Exception('Status code {} for search\nText: {}'.format(results.status_code, results.text))
        results = results.json()
        job_ids = []

        for r in results['included']:
            if r['$type'] == 'com.linkedin.voyager.dash.jobs.JobPostingCard' and 'referenceId' in r:
                object_value = {'job_id':int(strip_val(r['jobPostingUrn'], 1)), 'sponsored': False, 'title': r.get('jobPostingTitle')}
                for x in r['footerItems']:
                    if x.get('type') == 'PROMOTED':
                        object_value['sponsored'] = True
                        break
                job_ids.append(object_value)

        return job_ids
    
    def split_existing_jobs(self, jobs, existing_ids = []):
        ids = [job['job_id'] for job in jobs]
        if len(existing_ids) == 0:
            results = self.qdrant.retrieve(collection_name=self.COLLECTION_NAME, ids=ids)
            existing_ids = [job.id for job in results]
        to_insert = [job for job in jobs if job['job_id'] not in existing_ids]
        return to_insert, existing_ids+ids

    
    def search_jobs(self, loop_time, job_title):
        info = self.qdrant.get_collection(collection_name=self.COLLECTION_NAME)
        print(f"Total points now: {info.points_count}")

        existing_ids = []
        points = []
        for i in tqdm(range(loop_time)):
            jobs = self.get_jobs(quote(job_title),i*100)
            to_insert, existing_ids = self.split_existing_jobs(jobs, existing_ids)
            timestamp_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if len(to_insert) == 0:
                print('No new jobs found, breakkk!!!')
                break
            for job in to_insert:
                job['embedding'] = False
                job['enriched'] = False
                job['added_ts'] = timestamp_str
                points.append(PointStruct(id=job['job_id'], vector=self.dummy_vector, payload=job))
            if len(points) > 50:
                for j in range(0,len(points), 50):
                    if j >= len(points) - 50:
                        batch = points[j:]
                    else:
                        batch = points[j:j + 50]
                    self.qdrant.upsert(collection_name=self.COLLECTION_NAME, points=batch)
                points = []
            else:
                self.qdrant.upsert(collection_name=self.COLLECTION_NAME, points=points)
                points = []
            print('INSERTED {} NEW VALUES'.format(len(to_insert)))
        info_new = self.qdrant.get_collection(collection_name=self.COLLECTION_NAME)
        print(f"Total points inserted: {info_new.points_count - info.points_count}")
        print(f"Total points now: {info_new.points_count}")

if __name__ == '__main__':
    sessions = SessionManager('chrome')
    job_searcher = JobSearchRetriever(sessions.sessions)
    job_searcher.search_jobs(int(sys.argv[1]), sys.argv[2])