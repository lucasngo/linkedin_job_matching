import sys
# sys.path.insert(0, './scripts')
from session_manager import SessionManager
from job_detail_retriever import JobDetailRetriever
from job_search_retriever import JobSearchRetriever

def main():
    sessions = SessionManager('chrome')
    job_searcher = JobSearchRetriever(sessions.sessions)
    # list_of_jobs = ['data engineer', 'machine learning engineer', 'full stack engineer', 'frontend engineer', 
    #                 'backend engineer', 'devops engineer', 'product manager', 'product designer','AI trainer', 'Sport analysis']
    # for title in list_of_jobs:
    #     job_searcher.search_jobs(10, title)
    job_detail_retriever = JobDetailRetriever(sessions.sessions)
    job_detail_retriever.retrieve_job_details()

if __name__ == '__main__':
    main()
