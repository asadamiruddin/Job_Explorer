import requests
import os
import ast
import json
from datetime import datetime
from dotenv import load_dotenv
from configparser import ConfigParser

load_dotenv()

config = ConfigParser()
config.read('config.ini')

# api params
api_key = os.environ.get('muse_key')
muse_root_URL = config.get('API', 'ROOT_URL')
#location = config.get('API', 'LOCATION')
descending = config.get('API', 'DESCENDING')
categories = ast.literal_eval(config.get('API','CATEGORIES'))
muse_params = {'api_key': api_key, 'descending': descending}
page_limit = int(config.get('API', 'PAGE_LIMIT'))
max_retry = int(config.get('API', 'MAX_RETRY'))
result_key = config.get('API', 'RESPONSE_RESULT_KEY')
max_page_key = config.get('API', 'RESPONSE_MAX_PAGE_KEY')
max_job_key = config.get('API', 'RESPONSE_MAX_JOBS_KEY')
jobs_per_page = int(config.get('API', 'JOBS_PER_PAGE'))
job_id_key = config.get('API', 'JOB_ID_KEY')
job_title_key = config.get('API', 'JOB_TITLE_KEY')
publication_date_key = config.get('API', 'PUBLICATION_DATE_KEY')
job_description_key = config.get('API', 'JOB_DESCRIPTION_KEY')
location_key = config.get('API', 'LOCATION_KEY')
location_name = config.get('API', 'LOCATION_NAME')
job_category_key = config.get('API', 'JOB_CATEGORY_KEY')
job_category_name = config.get('API', 'JOB_CATEGORY_NAME')
job_level_key = config.get('API', 'JOB_LEVEL_KEY')
job_level_name = config.get('API', 'JOB_LEVEL_NAME')
company_key = config.get('API', 'COMPANY_KEY')
company_id = config.get('API', 'COMPANY_ID')
company_name = config.get('API', 'COMPANY_NAME')
job_type_key = config.get('API', 'JOB_TYPE_KEY')
job_reference_page_key = config.get('API', 'JOB_REFERENCE_PAGE_KEY')
job_reference_page_name = config.get('API', 'JOB_REFERENCE_PAGE_NAME')

datetime_today = datetime.today().strftime('%Y-%m-%d %H:%M:%S')


def get_jobs_by_cat(page_limit = page_limit, root_URL = muse_root_URL, 
                    parameters = muse_params, max_retry = max_retry, 
                    result_key = result_key):

    retries = 0
    all_jobs_by_cat = []
    parameters['page'] = 1

    while parameters["page"] <= page_limit:
        if retries == max_retry:
            with open('json/unfinished_jobs.json', 'w') as f:
                json.dump(all_jobs_by_cat, f)
            raise Exception(job_page.status_code, job_page.reason,
                 'pages extracted:', parameters["page"]-1)
        
        job_page = requests.get(root_URL, parameters)

        if job_page.status_code != 200:
            retries += 1
            continue
        
        job_page_json = job_page.json()

        if not job_page_json[result_key]:
            break

        all_jobs_by_cat.append(job_page_json)

        parameters["page"] += 1

    return all_jobs_by_cat


def parse_jobs_in_page(job_page, when_parsed, 
                        result_key = result_key):
    jobs_in_page = []
    for i in range(len(job_page)):
        job = job_page[i]            
        location_array = [location[location_name] for location in job[location_key]]
        category_array = [category[job_category_name] for category in job[job_category_key]]
        levels_array = [level[job_level_name] for level in job[job_level_key]]
        job_details = (job[job_id_key],
                    job[job_title_key],
                    job[publication_date_key],
                    job[job_description_key],
                    ','.join(location_array),
                    ','.join(category_array),
                    ','.join(levels_array),
                    job[company_key][company_id],
                    job[company_key][company_name],
                    job[job_type_key],
                    job[job_reference_page_key][job_reference_page_name],
                    when_parsed
                )
        jobs_in_page.append(job_details)
    return jobs_in_page


def prepare_jobs(all_jobs):
    prepared_jobs=[]
    if all_jobs: 
        i = 0
        while i < len(all_jobs):
            job_page = all_jobs[i][result_key]
            if not job_page:
                break
            jobs_in_page = parse_jobs_in_page(job_page, datetime_today)
            prepared_jobs.extend(jobs_in_page)
            i += 1
    return prepared_jobs


def get_jobs():
    all_jobs = []
    for cat in categories:
        muse_params['category'] = cat
        all_jobs_by_cat = get_jobs_by_cat(parameters = muse_params)
        prepared_jobs = prepare_jobs(all_jobs = all_jobs_by_cat)
        all_jobs.extend(prepared_jobs)
    return all_jobs