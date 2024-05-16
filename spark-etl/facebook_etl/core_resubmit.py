import requests
from cred import pg_password
from config import pg_name, pg_user, pg_host, pg_port, base_url, version
import psycopg2
import json
import traceback

conn = psycopg2.connect(dbname=pg_name, user =pg_user, host=pg_host, password=pg_password, port =pg_port)
cur = conn.cursor()

fetch_failed_jobs_sql = """
select 
    id
    ,account_id
    ,query_range
    ,report_scope 
from facebook_staging.job_manager 
    where report_status = 'Job Failed'
"""
insert_new_request_sql = """
insert into facebook_staging.job_manager 
    (account_id, report_id, query_range, report_scope, report_status) values
    (%s, %s, %s, %s, %s)
"""
update_failed_job_sql = """
update facebook_staging.job_manager 
set report_status = %s 
where id = %s
"""

def init_api_url():

    return f'{base_url}/{version}/'

def build_resubmit_list():
    resub_list = []
    cur.execute(fetch_failed_jobs_sql)
    for row in cur.fetchall():
        try:
            resub_list.append({'job_id':row[0], 'account_id':row[1], 'query_range':eval(row[2]), 'report_scope':row[3]})
        except NameError as e:
            false = False
            resub_list.append({'job_id':row[0], 'account_id':row[1], 'query_range':eval(row[2]), 'report_scope':row[3]})


    return resub_list

def submit_request(failed_job):

    new_req = requests.post(url = f"{init_api_url()}act_{failed_job['account_id']}/insights", params=failed_job['query_range'])
    try:
        new_job_id = new_req.json()['report_run_id']
    except(KeyError):
        print(new_req.json())
        traceback.print_exc()
    cur.execute(insert_new_request_sql, (failed_job['account_id'], new_job_id, json.dumps(failed_job['query_range']), failed_job['report_scope'], 'Job Posted'))
    cur.execute(update_failed_job_sql, ('Job Resubmitted', failed_job['job_id']))
    conn.commit()

def process_failed_jobs():
    counter = 0
    for job in build_resubmit_list():
        submit_request(job)
        counter += 1
    
    print(f'Resubmitted {counter} jobs to Job Manager')

if __name__== "__main__":
    process_failed_jobs()

