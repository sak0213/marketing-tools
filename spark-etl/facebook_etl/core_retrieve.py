import requests
from cred import fb_token, pg_password
from config import pg_name, pg_user, pg_host, pg_port, base_url, version
import psycopg2
import json

conn = psycopg2.connect(dbname=pg_name, user =pg_user, host=pg_host, password=pg_password, port =pg_port)
cur = conn.cursor()

fetch_ready_jobs_sql = """
select id, report_id, report_scope, account_id from facebook_staging.job_manager where report_status = 'Job Completed'
"""
load_response_sql = """
insert into facebook_staging.query_data (account_id, data, report_id, report_scope, staging_status) values (%s, %s, %s, %s, %s)
"""
update_job_manager_sql = """
update facebook_staging.job_manager set
report_status = 'Job Processed'
where id = %s
"""
def init_api_url():

    return f'{base_url}/{version}/'

def get_ready_jobs():
    cur.execute(fetch_ready_jobs_sql)
    ready_jobs = []
    for i in cur.fetchall():
        ready_jobs.append({'job_id':i[0],'report_id':i[1],'report_scope':i[2], 'account_id':i[3]})
    
    return ready_jobs

def paginate_reports(report):
    report_url = f"{init_api_url()}{report['report_id']}/insights?access_token={fb_token}"
    report_response = requests.get(report_url)
    paging_needed = True
    while paging_needed == True:
        try:
            cur.execute(load_response_sql, (report['account_id'], json.dumps(report_response.json()), report['report_id'], report['report_scope'], 'loaded'))
            conn.commit()
        except Exception as e:
            print(e)
            print(report_response.json())
            print('Who knows what happened? Error on Job retrieve, py line 44')
            break

        try:
            paging_link = report_response.json()['paging']['next']
            report_response = requests.get(paging_link)
        except(KeyError):
            paging_needed = False
            cur.execute(update_job_manager_sql, (report['job_id'],))
            conn.commit()

def core_retrieve():
    job_list = get_ready_jobs()
    counter = 0
    reset_counter = 0
    for job in job_list:
        paginate_reports(job) #lets build a counting mechanism on here
        counter += 1
        reset_counter += 1

        if reset_counter >= (len(job_list) / 10):
            print(f'{counter} out of {len(job_list)} completed')
            reset_counter = 0
    print('All jobs successfully retrieved')
    

if __name__== "__main__":
    core_retrieve()