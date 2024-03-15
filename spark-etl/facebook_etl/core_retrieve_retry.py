import requests
from cred import fb_token, pg_password
from config import pg_name, pg_user, pg_host, pg_port, base_url, version
import psycopg2
import json
import traceback

conn = psycopg2.connect(dbname=pg_name, user =pg_user, host=pg_host, password=pg_password, port =pg_port)
cur = conn.cursor()

fetch_failed_query_sql = """
select 
    account_id
    ,report_id
    ,report_scope 
from facebook_staging.query_data
where staging_status = 'failed'
"""

load_retried_responses_sql = """
update facebook_staging.query_data 
set 
    data = %s
    ,staging_status = 'loaded' 
where 
    account_id = %s 
    and report_id = %s 
    and report_scope = %s 
    and staging_status = 'failed'
"""

def init_api_url():

    return f'{base_url}/{version}/'

def fetch_failed_queries():
    jobs_to_retry = []
    cur.execute(fetch_failed_query_sql)
    for job in cur.fetchall():
        jobs_to_retry.append({'account_id':job[0],'report_id':job[1],'report_scope':job[2]})

    return jobs_to_retry
    

def paginate_reports(report):
    report_response = requests.get(f"{init_api_url()}{report['report_id']}/insights?access_token={fb_token}")
    paging_needed = True
    while paging_needed == True:
        try:
            cur.execute(load_retried_responses_sql, (json.dumps(report_response.json()), report['account_id'], report['report_id'], report['report_scope']))
            conn.commit()
        except Exception as e:
            traceback.print_exc()
            print(e)
            print(report)
            print('Who knows what happened? Error on Job retry, py line 53')
            break

        try:
            paging_link = report_response.json()['paging']['next']
            report_response = requests.get(paging_link)
        except(KeyError):
            paging_needed = False



def query_retry():
    counter = 0
    reports_to_retry = fetch_failed_queries()
    if len(reports_to_retry) > 0:
        for rep in reports_to_retry:
            counter += 1
            paginate_reports(rep)
        print(f'Query Retry: Success:\n --- {counter} queries repulled')
    else:
        print('Query Retry: No Failed Reports in query manager')
        



if __name__== "__main__":
    query_retry()