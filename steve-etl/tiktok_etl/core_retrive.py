from cred import pg_password, tt_token
from config import pg_name, pg_user, pg_host, pg_port
import psycopg2
import json
import requests


conn = psycopg2.connect(dbname=pg_name, user =pg_user, host=pg_host, password=pg_password, port =pg_port)
cur = conn.cursor()

job_fetch_sql = """
select id, account_id, query_range, report_scope from tiktok_staging.job_manager
where report_status = 'Job Created'
"""

query_data_insert = """
insert into tiktok_staging.query_data (account_id, data, report_id, report_scope, staging_status) values (%s, %s, %s, %s, %s)
"""

update_job_manager_sql = """
update tiktok_staging.job_manager set
report_status = 'Job Processed'
where id = %s
"""

def fetch_call_list():
    cur.execute(job_fetch_sql)
    call_list = []

    for row in cur.fetchall():
        call_list.append({'id':row[0], 'account_id':row[1], 'query':row[2], 'level':row[3]})

    return call_list


def initiate_call(call_row):
    query = eval(call_row['query'])

    r = requests.get(url=query['url'], headers={"Access-Token":tt_token}, json=query['params'])
    return r.json()

def insert_data(data, act_id, level, rpt_id):
    cur.execute(query_data_insert, (act_id, json.dumps(data), rpt_id, level, 'loaded'))

def update_manager(call_row):
    cur.execute(update_job_manager_sql, (call_row['id'],))

def core_retrieve():
    for row in fetch_call_list():
        insert_data(initiate_call(row), row['account_id'], row['level'], row['id'])
        update_manager(row)
        conn.commit()


if __name__== "__main__":
    core_retrieve()