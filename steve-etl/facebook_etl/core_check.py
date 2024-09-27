import requests
from cred import fb_token, pg_password
from config import pg_name, pg_user, pg_host, pg_port, base_url, version, timeout_limit
import psycopg2
import sys

conn = psycopg2.connect(dbname=pg_name, user =pg_user, host=pg_host, password=pg_password, port =pg_port)
cur = conn.cursor()


fetch_new_jobs_sql = """
        select id, report_id from facebook_staging.job_manager where report_status = 'Job Posted' or report_status like 'Job Running%' or report_status = 'Job Started'
        """

update_job_status_sql = """
        update facebook_staging.job_manager set
        report_status = %s
        where id = %s
        """

def init_api_url():
    return f'{base_url}/{version}/'

def fetch_posted_jobs():

    cur.execute(fetch_new_jobs_sql)
    jobs = []
    for i in cur.fetchall():
        jobs.append({'job_id':i[0],'report_id':i[1]})

    return jobs

def job_check(job_entry):
    check_url_call = requests.get(f"{init_api_url()}{job_entry['report_id']}?access_token={fb_token}")
    try:
        status = check_url_call.json()['async_status']
        if status == 'Job Running':
            percent_complete = check_url_call.json()['async_percent_completion']
            cur.execute(update_job_status_sql, (f'{status}: {percent_complete}', job_entry['job_id']))
        else:
            cur.execute(update_job_status_sql, (status, job_entry['job_id']))
        conn.commit()

        return status
    except(KeyError):
        print('\n\n-----Job Check Error: API limit likely reached---\n')
        print(check_url_call.json())
        print(eval(check_url_call.headers['x-business-use-case-usage']))
        proceed_input = input("Continue Job Check? y/n")
        if proceed_input == 'y':
            next
        else:
            cur.close()
            conn.close()
            sys.exit()

def core_check():
    jobs_checked = 0
    jobs_success = 0
    jobs_failed = 0
    job_other = 0
    for job in fetch_posted_jobs():
        stat = job_check(job)
        if stat == "Job Completed":
            jobs_success += 1
        elif stat == 'Job Running' or stat == 'Job Started':
            job_other += 1
        else:
            jobs_failed += 1
        jobs_checked += 1
    print(f'Job Check Completed\n ---Jobs checked:{jobs_checked}, Success: {jobs_success}, Jobs Failed: {jobs_failed}, Jobs Still running: {job_other}')

def manual_timeout_clear():
    check_reports_that_are_timed_out_sql = f"""
       select * from facebook_staging.job_manager
        where report_status like 'Job Running%'
        and now() at time zone 'utc' - time_generated > '00:{timeout_limit}:00';
    """
    cur.execute(check_reports_that_are_timed_out_sql)
    timeout_counter = 0
    for i in cur.fetchall():
        timeout_counter += 1

    if timeout_counter > 0:
        update_reports_that_are_timed_out_sql = f"""
        update facebook_staging.job_manager
            set report_status = 'Job Failed'
        where report_status like 'Job Running%'
            and now() at time zone 'utc' - time_generated > '00:{timeout_limit}:00';
        """
        cur.execute(update_reports_that_are_timed_out_sql)
        conn.commit()
        print(f'{timeout_counter} Jobs Timed Out. Need resubmit')

if __name__== "__main__":
    core_check()
    manual_timeout_clear()