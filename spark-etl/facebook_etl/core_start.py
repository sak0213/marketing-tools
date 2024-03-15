import requests
from cred import fb_token, pg_password
from config import pg_name, pg_user, pg_host, pg_port, base_url, version
import psycopg2
import json
import datetime as dt
import traceback

conn = psycopg2.connect(dbname=pg_name, user =pg_user, host=pg_host, password=pg_password, port =pg_port)
cur = conn.cursor()
orchestrator = []

job_manager_insert_sql = """
        insert into facebook_staging.job_manager 
        (account_id, report_id, query_range, report_scope, report_status) values
        (%s, %s, %s, %s, %s)
        """

def init_api_url():

    return f'{base_url}/{version}/'

def init_builder():
    """
    Initialize orchestrator builder. Pulls from \n
    DB to show account update status
    """

    builder = []

    initial_status_pull = "select id ,last_updated_key ,last_update_fact from facebook.accounts where status = 'active'"
    cur.execute(initial_status_pull)
    for row in cur.fetchall():
        builder.append({'act_id':row[0], 'key_update':row[1].strftime('%Y-%m-%d'), 'fact_update':row[2].strftime('%Y-%m-%d')})
    return builder

def init_orchestrator(builder):
    for row in builder:
        build_key_updates(row)
        build_fact_updates(row)
    return None

def date_list(start, end):
    
    time_range_list = []
    day_range = dt.datetime.strptime(end, '%Y-%m-%d') - dt.datetime.strptime(start, '%Y-%m-%d')
    time_0 = start

    for i in range(0, day_range.days):
        time_n = (dt.datetime.strptime(time_0,'%Y-%m-%d') + dt.timedelta(days=1)).strftime('%Y-%m-%d')
        time_frame = str({'since':time_0, 'until':time_0})
        time_range_list.append(time_frame)
        time_0 = time_n

    return time_range_list

def build_key_updates(builder_row):
    timeframe = str({'since':builder_row['key_update'], 'until':dt.date.today().strftime('%Y-%m-%d')})
    limit = 5000
    filters = "[{'field': 'spend','operator': 'GREATER_THAN','value': 0}]"

    orchestrator.append({'act':f"act_{builder_row['act_id']}", 'type':'campaign', 'params':{
            'access_token':fb_token,
            'fields':"account_id, campaign_id, campaign_name, created_time, updated_time,objective,optimization_goal",
            'time_range': timeframe,
            'level': 'campaign',
            'limit': limit,
            'filtering': filters
            }})

    orchestrator.append({'act':f"act_{builder_row['act_id']}", 'type':'adset', 'params':{
            'access_token':fb_token,
            'fields':"account_id, adset_id, adset_name, created_time, updated_time,objective,optimization_goal, attribution_setting",
            'time_range': timeframe,
            'level': 'adset',
            'limit': limit,
            'use_unified_attribution_setting':True,
            'filtering': filters
            }})

    orchestrator.append({'act':f"act_{builder_row['act_id']}", 'type':'ad', 'params':{
            'access_token':fb_token,
            'fields':"account_id, ad_id, ad_name, created_time, updated_time",
            'time_range': timeframe,
            'level': 'ad',
            'limit': limit,
            'filtering': filters
            }})

def build_fact_updates(builder_row):
    timeframes = date_list(builder_row['fact_update'], dt.date.today().strftime('%Y-%m-%d'))
    limit = 1000
    filters = "[{'field': 'spend','operator': 'GREATER_THAN','value': 0}]"

    for t_range in timeframes:
        orchestrator.append({'act':f"act_{builder_row['act_id']}", 'type':'daily_standard', 'params':{
            'access_token':fb_token,
            'fields':"ad_id, adset_id, campaign_id, date_start, impressions, inline_link_clicks, spend, video_play_actions, video_p25_watched_actions, video_p50_watched_actions, video_p75_watched_actions, video_p100_watched_actions, cost_per_thruplay",
            'time_range': t_range,
            'level': 'ad',
            'limit': limit,
            'time_increment': 1,
            'filtering': filters
        }})

        orchestrator.append({'act':f"act_{builder_row['act_id']}", 'type':'daily_actions', 'params':{
            'access_token':fb_token,
            'fields':"ad_id, adset_id, campaign_id, date_start, actions, action_values",
            'time_range': t_range,
            'level': 'ad',
            'limit': limit,
            'use_unified_attribution_setting': False,
            'action_attribution_windows':"['1d_click', '7d_click', '1d_view', '7d_view']",
            'time_increment': 1,
            'filtering': filters
        }})

    for incr in range(1,8):
        day_shifted = (dt.datetime.strptime(builder_row['fact_update'],'%Y-%m-%d') - dt.timedelta(days=incr)).strftime('%Y-%m-%d')
        orchestrator.append({'act':f"act_{builder_row['act_id']}", 'type':'daily_actions', 'params':{
            'access_token':fb_token,
            'fields':"ad_id, adset_id, campaign_id, date_start, actions, action_values",
            'time_range': str({'since':day_shifted, 'until':day_shifted}),
            'level': 'ad',
            'limit': limit,
            'use_unified_attribution_setting': False,
            'action_attribution_windows':"['1d_click', '7d_click', '1d_view', '7d_view']",
            'time_increment': 1,
            'filtering': filters
        }})

def init_requests():

    counter = 0
    reset_counter = 0
    try:
        for job in orchestrator:
            post_req = requests.post(url = f"{init_api_url()}{job['act']}/insights", params=job['params'])
            job_id = post_req.json()['report_run_id']
            cur.execute(job_manager_insert_sql, (job['act'][4:], job_id, json.dumps(job['params']), job['type'], 'Job Posted'))
            counter += 1
            reset_counter +=1

            if reset_counter >= (len(orchestrator) / 5):
                print(f'{counter} out of {len(orchestrator)} jobs started')
                reset_counter = 0
                conn.commit()
        conn.commit()
    except Exception as e:
        print(e)
        traceback.print_exc()
        print(job)
        print(post_req)
        print(post_req.text)
        

def core_start():
    init_orchestrator(init_builder()) #populate orchestrator/ request list
    print(f'FB Requests Gathered: {len(orchestrator)} requests')
    init_requests()
    print(f'All FB jobs posted from orchestrator at')
    cur.close()
    conn.close()

if __name__== "__main__":
    core_start()
