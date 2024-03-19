from cred import pg_password
from config import pg_name, pg_user, pg_host, pg_port, base_url, version
import psycopg2
import json
import datetime as dt

conn = psycopg2.connect(dbname=pg_name, user =pg_user, host=pg_host, password=pg_password, port =pg_port)
cur = conn.cursor()

job_manager_insert_sql = """
        insert into facebook_staging.job_manager 
        (account_id, report_id, query_range, report_scope, report_status) values
        (%s, %s, %s, %s, %s)
        """

params = {
    'advertiser_id': 'null',
    'fields': []
    }

def init_api_url():

    return f'{base_url}/{version}/'

def init_builder():
    """
    Initialize orchestrator builder. Pulls from \n
    DB to show account update status
    """

    builder = []

    initial_status_pull = """
        select id ,last_updated_key ,last_update_fact, ds.ad_count, name from tiktok.accounts
        left join (
            select account_id, count(*) as ad_count from tiktok.ads group by 1
            ) as ds on ds.account_id = id
        where status = 'active'
    """
    cur.execute(initial_status_pull)
    for row in cur.fetchall():
        builder.append({'act_id':str(row[0]), 'key_update':row[1].strftime('%Y-%m-%d'), 'fact_update':row[2].strftime('%Y-%m-%d'), 'ad_count':row[3], 'name':row[4]})

    return builder

def generate_timeframes(start, end = str(dt.date.today())):

    time_range_list = []
    day_range = dt.datetime.strptime(end, '%Y-%m-%d') - dt.datetime.strptime(start, '%Y-%m-%d')
    time_0 = start
    for i in range(0, day_range.days):
        time_range_list.append(time_0)
        time_0 = (dt.datetime.strptime(time_0,'%Y-%m-%d') + dt.timedelta(days=1)).strftime('%Y-%m-%d')
    
    return time_range_list

def generate_orchestrator(builder_entry, need_single_day=False):

    orchestrator = []

    field_keys_campaigns = [
                "campaign_id",
                "campaign_name",
                "advertiser_id",
                "objective_type",
                "objective",
                "create_time",
                "modify_time"
                ]

    field_keys_adgroups = [
                "adgroup_id",
                "adgroup_name",
                "advertiser_id",
                "optimization_goal",
                "bid_display_mode",
                "conversion_window",
                "create_time",
                "modify_time"
                ]
    
    field_keys_ads = [
                "ad_id",
                "advertiser_id",
                "ad_name",
                "create_time",
                "modify_time",
                "click_tracking_url",
                "impression_tracking_url", 
                "landing_page_url",
                "call_to_action",
                "identity_type",
                "ad_format",
                "campaign_id",
                "adgroup_id"
                ]

    dimension_fact_ds = [
                'ad_id',
                'stat_time_day'
                ]
    
    metric_fact_ds = [
                "spend",
                "impressions",
                "clicks",
                "likes",
                "comments",
                "shares",
                "follows",
                "video_play_actions",
                "video_views_p25",
                "video_views_p50",
                "video_views_p75",
                "video_views_p100"
                ]

    keys_campaign_call = {
                    'level':'key_campaign',
                    'url': f'{init_api_url()}campaign/get/',
                    'params':{
                        'advertiser_id':builder_entry['act_id'],
                        'fields': field_keys_campaigns
                        }
                    }

    keys_adgroup_call = {
                    'level':'key_adgroup',
                    'url': f'{init_api_url()}adgroup/get/',
                    'params':{
                        'advertiser_id':builder_entry['act_id'],
                        'fields': field_keys_adgroups
                        }
                    }

    keys_ad_call = {
                    'level':'key_ad',
                    'url': f'{init_api_url()}ad/get/',
                    'params':{
                        'advertiser_id':builder_entry['act_id'],
                        'fields': field_keys_ads
                        }
                    }

    page_size = 1000

    fact_ds_call = {
                    'level':'fact_dailystandard',
                    'url': f'{init_api_url()}report/integrated/get/',
                    'params': {
                        'advertiser_id':builder_entry['act_id'],
                        "service_type": "AUCTION",
                        "report_type": "BASIC",
                        "data_level": 'AUCTION_AD',
                        'dimensions': dimension_fact_ds,
                        'metrics': metric_fact_ds,
                        'start_date': builder_entry['fact_update'],
                        'end_date': str(dt.date.today()-dt.timedelta(days=1)),
                        'page_size': page_size
                        }
                    }
    
    orchestrator.append(keys_campaign_call)
    orchestrator.append(keys_adgroup_call)
    orchestrator.append(keys_ad_call)

    if builder_entry['fact_update'] >= str(dt.date.today()+dt.timedelta(days=-1)):
        print(f"Fact data is up to date for {builder_entry['act_id']}")
    
        return orchestrator

    elif builder_entry['fact_update'] < str(dt.date.today()):

        update_day_range_count = dt.datetime.strptime(str(dt.date.today()), '%Y-%m-%d') - dt.datetime.strptime(builder_entry['fact_update'], '%Y-%m-%d')
        
        if update_day_range_count.days >= 30:
            print('Generating daily fact calls: Date range beyond "stat_time_day" limit (30 days)')
            need_single_day=True
        
        if update_day_range_count.days * builder_entry['ad_count'] >= 1000:
            print('Generating daily fact calls: High expected result volume')
            need_single_day=True

        if need_single_day == False:
            orchestrator.append(fact_ds_call)

            return orchestrator

        elif need_single_day == True:

            for day in generate_timeframes(builder_entry['fact_update']):
                fact_ds_call['params']['start_date'] = day
                fact_ds_call['params']['end_date'] = day

                orchestrator.append(fact_ds_call)

            return orchestrator


def add_to_job_manager(orchestrator_row):
    sql = """insert into tiktok_staging.job_manager 
            (account_id, report_id, query_range, report_scope, report_status)
            values (%s, '000', %s, %s, 'Job Created')"""

    cur.execute(sql, (orchestrator_row['params']['advertiser_id'], json.dumps(orchestrator_row), orchestrator_row['level']))

def build_api_calls():

    for account in init_builder():
        for row in generate_orchestrator(account):
            add_to_job_manager(row)
        conn.commit()
        print(f"Jobs created for {account['name']}")

if __name__== "__main__":
    build_api_calls()
