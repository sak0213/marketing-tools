
create or replace procedure facebook_staging.key_standard()
language plpgsql
as $$ begin

-- Clearn query data
update facebook_staging.query_data
set staging_status = 'failed'
where length(data ->> 'error') > 0;


-- Parse and insert campaign keys
insert into facebook.campaigns (
    id
    ,account_id
    ,name
    ,objective
    ,optimization_goal
    ,date_created
    ,date_updated
)
select 
	cast(elems ->>'campaign_id' as bigint) as id
	,cast(elems ->> 'account_id' as bigint) as account_id
	,cast(elems ->>'campaign_name' as varchar(255)) as name
	,cast(elems ->> 'objective' as varchar(50)) as objective
	,cast(elems ->> 'optimization_goal' as varchar(50)) as optimization_goal
	,cast(elems ->> 'created_time' as date) as date_created
	,cast(elems ->> 'updated_time' as date) as date_updated
from(
select jsonb_array_elements(data::jsonb->'data') elems
from facebook_staging.query_data where staging_status = 'loaded' and report_scope = 'campaign') a

        on conflict on constraint campaigns_pkey do update
            set
                name = excluded.name
                ,date_updated = excluded.date_updated;

-- update campaign queue
update facebook_staging.query_data
set staging_status = 'parsed'
where report_scope = 'campaign' and staging_status = 'loaded';

-- Parse and insert adset keys
insert into facebook.adgroups (
    id
    ,account_id
    ,name
    ,objective
    ,optimization_goal
    ,attribution_setting
    ,date_created
    ,date_updated
)
select 
	cast(elems ->>'adset_id' as bigint) as id
	,cast(elems ->> 'account_id' as bigint) as account_id
	,cast(elems ->>'adset_name' as varchar(255)) as name
	,cast(elems ->> 'objective' as varchar(50)) as objective
	,cast(elems ->> 'optimization_goal' as varchar(50)) as optimization_goal
    ,cast(elems ->> 'attribution_setting' as varchar(50)) as attribution_setting
	,cast(elems ->> 'created_time' as date) as date_created
	,cast(elems ->> 'updated_time' as date) as date_updated
from(
select jsonb_array_elements(data::jsonb->'data') elems
from facebook_staging.query_data where staging_status = 'loaded' and report_scope = 'adset') a

        on conflict on constraint  adgroups_pkey do update
            set
                name = excluded.name
                ,date_updated = excluded.date_updated;

-- update adgroup queue
update facebook_staging.query_data
set staging_status = 'parsed'
where report_scope = 'adset' and staging_status = 'loaded';

-- Parse and insert ad keys
insert into facebook.ads (
    id
    ,account_id
    ,name
    ,date_created
    ,date_updated
)
select 
	cast(elems ->>'ad_id' as bigint) as id
	,cast(elems ->> 'account_id' as bigint) as account_id
	,cast(elems ->>'ad_name' as varchar(512)) as name
	,cast(elems ->> 'created_time' as date) as date_created
	,cast(elems ->> 'updated_time' as date) as date_updated
from(
select jsonb_array_elements(data::jsonb->'data') elems
from facebook_staging.query_data where staging_status = 'loaded' and report_scope = 'ad') a

        on conflict on constraint  ads_pkey do update
            set
                name = excluded.name
                ,date_updated = excluded.date_updated;

-- update ad queue
update facebook_staging.query_data
set staging_status = 'parsed'
where report_scope = 'ad' and staging_status = 'loaded';

end;$$;

--------------------------------
--------------------------------
-------------------------------
create or replace procedure facebook_staging.fact_daily_standard()
language plpgsql
as $$ begin

-- clear daily_fact_queue
insert into facebook.fact_daily_standard (
    ad_id
    ,adgroup_id
    ,campaign_id
    ,account_id
    ,date
    ,impressions
    ,link_clicks
    ,spend
    ,video_q25
    ,video_q50
    ,video_q75
    ,video_q100
    ,video_starts
    ,video_completes)
select
	cast(elems ->> 'ad_id' as bigint) as ad_id
	,cast(elems ->> 'adset_id' as bigint) as adgroup_id
	,cast(elems ->> 'campaign_id' as bigint) as campaign_id
	,cast(elems ->> 'account_id' as bigint) as account_id
	,cast(elems ->> 'date_start' as date) as date
	,coalesce(cast(elems ->> 'impressions' as int),0) as impressions
	,coalesce(cast(elems ->> 'inline_link_clicks' as int),0) as link_clicks
	,cast(elems ->> 'spend' as double precision) as spend
	,coalesce(cast(elems -> 'video_p25_watched_actions'->0->>'value' as int),0) as video_q25
	,coalesce(cast(elems -> 'video_p50_watched_actions'->0->>'value' as int),0) as video_q50
	,coalesce(cast(elems -> 'video_p75_watched_actions'->0->>'value' as int),0) as video_q75
	,coalesce(cast(elems -> 'video_p100_watched_actions'->0->>'value' as int),0) as video_q100
    ,coalesce(cast(elems -> 'video_play_actions'->0->>'value' as int),0) as video_starts
    ,cast(coalesce(coalesce(cast(elems ->> 'spend' as double precision),0) / Nullif(coalesce(cast(elems -> 'cost_per_thruplay'->0->>'value' as double precision),0),0)) as integer) as video_completes
from (
select
	account_id
	,jsonb_array_elements(data::jsonb->'data') elems
from facebook_staging.query_data
where report_scope = 'daily_standard' and staging_status = 'loaded'
) a
        on conflict on constraint fd_pkey do update
            set
                impressions = excluded.impressions
                ,link_clicks = excluded.link_clicks
                ,spend = excluded.spend
                ,video_q25 = excluded.video_q25
                ,video_q50 = excluded.video_q50
                ,video_q75 = excluded.video_q75
                ,video_q100 = excluded.video_q100
                ,video_starts = excluded.video_starts
                ,video_completes = excluded.video_completes;

-- update queue
update facebook_staging.query_data
set staging_status = 'parsed'
where report_scope = 'daily_standard' and staging_status = 'loaded';

end;$$;
--------------------------------
--------------------------------
-------------------------------

create or replace procedure facebook_staging.fact_conversion()
language plpgsql
as $$ begin


--- Parse Daily action counts
insert into facebook.fact_daily_actions (
    action_name
    ,ad_id
    ,adgroup_id
    ,campaign_id
    ,account_id
    ,date
    ,count_value
    ,count_1dc
    ,count_7dc
    ,count_28dc
    ,count_1dv
    ,count_7dv
    ,count_28dv
    ,count_1dev
)
select 
    cast(actions ->> 'action_type' as varchar(75)) as action_name
    ,ad_id
    ,adgroup_id
    ,campaign_id
    ,account_id
    ,date
    ,coalesce(cast(actions ->> 'value' as int), 0) as count_value
    ,coalesce(cast(actions ->> '1d_click' as int), 0) as count_1dc
    ,coalesce(cast(actions ->> '7d_click' as int), 0) as count_7dc
    ,0 as count_28dc
    ,coalesce(cast(actions ->> '1d_view' as int), 0) as count_1dv
    ,coalesce(cast(actions ->> '7d_view' as int), 0) as count_7dv
    ,0 as count_28dv
    ,0 as count_1dev
from (
    select 
        cast(account_id as bigint) as account_id
        ,cast(elems ->> 'ad_id' as bigint) as ad_id
        ,cast(elems ->> 'adset_id' as bigint) as adgroup_id
        ,cast(elems ->> 'campaign_id' as bigint) as campaign_id
        ,cast(elems ->> 'date_start' as date) as date
        ,jsonb_array_elements(elems::jsonb -> 'actions') as actions
    from(
            select 
                account_id
                ,jsonb_array_elements(data::jsonb->'data') elems
            from facebook_staging.query_data where staging_status = 'loaded' and report_scope = 'daily_actions') a ) b
    
    on conflict on constraint fdca_pkey do update
        set
            count_value = excluded.count_value
            ,count_1dc = excluded.count_1dc
            ,count_7dc = excluded.count_7dc
            ,count_28dc  = excluded.count_28dc
            ,count_1dv = excluded.count_1dv
            ,count_7dv = excluded.count_7dv
            ,count_28dv = excluded.count_28dv
            ,count_1dev = excluded.count_1dev;


insert into facebook.fact_daily_actions_values (
    action_name
    ,ad_id
    ,adgroup_id
    ,campaign_id
    ,account_id
    ,date
    ,value_value   
    ,value_1dc
    ,value_7dc
    ,value_28dc
    ,value_1dv
    ,value_7dv
    ,value_28dv
    ,value_1dev
)
select 
    cast(actions ->> 'action_type' as varchar(75)) as action_name
    ,ad_id
    ,adgroup_id
    ,campaign_id
    ,account_id
    ,date
    ,coalesce(cast(actions ->> 'value' as double precision), 0) as value_value
    ,coalesce(cast(actions ->> '1d_click' as double precision), 0) as value_1dc
    ,coalesce(cast(actions ->> '7d_click' as double precision), 0) as value_7dc
    ,0 as value_28dc
    ,coalesce(cast(actions ->> '1d_view' as double precision), 0) as value_1dv
    ,coalesce(cast(actions ->> '7d_view' as double precision), 0) as value_7dv
    ,0 as value_28dv
    ,0 as value_1dev
from (
    select 
        cast(account_id as bigint) as account_id
        ,cast(elems ->> 'ad_id' as bigint) as ad_id
        ,cast(elems ->> 'adset_id' as bigint) as adgroup_id
        ,cast(elems ->> 'campaign_id' as bigint) as campaign_id
        ,cast(elems ->> 'date_start' as date) as date
        ,jsonb_array_elements(elems::jsonb -> 'action_values') as actions
    from(
            select 
                account_id
                ,jsonb_array_elements(data::jsonb->'data') elems
            from facebook_staging.query_data where staging_status = 'loaded' and report_scope = 'daily_actions') a ) b
    
    on conflict on constraint fdcav_pkey do update
        set
            value_value = excluded.value_value
            ,value_1dc = excluded.value_1dc
            ,value_7dc = excluded.value_7dc
            ,value_28dc  = excluded.value_28dc
            ,value_1dv = excluded.value_1dv
            ,value_7dv = excluded.value_7dv
            ,value_28dv = excluded.value_28dv
            ,value_1dev = excluded.value_1dev;

-- update queue
update facebook_staging.query_data
set staging_status = 'parsed'
where report_scope = 'daily_actions' and staging_status = 'loaded';


end;$$;

--------------------------------
--------------------------------
-------------------------------

create or replace procedure facebook_staging.clear_queues()
language plpgsql
as $$ begin

drop table facebook_staging.job_manager;
drop table facebook_staging.query_data;

create table if not exists facebook_staging.job_manager (
    id serial
    ,time_generated timestamp not null default (now() at time zone 'utc')
    ,account_id bigint not null
    ,report_id bigint not null
    ,query_range text
    ,report_scope varchar(50)
    ,report_status varchar(20)
);

create table if not exists facebook_staging.query_data (
	account_id text
	,data json
    ,report_id text
    ,report_scope varchar(50)
    ,staging_status varchar(50)
	,initial_insert timestamp not null default (now() at time zone 'utc')
);

end;$$;