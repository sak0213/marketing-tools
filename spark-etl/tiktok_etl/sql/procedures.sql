create or replace procedure tiktok_staging.key_standard()
language plpgsql
as $$ begin

-- report errors
update tiktok_staging.query_data
set staging_status = 'failed'
where data ->> 'code' != '0' and staging_status = 'loaded';

-- campaign ELT

insert into tiktok.campaigns(
    id,account_id,name,objective,optimization_goal,date_created,date_updated)

select
	cast(elems ->>'campaign_id' as bigint) id
	,cast(elems ->>'advertiser_id' as bigint) account_id
	,cast(elems->>'campaign_name' as varchar(255)) as name
	,cast(elems->>'objective_type' as varchar(50)) as objective
	,cast(elems->>'objective' as varchar(50)) as optimization_goal
	,cast(elems->>'create_time' as date) as date_created
	,cast(elems->>'modify_time' as date) as date_updated
from (
select 
	cast(account_id as bigint) as account_id
	,report_id
	,jsonb_array_elements(data::jsonb -> 'data' -> 'list') as elems
	,report_scope
from tiktok_staging.query_data
where staging_status = 'loaded' and report_scope = 'key_campaign') as foo

    on conflict on constraint campaigns_pkey do update
            set
                name = excluded.name
                ,date_updated = excluded.date_updated;

update tiktok_staging.query_data
set staging_status = 'parsed'
where report_scope = 'key_campaign' and staging_status = 'loaded';

-- adgroup ELT
insert into tiktok.adgroups (
    id,account_id,name,objective,optimization_goal,attribution_setting,date_created,date_updated
)
select
	cast(elems ->>'adgroup_id' as bigint) id
	,cast(elems ->>'advertiser_id' as bigint) account_id
	,cast(elems->>'adgroup_name' as varchar(255)) as name
	,cast(elems->>'optimization_goal' as varchar(50)) as objective
	,cast(elems->>'bid_display_mode' as varchar(50)) as optimization_goal
	,cast(elems->>'conversion_window' as varchar(50)) as attribution_setting
	,cast(elems->>'create_time' as date) as date_created
	,cast(elems->>'modify_time' as date) as date_updated
from (
select 
	cast(account_id as bigint) as account_id
	,report_id
	,jsonb_array_elements(data::jsonb -> 'data' -> 'list') as elems
	,report_scope
from tiktok_staging.query_data
where staging_status = 'loaded' and report_scope = 'key_adgroup') as foo

        on conflict on constraint  adgroups_pkey do update
            set
                name = excluded.name
                ,date_updated = excluded.date_updated;

update tiktok_staging.query_data
set staging_status = 'parsed'
where report_scope = 'key_adgroup' and staging_status = 'loaded';

-- ad ELT

insert into tiktok.ads (
    id,account_id,name, landing_page_url, dcm_clicktag, dcm_imptag,
    call_to_action, identity_type, ad_format, campaign_id, adgroup_id,
    date_created, date_updated)

select
	cast(elems ->>'ad_id' as bigint) id
	,cast(elems ->>'advertiser_id' as bigint) account_id
	,cast(elems->>'ad_name' as varchar(255)) as name
	,cast(elems->>'landing_page_url' as varchar(512)) as landing_page_url
	,cast(elems->>'click_tracking_url' as varchar(512)) as dcm_clicktag
	,cast(elems->>'impression_tracking_url' as varchar(512)) as dcm_imptag
	,cast(elems->>'call_to_action' as varchar(32)) as call_to_action
	,cast(elems->>'identity_type' as varchar(32)) as identity_type
	,cast(elems->>'ad_format' as varchar(32)) as ad_format
	,cast(elems->>'campaign_id' as bigint) as campaign_id
	,cast(elems->>'adgroup_id' as bigint) as adgroup_id
	,cast(elems->>'create_time' as date) as date_created
	,cast(elems->>'modify_time' as date) as date_updated
from (
select 
	cast(account_id as bigint) as account_id
	,report_id
	,jsonb_array_elements(data::jsonb -> 'data' -> 'list') as elems
	,report_scope
from tiktok_staging.query_data
where staging_status = 'loaded' and report_scope = 'key_ad') as foo

        on conflict on constraint  ads_pkey do update
            set
                name = excluded.name
                ,landing_page_url = excluded.landing_page_url
                ,dcm_clicktag = excluded.dcm_clicktag
                ,dcm_imptag = excluded.dcm_imptag
                ,date_updated = excluded.date_updated;

update tiktok_staging.query_data
set staging_status = 'parsed'
where report_scope = 'key_ad' and staging_status = 'loaded';

end;$$;

-- daily facfts ELT
create or replace procedure tiktok_staging.fact_daily_standard()
language plpgsql
as $$ begin

insert into tiktok.fact_daily_standard (
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
    ,video_completes
	,ad_likes
	,ad_comments
	,ad_shares
	,page_likes)
select
	cast(elems->'dimensions'->>'ad_id' as bigint) as ad_id
	,000 as adgroup_id
	,000 as campaign_id
	,account_id
	,cast(elems->'dimensions'->>'stat_time_day' as date) as date
	,cast(elems->'metrics'->>'impressions' as int) as impressions
	,cast(elems->'metrics'->>'clicks' as int) as link_clicks
	,cast(elems->'metrics'->>'spend' as double precision) as spend
	,cast(elems->'metrics'->>'video_views_p25' as int) as video_q25
	,cast(elems->'metrics'->>'video_views_p50' as int) as video_q50
	,cast(elems->'metrics'->>'video_views_p75' as int) as video_q75
	,cast(elems->'metrics'->>'video_views_p100' as int) as video_q100
	,cast(elems->'metrics'->>'video_play_actions' as int) as video_starts
	,cast(elems->'metrics'->>'video_views_p100' as int) as video_completes
	,cast(elems->'metrics'->>'likes' as int) as ad_likes
	,cast(elems->'metrics'->>'comments' as int) as ad_comments
	,cast(elems->'metrics'->>'shares' as int) as ad_shares
	,cast(elems->'metrics'->>'follows' as int) as page_likes
from (
select 
	cast(account_id as bigint) as account_id
	,report_id
	,jsonb_array_elements(data::jsonb -> 'data' -> 'list') as elems
	,report_scope
from tiktok_staging.query_data
where staging_status = 'loaded' and report_scope = 'fact_dailystandard') as foo
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
                ,video_completes = excluded.video_completes
				,ad_likes = excluded.ad_likes
                ,ad_comments = excluded.ad_comments
                ,ad_shares = excluded.ad_shares
                ,page_likes = excluded.page_likes;

update tiktok_staging.query_data
set staging_status = 'parsed'
where report_scope = 'fact_dailystandard' and staging_status = 'loaded';

delete from tiktok.fact_daily_standard where
    impressions = 0
    and link_clicks = 0
    and spend = 0
    and video_starts = 0
    and ad_likes = 0
    and ad_comments = 0
    and ad_shares = 0
    and page_likes = 0;

    end;$$;


create or replace procedure tiktok_staging.clear_queues()
language plpgsql
as $$ begin

-- this is kind of funky. i think i overcomplicated it and we get weird updates. update table from table where table?
update tiktok.accounts
set last_updated_key = maxdate,
	last_update_fact = maxdate
from tiktok.accounts as acts
inner join (
	select account_id, max(date) as maxdate from tiktok.fact_daily_standard as fds
	left join tiktok.accounts as act on act.id = fds.account_id
	where act.status = 'active'
	group by account_id) as last_day_updated
on last_day_updated.account_id = acts.id
where tiktok.accounts.status = 'active';

delete from tiktok_staging.query_data where staging_status = 'parsed';

delete from tiktok_staging.job_manager where report_status = 'Job Processed';


    end;$$;
