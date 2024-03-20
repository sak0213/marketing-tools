create or replace procedure tiktok_prod.aflac_standard()
language plpgsql
as $$ begin

drop table tiktok_prod.aflac_standard;

create table tiktok_prod.aflac_standard as 
select 
	fds.account_id as account_id
    ,'TikTok' as platform
	,fds.campaign_id as campaign_id
    ,cmp.name as campaign_name
    ,cmp.objective as campaign_objective
    ,cmp.optimization_goal as campaign_opt_goal
	,fds.adgroup_id as adgroup_id
    ,ag.name as adgroup_name
    ,ag.objective as adgroup_objective
    ,ag.optimization_goal as adgroup_opt_goal
    ,ag.attribution_setting as adgroup_att_setting
	,fds.ad_id as ad_id
    ,ad.name as ad_name
	,fds.date as date
	,ad.dcm_clicktag as clicktag
	,coalesce(fds.impressions, 0) as impressions
	,coalesce(fds.link_clicks, 0) as clicks
	,coalesce(fds.spend, 0) as spend
	,coalesce(fds.video_starts, 0) as video_starts
	,coalesce(fds.video_completes, 0) as video_completes
    ,coalesce(fds.ad_likes, 0) as ad_like
	,coalesce(fds.ad_shares, 0) as ad_share
	,coalesce(fds.ad_comments, 0) as ad_comment
	,coalesce(fds.page_likes, 0) as page_like
    ,0 as ad_save
    ,0 as landing_page_views

from (
        select 
            account_id
            ,campaign_id
            ,adgroup_id
            ,ad_id
            ,date
            ,impressions
            ,link_clicks
            ,spend
            ,video_starts
            ,video_completes
			,ad_likes
			,ad_shares
			,ad_comments
			,page_likes
            from tiktok.fact_daily_standard
        where account_id in (7174850463932678145,7270655699670073345)) as fds
left join (
        select 
            name
            ,id
            ,account_id
            ,objective
            ,optimization_goal
        from tiktok.campaigns
        where account_id in (7174850463932678145,7270655699670073345)) as cmp on (cmp.id, cmp.account_id) = (fds.campaign_id, fds.account_id)
left join (
        select 
            name
            ,id
            ,account_id
            ,objective
            ,optimization_goal
            ,attribution_setting
        from tiktok.adgroups
		where account_id in (7174850463932678145,7270655699670073345)) as ag on (ag.account_id, ag.id) = (fds.account_id, fds.adgroup_id)
left join (
        select
            name
            ,id
            ,account_id
            ,dcm_clicktag
        from tiktok.ads
        where account_id in (7174850463932678145,7270655699670073345)) as ad on (ad.account_id, ad.id) = (fds.account_id, fds.ad_id);
end;$$;