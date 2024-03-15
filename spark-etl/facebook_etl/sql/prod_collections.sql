select 
	fds.account_id
	,fds.campaign_id
    ,cmp.name
    ,cmp.objective
    ,cmp.optimization_goal
	,fds.adgroup_id
    ,ag.name
    ,ag.objective
    ,ag.optimization_goal
    ,ag.attribution_setting
	,fds.ad_id
	,fds.date
	,coalesce(fds.impressions, 0) as impressions
	,coalesce(fds.link_clicks, 0) as clicks
	,coalesce(fds.spend, 0) as spend
	,coalesce(fds.video_starts, 0) as video_starts
	,coalesce(fds.video_completes, 0) as video_completes
    ,coalesce(cast(fda.count_value ->> 'post_reaction' as int), 0) as ad_like
    ,coalesce(cast(fda.count_value ->> 'post' as int), 0) as ad_share
    ,coalesce(cast(fda.count_value ->> 'comment' as int), 0) as ad_comment
    ,coalesce(cast(fda.count_value ->> 'like' as int), 0) as page_like
    ,coalesce(cast(fda.count_value ->> 'onsite_conversion.post_save' as int), 0) as ad_save
	,coalesce(cast(fda.count_7dc ->> 'purchase' as int), 0) as purchases_7dc
	,coalesce(cast(fda.count_7dc ->> 'add_to_cart' as int), 0) as atc_7dc
	,coalesce(cast(fda.count_7dc ->> 'view_content' as int), 0) as view_content_7dc
	,coalesce(cast(fda.count_1dv ->> 'purchase' as int), 0) as purchases_1dv
	,coalesce(cast(fda.count_1dv ->> 'add_to_cart' as int), 0) as atc_1dv
	,coalesce(cast(fda.count_1dv ->> 'view_content' as int), 0) as view_content_1dv
	,coalesce(cast(fdav.value_7dc ->> 'purchase' as double precision), 0) as purchases_value_7dc
	,coalesce(cast(fdav.value_7dc ->> 'add_to_cart' as double precision), 0) as atc_value_7dc
	,coalesce(cast(fdav.value_7dc ->> 'view_content' as double precision), 0) as view_content_value_7dc
	,coalesce(cast(fdav.value_1dv ->> 'purchase' as double precision), 0) as purchases_value_1dv
	,coalesce(cast(fdav.value_1dv ->> 'add_to_cart' as double precision), 0) as atc_value_1dv
	,coalesce(cast(fdav.value_1dv ->> 'view_content' as double precision), 0) as view_content_value_1dv
from (
        select 
            fds.account_id
            ,fds.campaign_id
            ,fds.adgroup_id
            ,fds.ad_id
            ,fds.date
            ,fds.impressions
            ,fds.link_clicks
            ,fds.spend
            ,fds.video_starts
            ,fds.video_completes
            from facebook.fact_daily_standard as fds
        where account_id = 911512468920156) as fds
left join (
        select 
            ad_id
            ,adgroup_id
            ,campaign_id
            ,account_id
            ,date
            ,json_object_agg(action_name, count_7dc) as count_7dc
            ,json_object_agg(action_name, count_1dv) as count_1dv
        ,json_object_agg(action_name, count_1dv) as count_value
        from facebook.fact_daily_actions
        where account_id = 911512468920156
        group by 1,2,3,4,5
        order by date) as fda on (fda.account_id, fda.ad_id, fda.date) = (fds.account_id, fds.ad_id, fds.date)
left join (
        select 
            ad_id
            ,adgroup_id
            ,campaign_id
            ,account_id
            ,date
            ,json_object_agg(action_name, value_7dc) as value_7dc
            ,json_object_agg(action_name, value_1dc) as value_1dv
        from facebook.fact_daily_actions_values
        where account_id = 911512468920156
        group by 1,2,3,4,5
        order by date) as fdav on (fdav.account_id, fdav.ad_id, fdav.date) = (fds.account_id, fds.ad_id, fds.date)
left join (
        select 
            name
            ,id
            ,account_id
            ,objective
            ,optimization_goal
        from facebook.campaigns
        where account_id = 911512468920156) as cmp on (cmp.account_id, cmp.id) = (fds.account_id, fds.campaign_id)
left join (
        select 
            name
            ,id
            ,account_id
            ,objective
            ,optimization_goal
            ,attribution_setting
        from facebook.adgroups
        where account_id = 911512468920156) as ag on (ag.account_id, ag.id) = (fds.account_id, fds.adgroup_id)
left join (
        select
            name
            ,id
            ,account_id
        from facebook.ads
        where account_id = 911512468920156) as ad on (ad.account_id, ad.id) = (fds.account_id, fds.ad_id)



create or replace procedure facebook_prod.napa_standard()
language plpgsql
as $$ begin

drop table facebook_prod.napa_standard;

create table facebook_prod.napa_standard as
select 
	fds.account_id as account_id
    ,'Meta' as platform
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
	,coalesce(fds.impressions, 0) as impressions
	,coalesce(fds.link_clicks, 0) as clicks
	,coalesce(fds.spend, 0) as spend
	,coalesce(fds.video_starts, 0) as video_starts
	,coalesce(fds.video_completes, 0) as video_completes
    ,coalesce(cast(fda.count_value ->> 'post_reaction' as int), 0) as ad_like
    ,coalesce(cast(fda.count_value ->> 'post' as int), 0) as ad_share
    ,coalesce(cast(fda.count_value ->> 'comment' as int), 0) as ad_comment
    ,coalesce(cast(fda.count_value ->> 'like' as int), 0) as page_like
    ,coalesce(cast(fda.count_value ->> 'onsite_conversion.post_save' as int), 0) as ad_save
	,coalesce(cast(fda.count_7dc ->> 'omni_purchase' as int), 0) as purchases_7dc
	,coalesce(cast(fda.count_7dc ->> 'add_to_cart' as int), 0) as atc_7dc
	,coalesce(cast(fda.count_7dc ->> 'view_content' as int), 0) as view_content_7dc
	,coalesce(cast(fda.count_1dv ->> 'omni_purchase' as int), 0) as purchases_1dv
	,coalesce(cast(fda.count_1dv ->> 'add_to_cart' as int), 0) as atc_1dv
	,coalesce(cast(fda.count_1dv ->> 'view_content' as int), 0) as view_content_1dv
	,coalesce(cast(fdav.value_7dc ->> 'omni_purchase' as double precision), 0) as purchases_value_7dc
	,coalesce(cast(fdav.value_7dc ->> 'add_to_cart' as double precision), 0) as atc_value_7dc
	,coalesce(cast(fdav.value_7dc ->> 'view_content' as double precision), 0) as view_content_value_7dc
	,coalesce(cast(fdav.value_1dv ->> 'omni_purchase' as double precision), 0) as purchases_value_1dv
	,coalesce(cast(fdav.value_1dv ->> 'add_to_cart' as double precision), 0) as atc_value_1dv
	,coalesce(cast(fdav.value_1dv ->> 'view_content' as double precision), 0) as view_content_value_1dv
from (
        select 
            fds.account_id
            ,fds.campaign_id
            ,fds.adgroup_id
            ,fds.ad_id
            ,fds.date
            ,fds.impressions
            ,fds.link_clicks
            ,fds.spend
            ,fds.video_starts
            ,fds.video_completes
            from facebook.fact_daily_standard as fds
        where account_id = 911512468920156) as fds
left join (
        select 
            ad_id
            ,adgroup_id
            ,campaign_id
            ,account_id
            ,date
            ,json_object_agg(action_name, count_7dc) as count_7dc
            ,json_object_agg(action_name, count_1dv) as count_1dv
        ,json_object_agg(action_name, count_value) as count_value
        from facebook.fact_daily_actions
        where account_id = 911512468920156
        group by 1,2,3,4,5
        order by date) as fda on (fda.account_id, fda.ad_id, fda.date) = (fds.account_id, fds.ad_id, fds.date)
left join (
        select 
            ad_id
            ,adgroup_id
            ,campaign_id
            ,account_id
            ,date
            ,json_object_agg(action_name, value_7dc) as value_7dc
            ,json_object_agg(action_name, value_1dv) as value_1dv
        from facebook.fact_daily_actions_values
        where account_id = 911512468920156
        group by 1,2,3,4,5
        order by date) as fdav on (fdav.account_id, fdav.ad_id, fdav.date) = (fds.account_id, fds.ad_id, fds.date)
left join (
        select 
            name
            ,id
            ,account_id
            ,objective
            ,optimization_goal
        from facebook.campaigns
        where account_id = 911512468920156) as cmp on (cmp.account_id, cmp.id) = (fds.account_id, fds.campaign_id)
left join (
        select 
            name
            ,id
            ,account_id
            ,objective
            ,optimization_goal
            ,attribution_setting
        from facebook.adgroups
        where account_id = 911512468920156) as ag on (ag.account_id, ag.id) = (fds.account_id, fds.adgroup_id)
left join (
        select
            name
            ,id
            ,account_id
        from facebook.ads
        where account_id = 911512468920156) as ad on (ad.account_id, ad.id) = (fds.account_id, fds.ad_id);
		
end;$$;



-- aflac proc
select 
	fds.account_id as account_id
    ,'Meta' as platform
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
	,coalesce(fds.impressions, 0) as impressions
	,coalesce(fds.link_clicks, 0) as clicks
	,coalesce(fds.spend, 0) as spend
	,coalesce(fds.video_starts, 0) as video_starts
	,coalesce(fds.video_completes, 0) as video_completes
    ,coalesce(cast(fda.count_value ->> 'post_reaction' as int), 0) as ad_like
    ,coalesce(cast(fda.count_value ->> 'post' as int), 0) as ad_share
    ,coalesce(cast(fda.count_value ->> 'comment' as int), 0) as ad_comment
    ,coalesce(cast(fda.count_value ->> 'like' as int), 0) as page_like
    ,coalesce(cast(fda.count_value ->> 'onsite_conversion.post_save' as int), 0) as ad_save
	,coalesce(cast(fda.count_value ->> 'landing_page_view' as int), 0) as landing_page_views
	,coalesce(cast(fda.count_7dc ->> 'lead' as int), 0) as leads_general_7dc
	,coalesce(cast(fda.count_7dc ->> 'view_content' as int), 0) as b2c_lead_7dc
	,coalesce(cast(fda.count_7dc ->> 'complete_registration' as int), 0) as agent_lead_7dc
	,coalesce(cast(fda.count_1dv ->> 'lead' as int), 0) as leads_general_1dv
	,coalesce(cast(fda.count_1dv ->> 'view_content' as int), 0) as b2c_lead_1dv
	,coalesce(cast(fda.count_1dv ->> 'complete_registration' as int), 0) as agent_lead_1dv
from (
        select 
            fds.account_id
            ,fds.campaign_id
            ,fds.adgroup_id
            ,fds.ad_id
            ,fds.date
            ,fds.impressions
            ,fds.link_clicks
            ,fds.spend
            ,fds.video_starts
            ,fds.video_completes
            from facebook.fact_daily_standard as fds
        where account_id in (1300771580704671,868776537476241)) as fds
left join (
        select 
            ad_id
            ,adgroup_id
            ,campaign_id
            ,account_id
            ,date
            ,json_object_agg(action_name, count_7dc) as count_7dc
            ,json_object_agg(action_name, count_1dv) as count_1dv
        ,json_object_agg(action_name, count_value) as count_value
        from facebook.fact_daily_actions
        where account_id in (1300771580704671,868776537476241)
        group by 1,2,3,4,5
        order by date) as fda on (fda.account_id, fda.ad_id, fda.date) = (fds.account_id, fds.ad_id, fds.date)
left join (
        select 
            name
            ,id
            ,account_id
            ,objective
            ,optimization_goal
        from facebook.campaigns
        where account_id in (1300771580704671,868776537476241)) as cmp on (cmp.account_id, cmp.id) = (fds.account_id, fds.campaign_id)
left join (
        select 
            name
            ,id
            ,account_id
            ,objective
            ,optimization_goal
            ,attribution_setting
        from facebook.adgroups
		where account_id in (1300771580704671,868776537476241)) as ag on (ag.account_id, ag.id) = (fds.account_id, fds.adgroup_id)
left join (
        select
            name
            ,id
            ,account_id
        from facebook.ads
        where account_id in (1300771580704671,868776537476241)) as ad on (ad.account_id, ad.id) = (fds.account_id, fds.ad_id);
