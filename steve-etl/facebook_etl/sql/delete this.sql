select report_status, count(*) from facebook_staging.job_manager group by 1;

select staging_status, count(*) from facebook_staging.query_data group by 1;

call facebook_staging.fact_daily_standard();
-- select * from facebook_staging.query_data;
-- select * from facebook_staging.job_manager;
select qd.account_id
	,qd.report_id
	,qd.report_scope
	,jm.query_range
	from facebook_staging.query_data as qd

left join facebook_staging.job_manager as jm on (jm.report_id::bigint) = (qd.report_id::bigint)
where length(qd.data ->> 'error') > 0




select
	da.action_name
	,da.ad_id
	,look.adgroup_id
	,look.campaign_id
	,da.account_id
	,da.date
	,da.count_value
	,da.count_1dc
	,da.count_7dc
	,da.count_28dc
	,da.count_1dv
	,da.count_7dv
	,da.count_28dv
	,da.count_1dev
from facebook.all_action as da
left join facebook.ads as ad on (ad.id) = (da.ad_id)
left join (select ad_id, adgroup_id, campaign_id, account_id from facebook.fact_daily_standard group by 1,2,3,4) as look on (look.account_id, look.ad_id) = (da.account_id, da.ad_id)
where da.date <= '2023-12-31'