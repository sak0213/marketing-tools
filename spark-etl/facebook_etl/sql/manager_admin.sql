call facebook_staging.key_standard();
call facebook_staging.fact_daily_standard();
call facebook_staging.fact_conversion();

select report_status, count(*) from facebook_staging.job_manager group by 1;

select staging_status, count(*) from facebook_staging.query_data group by 1;

update facebook.accounts
set
	last_updated_key = '2024-07-15 00:00:00',
	last_update_fact = '2024-07-15 00:00:00'
from (select cast(account_id as bigint) as account_id from facebook_staging.query_data group by 1) as a
where facebook.accounts.id = a.account_id;


call facebook_staging.clear_queues();
call facebook_prod.napa_standard();
call facebook_prod.jameshardie_standard();
call facebook_prod.aflac_standard();

