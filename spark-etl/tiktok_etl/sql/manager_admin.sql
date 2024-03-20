call tiktok_staging.key_standard();
call tiktok_staging.fact_daily_standard()

select report_status, count(*) from tiktok_staging.job_manager group by 1;

select staging_status, count(*) from tiktok_staging.query_data group by 1;

call tiktok_staging.clear_queues();
call tiktok_prod.aflac_standard();
