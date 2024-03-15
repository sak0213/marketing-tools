create schema if not exists tiktok;
create schema if not exists tiktok_staging;
create schema if not exists tiktok_prod;

create table if not exists tiktok.accounts (
    id bigint primary key not null
    ,name varchar (100) not null
    ,client_id integer not null
    ,initial_insert timestamp not null default (now() at time zone 'utc')
    ,last_updated_key timestamp not null default (now() at time zone 'utc')
    ,last_update_fact timestamp not null default (now() at time zone 'utc')
    ,status varchar(10) default 'active'
);

create table if not exists tiktok.campaigns (
    id bigint not null
    ,account_id bigint not null
    ,resource_name varchar(255)
    ,name varchar(255) not null
    ,objective varchar(50) not null  --objective_type
    ,optimization_goal varchar (50)   --objective
    ,date_created date default --create_time
    ,date_updated date not null --modify_time
    ,initial_insert timestamp not null default (now() at time zone 'utc')
    ,primary key (id, account_id)
    ,constraint fk_cmp_act foreign key (account_id) references tiktok.accounts(id)
);

create table if not exists tiktok.adgroups (
    id bigint not null
    ,account_id bigint not null
    ,resource_name varchar(255)
    ,name varchar(512) not null
    ,objective varchar(50) not null --optimization_goal
    ,optimization_goal varchar(50) --bid_display_mode
    ,attribution_setting varchar(50)  --conversion_window
    ,date_created date not null --create_time
    ,date_updated date not null --modify_time
    ,initial_insert timestamp not null default (now() at time zone 'utc')
    ,primary key (id, account_id)
    ,constraint fk_adset_act foreign key (account_id) references tiktok.accounts(id)
);

create table if not exists tiktok.ads (
    id bigint not null
    ,account_id bigint not null
    ,resource_name varchar(255)
    ,name varchar(512) not null
    ,landing_page_url varchar(512)
    ,dcm_clicktag varchar(512) --click_tracking_url
    ,dcm_imptag varchar(512) --impression_tracking_url
    ,call_to_action varchar(32)
    ,identity_type varchar(32)
    ,ad_format varchar(32)
    ,campaign_id bigint
    ,adgroup_id bigint
    ,date_created date not null--create_time
    ,date_updated date not null --modify_time
    ,initial_insert timestamp not null default (now() at time zone 'utc')
    ,primary key (id, account_id)
    ,constraint fk_ad_act foreign key (account_id) references tiktok.accounts(id)
);

create table if not exists tiktok.fact_daily_standard (
    ad_id bigint not null
    ,adgroup_id bigint not null
    ,campaign_id bigint not null
    ,account_id bigint not null
    ,date date not null
    ,impressions int
    ,link_clicks int
    ,spend double precision
    ,video_q25 integer
    ,video_q50 integer
    ,video_q75 integer
    ,video_q100 integer
    ,video_starts integer
    ,video_completes integer
    ,ad_likes integer
    ,ad_comments integer
    ,ad_shares integer
    ,page_likes integer
    ,constraint fd_pkey primary key (ad_id, account_id, date)
);

create index if not exists fb_ds on tiktok.fact_daily_standard using btree (
    ad_id, account_id, date
);

-- create table if not exists tiktok.fact_daily_actions (
-- 	action_name varchar(75) not null
--     ,ad_id bigint not null
--     ,adgroup_id bigint not null
--     ,campaign_id bigint not null
--     ,account_id bigint not null
-- 	,date date not null
-- 	,count_value int
-- 	,count_1dc int
-- 	,count_7dc int
-- 	,count_28dc int
-- 	,count_1dv int
-- 	,count_7dv int
-- 	,count_28dv int
-- 	,count_1dev int
--     ,constraint fdca_pkey primary key (ad_id, account_id, date, action_name)
-- );

-- create index if not exists fb_da on tiktok.fact_daily_actions using btree (
--     ad_id, account_id, date
-- );

-- create table if not exists tiktok.fact_daily_actions_values (
-- 	action_name varchar(75) not null
--     ,ad_id bigint not null
--     ,adgroup_id bigint not null
--     ,campaign_id bigint not null
--     ,account_id bigint not null
-- 	,date date not null
-- 	,value_value double precision
-- 	,value_1dc double precision
-- 	,value_7dc double precision
-- 	,value_28dc double precision
-- 	,value_1dv double precision
-- 	,value_7dv double precision
-- 	,value_28dv double precision
-- 	,value_1dev double precision
--     ,constraint fdcav_pkey primary key (ad_id, account_id, date, action_name)
-- );

-- create index if not exists fb_dav on tiktok.fact_daily_actions_values using btree (
--     ad_id, account_id, date
-- );

create table if not exists tiktok_staging.job_manager (
    id serial
    ,time_generated timestamp not null default (now() at time zone 'utc')
    ,account_id bigint not null
    ,report_id bigint not null
    ,query_range text
    ,report_scope varchar(50)
    ,report_status varchar(20)
);

create table if not exists tiktok_staging.query_data (
	account_id text
	,data json
    ,report_id text
    ,report_scope varchar(50)
    ,staging_status varchar(50)
	,initial_insert timestamp not null default (now() at time zone 'utc')
);