create or replace procedure public.reset_newDB()
language plpgsql
as $$ begin

drop table if exists facebook.fact_daily_actions_values;
drop table if exists facebook.fact_daily_actions;
drop table if exists facebook.fact_daily_standard;
drop table if exists facebook.ads;
drop table if exists facebook.adgroups;
drop table if exists facebook.campaigns;
drop table if exists facebook.accounts;

end;
$$;

