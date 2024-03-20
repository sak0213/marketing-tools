-- Campaign Resource Name on Update
create or replace function tiktok.generate_campaign_resource () 
	returns trigger as $re_name$ begin
	update tiktok.campaigns
		set resource_name = 'platform/2/account/' || cast(new.account_id as text) || '/campaign/' || cast(new.id as text)
		where id = new.id;
		return null;
		end; $re_name$ language plpgsql;
		
create trigger campaigns_insert_trigger
after insert on tiktok.campaigns
for each row
	execute function tiktok.generate_campaign_resource();

-- Adgroup Resource Name on Update
create or replace function tiktok.generate_adgroup_resource () 
	returns trigger as $re_name$ begin
	update tiktok.adgroups
		set resource_name = 'platform/2/account/' || cast(new.account_id as text) || '/adgroup/' || cast(new.id as text)
		where id = new.id;
		return null;
		end; $re_name$ language plpgsql;
		
create trigger adgroups_insert_trigger
after insert on tiktok.adgroups
for each row
	execute function tiktok.generate_adgroup_resource();

-- Ad Resource Name on Update
create or replace function tiktok.generate_ad_resource () 
	returns trigger as $re_name$ begin
	update tiktok.ads
		set resource_name = 'platform/2/account/' || cast(new.account_id as text) || '/ad/' || cast(new.id as text)
		where id = new.id;
		return null;
		end; $re_name$ language plpgsql;
		
create trigger ad_insert_trigger
after insert on tiktok.ads
for each row
	execute function tiktok.generate_ad_resource();

-- Ad ID Relations

create or replace function tiktok.fds_relation_build ()
	returns trigger as $re_id$ begin
update tiktok.fact_daily_standard fds
		set adgroup_id = ad.adgroup_id,
			campaign_id = ad.campaign_id
	from tiktok.ads as ad
where fds.ad_id = ad.id and fds.account_id = ad.account_id;
		return null;
		end; $re_id$ language plpgsql;
	
create trigger fds_id_insert_trigger
after insert on tiktok.fact_daily_standard
for each row
	execute function tiktok.fds_relation_build();