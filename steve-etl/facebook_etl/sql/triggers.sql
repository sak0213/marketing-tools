-- Campaign Resource Name on Update
create or replace function facebook.generate_campaign_resource () 
	returns trigger as $re_name$ begin
	update facebook.campaigns
		set resource_name = 'platform/1/account/' || cast(new.account_id as text) || '/campaign/' || cast(new.id as text)
		where id = new.id;
		return null;
		end; $re_name$ language plpgsql;
		
create trigger campaigns_insert_trigger
after insert on facebook.campaigns
for each row
	execute function facebook.generate_campaign_resource();

-- Adgroup Resource Name on Update
create or replace function facebook.generate_adgroup_resource () 
	returns trigger as $re_name$ begin
	update facebook.adgroups
		set resource_name = 'platform/1/account/' || cast(new.account_id as text) || '/adgroup/' || cast(new.id as text)
		where id = new.id;
		return null;
		end; $re_name$ language plpgsql;
		
create trigger adgroups_insert_trigger
after insert on facebook.adgroups
for each row
	execute function facebook.generate_adgroup_resource();

-- Ad Resource Name on Update
create or replace function facebook.generate_ad_resource () 
	returns trigger as $re_name$ begin
	update facebook.ads
		set resource_name = 'platform/1/account/' || cast(new.account_id as text) || '/ad/' || cast(new.id as text)
		where id = new.id;
		return null;
		end; $re_name$ language plpgsql;
		
create trigger ad_insert_trigger
after insert on facebook.ads
for each row
	execute function facebook.generate_ad_resource();