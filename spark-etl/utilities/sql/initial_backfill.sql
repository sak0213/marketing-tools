-- accounts
insert into facebook.accounts (id, name, client_id)
select account_id, account_name, client_id from public.accounts where platform_id =1;

-- set which acts are active
update facebook.accounts
set status = 'inactive'
where id in (1300771580704671, 10152420163408268, 29791541);

-- fill cmapaigns
