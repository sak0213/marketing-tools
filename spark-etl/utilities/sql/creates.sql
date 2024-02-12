create schema if not exists utility;

create table if not exists utility.log (
    id serial
    ,created timestamp not null default (now() at time zone 'utc')
    ,platform varchar(25)
    ,status varchar(20)
    ,event varchar (100)
    ,note text
);

