drop database if exists raw;
drop database if exists analytics;

create database raw;
create database analytics;

create role db_load_transform;
grant all privileges on database raw to db_load_transform;
grant all privileges on database analytics to db_load_transform;

create user nnguyen with password 'y8wljAV0ol{Z';
grant db_load_transform to nnguyen;
