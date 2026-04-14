
select current_user \gset

create user engine_user;
grant all on schema public to engine_user;

\c - engine_user

create table engine_permissions(i int) using columnar;
insert into engine_permissions values(1);
alter table engine_permissions add column j int;
insert into engine_permissions values(2,20);
vacuum engine_permissions;
truncate engine_permissions;
drop table engine_permissions;

\c - :current_user

