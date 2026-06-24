-- @depends_on ./insert_into_t1_02.sql

insert into t1 values (5) on conflict (v1) do nothing;
 