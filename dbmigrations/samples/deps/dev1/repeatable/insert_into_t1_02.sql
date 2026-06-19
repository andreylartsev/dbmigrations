-- @depends_on ./insert_into_t1_01.sql

insert into t1 values (2) on conflict (v1) do nothing; 