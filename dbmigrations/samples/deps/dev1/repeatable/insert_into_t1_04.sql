-- @depends_on ./insert_into_t1_02x.sql

insert into t1 values (4) on conflict (v1) do nothing; 
