SELECT pg_catalog.set_config('search_path', 'test1,test2,esbdb,public', false);
-- Baseline scripts for version 'V000'
--'test1/baseline/V000/00_create_t1.sql'
BEGIN;
create table t1 (
    v1 serial not null primary key
);

COMMIT;
--'test1/baseline/V000/01_insert_into_t1.sql'
BEGIN;
insert into t1 values (1);
insert into t1 values (2);
COMMIT;
BEGIN;
INSERT INTO "test3".dbmigration_versions (version_id, is_baseline) VALUES ('V000', TRUE);
COMMIT;
-- Versioned scripts for version 'V001'
BEGIN;
--'test1/versions/V001/00_create_t2.sql'
create table t2 (
    kk varchar(36) not null primary key,
    created_at timestamp with time zone not null default current_timestamp
);

--'test1/versions/V001/01_insert_into_t2.sql'
insert into t2 values ('1');
insert into t2 values ('2');
INSERT INTO "test3".dbmigration_versions (version_id, is_baseline) VALUES ('V001', FALSE);
COMMIT;
-- Versioned scripts for version 'V002'
BEGIN;
--'test1/versions/V002/dummy.sql'
DO $$
BEGIN
    NULL;
END
$$;
INSERT INTO "test3".dbmigration_versions (version_id, is_baseline) VALUES ('V002', FALSE);
COMMIT;
-- Repeatable scripts for version 'V002'
--'test1/repeatable/00_create_view_latest_t1.sql'
BEGIN;
drop view if exists latest_t1;

create view latest_t1 as 
    select max(v1) as v1 from t1;



INSERT INTO "test3".dbmigration_repeatable (sha256sum, version_id, relative_path) VALUES ('1c996d791f080a72a4985be6691ff61f52278d67fac7f7f29a061cf79bffbd83', 'V002', 'test1/repeatable/00_create_view_latest_t1.sql');
COMMIT;
