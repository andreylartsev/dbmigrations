SELECT pg_catalog.set_config('search_path', 'test1,test2,test3,public', false);
-- Baseline scripts for version 'V000'
-- E'dbmigrations\\samples\\envs\\dev1\\baseline\\V000\\..\\..\\..\\common\\baseline\\V000\\_cleanup.sql'
BEGIN;
drop table if exists t1 cascade;
COMMIT;
-- E'dbmigrations\\samples\\envs\\dev1\\baseline\\V000\\..\\..\\..\\common\\baseline\\V000\\00_create_t1.sql'
BEGIN;
create table t1 (
    v1 serial not null primary key
);

COMMIT;
-- E'dbmigrations\\samples\\envs\\dev1\\baseline\\V000\\..\\..\\..\\common\\baseline\\V000\\01_insert_into_t1.sql'
BEGIN;
insert into t1 values (1);
insert into t1 values (2);
COMMIT;
BEGIN;
INSERT INTO "test1".dbmigration_versions (version_id, is_baseline) VALUES ('V000', TRUE);
COMMIT;
-- Versioned scripts for version 'V001'
BEGIN;
-- E'dbmigrations\\samples\\envs\\dev1\\versions\\V001\\..\\..\\..\\common\\versions\\V001\\_cleanup.sql'
drop table if exists t2 cascade; 
-- E'dbmigrations\\samples\\envs\\dev1\\versions\\V001\\..\\..\\..\\common\\versions\\V001\\00_create_t2.sql'
create table t2 (
    kk varchar(36) not null primary key,
    created_at timestamp with time zone not null default current_timestamp
);

-- E'dbmigrations\\samples\\envs\\dev1\\versions\\V001\\..\\..\\..\\common\\versions\\V001\\01_insert_into_t2.sql'
insert into t2 values ('1');
insert into t2 values ('2');
INSERT INTO "test1".dbmigration_versions (version_id, is_baseline) VALUES ('V001', FALSE);
COMMIT;
-- Versioned scripts for version 'V002'
BEGIN;
-- E'dbmigrations\\samples\\envs\\dev1\\versions\\V002\\..\\..\\..\\common\\versions\\V002\\dummy.sql'
DO $$
BEGIN
    NULL;
END
$$;
INSERT INTO "test1".dbmigration_versions (version_id, is_baseline) VALUES ('V002', FALSE);
COMMIT;
-- Repeatable scripts for version 'V002'
-- E'dbmigrations\\samples\\envs\\dev1\\repeatable\\00_fn_get_environment_name.sql'
BEGIN;
CREATE OR REPLACE FUNCTION fn_get_environment_name()
RETURNS text 
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    RETURN 'dev01';
END;
$$;
INSERT INTO "test1".dbmigration_repeatable (sha256sum, version_id, relative_path) VALUES ('32ff44c939b7f0b59d65ad7011ff1962573bec55961c07dc2c65540fe8f449b9', 'V002',  E'dbmigrations\\samples\\envs\\dev1\\repeatable\\00_fn_get_environment_name.sql');
COMMIT;
-- E'dbmigrations\\samples\\envs\\dev1\\repeatable\\..\\..\\common\\repeatable\\01_create_view_latest_t1.sql'
BEGIN;
drop view if exists latest_t1;

create view latest_t1 as 
    select max(v1) as v1 from t1;

INSERT INTO "test1".dbmigration_repeatable (sha256sum, version_id, relative_path) VALUES ('90de5d9254461944fab716771b3c6c29fc9b57c924e23dc1e67f2bcb31024a93', 'V002',  E'dbmigrations\\samples\\envs\\dev1\\repeatable\\..\\..\\common\\repeatable\\01_create_view_latest_t1.sql');
COMMIT;
