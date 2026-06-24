SELECT pg_catalog.set_config('search_path', 'test2', false);
-- Baseline scripts for version 'V000'
--'common/baseline/V000/00_create_t1.sql'
BEGIN;
create table t1 (
    v1 serial not null primary key
);

COMMIT;
BEGIN;
INSERT INTO "test2".dbmigration_versions (version_id, is_baseline) VALUES ('V000', TRUE);
COMMIT;
-- Repeatable scripts for version 'V000'
--'common/repeatable/fn_get_environment_name.sql'
BEGIN;
CREATE OR REPLACE FUNCTION fn_get_environment_name()
RETURNS text 
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    RAISE EXCEPTION 'Environment name is undefined!';
END;
$$;
INSERT INTO "test2".dbmigration_repeatable (git_blob_sha1, version_id, relative_path) VALUES ('ced95c6dee2234a607ddf08ef44b09da5fa94b5b', 'V000', 'common/repeatable/fn_get_environment_name.sql');
COMMIT;
--'dev1/repeatable/fn_get_environment_name.sql'
BEGIN;
-- @depends_on @common/fn_get_environment_name.sql

CREATE OR REPLACE FUNCTION fn_get_environment_name()
RETURNS text 
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    RETURN 'dev1'; 
END;
$$;
INSERT INTO "test2".dbmigration_repeatable (git_blob_sha1, version_id, relative_path) VALUES ('ae6980bba3bb3cb3567695b4449b4264ee9fe649', 'V000', 'dev1/repeatable/fn_get_environment_name.sql');
COMMIT;
--'dev1/repeatable/use_get_environment_name.sql'
BEGIN;
-- @depends_on ./fn_get_environment_name.sql

DO $$
DECLARE
    v_env VARCHAR := fn_get_environment_name();
BEGIN
    RAISE NOTICE 'Environment name is %', v_env; 
END
$$;

INSERT INTO "test2".dbmigration_repeatable (git_blob_sha1, version_id, relative_path) VALUES ('da50070f499c173e325e41a53df6746939571060', 'V000', 'dev1/repeatable/use_get_environment_name.sql');
COMMIT;
--'common/repeatable/insert_into_t1_00.sql'
BEGIN;
insert into t1 values (0);
INSERT INTO "test2".dbmigration_repeatable (git_blob_sha1, version_id, relative_path) VALUES ('55e1736c41e85e9d1de59c627beb19367af3d6b1', 'V000', 'common/repeatable/insert_into_t1_00.sql');
COMMIT;
--'dev1/repeatable/insert_into_t1_01.sql'
BEGIN;
insert into t1 values (1) on conflict (v1) do nothing;
INSERT INTO "test2".dbmigration_repeatable (git_blob_sha1, version_id, relative_path) VALUES ('1d5cf3916ac0c15809573d2b0119a100255073df', 'V000', 'dev1/repeatable/insert_into_t1_01.sql');
COMMIT;
--'dev1/repeatable/insert_into_t1_02.sql'
BEGIN;
-- @depends_on ./insert_into_t1_01.sql

insert into t1 values (2) on conflict (v1) do nothing; 
INSERT INTO "test2".dbmigration_repeatable (git_blob_sha1, version_id, relative_path) VALUES ('1ef0147aad397a92657507ff71dd6fc7d7162d22', 'V000', 'dev1/repeatable/insert_into_t1_02.sql');
COMMIT;
--'dev1/repeatable/insert_into_t1_03.sql'
BEGIN;
-- @depends_on ./insert_into_t1_02.sql

insert into t1 values (3) on conflict (v1) do nothing;

INSERT INTO "test2".dbmigration_repeatable (git_blob_sha1, version_id, relative_path) VALUES ('08e408c4f14bfdc767a6447487b32c4f4ad1067c', 'V000', 'dev1/repeatable/insert_into_t1_03.sql');
COMMIT;
--'dev1/repeatable/insert_into_t1_04.sql'
BEGIN;
-- @depends_on ./insert_into_t1_02.sql

insert into t1 values (4) on conflict (v1) do nothing; 

INSERT INTO "test2".dbmigration_repeatable (git_blob_sha1, version_id, relative_path) VALUES ('fac92fec50ce68704905d1cf6d26bb399328293d', 'V000', 'dev1/repeatable/insert_into_t1_04.sql');
COMMIT;
--'dev1/repeatable/insert_into_t1_05.sql'
BEGIN;
-- @depends_on ./insert_into_t1_02.sql

insert into t1 values (5) on conflict (v1) do nothing; 
INSERT INTO "test2".dbmigration_repeatable (git_blob_sha1, version_id, relative_path) VALUES ('2a553e80ef9d9c5e72ad98dee94f9a4d5c7d47da', 'V000', 'dev1/repeatable/insert_into_t1_05.sql');
COMMIT;
