-- Setting session search path to: dev2
SELECT pg_catalog.set_config('search_path', 'dev2', false);

-- --------- BASELINE VERSION: V000 ---------
BEGIN;
-- Apply script: [common/baseline/V000/00_create_t1.sql (OID:9bdf76b3)]
create table t1 (
    v1 serial not null primary key
);

-- End of script.
COMMIT;
BEGIN;
-- Apply script: [common/baseline/V000/01_insert_into_t1.sql (OID:2d3fb169)]
insert into t1 values (1);
insert into t1 values (2);
-- End of script.
COMMIT;
BEGIN;
INSERT INTO "dev2".dbmigration_versions (version_id, is_baseline) VALUES ('V000', TRUE);
INSERT INTO "dev2".dbmigration_version_scripts (version_id, relative_path, git_blob_sha1) VALUES ('V000', 'common/baseline/V000/00_create_t1.sql','9bdf76b3fe019f97e6cd603db08cb869e64896a6');
INSERT INTO "dev2".dbmigration_version_scripts (version_id, relative_path, git_blob_sha1) VALUES ('V000', 'common/baseline/V000/01_insert_into_t1.sql','2d3fb169511cf4596557955a64a4afbb770b5c16');
COMMIT;
-- --------- VERSION: V001 ---------

BEGIN;
-- Apply script: [common/versions/V001/00_create_t2.sql (OID:a3e53fb6)]
create table t2 (
    kk varchar(36) not null primary key,
    created_at timestamp with time zone not null default current_timestamp
);

-- End of script.
-- Apply script: [common/versions/V001/01_insert_into_t2.sql (OID:ff5717bd)]
insert into t2 values ('1');
insert into t2 values ('2');
-- End of script.
INSERT INTO "dev2".dbmigration_versions (version_id, is_baseline) VALUES ('V001', FALSE);
INSERT INTO "dev2".dbmigration_version_scripts (version_id, relative_path, git_blob_sha1) VALUES ('V001', 'common/versions/V001/00_create_t2.sql','a3e53fb6862ad9782f091a89482fb105f19799df');
INSERT INTO "dev2".dbmigration_version_scripts (version_id, relative_path, git_blob_sha1) VALUES ('V001', 'common/versions/V001/01_insert_into_t2.sql','ff5717bdc405de2b9f7ae50f3b7b0896d3a59071');
COMMIT;
-- --------- VERSION: V002 ---------

BEGIN;
-- Apply script: [common/versions/V002/dummy.sql (OID:384d538d)]
DO $$
BEGIN
    NULL;
END
$$;
-- End of script.
INSERT INTO "dev2".dbmigration_versions (version_id, is_baseline) VALUES ('V002', FALSE);
INSERT INTO "dev2".dbmigration_version_scripts (version_id, relative_path, git_blob_sha1) VALUES ('V002', 'common/versions/V002/dummy.sql','384d538d26551be2c6c697c832c209e84c2a73d2');
COMMIT;
-- --------- REPEATABLE SCRIPTS FOR VERSION: V002 ---------

BEGIN;
-- Apply script: [common/repeatable/00_fn_get_environment_name.sql (OID:747ad7e6)]
CREATE OR REPLACE FUNCTION fn_get_environment_name()
RETURNS text 
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    RAISE EXCEPTION 'Environment name is undefined!'; 
END;
$$;
-- End of script.
INSERT INTO "dev2".dbmigration_repeatable_scripts (git_blob_sha1, version_id, relative_path) VALUES ('747ad7e6b44c99291a7f45e9d6a2f907f7fae5fa', 'V002', 'common/repeatable/00_fn_get_environment_name.sql');
COMMIT;

BEGIN;
-- Apply script: [dev2/repeatable/00_fn_get_environment_name.sql (OID:0ae41ed8)]
-- @depends_on @common/00_fn_get_environment_name.sql


CREATE OR REPLACE FUNCTION fn_get_environment_name()
RETURNS text 
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    RETURN 'dev2';
END;
$$;
-- End of script.
INSERT INTO "dev2".dbmigration_repeatable_scripts (git_blob_sha1, version_id, relative_path) VALUES ('0ae41ed805c66afea6b7bc4fb58b7b2fccee8c08', 'V002', 'dev2/repeatable/00_fn_get_environment_name.sql');
COMMIT;

BEGIN;
-- Apply script: [common/repeatable/01_create_view_latest_t1.sql (OID:688edc8d)]
drop view if exists latest_t1;

create view latest_t1 as 
    select max(v1) as v1 from t1;

-- End of script.
INSERT INTO "dev2".dbmigration_repeatable_scripts (git_blob_sha1, version_id, relative_path) VALUES ('688edc8d28920b17b0db977b471aa167c6d1690b', 'V002', 'common/repeatable/01_create_view_latest_t1.sql');
COMMIT;
