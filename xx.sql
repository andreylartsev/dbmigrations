SELECT pg_catalog.set_config('search_path', 'test3', false);
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
INSERT INTO "test3".dbmigration_version_scripts (version_id, relative_path, git_blob_sha1) VALUES ('V000', 'test1/baseline/V000/00_create_t1.sql','9bdf76b3fe019f97e6cd603db08cb869e64896a6');
INSERT INTO "test3".dbmigration_version_scripts (version_id, relative_path, git_blob_sha1) VALUES ('V000', 'test1/baseline/V000/01_insert_into_t1.sql','2d3fb169511cf4596557955a64a4afbb770b5c16');
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
INSERT INTO "test3".dbmigration_version_scripts (version_id, relative_path, git_blob_sha1) VALUES ('V001', 'test1/versions/V001/00_create_t2.sql','a3e53fb6862ad9782f091a89482fb105f19799df');
INSERT INTO "test3".dbmigration_version_scripts (version_id, relative_path, git_blob_sha1) VALUES ('V001', 'test1/versions/V001/01_insert_into_t2.sql','ff5717bdc405de2b9f7ae50f3b7b0896d3a59071');
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
INSERT INTO "test3".dbmigration_version_scripts (version_id, relative_path, git_blob_sha1) VALUES ('V002', 'test1/versions/V002/dummy.sql','384d538d26551be2c6c697c832c209e84c2a73d2');
COMMIT;
-- Repeatable scripts for version 'V002'
--'test1/repeatable/00_create_view_latest_t1.sql'
BEGIN;
drop view if exists latest_t1;

create view latest_t1 as  
    select max(v1) as v1 from t1;



INSERT INTO "test3".dbmigration_repeatable_scripts (git_blob_sha1, version_id, relative_path) VALUES ('24efdcda0030f25c322a8e1bf6caac93b5b576d0', 'V002', 'test1/repeatable/00_create_view_latest_t1.sql');
COMMIT;
--'test1/repeatable/01_create_view_max_t2_kk.sql'
BEGIN;
drop view if exists max_t2_kk;

create view max_t2_kk as   
    select max(kk) as kk from t2;
INSERT INTO "test3".dbmigration_repeatable_scripts (git_blob_sha1, version_id, relative_path) VALUES ('1a84871fe57372b1176cf52daaab463c1782b8d6', 'V002', 'test1/repeatable/01_create_view_max_t2_kk.sql');
COMMIT;
