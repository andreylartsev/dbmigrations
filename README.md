# Simple PostgreSQL database migration tool

## Purpose of the tool:
- Ability to deploy a database from source code stored in a source code management system (Git);
- Streamline the delivery of changes in source code to test and production environments;

## Basic requirements:
- Delivery of changes in source code (database scripts/migrations) to the test environment;
- Delivery of changes in source code to the production environment;
- Ability to preserve data in existing databases (don't recreate db when you need apply some changes);
- Support the following types of changes:
  - Schema changes (tables, views, stored procedures)
  - Data changes;
- Ability to check which changes will be applied to a given database instance;
- Ability to generate an SQL script for use on a specific database for preliminary code review;
- Ability to use baseline scripts from the production environment (after anonymization, of course);

## Why we need a tool?
- Why do you need a tool? Why not just embed the version control into the scripts themselves and deploy scripts them with a .bat/.ps/.sh file?
- A tool is necessary to simplify DDL/DML scripts themselves and remove from them the boilerplate code like the following: 
  - explicit transaction management; 
  - database version control code;
  - interception and logging of execution errors;
- Additional features should simplify the life: 
  - checking which scripts will be applied, generating DDL/DML scripts for DB administrator review.
- The defined process of db changes reduce the likelihood of shooting yourself in the foot.

In the end it is a sort of sample implementation that could be forked and customized for your needs

More you can find in the [docs folder](./doc) 

## The best way to learn about the functionality of a tool is to call the embedded "help" subcommand:
```
  (.venv) PS C:\Users\andrey.larcev\Projects\dbmigrations> python3.exe .\dbmigrations\dbmigration.py -h
  usage: dbmigration.py [-h] {update,verify,init,run-tests} ...

  Simple database migrations tool

  positional arguments:
    {update,verify,init,run-tests}
                          Available subcommands
      update              Applies base, versioned, and repeatable scripts to the target database schema.
      verify              Validates the target schema and lists versioned and reproducible scripts to apply if the 'update' command is executed.
      init                Creates version control tables in an empty database schema.
      run-tests           Runs db unit test scripts to the target database schema.

  options:
    -h, --help            show this help message and exit
```

To try use it you'll need install __python 3.11+__ (mostly because of __tomllib__ package using for parsing configuration) + __psycopg__ package with its dependencies.  
The easiest way to install dependencies w/o affect on the whole system is to create virtual python environment. 

## How to create local python environment for installing dependencies:

Note: All required packages are listed in requirements.txt
 
```  
PS C:\Users\andrey.larcev\Projects\dbmigrations> python3.12.exe -m venv .venv
PS C:\Users\andrey.larcev\Projects\dbmigrations> python3.12.exe -m pip install --upgrade pip
PS C:\Users\andrey.larcev\Projects\dbmigrations> python3.12.exe -m pip install -r requirements.txt
PS C:\Users\andrey.larcev\Projects\dbmigrations>  Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
PS C:\Users\andrey.larcev\Projects\dbmigrations> .\.venv\Scripts\Activate.ps1
(.venv) PS C:\Users\andrey.larcev\Projects\dbmigrations>

```

## Sample DDL/DML scripts

The [samples folder](./dbmigrations/samples/) includes sample DML/DDL scripts repositories and you can try the tool with them:

- [test1](./dbmigrations/samples/test1) - included baseline, versioned and repeatable scripts
  - baseline/V000 - should create the table t1 and insert one record  
  - versions/V001 - should create the table t2 and insert one record 
  - repeatable/ - includes the script that should drop/create view on the table t1 
- [test1_empty_version](./dbmigrations/samples/test1_empty_version) - tests that empty versions are not allowed
- [test1_only_repeatable](./dbmigrations/samples/test1_only_repeatable) - shows that baseline and versioned scripts are not necessary if you specified target version in the target_version.txt file 
- and so on 

## Here is sample tool usage how you can update db

1. Before of all it is needed create empty schema. Open up console window, run __psql__ command line tool and execute the following DDL command: 

  ```
  CREATE SCHEMA test1;
  ```

2. Now you are ready to initialize schema with version control tables:
  ```
  (.venv) PS C:\Users\andrey.larcev\Projects\dbmigrations> $env:USER_PASSWORD=topsecret123
  (.venv) PS C:\Users\andrey.larcev\Projects\dbmigrations> python3 .\dbmigrations\dbmigration.py init test2
  Opened db connection
  Creating the version control tables...
  Created.
  Closed db connection
  ```
The first argument is a command name __"init"__ and second argument is a schema name __"test1"__. 
Note that we did not specified neither server host nor database name, this is because these parameters could be taken from [dbmigration.toml](./dbmigrations/dbmigration.toml) 
But the user's password must be passed via environment variable "USER_PASSWORD" (like in the example above) due to security reasons ))

3. Now you can update target schema with DDL/DML scripts: 
  ```
  (.venv) PS C:\Users\andrey.larcev\Projects\dbmigrations> python3.exe .\dbmigrations\dbmigration.py update test2 .\dbmigrations\samples\test1\
  Opened db connection by role 'postgres'
  Using 'test2,public' as a session 'search_path'
  Performing updates from scripts repository: 'dbmigrations\samples\test1'
  Performing a cross-check for consistency between the target version's repeatable scripts and the versioned scripts in: dbmigrations\samples\test1
  Completed.
  The baseline version to install V000.
  Apply baseline scripts...
  Running script: [test1/baseline/V000/00_create_t1.sql]...
  Committed.
  Running script: [test1/baseline/V000/01_insert_into_t1.sql]...
  Committed.
  Setting the baseline version as 'V000'.
  Committed.
  The baseline scripts were applied.
  The latest installed version is V000.
  Found 2 new versions for installation.
  Apply versioned scripts...
  Apply version V001...
  Running script: [test1/versions/V001/00_create_t2.sql]...
  Running script: [test1/versions/V001/01_insert_into_t2.sql]...
  Committed.
  Apply version V002...
  Running script: [test1/versions/V002/dummy.sql]...
  Committed.
  The versioned scripts were applied.
  Check repeatable scripts...
  Target version matches the latest installed version 'V002'
  Found 1 scripts to re-run
  Apply repeatable scripts...
  Running script: [test1/repeatable/00_create_view_latest_t1.sql]...
  Committed.
  The repeatable scripts were applied.
  Updated.
  Closed db connection.
```
The first argument is a command name __"update"__ and second argument is a schema name __"test1"__ and third parameter is path to scripts folder __".\dbmigrations\samples\test1\"__ 

4. And now let's look at the results inside the database:

Try connect to database using __psql__ and execute following commands:

```
  (.venv) PS C:\Users\andrey.larcev\Projects\dbmigrations> psql -U postgres test1
  test1=# SET search_path TO test2, public;
  SET
  test1=# SELECT * FROM dbmigration_versions;
  version_id | is_baseline |          created_at          | created_by | created_from
  ------------+-------------+------------------------------+------------+--------------
  V000       | t           | 2026-05-28 13:04:18.86726+03 | postgres   | 127.0.0.1
  V001       | f           | 2026-05-28 13:04:18.8769+03  | postgres   | 127.0.0.1
  V002       | f           | 2026-05-28 13:04:18.88653+03 | postgres   | 127.0.0.1
  (3 rows)


  test1=# SELECT * FROM dbmigration_repeatable;
  version_id |                 relative_path                 |          created_at           |                            sha256sum                             | created_by | created_from
  ------------+-----------------------------------------------+-------------------------------+------------------------------------------------------------------+------------+--------------
  V002       | test1/repeatable/00_create_view_latest_t1.sql | 2026-05-28 13:04:18.909018+03 | 1c996d791f080a72a4985be6691ff61f52278d67fac7f7f29a061cf79bffbd83 | postgres   | 127.0.0.1
  (1 row)


  test1=#
```

## How can we make script for code review

In additional to __update__ the tool supports the command __verify__ that verifies consistency of scripts directory and lists those of them that that will be applied on db in case __update__ command use. 
The __verify__ command have the option __--build-update-script__ that instructs the tool to build update script for code review.

```
(.venv) PS C:\Users\andrey.larcev\Projects\dbmigrations> python3.exe .\dbmigrations\dbmigration.py verify test2 .\dbmigrations\samples\test1\ --build-update-script xx.sql
Opened db connection by role 'postgres'
Using 'test1,test2,test3,public' as a session 'search_path'
Performing a cross-check for consistency between the target version's repeatable scripts and the versioned scripts in: dbmigrations\samples\test1
Completed.
Using 'test2,public' as a session 'search_path'
The baseline scripts to install:
[dbmigrations\samples\test1\baseline\V000\00_create_t1.sql]
[dbmigrations\samples\test1\baseline\V000\01_insert_into_t1.sql]
The versioned scripts to install:
[dbmigrations\samples\test1\versions\V001\00_create_t2.sql]
[dbmigrations\samples\test1\versions\V001\01_insert_into_t2.sql]
[dbmigrations\samples\test1\versions\V002\dummy.sql]
No versions were installed in the database schema.
The target version for repeatable scripts is V002.
The repeatable scripts to (re)install:
[dbmigrations\samples\test1\repeatable\00_create_view_latest_t1.sql]
The update script is written to 'xx.sql'.
Closed db connection.
```

The resulting script includes all necessary updates to db, transaction control statements to make it safe and inserts into version control tables. 

It looks like this: 

```
(.venv) PS C:\Users\andrey.larcev\Projects\dbmigrations> cat .\xx.sql
SELECT pg_catalog.set_config('search_path', 'test1,test2,test3,public', false);
-- Baseline scripts for version 'V000'
-- E'dbmigrations\\samples\\test1\\baseline\\V000\\00_create_t1.sql'
BEGIN;
create table t1 (
    v1 serial not null primary key
);

COMMIT;
-- E'dbmigrations\\samples\\test1\\baseline\\V000\\01_insert_into_t1.sql'
BEGIN;
insert into t1 values (1);
insert into t1 values (2);
COMMIT;
BEGIN;
INSERT INTO "test2".dbmigration_versions (version_id, is_baseline) VALUES ('V000', TRUE);
COMMIT;
-- Versioned scripts for version 'V001'
BEGIN;
-- E'dbmigrations\\samples\\test1\\versions\\V001\\00_create_t2.sql'
create table t2 (
    kk varchar(36) not null primary key,
    created_at timestamp with time zone not null default current_timestamp
);

-- E'dbmigrations\\samples\\test1\\versions\\V001\\01_insert_into_t2.sql'
insert into t2 values ('1');
insert into t2 values ('2');
INSERT INTO "test2".dbmigration_versions (version_id, is_baseline) VALUES ('V001', FALSE);
COMMIT;
-- Versioned scripts for version 'V002'
BEGIN;
-- E'dbmigrations\\samples\\test1\\versions\\V002\\dummy.sql'
DO $$
BEGIN
    NULL;
END
$$;
INSERT INTO "test2".dbmigration_versions (version_id, is_baseline) VALUES ('V002', FALSE);
COMMIT;
-- Repeatable scripts for version 'V002'
--'test1/repeatable/00_create_view_latest_t1.sql'
BEGIN;
drop view if exists latest_t1;

create view latest_t1 as
    select max(v1) as v1 from t1;



INSERT INTO "test2".dbmigration_repeatable (sha256sum, version_id, relative_path) VALUES ('1c996d791f080a72a4985be6691ff61f52278d67fac7f7f29a061cf79bffbd83', 'V002', 'test1/repeatable/00_create_view_latest_t1.sql');
COMMIT;
(.venv) PS C:\Users\andrey.larcev\Projects\dbmigrations>
```

And of course after code review you can apply it to db using plain __psql__ tool:

```
(.venv) andreylartsev@MacBook-Pro-Andrey Projects/dbmigrations$ psql -U postgres test1 -f xx.sql 
SET
BEGIN
CREATE TABLE
COMMIT
BEGIN
INSERT 0 1
INSERT 0 1
COMMIT
BEGIN
INSERT 0 1
COMMIT
BEGIN
CREATE TABLE
INSERT 0 1
INSERT 0 1
INSERT 0 1
COMMIT
BEGIN
NOTICE:  view "latest_t1" does not exist, skipping
DROP VIEW
CREATE VIEW
INSERT 0 1
COMMIT
(.venv) andreylartsev@MacBook-Pro-Andrey Projects/dbmigrations$ 
```

## Let's assume that you are reach enough to be ready to write & use unit tests on the database level 

The test scripts should be located within the "tests" subfolder of the script repository root.
Each of test script running within own transaction and that is rolls back thereafter.

### There are three types of test scripts:

1. Prefixed with __assure_that___ should just finish w/o errors;
2. Prefixed with __is_true_that___ should return one record with one boolean value = "true"; 
3. Prefixed with __detect_missing___ should return empty result set;
4. The script named ___setup.sql__ is a special script type that is needed for preparation test data. The tool placing savepoint before calling each setup script and rolls back to this savepoint thereafter

There are number of samples within [samples](./dbmigrations/samples/) folder.

Here is how result of running tests looks like:
```
(.venv) PS C:\Users\andrey.larcev\Projects\dbmigrations> python3.exe .\dbmigrations\dbmigration.py run-tests test2 .\dbmigrations\samples\test1\
Opened db connection by role 'postgres'
Using 'test2,public' as a session 'search_path'
Running unit tests for scripts repository: 'dbmigrations\samples\test1'
Target version matches the latest installed version 'V002'
Make savepoint...
Running setup: 'dbmigrations\samples\test1\tests\_setup.sql'...DONE
Make savepoint...
Running setup: 'dbmigrations\samples\test1\tests\00\_setup.sql'...DONE
Running test: 'dbmigrations\samples\test1\tests\00\is_true_that_setup_data_is_populated.sql'...PASS
Rolled back to savepoint.
Make savepoint...
Running setup: 'dbmigrations\samples\test1\tests\t1\_setup.sql'...DONE
Running test: 'dbmigrations\samples\test1\tests\t1\is_true_that_setup_data_is_populated.sql'...PASS
Running test: 'dbmigrations\samples\test1\tests\t1\assure_that_t1_exists.sql'...PASS
Running test: 'dbmigrations\samples\test1\tests\t1\assure_that_t1_fields_are_present.sql'...PASS
Running test: 'dbmigrations\samples\test1\tests\t1\detect_missing_t1_records.sql'...FAIL. (2) Missing records:
=================================
id: 3
FAIL. Expected no results!
Rolled back to savepoint.
Running test: 'dbmigrations\samples\test1\tests\t2\assure_that_t2_is_ok.sql'...PASS
Running test: 'dbmigrations\samples\test1\tests\t2\detect_missing_t2_records.sql'...PASS
Running test: 'dbmigrations\samples\test1\tests\latest_t1\assure_that_latest_t1_exists.sql'...PASS
Running test: 'dbmigrations\samples\test1\tests\latest_t1\is_true_that_latest_t1_returns_max_value.sql'...FAIL. (2) Expected true, got False!
Running test: 'dbmigrations\samples\test1\tests\is_true_that_setup_data_is_populated.sql'...PASS
Rolled back transaction.
Closed db connection.
Command error: Tests failed: 2, passed: 8.
```


## Now let's take a quick look at the utility's built-in subcommand help:

### __Init__ subcommand help:

```
(.venv) PS C:\Users\andrey.larcev\Projects\dbmigrations> python3.exe .\dbmigrations\dbmigration.py init --help
usage: dbmigration.py init [-h] [--dbenv DBENV] [--host HOST] [--port PORT] [--dbname DBNAME] [--user USER] [-n] [--force-init] schema_name

positional arguments:
  schema_name        the name of target database schema

options:
  -h, --help         show this help message and exit
  --dbenv DBENV      db environment name within TOML config
  --host HOST        db server host name
  --port PORT        db server port
  --dbname DBNAME    database name
  --user USER        user name
  -n, --no-password  dont ask user password
  --force-init       Force create version control tables even on non empty schema
```

### __Update__ subcommand help:

```
(.venv) PS C:\Users\andrey.larcev\Projects\dbmigrations> python3.exe .\dbmigrations\dbmigration.py update --help
usage: dbmigration.py update [-h] [--dbenv DBENV] [--host HOST] [--port PORT] [--dbname DBNAME] [--user USER] [-n] [--force-reapply-latest-version]
                             [--force-reapply-all-repeatable] [--force-run-cleanup]
                             schema_name scripts_path

positional arguments:
  schema_name           the name of target database schema
  scripts_path          source scripts repository path

options:
  -h, --help            show this help message and exit
  --dbenv DBENV         db environment name within TOML config
  --host HOST           db server host name
  --port PORT           db server port
  --dbname DBNAME       database name
  --user USER           user name
  -n, --no-password     dont ask user password
  --force-reapply-latest-version
                        clean up the latest version within the database and reapply the included *.sql scripts.
  --force-reapply-all-repeatable
                        reapply all repeatable scripts, regardless of changes.
  --force-run-cleanup   run the cleanup script before executing version-specific scripts.
```
### __Verify__ subcommand help:

```
(.venv) PS C:\Users\andrey.larcev\Projects\dbmigrations> python3.exe .\dbmigrations\dbmigration.py verify --help
usage: dbmigration.py verify [-h] [--dbenv DBENV] [--host HOST] [--port PORT] [--dbname DBNAME] [--user USER] [-n]
                             [--build-update-script BUILD_UPDATE_SCRIPT]
                             schema_name scripts_path

positional arguments:
  schema_name           the name of target database schema
  scripts_path          source scripts repository path

options:
  -h, --help            show this help message and exit
  --dbenv DBENV         db environment name within TOML config
  --host HOST           db server host name
  --port PORT           db server port
  --dbname DBNAME       database name
  --user USER           user name
  -n, --no-password     dont ask user password
  --build-update-script BUILD_UPDATE_SCRIPT
                        the update script path if you want one as an additional result of the verify command
```

### __Run Tests__ subcommand help:

```
(.venv) PS C:\Users\andrey.larcev\Projects\dbmigrations> python3.exe .\dbmigrations\dbmigration.py run-tests --help
usage: dbmigration.py run-tests [-h] [--dbenv DBENV] [--host HOST] [--port PORT] [--dbname DBNAME] [--user USER] [-n] schema_name scripts_path

positional arguments:
  schema_name        the name of target database schema
  scripts_path       source scripts repository path

options:
  -h, --help         show this help message and exit
  --dbenv DBENV      db environment name within TOML config
  --host HOST        db server host name
  --port PORT        db server port
  --dbname DBNAME    database name
  --user USER        user name
  -n, --no-password  dont ask user password
```

For now it is all and good luck dude in your fight. 
If you have any questions or suggestions, please post me a comment on the issues board  [Discussion](https://github.com/andreylartsev/dbmigrations/issues/1)