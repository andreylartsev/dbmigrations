# Simple PostgreSQL Database Migration Tool

🌐 **Read this in other languages: [Русский](README.ru.md)**

An automation and migration management tool for PostgreSQL databases. It enables you to deploy database schemas directly from source code stored in Git and safely deliver DDL/DML changes to test and production environments.

---

## 📌 Purpose of the Tool

* **Git Integration**: Deploy and track database schemas from your version control system.
* **CI/CD Pipelines**: Streamline the delivery of database changes (DDL/DML) to staging, test, and production environments.
* **Data Preservation**: Apply incremental updates to existing databases without wiping out data.
* **Schema & Data Support**: Manage structural updates (tables, views, stored procedures) and data seeds/migrations.
* **Dry-Run Inspection**: Preview exactly which migration scripts will be executed.
* **SQL Generation for Code Review**: Compile a single, transaction-safe SQL script for DBA review.
* **Database-Level Unit Testing**: Run automated unit tests directly inside isolated, rollback-safe transactions.

---

## ❓ Why Use a Dedicated Tool?

Deployment scripts themselves often get cluttered with boilerplate. A dedicated tool removes it and standardizes the change process:

* **Automated Transaction Management**: No need to write explicit `BEGIN` and `COMMIT` blocks.
* **Built-in Schema Versioning**: Automatically maintains history and state logs inside the DB.
* **Error Interception**: Gracefully catches and logs execution errors.
* **Enhanced Safety**: Standardized execution workflows prevent manual deployment mistakes.

In the end it is a sample implementation that could be forked and customized for your needs.

---

## 🚀 Requirements & Installation

* **Python 3.11+** (mostly because of the `tomllib` package used for configuration parsing)
* **psycopg** library

```powershell
python3.exe -m venv .venv
.\.venv\Scripts\Activate.ps1
python3.exe -m pip install -r requirements-dev.txt
```

> ⚠️ **Important Security Note**: Set `USER_PASSWORD` via environment variables.

---

## 📁 Migration Repository Structure

A scripts repository is a folder (usually a Git repo) with the following structure:

* `baseline/V000/` — Baseline scripts (anonymized/cleaned-up production database dump). Applied once;
* `versions/V001...VNNN/` — Versioned, incremental migration scripts (main tables, data). Applied once per version in ascending order;
* `repeatable/` — Idempotent scripts (views, triggers, functions, configuration data). Re-applied on every `update` if changed;
* `tests/` — SQL unit tests (see [Database unit tests](#straight-arrow-testing-run-tests-unit-testing) below);

The execution order within a folder is defined by the `script_list.txt` file when present, otherwise by alphabetical sorting of the script names.

---

## 💻 Command Line Interface & Usage

The tool has 5 subcommands: `init`, `update`, `verify`, `run-tests`, `tui`.

### 1. Initialize (`init`)
Creates the version control tables in an empty database schema.
```bash
\$env:USER_PASSWORD="topsecret123"
python3 .\dbmigrations\dbmigration.py init test2 .\dbmigrations\samples\test1\
```

### 2. Apply Migrations (`update`)
Executes migrations sequentially.
```bash
python3 .\dbmigrations\dbmigration.py update test2 .\dbmigrations\samples\test1\ --skip-confirmation
```

### 3. Verify Changes (`verify`)
Validates repository consistency and previews changes. By default it prints unified text diffs inline under each listed script (the version applied in the DB by git OID vs. the current file in the repository); use `--skip-diffs` to show the list only. Build update script for code review by DB admins. Show recent changes applied to database schema grouped by Git commits.
```bash
python3 .\dbmigrations\dbmigration.py verify test2 .\dbmigrations\samples\test1\ --build-update-script review_patch.sql
```

### 4. Run Unit Tests (`run-tests`)
Executes database-level tests within transactions.
```bash
python3 .\dbmigrations\dbmigration.py run-tests test2 .\dbmigrations\samples\test1\
```

### 5. Text User Interface (`tui`)
Opens an interactive TUI (built on [Textual](https://textual.io)) with a live central log, a right-hand panel of available commands and a status line:
```bash
python3 .\dbmigrations\dbmigration.py tui test2 .\dbmigrations\samples\test1\
```

On open, the central area immediately runs the full `verify` and streams its output line by line. If the schema is not initialized yet, a hint runs `init` instead. The right panel lists the commands available for the current schema/repository state (`init`, `update`, `verify`, `run-tests`), each with its option checkboxes. `update` without `--skip-confirmation` asks for confirmation in a modal dialog. The status line shows `idle / running <current script> / cancel requested` plus the final exit code, and the currently running script is highlighted in the log.

| Shortcut | Action |
|----------|--------|
| `ctrl+r` | Run the `verify` check against the current schema/repository |
| `ctrl+c` | Cancel the running command (graceful) |
| `ctrl+shift+c` | Copy the whole log to the clipboard (fallback: `alt+c`) |
| `ctrl+shift+x` | Copy the visible part of the log to the clipboard |
| `r`      | Re-probe the schema/repository state (Refresh); the log then shows a summary of what changed (available commands, git repository) |
| `q`      | Quit |

Lines that carry a git `(OID: …)` reference act as links: a click opens a viewer with the script file contents.

The TUI needs an interactive terminal; plain CLI subcommands keep working without it.

Run `python3 .\dbmigration.py -h` to see global choices, or see the complete [CLI Argument Reference](#cli-argument-reference) below.

---

## 🏁 Getting Started

Let's deploy the [sample repository](./dbmigrations/samples/test1) step by step.

### 1. Create an empty schema

```bash
CREATE SCHEMA test2;
```

### 2. Initialize the schema with the version control tables

```powershell
(.venv) PS C:\Users\andrey.larcev\Projects\dbmigrations> $env:USER_PASSWORD="topsecret123"
(.venv) PS C:\Users\andrey.larcev\Projects\dbmigrations> python3 .\dbmigrations\dbmigration.py init test2 .\dbmigrations\samples\test1\
Opened db connection: 'postgres@localhost:5432/test1'
Set session search path to: 'test2'.
Creating the version control tables with environment ID: '4a40342c-4546-4776-bf97-b02b2a858924'
Created.
Closed db connection.
```

The first argument is the target schema name (`test2`), the second is the scripts repository path.
The database server address and credentials are taken from `dbmigration.toml` (see [Configuration](#configuration)); only the password must be passed via the `USER_PASSWORD` environment variable.

### 3. Apply the migrations

```powershell
(.venv) PS C:\Users\andrey.larcev\Projects\dbmigrations> python3 .\dbmigrations\dbmigration.py update test2 .\dbmigrations\samples\test1\ --skip-confirmation
Opened db connection: 'postgres@localhost:5432/test1'
Set session search path to: 'test2'.
Target schema environment ID matches the scripts directory ID: 4a40342c-4546-4776-bf97-b02b2a858924
Performing updates from scripts repository: '/workspace/dbmigrations/samples/test1'
Performing a cross-check for consistency between the target version's repeatable scripts and the versioned scripts...
Completed.
The baseline version to install V000.
Apply baseline scripts...
Running script: [test1/baseline/V000/00_create_t1.sql (OID: 9bdf76b3)]...
Committed.
Running script: [test1/baseline/V000/01_insert_into_t1.sql (OID: 2d3fb169)]...
Committed.
Setting the baseline version to: 'V000'.
Committed.
Baseline scripts applied.
The latest installed version is V000.
Found 2 new versions for installation.
Apply versioned scripts...
Apply version V001...
Running script: [test1/versions/V001/00_create_t2.sql (OID: a3e53fb6)]...
Running script: [test1/versions/V001/01_insert_into_t2.sql (OID: ff5717bd)]...
Committed.
Apply version V002...
Running script: [test1/versions/V002/dummy.sql (OID: 384d538d)]...
Committed.
Versioned scripts applied.
Check repeatable scripts...
Target version matches the latest installed version: 'V002'.
Found 2 scripts to re-run
Apply repeatable scripts...
Running script: [test1/repeatable/00_create_view_latest_t1.sql (OID: 1504cd9a)]...
Server: NOTICE - view "latest_t1" does not exist, skipping
Committed.
Running script: [test1/repeatable/01_create_view_max_t2_kk.sql (OID: 1278759c)]...
Server: NOTICE - view "max_t2_kk" does not exist, skipping
Committed.
Repeatable scripts applied.
Updated.
Closed db connection.
```

### 4. Look at the results inside the database

```bash
psql -h localhost -U postgres -d test1
```

```sql
SET search_path TO test2;
SELECT * FROM test2.dbmigration_versions ORDER BY version_id;
```

```
 version_id | is_baseline |          created_at           | created_by | created_from
------------+-------------+-------------------------------+------------+--------------
 V000       | t           | 2026-09-18 07:10:23.835956+00 | postgres   | ::1
 V001       | f           | 2026-09-18 07:10:24.041018+00 | postgres   | ::1
 V002       | f           | 2026-09-18 07:10:24.136315+00 | postgres   | ::1
(3 rows)
```

Every installed script is tracked with its Git OID, so the tool always knows what exactly was applied:

```sql
SELECT version_id, relative_path, left(git_blob_sha1, 10) AS oid
FROM test2.dbmigration_version_scripts
ORDER BY version_id, relative_path;
```

```
 version_id |               relative_path               |    oid
------------+-------------------------------------------+------------
 V000       | test1/baseline/V000/00_create_t1.sql      | 9bdf76b3fe
 V000       | test1/baseline/V000/01_insert_into_t1.sql | 2d3fb16951
 V001       | test1/versions/V001/00_create_t2.sql      | a3e53fb686
 V001       | test1/versions/V001/01_insert_into_t2.sql | ff5717bdc4
 V002       | test1/versions/V002/dummy.sql             | 384d538d26
(5 rows)
```

```sql
SELECT version_id, relative_path, left(git_blob_sha1, 10) AS oid
FROM test2.dbmigration_repeatable_scripts
ORDER BY relative_path;
```

```
 version_id |                 relative_path                 |    oid
------------+-----------------------------------------------+------------
 V002       | test1/repeatable/00_create_view_latest_t1.sql | 1504cd9a01
 V002       | test1/repeatable/01_create_view_max_t2_kk.sql | 1278759c33
(2 rows)
```

---

## 📝 Code Review and Dry Run (`verify --build-update-script`)

`verify` checks the consistency of the scripts repository and lists everything that would be applied to the database by `update`. The `--build-update-script` option additionally compiles all the updates into a single transaction-safe SQL script for DBA review.

Let's verify a freshly initialized schema:

```powershell
(.venv) PS C:\Users\andrey.larcev\Projects\dbmigrations> python3 .\dbmigrations\dbmigration.py verify test3 .\dbmigrations\samples\test1\ --build-update-script xx.sql
Opened db connection: 'postgres@localhost:5432/test1'
Set session search path to: 'test3'.
Target schema environment ID matches the scripts directory ID: 4a40342c-4546-4776-bf97-b02b2a858924
Performing a cross-check for consistency between the target version's repeatable scripts and the versioned scripts...
Completed.
Baseline scripts to install:
[64b571a4] 2026-03-30 - intermediate results
  Author: Andrey Lartsev
    [test1/baseline/V000/01_insert_into_t1.sql (OID: 2d3fb169)]
    + New file, will be applied in full.
[c24a3a35] 2026-03-30 - added sample script repo
  Author: Andrey Lartsev
    [test1/baseline/V000/00_create_t1.sql (OID: 9bdf76b3)]
    + New file, will be applied in full.
Versioned scripts to install:
[64b571a4] 2026-03-30 - intermediate results
  Author: Andrey Lartsev
    [test1/versions/V001/00_create_t2.sql (OID: a3e53fb6)]
    + New file, will be applied in full.
    [test1/versions/V001/01_insert_into_t2.sql (OID: ff5717bd)]
    + New file, will be applied in full.
[36ceff6c] 2026-04-21 - initial implementation of added field version_id to dbmigration_repeatable table
  Author: Andrey Lartsev
    [test1/versions/V002/dummy.sql (OID: 384d538d)]
    + New file, will be applied in full.
No versions are installed in the database schema.
Target version for repeatable scripts: 'V002'.
Repeatable scripts to (re)install:
[563cf87e] 2026-07-21 - more use of ScriptInfo
  Author: Andrey Lartsev
    [test1/repeatable/00_create_view_latest_t1.sql (OID: 1504cd9a)]
    + New file, will be applied in full.
[2afeb5db] 2026-07-21 - more use of script info
  Author: Andrey Lartsev
    [test1/repeatable/01_create_view_max_t2_kk.sql (OID: 1278759c)]
    + New file, will be applied in full.
Update script is written to 'xx.sql'.
Closed db connection.
```

> Scripts that are new and were never applied are shown collapsed as `+ New file, will be applied in full.`. For scripts that were already applied, `verify` prints a unified text diff of the applied version (by git OID) versus the current file in the repository. Use `--skip-diffs` to hide the diffs.

The resulting script contains all the required updates: transaction control statements to make it safe and inserts into the version control tables:

```sql
-- Setting session search path to: test3
SELECT pg_catalog.set_config('search_path', 'test3', false);

-- --------- BASELINE VERSION: V000 ---------
BEGIN;
-- Apply script: [test1/baseline/V000/00_create_t1.sql (OID:9bdf76b3)]
create table t1 (
    v1 serial not null primary key
);
-- End of script.
COMMIT;
BEGIN;
-- Apply script: [test1/baseline/V000/01_insert_into_t1.sql (OID:2d3fb169)]
insert into t1 values (1);
insert into t1 values (2);
-- End of script.
COMMIT;
BEGIN;
INSERT INTO "test3".dbmigration_versions (version_id, is_baseline) VALUES ('V000', TRUE);
INSERT INTO "test3".dbmigration_version_scripts (version_id, relative_path, git_blob_sha1) VALUES ('V000', 'test1/baseline/V000/00_create_t1.sql','9bdf76b3fe019f97e6cd603db08cb869e64896a6');
INSERT INTO "test3".dbmigration_version_scripts (version_id, relative_path, git_blob_sha1) VALUES ('V000', 'test1/baseline/V000/01_insert_into_t1.sql','2d3fb169511cf4596557955a64a4afbb770b5c16');
COMMIT;
-- --------- VERSION: V001 ---------
BEGIN;
-- Apply script: [test1/versions/V001/00_create_t2.sql (OID:a3e53fb6)]
create table t2 (
    kk varchar(36) not null primary key,
    created_at timestamp with time zone not null default current_timestamp
);
-- End of script.
-- Apply script: [test1/versions/V001/01_insert_into_t2.sql (OID:ff5717bd)]
insert into t2 values ('1');
insert into t2 values ('2');
-- End of script.
INSERT INTO "test3".dbmigration_versions (version_id, is_baseline) VALUES ('V001', FALSE);
INSERT INTO "test3".dbmigration_version_scripts (version_id, relative_path, git_blob_sha1) VALUES ('V001', 'test1/versions/V001/00_create_t2.sql','a3e53fb6862ad9782f091a89482fb105f19799df');
INSERT INTO "test3".dbmigration_version_scripts (version_id, relative_path, git_blob_sha1) VALUES ('V001', 'test1/versions/V001/01_insert_into_t2.sql','ff5717bdc405de2b9f7ae50f3b7b0896d3a59071');
COMMIT;
-- --------- VERSION: V002 ---------
BEGIN;
-- Apply script: [test1/versions/V002/dummy.sql (OID:384d538d)]
DO $$
BEGIN
    NULL;
END
$$;
-- End of script.
INSERT INTO "test3".dbmigration_versions (version_id, is_baseline) VALUES ('V002', FALSE);
INSERT INTO "test3".dbmigration_version_scripts (version_id, relative_path, git_blob_sha1) VALUES ('V002', 'test1/versions/V002/dummy.sql','384d538d26551be2c6c697c832c209e84c2a73d2');
COMMIT;
-- --------- REPEATABLE SCRIPTS FOR VERSION: V002 ---------
BEGIN;
-- Apply script: [test1/repeatable/00_create_view_latest_t1.sql (OID:1504cd9a)]
drop view if exists latest_t1;

create view latest_t1 as
    select max(v1) as v1 from t1;
-- End of script.
INSERT INTO "test3".dbmigration_repeatable_scripts (git_blob_sha1, version_id, relative_path) VALUES ('1504cd9a0133594c04438c9022acce4aa1e60a33', 'V002', 'test1/repeatable/00_create_view_latest_t1.sql');
COMMIT;

BEGIN;
-- Apply script: [test1/repeatable/01_create_view_max_t2_kk.sql (OID:1278759c)]
drop view if exists max_t2_kk;

create view max_t2_kk as
    select max(kk) as kk from t2;
-- End of script.
INSERT INTO "test3".dbmigration_repeatable_scripts (git_blob_sha1, version_id, relative_path) VALUES ('1278759c33e8b9349e663656c998023afb5491ea', 'V002', 'test1/repeatable/01_create_view_max_t2_kk.sql');
COMMIT;
```

After the code review you can apply it with plain `psql`:

```bash
psql -U postgres test1 -f xx.sql
```

---

## 🧪 Database Unit Testing (`run-tests`)

Test scripts live in the `tests/` subfolder of the scripts repository. Each test runs within its own transaction that is rolled back afterwards; data prepared by one test never leaks into another.

There are three types of test scripts:

1. Prefixed with `assure_that_` — should simply finish without errors;
2. Prefixed with `is_true_that_` — should return a single record with a single boolean value of `true`;
3. Prefixed with `detect_missing_` — should return an empty result set.

The special `_setup.sql` script prepares test data. The tool places a savepoint before each setup script and rolls back to it afterwards.

```powershell
(.venv) PS C:\Users\andrey.larcev\Projects\dbmigrations> python3 .\dbmigrations\dbmigration.py run-tests test2 .\dbmigrations\samples\test1\
Opened db connection: 'postgres@localhost:5432/test1'
Set session search path to: 'test2'.
Target schema environment ID matches the scripts directory ID: 4a40342c-4546-4776-bf97-b02b2a858924
Running unit tests on scripts repository: '/workspace/dbmigrations/samples/test1'
Target version matches the latest installed version: 'V002'
Make savepoint...
Running setup: 'test1/tests/_setup.sql'...DONE
Running test: 'test1/tests/is_true_that_setup_data_is_populated.sql'...PASS
Make savepoint...
Running setup: 'test1/tests/table_t1/_setup.sql'...DONE
Running test: 'test1/tests/table_t1/is_true_that_setup_data_is_populated.sql'...PASS
Running test: 'test1/tests/table_t1/assure_that_t1_exists.sql'...PASS
Running test: 'test1/tests/table_t1/detect_missing_t1_records.sql'...FAIL. (2) Missing records:
=================================
id: 33
FAIL. Expected no results!
Rolled back to savepoint.
Running test: 'test1/tests/table_t2/assure_that_t2_is_ok.sql'...PASS
Running test: 'test1/tests/table_t2/detect_missing_t2_records.sql'...PASS
Running test: 'test1/tests/view_latest_t1/assure_that_view_latest_t1_exists.sql'...PASS
Running test: 'test1/tests/view_latest_t1/is_true_that_view_latest_t1_returns_max_value.sql'...PASS
Rolled back transaction.
Closed db connection.
Command error: Tests failed: 1, passed: 8.
```

---

## ⚙️ Configuration

The tool reads its settings from `dbmigration.toml` located next to `dbmigration.py`:

* `default_dbenv` — name of the database environment group to use when no `--dbenv` option is passed;
* `[dbenvs.<name>]` — database environment groups. Each group may contain any of the libpq/psycopg connection options (`host`, `port`, `dbname`, `user`, `connect_timeout`, ...) plus tool-specific options such as `no_password` or `run_tests_by`;
* `[options]` — script file masks (`file_glob_filters`), scripts encoding (`file_read_encoding`, `file_read_encoding_errors`), and the interface language (`language`, e.g. `"ru"`); when `language` is not set, the standard `LANG`/`LC_MESSAGES` environment variables apply, and the `DBMIGRATION_LANGUAGE` environment variable overrides both;
* `[tools.<name>]` — external tools used to apply baseline dumps (`psql`, `pg_restore`) when a baseline subfolder contains a `use_tool.txt` file.

Connection settings for a group can be overridden on the command line via `--host`, `--port`, `--dbname`, `--user`, `-n/--no-password`. The user password is read from the `USER_PASSWORD` environment variable and must not be stored in the configuration file.

---

## ❓ CLI Argument Reference

Global choices:

```
usage: dbmigration.py [-h] {update,verify,init,run-tests,tui} ...

Simple database migrations tool

positional arguments:
  {update,verify,init,run-tests,tui}
                        Available subcommands
    update              Applies base, versioned, and repeatable scripts to the
                        target database schema.
    verify              Validates the target schema and lists versioned and
                        reproducible scripts to apply if the 'update' command
                        is executed.
    init                Creates version control tables in an empty database
                        schema.
    run-tests           Runs db unit test scripts to the target database
                        schema.
    tui                 Open the interactive Textual user interface

options:
  -h, --help            show this help message and exit
```

### `init`

```
usage: dbmigration.py init [-h] [--dbenv DBENV] [--host HOST] [--port PORT]
                           [--dbname DBNAME] [--user USER] [-n] [--force-init]
                           schema_name scripts_path

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
  -n, --no-password  don't ask user password
  --force-init       Force create version control tables even on non empty
                     schema
```

> Note: to allow `--skip-git-checks` and git-OID-based features, `verify` and `update` rely on the `git` command line and a Git repository.

### `update`

```
usage: dbmigration.py update [-h] [--dbenv DBENV] [--host HOST] [--port PORT]
                             [--dbname DBNAME] [--user USER] [-n]
                             [--force-reapply-latest-version]
                             [--force-reapply-all-repeatable]
                             [--force-run-cleanup] [--skip-confirmation]
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
  -n, --no-password     don't ask user password
  --force-reapply-latest-version
                        clean up the latest version within the database and
                        reapply the included *.sql scripts.
  --force-reapply-all-repeatable
                        reapply all repeatable scripts, regardless of changes.
  --force-run-cleanup   run the cleanup script before executing version-
                        specific scripts.
  --skip-confirmation   skip confirmation before executing updates.
```

### `verify`

```
usage: dbmigration.py verify [-h] [--dbenv DBENV] [--host HOST] [--port PORT]
                             [--dbname DBNAME] [--user USER] [-n]
                             [--skip-git-checks] [--skip-diffs]
                             [--skip-display-recent-changes]
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
  -n, --no-password     don't ask user password
  --skip-git-checks     skip grouping changes by git commits
  --skip-diffs          skip unified text diffs between scripts applied in the
                        database (by git OID) and the current script files in
                        the repository
  --skip-display-recent-changes
                        skip display recent changes stored within target db
                        schema
  --build-update-script BUILD_UPDATE_SCRIPT
                        the update script path if you want one as an
                        additional result of the verify command
```

`verify` shows unified text diffs by default: each listed script is followed inline by the diff of the version applied in the DB (by git OID) vs. the current file in the repository. `--skip-diffs` suppresses the diffs and prints the script list only. Brand-new scripts (never applied) are shown collapsed as `+ New file, will be applied in full.` Diff display requires a Git repository and the `git` command line.

### `run-tests`

```
usage: dbmigration.py run-tests [-h] [--dbenv DBENV] [--host HOST]
                                [--port PORT] [--dbname DBNAME] [--user USER]
                                [-n] [--skip-env-checks]
                                schema_name scripts_path

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
  -n, --no-password  don't ask user password
  --skip-env-checks  Skip version and environment ID checks to run tests in
                     any plain environment not made by the tool itself
```

### `tui`

```
usage: dbmigration.py tui [-h] [--dbenv DBENV] [--host HOST] [--port PORT]
                          [--dbname DBNAME] [--user USER] [-n]
                          schema_name scripts_path

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
  -n, --no-password  don't ask user password
```

Runs the interactive Textual UI (see [Text User Interface](#5-text-user-interface-tui)); requires an interactive terminal.

## ❓ How to build & use Docker image


Build on Windows host:

``` powershell
(.venv) PS C:\Users\andrey.larcev\Projects\dbmigrations\dbmigrations> docker build -t dbmigration .
[+] Building 9.3s (19/19) FINISHED                                                                                                                                                          docker:desktop-linux
 => [internal] load build definition from Dockerfile                                                                                                                                                        0.1s
 => => transferring dockerfile: 952B                                                                                                                                                                        0.0s
 => [internal] load metadata for docker.io/library/python:3.11-alpine                                                                                                                                       1.4s
 => [internal] load .dockerignore                                                                                                                                                                           0.1s
 => => transferring context: 2B                                                                                                                                                                             0.0s
 => [internal] load build context                                                                                                                                                                           0.1s
 => => transferring context: 3.20kB                                                                                                                                                                         0.0s
 => [builder 1/5] FROM docker.io/library/python:3.11-alpine@sha256:6857d2dae63e052057f2db389a7061188ac9a92a3fa8d402bde68f36df6fada1                                                                         0.2s
 => => resolve docker.io/library/python:3.11-alpine@sha256:6857d2dae63e052057f2db389a7061188ac9a92a3fa8d402bde68f36df6fada1                                                                                 0.2s
 => CACHED [builder 2/5] WORKDIR /app                                                                                                                                                                       0.0s
 => CACHED [builder 3/5] RUN python -m pip install --no-cache-dir --upgrade pip     && python -m venv /opt/venv                                                                                             0.0s
 => CACHED [builder 4/5] COPY ../requirements-docker.txt .                                                                                                                                                  0.0s
 => CACHED [builder 5/5] RUN pip install --no-cache-dir -r ./requirements-docker.txt                                                                                                                        0.0s
 => CACHED [runner  3/11] COPY --from=builder /opt/venv /opt/venv                                                                                                                                           0.0s
 => CACHED [runner  4/11] RUN /opt/venv/bin/python -m compileall -q /opt/venv                                                                                                                               0.0s
 => CACHED [runner  5/11] RUN apk add --no-cache git                                                                                                                                                        0.0s
 => CACHED [runner  6/11] COPY dbmigration.py .                                                                                                                                                             0.0s
 => [runner  7/11] COPY dbmigration.toml .                                                                                                                                                                  0.4s
 => [runner  8/11] COPY ./translations/ru/LC_MESSAGES/ ./translations/ru/LC_MESSAGES/                                                                                                                       0.4s
 => [runner  9/11] RUN /opt/venv/bin/python -m compileall -q /app                                                                                                                                           0.8s
 => [runner 10/11] RUN adduser -D appuser                                                                                                                                                                   0.8s
 => [runner 11/11] RUN /usr/bin/git config --global --add safe.directory '/migrations'                                                                                                                      0.7s
 => exporting to image                                                                                                                                                                                      4.3s
 => => exporting layers                                                                                                                                                                                     3.3s
 => => exporting manifest sha256:e735b4431ec467dc705e8032841183329bd976703f6934e33cc7fc30dc8200f5                                                                                                           0.1s
 => => exporting config sha256:1c360f8d681a5f23ab48cdb722a6a8ab1a20f3a33bacd1bcdcf4170547d64a7c                                                                                                             0.1s
 => => exporting attestation manifest sha256:69505f827f0b84f789e9df2e455ac5c2ce7c29bfa2deae23dca8b033d7a80e95                                                                                               0.1s
 => => exporting manifest list sha256:038de13757639259910506b25c7dcf02ced1e94a185e24a6a68080fc9300674                                                                                                      0.1s
 => => naming to docker.io/library/dbmigration:latest                                                                                                                                                       0.0s
 => => unpacking to docker.io/library/dbmigration:latest                                                                                                                                                    0.7s
 ```

Use on Windows: (note using /migrations mount point as allowed Git repository root, see [Dockerfile](dbmigrations/Dockerfile))

``` powershell
(.venv) PS C:\Users\andrey.larcev\Projects\dbmigrations\dbmigrations> docker run --rm -it -e PGPASSWORD="1234561" -v "..:/migrations:ro" -v ".\dbmigration.toml:/app/dbmigration.toml:ro" -v ".:/out/" dbmigration verify dev1 /migrations/dbmigrations/samples/deps/dev1 --dbenv local_docker_test1 --skip-git-checks --build-update-script /out/xxx.sql
Opened db connection: 'postgres@host.docker.internal:5432/test1'
Set session search path to: 'dev1'.
Target schema environment ID matches the scripts directory ID: f47ac10b-58cc-4372-a567-0e02b2c3d479
Performing a cross-check for consistency between the target version's repeatable scripts and the versioned scripts...
Completed.
Baseline scripts to install:
  [common/baseline/V000/00_create_t1.sql (OID: 9bdf76b3)]
The scripts directory '/migrations/dbmigrations/samples/deps/dev1' is missing 'versions' subdirectory. Version scripts will be skipped.
No versions are installed in the database schema.
Target version for repeatable scripts: 'V000'.
Repeatable scripts to (re)install:
  [common/repeatable/fn_get_environment_name.sql (OID: ced95c6d)]
  [dev1/repeatable/fn_get_environment_name.sql (OID: ae6980bb)]
  [dev1/repeatable/use_get_environment_name.sql (OID: da50070f)]
  [common/repeatable/insert_into_t1_00.sql (OID: 55e1736c)]
  [dev1/repeatable/insert_into_t1_01.sql (OID: 2f3fd6c5)]
  [dev1/repeatable/insert_into_t1_02.sql (OID: 1ef0147a)]
  [dev1/repeatable/insert_into_t1_03.sql (OID: 08e408c4)]
  [dev1/repeatable/insert_into_t1_04.sql (OID: fac92fec)]
  [dev1/repeatable/insert_into_t1_05.sql (OID: 23240e12)]
Update script is written to '/out/xxx.sql'.
Closed db connection.
(.venv) PS C:\Users\andrey.larcev\Projects\dbmigrations\dbmigrations> cat .\xxx.sql
-- Setting session search path to: dev1
SELECT pg_catalog.set_config('search_path', 'dev1', false);

-- --------- BASELINE VERSION: V000 ---------
BEGIN;
-- Apply script: [common/baseline/V000/00_create_t1.sql (OID:9bdf76b3)]
create table t1 (
    v1 serial not null primary key
);
...
```

And use on WSL as well:

``` bash
avl@n-LarcevAV:~/WinProjects/dbmigrations$ docker run --rm -it -e PGPASSWORD="***" -v ".:/migrations" -v ".\dbmigration.toml:/app/dbmigration.toml" dbmigration init dev1 /migrations/dbmigrations/samples/deps/dev1
Opened db connection: 'postgres@host.docker.internal:5432/test1'
Set session search path to: 'dev1'.
Creating the version control tables with environment ID: 'f47ac10b-58cc-4372-a567-0e02b2c3d479'
Created.
Closed db connection.
avl@n-LarcevAV:~/WinProjects/dbmigrations$ docker run --rm -it -e PGPASSWORD="***" -v ".:/migrations" -v ".\dbmigration.toml:/app/dbmigration.toml" dbmigration update dev1 /migrations/dbmigrations/samples/deps/dev1
Opened db connection: 'postgres@host.docker.internal:5432/test1'
You are going to run updates. Would you like to continue? [y/N]: y
Set session search path to: 'dev1'.
Target schema environment ID matches the scripts directory ID: f47ac10b-58cc-4372-a567-0e02b2c3d479
Performing updates from scripts repository: '/migrations/dbmigrations/samples/deps/dev1'
Performing a cross-check for consistency between the target version's repeatable scripts and the versioned scripts...
Completed.
The baseline version to install V000.
Apply baseline scripts...
Running script: [common/baseline/V000/00_create_t1.sql (OID: 9bdf76b3)]...
Committed.
Setting the baseline version to: 'V000'.
Committed.
Baseline scripts applied.
The scripts directory '/migrations/dbmigrations/samples/deps/dev1' is missing 'versions' subdirectory. Version scripts will be skipped.
Check repeatable scripts...
Target version matches the latest installed version: 'V000'.
Found 9 scripts to re-run
Apply repeatable scripts...
Running script: [common/repeatable/fn_get_environment_name.sql (OID: ced95c6d)]...
Committed.
Running script: [dev1/repeatable/fn_get_environment_name.sql (OID: ae6980bb)]...
Committed.
Running script: [dev1/repeatable/use_get_environment_name.sql (OID: da50070f)]...
Committed.
Running script: [common/repeatable/insert_into_t1_00.sql (OID: 55e1736c)]...
Committed.
Running script: [dev1/repeatable/insert_into_t1_01.sql (OID: 2f3fd6c5)]...
Committed.
Running script: [dev1/repeatable/insert_into_t1_02.sql (OID: 1ef0147a)]...
Committed.
Running script: [dev1/repeatable/insert_into_t1_03.sql (OID: 08e408c4)]...
Committed.
Running script: [dev1/repeatable/insert_into_t1_04.sql (OID: fac92fec)]...
Committed.
Running script: [dev1/repeatable/insert_into_t1_05.sql (OID: 23240e12)]...
Committed.
Repeatable scripts applied.
Updated.
Closed db connection.
```