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

* **Automated Transaction Management**: No need to write explicit `BEGIN` and `COMMIT` blocks.
* **Built-in Schema Versioning**: Automatically maintains history and state logs inside the DB.
* **Error Interception**: Gracefully catches and logs execution errors.
* **Enhanced Safety**: Standardized execution workflows prevent manual deployment mistakes.

---

## 🚀 Requirements & Installation

* **Python 3.11+**
* **psycopg** library

```powershell
python3.exe -m venv .venv
.\.venv\Scripts\Activate.ps1
python3.exe -m pip install -r requirements-dev.txt
```

> ⚠️ **Important Security Note**: Set `USER_PASSWORD` via environment variables.

---

## 📁 Migration Repository Structure

* `baseline/V000/` — Baseline scripts (anonymized/cleaned up the production database dump).
* `versions/V001...VNNN/` — Versioned, incremental migration scripts (main tables, data).
* `repeatable/` — Idempotent scripts (views, triggers, functions, configuration data).
* `tests/` — SQL unit tests.

---

## 💻 Command Line Interface & Usage

### 1. Initialize (`init`)
Creates necessary tracking tables.
```bash
\$env:USER_PASSWORD="topsecret123"
python3 .\dbmigrations\dbmigration.py init test2 .\dbmigrations\samples\test1\
```

### 2. Apply Migrations (`update`)
Executes migrations sequentially.
```bash
python3 .\dbmigrations\dbmigration.py update test2 .\dbmigrations\samples\test1\
```

### 3. Verify Changes (`verify`)
Validates repository consistency and previews changes. Build update script for code review by DB admins. Show recent changes applied to database schema grouped by Git commits.
```bash
python3 .\dbmigrations\dbmigration.py verify test2 .\dbmigrations\samples\test1\ --build-update-script review_patch.sql
```

### 4. Run Unit Tests (`run-tests`)
Executes database-level tests within transactions.
```bash
python3 .\dbmigrations\dbmigration.py run-tests test2 .\dbmigrations\samples\test1\
```

---

## ❓ CLI Argument Reference

Run `python3 .\dbmigration.py -h` to see global choices.

**Key Flags:**
* `init`: `--force-init`
* `verify`: `--skip-git-checks`, `--build-update-script`
* `update`: `--force-reapply-latest-version`, `--force-reapply-all-repeatable`, `--skip-confirmation`

## ❓ How to build & use Docker image


Build on Windows host:

``` powershell
(.venv) PS C:\Users\andrey.larcev\Projects\dbmigrations\dbmigrations> docker build -t dbmigration .
[+] Building 9.3s (19/19) FINISHED                                                                                                                                                          docker:desktop-linux
 => [internal] load build definition from Dockerfile                                                                                                                                                        0.1s
 => => transferring dockerfile: 952B                                                                                                                                                                        0.0s
 => [internal] load metadata for docker.io/library/python:3.11-alpine                                                                                                                                       1.4s
 => [internal] load .dockerignore                                                                                                                                                                           0.0s
 => => transferring context: 2B                                                                                                                                                                             0.0s
 => [internal] load build context                                                                                                                                                                           0.0s
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
 => => exporting manifest list sha256:0383de13757639259910506b25c7dcf02ced1e94a185e24a6a68080fc9300674                                                                                                      0.1s
 => => naming to docker.io/library/dbmigration:latest                                                                                                                                                       0.0s
 => => unpacking to docker.io/library/dbmigration:latest                                                                                                                                                    0.7s
```

Use on Windows: (note using /migrations mount point as allowed Git repository root, see [Dockerfile](dbmigrations/Dockerfile))

``` powershell
(.venv) PS C:\Users\andrey.larcev\Projects\dbmigrations\dbmigrations> docker run --rm -it -e PGPASSWORD="***" -v "..:/migrations" dbmigration verify dev1 /migrations/dbmigrations/samples/deps/dev1 --dbenv local_docker_test1
Opened db connection: 'postgres@host.docker.internal:5432/test1'
Set session search path to: 'dev1'.
Target schema environment ID matches the scripts directory ID: f47ac10b-58cc-4372-a567-0e02b2c3d479
Performing a cross-check for consistency between the target version's repeatable scripts and the versioned scripts...
Completed.
The target schema has the baseline version installed: V000
The scripts directory '/migrations/dbmigrations/samples/deps/dev1' is missing 'versions' subdirectory. Version scripts will be skipped.
Target version for repeatable scripts: 'V000'.
No modified repeatable scripts found for (re)installation.
The list of recent changes were applied to the target schema:
[8498507e] 2026-06-30 - updated samples/envs, added samples/envs/dev2
  Author: Andrey Lartsev
    [2026-08-12 18:11:01 | repeatable | V000   | common/repeatable/fn_get_environment_name.sql (OID: ced95c6d)]
[47a744b4] 2026-06-24 - reworked own version control migrations
  Author: Andrey Lartsev
    [2026-08-12 18:11:01 | repeatable | V000   | dev1/repeatable/insert_into_t1_05.sql (OID: 23240e12)]
[2d65334b] 2026-06-24 - update insert_into_t1_01.sql
  Author: Andrey Lartsev
    [2026-08-12 18:11:01 | repeatable | V000   | dev1/repeatable/insert_into_t1_01.sql (OID: 2f3fd6c5)]
[afc8d20e] 2026-06-23 - reworked own version control tables migrations
  Author: Andrey Lartsev
    [2026-08-12 18:11:01 | repeatable | V000   | dev1/repeatable/insert_into_t1_04.sql (OID: fac92fec)]
[a79ebc85] 2026-06-19 - initial dependency checking implementation
  Author: Andrey Lartsev
    [2026-08-12 18:11:01 | repeatable | V000   | dev1/repeatable/insert_into_t1_03.sql (OID: 08e408c4)]
    [2026-08-12 18:11:01 | repeatable | V000   | dev1/repeatable/insert_into_t1_02.sql (OID: 1ef0147a)]
    [2026-08-12 18:11:01 | repeatable | V000   | dev1/repeatable/use_get_environment_name.sql (OID: da50070f)]
    [2026-08-12 18:11:01 | repeatable | V000   | dev1/repeatable/fn_get_environment_name.sql (OID: ae6980bb)]
[5361d3a9] 2026-06-19 - modified deps sample
  Author: Andrey Lartsev
    [2026-08-12 18:11:01 | repeatable | V000   | common/repeatable/insert_into_t1_00.sql (OID: 55e1736c)]
[7eb51752] 2026-05-28 - updated readme.md
  Author: Andrey Lartsev
    [2026-08-12 18:11:01 | versioned  | V000   | common/baseline/V000/00_create_t1.sql (OID: 9bdf76b3)]
[UNCOMMITTED] ------- - Content hash (OID) is completely untracked or modified locally
  Author: Unknown author
    [2026-08-12 18:11:01 | repeatable | V000   | dev1/repeatable/fn_get_null.sql (OID: 1ddc839b)]
Closed db connection.
```
And use on WSL as well:

``` bash
avl@n-LarcevAV:~/WinProjects/dbmigrations$ docker run --rm -it -e PGPASSWORD="***" -v ".:/migrations" dbmigration init dev1 /migrations/dbmigrations/samples/deps/dev1
Opened db connection: 'postgres@host.docker.internal:5432/test1'
Set session search path to: 'dev1'.
Creating the version control tables with environment ID: 'f47ac10b-58cc-4372-a567-0e02b2c3d479'
Created.
Closed db connection.
avl@n-LarcevAV:~/WinProjects/dbmigrations$ docker run --rm -it -e PGPASSWORD="***" -v ".:/migrations" dbmigration update dev1 /migrations/dbmigrations/samples/deps/dev1
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