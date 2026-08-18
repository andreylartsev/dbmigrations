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

``` powershell
(.venv) PS C:\Users\andrey.larcev\Projects\dbmigrations\dbmigrations> docker build .
[+] Building 4.8s (15/15) FINISHED                                                                                                                                                          docker:desktop-linux
 => [internal] load build definition from Dockerfile                                                                                                                                                        0.1s
 => => transferring dockerfile: 810B                                                                                                                                                                        0.0s
 => [internal] load metadata for docker.io/library/python:3.11-slim                                                                                                                                         0.9s
 => [internal] load .dockerignore                                                                                                                                                                           0.1s
 => => transferring context: 2B                                                                                                                                                                             0.0s
 => [internal] load build context                                                                                                                                                                           0.3s
 => => transferring context: 23.09kB                                                                                                                                                                        0.1s
 => [builder 1/6] FROM docker.io/library/python:3.11-slim@sha256:9c900dea9e8fb7e16277c179b555cc72d29a352dbc33cff48ad5a0412fd5bfc7                                                                           0.5s
 => => resolve docker.io/library/python:3.11-slim@sha256:9c900dea9e8fb7e16277c179b555cc72d29a352dbc33cff48ad5a0412fd5bfc7                                                                                   0.4s
 => CACHED [builder 2/6] WORKDIR /app                                                                                                                                                                       0.0s
 => CACHED [runner 3/6] RUN apt-get update && apt-get install -y --no-install-recommends     git     && rm -rf /var/lib/apt/lists/*                                                                         0.0s
 => CACHED [builder 3/6] RUN python -m pip install --no-cache-dir --upgrade pip     && python -m venv /opt/venv                                                                                             0.0s
 => CACHED [builder 4/6] COPY ../requirements-docker.txt .                                                                                                                                                  0.0s
 => CACHED [builder 5/6] COPY ../requirements-dev.txt .                                                                                                                                                     0.0s
 => CACHED [builder 6/6] RUN pip install --no-cache-dir -r ./requirements-docker.txt                                                                                                                        0.0s
 => CACHED [runner 4/6] COPY --from=builder /opt/venv /opt/venv                                                                                                                                             0.0s
 => CACHED [runner 5/6] RUN useradd --create-home appuser                                                                                                                                                   0.0s
 => [runner 6/6] COPY . .                                                                                                                                                                                   0.6s
 => exporting to image                                                                                                                                                                                      1.6s
 => => exporting layers                                                                                                                                                                                     0.8s
 => => exporting manifest sha256:59f6a061fc704b291eebbee778735eab2c53740b16519032cd844bd3d872a868                                                                                                           0.1s
 => => exporting config sha256:f934b243fae623df8a1b1d174531aa0ee5494470f00cc0aac97e5cf1173832f7                                                                                                             0.1s
 => => exporting attestation manifest sha256:3f54cae552ee4d243c0615c8c3eb3014a9ccf7180fd796f7fa6642c5c9e64da8                                                                                               0.2s
 => => exporting manifest list sha256:c405f3b0d2addb27c4b8948d994ceeafc06598b6d16534ec3f35863929f7e4a8                                                                                                      0.1s
 => => naming to moby-dangling@sha256:c405f3b0d2addb27c4b8948d994ceeafc06598b6d16534ec3f35863929f7e4a8                                                                                                      0.0s
 => => unpacking to moby-dangling@sha256:c405f3b0d2addb27c4b8948d994ceeafc06598b6d16534ec3f35863929f7e4a8                                                                                                   0.3s
```

``` powershell
(.venv) PS C:\Users\andrey.larcev\Projects\dbmigrations\dbmigrations> docker run --rm -e PGPASSWORD="1234561" -v "C:\Users\andrey.larcev\Projects\dbmigrations:/repo" -w /repo dbmigration verify test3 dbmigrations/samples/test1 --host host.docker.internal --skip-git-checks
Opened db connection: 'postgres@host.docker.internal:5432/test1'
Set session search path to: 'test3'.
Target schema environment ID matches the scripts directory ID: 4a40342c-4546-4776-bf97-b02b2a858924
Performing a cross-check for consistency between the target version's repeatable scripts and the versioned scripts...
Completed.
The target schema has the baseline version installed: V000
The latest installed version is V002. No newer scripts found for installation.
Target version for repeatable scripts: 'V002'.
No modified repeatable scripts found for (re)installation.
The list of recent changes were applied to the target schema:
    [2026-08-19 00:55:03 | repeatable | V002   | test1/repeatable/01_create_view_max_t2_kk.sql (OID: 1278759c)]
    [2026-08-19 00:55:03 | repeatable | V002   | test1/repeatable/00_create_view_latest_t1.sql (OID: 1504cd9a)]
    [2026-08-19 00:55:03 | versioned  | V002   | test1/versions/V002/dummy.sql (OID: 384d538d)]
    [2026-08-19 00:55:03 | versioned  | V002   | test1/versions/V002/_cleanup.sql (OID: 384d538d)]
    [2026-08-19 00:55:02 | versioned  | V001   | test1/versions/V001/01_insert_into_t2.sql (OID: ff5717bd)]
    [2026-08-19 00:55:02 | versioned  | V001   | test1/versions/V001/00_create_t2.sql (OID: a3e53fb6)]
    [2026-08-19 00:55:02 | versioned  | V000   | test1/baseline/V000/01_insert_into_t1.sql (OID: 2d3fb169)]
    [2026-08-19 00:55:02 | versioned  | V000   | test1/baseline/V000/00_create_t1.sql (OID: 9bdf76b3)]
Closed db connection.
```

``` bash
avl@n-LarcevAV:~/WinProjects/dbmigrations/dbmigrations$ docker run --rm -e PGPASSWORD="1234561" -v "/home/avl/WinProjects/dbmigrations:/repo" -w /repo dbmigration verify test3 dbmigrations
/samples/test1 --host host.docker.internal
Opened db connection: 'postgres@host.docker.internal:5432/test1'
Set session search path to: 'test3'.
Target schema environment ID matches the scripts directory ID: 4a40342c-4546-4776-bf97-b02b2a858924
Performing a cross-check for consistency between the target version's repeatable scripts and the versioned scripts...
Completed.
The target schema has the baseline version installed: V000
The latest installed version is V002. No newer scripts found for installation.
Target version for repeatable scripts: 'V002'.
No modified repeatable scripts found for (re)installation.
The list of recent changes were applied to the target schema:
[563cf87e] 2026-07-21 - more use of ScriptInfo
  Author: Andrey Lartsev
    [2026-08-19 00:55:03 | repeatable | V002   | test1/repeatable/00_create_view_latest_t1.sql (OID: 1504cd9a)]
[2afeb5db] 2026-07-21 - more use of script info
  Author: Andrey Lartsev
    [2026-08-19 00:55:03 | repeatable | V002   | test1/repeatable/01_create_view_max_t2_kk.sql (OID: 1278759c)]
[5361d3a9] 2026-06-19 - modified deps sample
  Author: Andrey Lartsev
    [2026-08-19 00:55:02 | versioned  | V000   | test1/baseline/V000/01_insert_into_t1.sql (OID: 2d3fb169)]
[7eb51752] 2026-05-28 - updated readme.md
  Author: Andrey Lartsev
    [2026-08-19 00:55:02 | versioned  | V000   | test1/baseline/V000/00_create_t1.sql (OID: 9bdf76b3)]
[236b0868] 2026-05-18 - fixed running of _cleanup.sql within environment specific folders
  Author: Andrey Lartsev
    [2026-08-19 00:55:03 | versioned  | V002   | test1/versions/V002/dummy.sql (OID: 384d538d)]
    [2026-08-19 00:55:03 | versioned  | V002   | test1/versions/V002/_cleanup.sql (OID: 384d538d)]
[d606d6a9] 2026-05-16 - added environment inheritance sample
  Author: Andrey Lartsev
    [2026-08-19 00:55:02 | versioned  | V001   | test1/versions/V001/01_insert_into_t2.sql (OID: ff5717bd)]
    [2026-08-19 00:55:02 | versioned  | V001   | test1/versions/V001/00_create_t2.sql (OID: a3e53fb6)]
Closed db connection.
```