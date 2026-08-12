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
