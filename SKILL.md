---
name: dbmigrations
description: Use when applying, verifying, or testing PostgreSQL schema migrations with the dbmigration.py tool (init/update/verify/run-tests/tui) from a Git-versioned scripts repository.
---

# DBMigrations

PostgreSQL migration tool with 5 subcommands: `init`, `update`, `verify`, `run-tests`, `tui`.
It applies DDL/DML scripts from a scripts repository, tracks what was applied by Git blob OID
inside the target schema, and can generate a reviewable SQL script for production DBAs.

## Before you run anything

- The tool does **not** create databases, users, roles or schemas. Create them first, e.g.
  `CREATE SCHEMA <schema>;`. `init` fails if the target schema does not exist.
- The target schema must be **empty** for `init` (use `--force-init` only intentionally).
- Connection settings come from `dbmigrations/dbmigration.toml` next to `dbmigration.py`.
- The password is read from the `USER_PASSWORD` environment variable. Never store it in TOML.
  `no_password = true` in the dbenv group or `-n/--no-password` disables the prompt.
- Run from `/workspace/dbmigrations` so the flat modules resolve:
  `python3 dbmigration.py ...`. The tool is the module `dbmigration` (singular).
- `verify` is read-only. `update`, `init` and `run-tests` write to the database
  (`run-tests` rolls its transaction back).
- Run `verify` before `update` against any shared or production database.
- Updates are incremental and preserve data. There are no rollback scripts: use fix-forward.
- Back up a production database before applying changes.

## Repository layout

```
repo/
  baseline/V000/            # applied once, when no baseline is recorded yet
  versions/V001/ ... VNNN/  # applied once per version, ascending
  repeatable/               # re-applied when the script changes
  tests/                    # SQL tests for run-tests
  target_environment_id.txt # optional
  set_search_path.txt       # optional
```

Example repositories under `dbmigrations/samples/` are safe to use for local experimentation.

Environment-specific repositories can share sibling scripts:

```
repo/
  common/{baseline,versions,repeatable,tests}/
  dev1/{baseline,versions,repeatable,tests}/
```

`script_list.txt` in a folder overrides alphabetical order. In environment-specific repos it
also pulls shared scripts with `@common/*`; individual SQL scripts can request a prerequisite
with a comment such as `-- @depends_on @common/fn.sql` or `-- @depends_on ./other.sql`.

Special files:

| File | Meaning |
|------|---------|
| `script_list.txt` | Execution order inside one folder; `@common/*` pulls shared scripts from a sibling env folder. |
| `repeatable/target_version.txt` | Version the repeatable scripts belong to; must equal the latest baseline/versioned version. |
| `tests/target_version.txt` | Version the tests expect; must match the installed version unless `run-tests --skip-env-checks`. |
| `set_search_path.txt` | Overrides the session search path; the target schema must have that exact name. |
| `target_environment_id.txt` | Expected environment ID. If absent, the repo directory name is used. |
| `use_tool.txt` in a baseline version folder | Names an external tool from `[tools.<name>]` (e.g. `psql`, `pg_restore`). |
| `versions/VNNN/_cleanup.sql` | Cleanup script run before a version is reapplied or when `--force-run-cleanup` is used. |

Apply order is: baseline, then versioned scripts ascending, then changed repeatable scripts.
A missing `versions/` directory is not an error; versioned scripts are skipped. Versioned scripts
require the baseline version to be installed first.

## State in the database

The target schema contains these tables. Do not edit them by hand:

- `dbmigration_environment_id` - environment identity written by `init`
- `dbmigration_versions` - applied baseline and versioned versions
- `dbmigration_version_scripts` - paths and Git blob OIDs of applied scripts
- `dbmigration_repeatable_scripts` - paths, versions and Git blob OIDs of repeatable scripts

`update`, `verify` and `run-tests` refuse to work when the environment ID in the schema does not
match the scripts repository.

## Commands

All commands share connection flags: `--dbenv <name>`, `--host`, `--port`, `--dbname`,
`--user`, `-n/--no-password`.

### init

```bash
export USER_PASSWORD=...
python3 dbmigration.py init <schema> <repo_path> --dbenv <env>
```

Creates the version control tables and records the environment ID. It does not apply baseline
scripts; run `update` afterwards. On a non-empty schema it fails unless `--force-init` is given.

### update

```bash
python3 dbmigration.py update <schema> <repo_path> --dbenv <env> --skip-confirmation
```

Applies baseline once, then new versions, then changed repeatable scripts. All scripts of one
version and its version-control rows are committed in a single transaction. Baseline scripts are
committed individually and their version-control rows in a separate transaction; each repeatable
script is committed together with its control row. Without `--skip-confirmation` it asks for
confirmation first; use the flag only in automation or after explicit approval.

Flags:

- `--force-reapply-latest-version` - remove the latest installed version and reapply it
  (runs `_cleanup.sql` first).
- `--force-reapply-all-repeatable` - reapply all repeatable scripts regardless of changes.
- `--force-run-cleanup` - run `_cleanup.sql` before applying version scripts.
- `--skip-confirmation` - do not prompt.

### verify

```bash
python3 dbmigration.py verify <schema> <repo_path> --dbenv <env>
python3 dbmigration.py verify <schema> <repo_path> --dbenv <env> \
  --build-update-script /tmp/review.sql
```

Read-only preview: validates repository consistency, lists baseline/versioned/repeatable scripts
that `update` would apply, shows inline diffs of already-applied scripts, and shows recent changes
recorded in the database. With `--build-update-script`, writes a transaction-safe SQL script that a
DBA can review and apply with `psql`. The output file must not already exist.

Flags:

- `--skip-diffs` - list scripts without unified diffs.
- `--skip-git-checks` - disable Git grouping (this also disables diffs and Git-based history).
- `--skip-display-recent-changes` - hide the recent changes audit.
- `--build-update-script <path>` - write the reviewable update script.

Git diffs and commit grouping require the `git` CLI and a Git repository. `update` itself works
without Git because the tool computes Git blob SHA-1 values directly.

### run-tests

```bash
python3 dbmigration.py run-tests <schema> <repo_path> --dbenv <env>
```

Runs SQL tests in one transaction that is rolled back at the end. `_setup.sql` files create test
data under savepoints; leaving a folder rolls back its setup. Test file names must start with:

- `assure_that_` - must execute without errors.
- `is_true_that_` - must return a single boolean `true`.
- `detect_missing_` - must return no rows.

Any other prefix fails. The command exits non-zero if any test fails, with a summary such as
`Command error: Tests failed: 2, passed: 9.` If `run_tests_by` is configured, tests connect as
that user, using `TESTER_PASSWORD` and falling back to `USER_PASSWORD`.
`--skip-env-checks` skips the version/environment checks for plain non-tool-managed schemas.

### tui

```bash
python3 dbmigration.py tui <schema> <repo_path> --dbenv <env>
```

Requires an interactive terminal. It runs `verify` on start and streams logs. Shortcuts:
`ctrl+r` verify, `ctrl+c` cancel, `ctrl+shift+c` copy all, `ctrl+shift+x` copy visible,
`r` refresh state, `q` quit. Clicking a line with `(OID: ...)` opens the script. The TUI always
confirms before `update`; the skip-confirmation option is intentionally unavailable there.

## Configuration

`dbmigrations/dbmigration.toml`:

- `default_dbenv` - default environment group.
- `[dbenvs.<name>]` - libpq/psycopg options (`host`, `port`, `dbname`, `user`, ...) plus
  `no_password` and `run_tests_by`.
- `[options]` - `file_glob_filters`, `file_read_encoding`, `file_read_encoding_errors`,
  `log_max_lines`, `language`.
- `[tools.<name>]` - `executable`, `args`, `success_result_code` for external baseline tools;
  args support `${host}`, `${port}`, `${user}`, `${dbname}`, `${file}`, `${schema_name}`.
- `git_cmd_path` - optional path to the Git executable.

CLI flags override connection settings. `language` can be set in TOML; standard `LANG`/
`LC_MESSAGES` apply otherwise, and `DBMIGRATION_LANGUAGE` overrides both.

## Common failures

| Symptom | Cause / fix |
|---------|-------------|
| `The target schema '...' is not accessible` | Create the schema first with `CREATE SCHEMA`. |
| `The stored environment ID ... does not match ...` | Schema was initialized from a different repo. Use the matching repo; do not force blindly. |
| `The target version ... does not match ...` | Fix `repeatable/target_version.txt` or `tests/target_version.txt`, or install the expected version. |
| `missing 'versions' subdirectory` | Informational; versioned scripts are skipped. |
| `The specified script file ... already exists` | Choose a new path for `--build-update-script`. |
| `Git executable was not found` | Install `git` or set `git_cmd_path`; diffs and Git history will be unavailable. |
| `Tests failed: N, passed: M` | A DB test failed; inspect the `FAIL` lines in the output. |

## Working on the tool itself

Read `AGENTS.md` first. `dbmigration.py` is a thin facade; new code goes into the package
modules (`commands/`, `tui/`, `_*.py`) and is re-exported from the facade. Translation catalogs
must be updated for user-facing strings.

```bash
cd /workspace/dbmigrations
VENV=/tmp/venv   # or /tmp/opencode/venv
USER_PASSWORD=dummy "$VENV/bin/python" -m pytest unit_tests/ -q          # fast, no DB
USER_PASSWORD=dummy "$VENV/bin/python" -m pytest unit_tests/ tests/ -q   # full suite
```

The e2e tests run the real CLI against PostgreSQL, create/drop their own schemas (`test3` for the
`test1` sample, `esbdb` for the dump sample) in the database from the default dbenv, and force
`DBMIGRATION_LANGUAGE=en`. They need PostgreSQL running and `dbmigration.toml` pointing at the
local `psql`/`pg_restore`/`git`. There is no linter or typechecker; pytest is the gate.
