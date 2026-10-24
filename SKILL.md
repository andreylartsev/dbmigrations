---
name: dbmigrations
description: PostgreSQL database migration tool - init/update/verify/run-tests/tui for managing schema migrations from Git repos with versioned/repeatable scripts
---

# DBMigrations Tool

PostgreSQL migration tool with 5 subcommands: `init`, `update`, `verify`, `run-tests`, `tui`. Manages database schema migrations from Git-versioned script repositories.

## Repository Structure

Scripts repo contains:
- `baseline/V000/` - baseline scripts (applied once)
- `versions/V001...VNNN/` - versioned incremental migrations (applied once per version ascending)
- `repeatable/` - idempotent scripts (re-applied on every update if changed)
- `tests/` - SQL unit tests for run-tests

Execution order: by `script_list.txt` if present, else alphabetical. Tool tracks state in DB control tables in target schema.

## Prerequisites

- PostgreSQL running
- Target schema must exist before `init`: `CREATE SCHEMA <env>;`
- Config at `dbmigrations/dbmigration.toml` (copy from `dbmigration.example.toml`)
- Password via env var `USER_PASSWORD` (or per config)
- Run from `dbmigrations/` directory so `dbmigration` module resolves

## CLI Usage

```bash
cd /workspace/dbmigrations
python3 dbmigration.py <command> <schema> <scripts_repo> [flags]
```

### Commands

1. **init** - Create version control tables in empty schema
```bash
export USER_PASSWORD=...
python3 dbmigration.py init <schema> <repo_path>
```

2. **update** - Apply migrations sequentially
```bash
python3 dbmigration.py update <schema> <repo_path> [--skip-confirmation]
```

3. **verify** - Validate repo consistency, show diffs, build update script for review
```bash
python3 dbmigration.py verify <schema> <repo_path> [--skip-diffs] [--build-update-script review.sql]
```

4. **run-tests** - Run SQL unit tests in rollback-safe transactions
```bash
python3 dbmigration.py run-tests <schema> <repo_path>
```

5. **tui** - Interactive Textual UI
```bash
python3 dbmigration.py tui <schema> <repo_path>
```

## Configuration

Edit `dbmigrations/dbmigration.toml`:
- `default_dbenv` - default environment
- `[dbenvs.<name>]` - DB connection (host, port, dbname, user, etc.)
- `[options]` - file filters, encoding, language
- `[tools.psql]`, `[tools.pg_restore]` - external tools for baselines

## Testing

```bash
cd /workspace/dbmigrations
export USER_PASSWORD=dummy
# Unit tests (fast, no DB)
python3 -m pytest unit_tests/ -q
# E2E tests (need PostgreSQL up, schema created for init tests)
python3 -m pytest tests/ -q
```

## Key Notes

- Always run from `dbmigrations/` directory (module name is `dbmigration`, not `dbmigrations`)
- Target schema must be created before `init`
- E2E tests create fresh schemas in db `test1`
- Config language can be `ru` or `en` (controlled by `DBMIGRATION_LANGUAGE` env var in tests)
- Git integration used for diffs/verification
