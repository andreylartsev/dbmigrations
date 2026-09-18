# AGENTS.md

Instructions for AI coding agents working in this repository.

## Repository layout

Git repo root is `/workspace` (this file lives there). The tool is a single-file CLI
module, not a package:

```
/workspace/
  AGENTS.md                 this file
  README.md / README.ru.md  user docs (keep in sync for user-facing changes)
  dbmigrations/
    dbmigration.py          the whole tool (single module, ~150K)
    dbmigration.toml        local config (see "Environment")
    dbmigration.example.toml
    requirements-dev.txt    dev/test deps (pytest, psycopg, Babel, ...)
    requirements-docker.txt docker deps
    Dockerfile
    samples/                sample migration repos used by e2e tests/READMEs
    unit_tests/             pytest unit tests (mock `GitChecker`, no DB needed)
    tests/                  end-to-end pytest tests (`flow1_*`, `flow2_use_tool/...`)
    translations/           gettext catalogs: messages.pot + ru/LC_MESSAGES/{messages.po,messages.mo}
  scripts/
    provision_env.sh        idempotent environment bootstrap (Alpine container)
  doc/                      design docs, diagrams (do not edit unless asked)
```

## What the tool is

PostgreSQL migration tool with 4 subcommands: `init`, `update`, `verify`, `run-tests`.

Key facts an agent must know:

- `dbmigration` is a **module name**: tests do `from dbmigration import ...`. Never
  import it as `from dbmigrations...`. Always run Python from the `dbmigrations/`
  directory (`/workspace/dbmigrations`) so the module resolves.
- Tool path: `python /workspace/dbmigrations/dbmigration.py <cmd> <env> <repo> [flags]`.
- **Generic scripts repo** for `verify`/`update` is a git repo; the tool relies on the
  `git` CLI and git OIDs stored in the DB control tables.
- The tool patches `dbmigration.toml` per-run for the target env / tool paths — do not
  edit `dbmigration.toml` to change behavior in a task; it is local runtime config.
- Config language key `language = "ru"` localizes output via `gettext`; after changing
  any user-facing string also update `translations/messages.pot`,
  `translations/ru/LC_MESSAGES/messages.po` and recompile the `.mo`.

## How to bootstrap the environment

Everything is idempotent and root-runnable:

```sh
/workspace/scripts/provision_env.sh
```

It installs system packages (git, postgresql18, python3, py3-pip), initializes/starts
PostgreSQL 18 on port 5432 with trust auth, creates db `test1`, adds
`host.docker.internal` to `/etc/hosts`, creates a venv and patches
`dbmigration.toml` to point tools at the Linux binaries
(`psql`, `pg_restore`) and to `git` (host toolchain).

Notes:

- The script's venv lives at `/tmp/opencode/venv`. The venv currently in use in this
  container is `/tmp/venv` — use whichever exists (`-x "$VENV/bin/python"`), it already
  has all `requirements-dev.txt` deps installed.
- Original `dbmigration.toml` is backed up once to `/tmp/opencode/dbmigration.toml.bak`;
  restore from there if you need the pristine copy.
- **PostgreSQL control tables live in the target schema, not the database.** The
  target schema must be created first (`CREATE SCHEMA <env>;`) before running
  `init`, otherwise `init` fails with
  `Command error: The target schema '...' is not accessible`.

Manual checklist (if the script was not run):

1. `apk add git postgresql18 postgresql18-client python3 py3-pip`
2. Init/start PostgreSQL: `su postgres -c initdb` + `pg_ctl ... start` (data dir
   `/var/lib/postgresql/data`), prepend `trust` rules to `pg_hba.conf`,
   `su postgres -c createdb test1`.
3. `echo "127.0.0.1 host.docker.internal" >> /etc/hosts`
4. `python3 -m venv /tmp/venv && /tmp/venv/bin/pip install -r requirements-dev.txt`
5. Patch `dbmigration.toml`: `psql`/`pg_restore` → `/usr/bin/...`, args for
   `pg_restore` → `["--exit-on-error", "-d", "${dbname}", "${file}"]`, disable
   `*.exe`/macOS entries (see `provision_env.sh` step 6).

## Commands to verify work

Run from `/workspace/dbmigrations`:

```sh
VENV=/tmp/venv   # or /tmp/opencode/venv, whichever exists
USER_PASSWORD=dummy "$VENV/bin/python" -m pytest unit_tests/ -q   # unit tests (fast, no DB)
USER_PASSWORD=dummy "$VENV/bin/python" -m pytest unit_tests/ tests/ -q  # full suite (~88 tests)
USER_PASSWORD=dummy "$VENV/bin/python" -m pytest tests/flow2_use_tool/14_verify_diffs_test.py -q  # single e2e
```

There is no configured linter/typechecker — the pytest suite is the verification gate.
Always run at least the affected e2e test and the full suite before reporting done.

## E2E test constraints

- E2e tests create a fresh schema in db `test1` and run the real tool against it —
  they need PostgreSQL up and `dbmigration.toml` patched (see above).
- Tests that call `init` must create the target schema first, e.g.
  `psql ... -c "CREATE SCHEMA <env>;"` before `init` or the test fails with the
  "not accessible" error.
- E2e tests may write an update script / other artifacts into temp dirs under the
  repo — clean up after yourself.

## Translations workflow

```sh
# after editing messages.po / messages.pot
$VENV/bin/pybabel compile -d translations -l ru
```

## Agent conventions

- Do not add code comments unless the user asks.
- Do not commit, push, or create PRs unless explicitly asked.
- When a change affects user-facing output, keep `README.md` and `README.ru.md` and
  the Russian translation (`messages.po` + `.mo`) in sync with it.
- Prefer existing patterns and the existing style of `dbmigration.py`; it is the only
  source of behavior truth.