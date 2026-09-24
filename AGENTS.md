# AGENTS.md

Instructions for AI coding agents working in this repository.

## Repository layout

Git repo root is `/workspace` (this file lives there). The tool is a thin **facade**
(`dbmigration.py`) over a package of modules – not a single-file module:

```
/workspace/
  AGENTS.md                 this file
  README.md / README.ru.md  user docs (keep in sync for user-facing changes)
  dbmigrations/
    dbmigration.py          THIN FACADE: main(), build_parser, re-export, __main__ guard
    _constants.py           all module-level constants (TOML file name, folder names, patterns)
    _i18n.py                gettext: _(), setup_translations(lang)
    _config.py              TOML config + connection settings + add_common_db_arguments
    _options.py             CommonCliOptions + per-subcommand option dataclasses
    _output.py              Output sink: stdout (CLI) or QueueOutput (TUI worker)
    _confirmation.py        Confirmation: ConsoleConfirm (get_char) / TuiConfirm (modal)
    _db.py                  DbConnection (accepts quiet=True + Output)
    _scripts.py             script parsing/sorting/cleanup/diff rendering
    _git.py                 GitChecker, commit grouping by OID
    _tool.py                ExternalTool (reads TOOL_* via `dbmigration` module at call time)
    _migrations.py          OwnMigration + migration checks
    _launch.py              launch_command(cmd_cls, opts, config, out, confirm)
    _state.py               StateProbe: read-only introspection of schema/repo state
    commands/               Init/Update/Verify/RunTestsCommand + BaseCommand
    tui/                    Textual app: app.py (MainApp), worker.py, widgets/, screens/
    dbmigration.toml        local config (see "Environment")
    dbmigration.example.toml
    requirements-dev.txt    dev/test deps (pytest, psycopg, Babel, textual, ...)
    requirements-docker.txt docker deps
    Dockerfile              copies all package modules + commands/ + tui/ + translations
    samples/                sample migration repos used by e2e tests/READMEs
    unit_tests/             pytest unit tests (mock `GitChecker`, no DB needed)
    tests/                  end-to-end pytest tests (`flow1_*`, `flow2_use_tool/...`)
    translations/           gettext catalogs: messages.pot + ru/LC_MESSAGES/{messages.po,messages.mo}
  scripts/
    provision_env.sh        idempotent environment bootstrap (Alpine container)
  doc/                      design docs, diagrams (do not edit unless asked)
```

## What the tool is

PostgreSQL migration tool with 5 subcommands: `init`, `update`, `verify`, `run-tests`, `tui`.

Key facts an agent must know:

- `dbmigration` is a **module name**: tests do `from dbmigration import ...`. Never
  import it as `from dbmigrations...`. Always run Python from the `dbmigrations/`
  directory (`/workspace/dbmigrations`) so the module resolves.
- Tool path: `python /workspace/dbmigrations/dbmigration.py <cmd> <env> <repo> [flags]`.
- `tui` opens a Textual TUI (needs an interactive terminal); it lazy-imports the
  `tui` package, so plain CLI subcommands work without `textual` installed.
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
USER_PASSWORD=dummy "$VENV/bin/python" -m pytest unit_tests/ tests/ -q  # full suite (~110 tests)
USER_PASSWORD=dummy "$VENV/bin/python" -m pytest tests/flow2_use_tool/14_verify_diffs_test.py -q  # single e2e
USER_PASSWORD=dummy "$VENV/bin/python" -m pytest unit_tests/test_tui.py -q  # TUI unit tests (headless Pilot)
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

When a user-facing string changes/added (any `_()` call in `dbmigration.py`, `_*.py`,
`commands/`, or `tui/`) the catalogs must stay in sync:

```sh
# 1. re-extract the template (scans the package; skips tests/translations)
$VENV/bin/pybabel extract --input-dirs=. --ignore-dirs=unit_tests --ignore-dirs=tests \
  --ignore-dirs=translations --keywords=_ --output=translations/messages.pot
# 2. merge into the Russian catalog
$VENV/bin/pybabel update -i translations/messages.pot -o translations/ru/LC_MESSAGES/messages.po -l ru
# 3. fill new msgstr entries in messages.po, then compile
$VENV/bin/pybabel compile -d translations -l ru
```

## Agent conventions

- Do not add code comments unless the user asks.
- Do not commit, push, or create PRs unless explicitly asked.
- When a change affects user-facing output, keep `README.md` and `README.ru.md` and
  the Russian translation (`messages.po` + `.mo`) in sync with it.
- Prefer existing patterns and the existing style of `dbmigration.py`; it is the only
  source of behavior truth.
- Keep `dbmigration.py` a thin facade: new modules live in the package, the facade
  re-exports whatever the tests/READMEs import (`from dbmigration import ...`).
  Check with `grep -rn "from dbmigration import"` after moving code.
- TUI-strings must go through `_()` from `_i18n`; do not hardcode user-facing labels
  in `tui/`. `setup_translations()` runs in `main()` before the `tui` handler imports
  the package.
- Keep README user-facing descriptions terse: modal-window behavior should be self-evident
  from the UI (buttons, hints like `Esc`); do not document "how the modal works" in detail.

## TUI modal windows (internal notes)

- `tui/screens/confirm_screen.py` — `ConfirmScreen`, modal `update` confirmation.
  Compact, auto-sized to the question text (`width: auto; height: auto; max-width: 80%`),
  no window title, no scroll area: a `Static` with the question + `Yes`/`No` buttons.
  Key bindings: `y`/`enter` confirm, `n`/`escape` abort; the trailing `[y/N]: ` part of
  the question text comes from `UpdateCommand` and is stripped before display.
- `tui/screens/file_viewer.py` — `FileViewerScreen(title, script_loader, *, oid="", diff_loader=None)`,
  `ModalScreen` opened (from `LogPanel.OidActivated`) to view a git blob. The caption is set on
  the screen itself (`Header(show_clock=False)` + `self.sub_title`, typically
  `"<relative path> (OID: <oid>)"`); the dialog has no title row. It shows an indeterminate
  `ProgressBar` + "Loading script content..." until the content arrives; `_load()` fetches
  via `asyncio.to_thread(loader)` and writes the text into the central `#file_content`
  RichLog (`markup=False`); load failures / missing blob are written in red. The right panel
  (`#file_viewer_panel`) holds a `RadioSet` – `#radio_script` ("Show script text") /
  `#radio_diff` ("Show as diff"); switching modes reloads the content via a second loader
  (`diff_loader`, used to show a git diff against the current file; a generation counter
  `_gen` drops stale loads). A click on the dimmed backdrop outside the dialog
  (`on_click` + `dialog.region.contains(screen_x, screen_y)`) and a `Footer` binding both
  close the screen. Bindings: `escape`/`q`/`enter` → `action_close` (dismiss).
- `tui/widgets/log_panel.py` — `LogPanel` (RichLog). Mouse text-selection is intentionally
  removed (unusable in Textual); a plain left click posts `OidActivated` via
  `on_mouse_up` + `oid_info_at()` which also handles entries wrapped across rows
  (scans down the next rows; even combines two rows if the hex is split by the wrap).
  `append_error()` renders red without markup. `copy_visible()`/`as_plain_text()` back
  the `ctrl+shift+c`/`ctrl+shift+x` actions in `tui/app.py`.
- Textual 8.2.8 gotchas: CSS uses `dock:` (not `docking:`), no `column-gap` (use
  margins); `DOMQuery` has no `__eq__` so `query(...) == []` is always False (use
  truthiness); `Strip` yields `(text, style, control)` segments.