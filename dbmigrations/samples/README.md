# Sample DDL/DML scripts

This folder includes sample DML/DDL scripts repositories and you can try the tool with them.

Every sample repository below was verified in the local environment with
`init <schema> <repo> --dbenv local_windows` followed by
`update <schema> <repo> --dbenv local_windows --skip-confirmation` against a freshly
created empty target schema. Common prerequisites:

* The target schema must already exist in the database (`init` fails otherwise).
* If the repository contains `set_search_path.txt`, the target schema must have
  exactly the name specified inside it (see `envs/` and `test1_baseline_with_dump`).
* If the repository contains `target_environment_id.txt`, the environment ID stored
  in the schema by `init` must match it.

## Repositories that apply cleanly

- [test1](./test1) - Includes all the variety of possible script types i.e. baseline, versioned and repeatable scripts
  - baseline/V000 - creates the table t1 and inserts one record;
  - versions/V001 - creates the table t2 and inserts one record;
  - versions/V002 - a dummy version script plus a cleanup script;
  - repeatable/ - idempotent scripts that (re)create views on tables t1 and t2;
  - tests/ - a set of SQL unit tests organized into subfolders.
- [test1_baseline_with_dump](./test1_baseline_with_dump) - Baseline scripts applied with the external `psql` tool
  (`use_tool.txt` inside `baseline/V000`). Contains `00_esbdb_schema.sql` (DDL)
  and `01_esbdb_data.sql` (COPY-style dump of data). The target schema must be
  named `esbdb` because of `set_search_path.txt`.
- [test1_empty_baseline_and_repeatable](./test1_empty_baseline_and_repeatable) - A dummy baseline script + repeatable script(s).
  Demonstration of extending another system's schema, i.e. integration via the DB. Yes, a well known anti-pattern;
- [test1_just_baseline](./test1_just_baseline) - Checks that a repository folder can contain only baseline scripts;
- [test1_no_repeatable](./test1_no_repeatable) - Checks that a repository folder can contain only baseline and versioned scripts;
- [test1_reapply_latest](./test1_reapply_latest) - Baseline + one version with a `_cleanup.sql`.
  Use it together with `update --force-reapply-latest-version` to demonstrate how the
  latest installed version is dropped and reapplied;
- [test1_with_lists](./test1_with_lists) - Demonstration of the `script_list.txt` files that define the
  execution order of scripts instead of relying on alphabetical sorting; the requested feature.
  The files order scripts inside `baseline/V000`, `versions/V001` and `repeatable/`;
- [deps/dev1](./deps/dev1) - A dependencies repository: baseline and repeatable scripts are taken
  from the shared sibling folder `deps/common` (see `@common/*` entries in `script_list.txt`).
  Applied cleanly with the target schema created on-the-fly;
- [envs/dev1](./envs/dev1) and [envs/dev2](./envs/dev2) - Environment-specific repositories that
  override the shared scripts from `envs/common`. `set_search_path.txt` contains the
  target schema name (`dev1`, `dev2`), so the schema must be created with that exact name.

## Validation demos that intentionally fail

- [test1_empty_version](./test1_empty_version) - Tests that empty versions are not allowed;
- [test1_with_wrong_version_following](./test1_with_wrong_version_following) - Demonstration of the
  cross-check of the correct order between the baseline version and the versioned scripts:
  a versioned subfolder named `A001` (below the baseline `V000`) is rejected.

## Folder-layout demos with prerequisites

- [test1_latest_version_only_and_repeatable](./test1_latest_version_only_and_repeatable) - Checks that a
  repository folder can contain just one latest version and repeatable scripts. `init`
  and the folder cross-checks pass; `update` requires the baseline version to be
  already installed in the schema (it is not a part of this sample);
- [test1_only_repeatable](./test1_only_repeatable) - Shows that baseline and versioned scripts are not
  necessary if the target version is specified in `repeatable/target_version.txt`.
  `init` and the folder cross-checks pass; `update` requires a version to be already
  installed in the schema (otherwise "Unable to get latest installed version").

[See also](../../README.md)