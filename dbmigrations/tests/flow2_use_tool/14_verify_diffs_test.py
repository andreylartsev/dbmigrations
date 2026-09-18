import subprocess
from pathlib import Path

import psycopg


def _run_git(repo_dir: Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", "-C", str(repo_dir), *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )


def test_dbmigration_verify_prints_inline_diffs_by_default(session_cfg, tmp_path):
    """End-to-end check: verify prints text diffs inline (by default) both for the
    scripts to (re)install and for the recent changes recorded in the database,
    and --skip-diffs suppresses them."""
    target_schema = "esbdb_verify_diffs"

    with psycopg.connect(**session_cfg.DBCONN_CONFIG) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{target_schema}" CASCADE')
            cur.execute(f'CREATE SCHEMA "{target_schema}"')

    try:
        # ---------------------------------------------------------------
        # Prepare a self-contained Git repository with baseline + repeatable scripts
        # ---------------------------------------------------------------
        repo_dir = tmp_path / "verify_diffs_repo"
        baseline_dir = repo_dir / "baseline" / "V000"
        baseline_dir.mkdir(parents=True)
        (baseline_dir / "00_create_t.sql").write_text("CREATE TABLE t_diffs (id INT);\n")
        (baseline_dir / "script_list.txt").write_text("00_create_t.sql\n")

        repeatable_dir = repo_dir / "repeatable"
        repeatable_dir.mkdir()
        (repeatable_dir / "target_version.txt").write_text("V000\n")
        repeat_sql = repeatable_dir / "00_current_value.sql"
        repeat_sql.write_text("CREATE OR REPLACE VIEW v_current AS SELECT 1 AS value;\n")

        git_cfg = ["-c", "user.name=Test", "-c", "user.email=test@example.com"]
        _run_git(repo_dir, "init", "-q")
        _run_git(repo_dir, *git_cfg, "add", ".")
        _run_git(repo_dir, *git_cfg, "commit", "-q", "-m", "initial")

        py = session_cfg.PYTHON_EXE
        tool = str(session_cfg.DBMIGRATION_PY_PATH)
        dbenv = ["--dbenv", session_cfg.DB_ENV]

        # ---------------------------------------------------------------
        # Initialize version control tables and apply the initial scripts
        # ---------------------------------------------------------------
        init_res = subprocess.run(
            [py, tool, "init", target_schema, str(repo_dir), *dbenv],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
        )
        assert init_res.returncode == 0, f"init failed:\n{init_res.stdout}\n{init_res.stderr}"

        update_res = subprocess.run(
            [py, tool, "update", target_schema, str(repo_dir), *dbenv, "--skip-confirmation"],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
        )
        assert update_res.returncode == 0, f"update failed:\n{update_res.stdout}\n{update_res.stderr}"

        # ---------------------------------------------------------------
        # Modify the repeatable script and commit the change
        # ---------------------------------------------------------------
        repeat_sql.write_text("CREATE OR REPLACE VIEW v_current AS SELECT 42 AS value;\n")
        _run_git(repo_dir, *git_cfg, "add", ".")
        _run_git(repo_dir, *git_cfg, "commit", "-q", "-m", "update repeatable value")

        # ---------------------------------------------------------------
        # Run verify without flags: diffs must be shown inline by default
        # ---------------------------------------------------------------
        verify_res = subprocess.run(
            [py, tool, "verify", target_schema, str(repo_dir), *dbenv],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
        )

        print("\n=== VERIFY STDOUT ===")
        print(verify_res.stdout or "EMPTY")
        print("=== VERIFY STDERR ===")
        print(verify_res.stderr or "EMPTY")

        assert verify_res.returncode == 0, f"verify failed:\n{verify_res.stdout}\n{verify_res.stderr}"
        assert "Repeatable scripts to (re)install:" in verify_res.stdout
        assert "Script text differences" not in verify_res.stdout
        assert "repeatable/00_current_value.sql" in verify_res.stdout
        assert "-CREATE OR REPLACE VIEW v_current AS SELECT 1 AS value" in verify_res.stdout
        assert "+CREATE OR REPLACE VIEW v_current AS SELECT 42 AS value" in verify_res.stdout

        # ---------------------------------------------------------------
        # Run verify with --skip-diffs: entry stays, diffs are suppressed
        # ---------------------------------------------------------------
        skip_diffs_res = subprocess.run(
            [py, tool, "verify", target_schema, str(repo_dir), *dbenv, "--skip-diffs"],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
        )

        print("\n=== VERIFY --skip-diffs STDOUT ===")
        print(skip_diffs_res.stdout or "EMPTY")

        assert skip_diffs_res.returncode == 0, (
            f"verify --skip-diffs failed:\n{skip_diffs_res.stdout}\n{skip_diffs_res.stderr}"
        )
        assert "repeatable/00_current_value.sql" in skip_diffs_res.stdout
        assert "-CREATE OR REPLACE VIEW v_current AS SELECT 1 AS value" not in skip_diffs_res.stdout
        assert "+CREATE OR REPLACE VIEW v_current AS SELECT 42 AS value" not in skip_diffs_res.stdout

        # ---------------------------------------------------------------
        # Apply the modified repeatable script so the database history keeps
        # two applications, then verify recent changes are shown as inline diffs
        # ---------------------------------------------------------------
        update2_res = subprocess.run(
            [py, tool, "update", target_schema, str(repo_dir), *dbenv, "--skip-confirmation"],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
        )
        assert update2_res.returncode == 0, (
            f"second update failed:\n{update2_res.stdout}\n{update2_res.stderr}"
        )

        verify2_res = subprocess.run(
            [py, tool, "verify", target_schema, str(repo_dir), *dbenv],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
        )

        print("\n=== VERIFY2 STDOUT ===")
        print(verify2_res.stdout or "EMPTY")
        print("=== VERIFY2 STDERR ===")
        print(verify2_res.stderr or "EMPTY")

        assert verify2_res.returncode == 0, (
            f"second verify failed:\n{verify2_res.stdout}\n{verify2_res.stderr}"
        )
        assert "Recent changes text differences" not in verify2_res.stdout
        assert "The list of recent changes were applied to the target schema:" in verify2_res.stdout
        assert "repeatable/00_current_value.sql" in verify2_res.stdout
        assert "-CREATE OR REPLACE VIEW v_current AS SELECT 1 AS value" in verify2_res.stdout
        assert "+CREATE OR REPLACE VIEW v_current AS SELECT 42 AS value" in verify2_res.stdout
    finally:
        with psycopg.connect(**session_cfg.DBCONN_CONFIG) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f'DROP SCHEMA IF EXISTS "{target_schema}" CASCADE')