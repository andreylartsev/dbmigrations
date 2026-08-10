import subprocess
import re
import uuid
import pytest

def test_dbmigration_verify_skip_git_checks_success(cfg):
    """Test checks the successful verification of the migration scripts via the verify subcommand."""
    
    # Construct the path to the specific samples folder
    target_sample_path = cfg.SCRIPTS_PATH
    
    # Construct the CLI command including the new script generation flag
    command = [
        cfg.PYTHON_EXE,
        str(cfg.DBMIGRATION_PY_PATH),
        "verify",
        cfg.TARGET_SCHEMA,
        str(target_sample_path),
        "--dbenv", cfg.DB_ENV,
        "--skip-git-checks"
    ]
    
    # Run the database migration script in verification mode
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding="utf-8-sig"  # Handles Windows console BOM character matching properly
    )
    
    # Print outputs with a fallback to "EMPTY" if stdout/stderr are empty or contain only whitespaces
    print("\n=== STDOUT ===")
    print(result.stdout.strip() or "EMPTY")
    print("=== STDERR ===")
    print(result.stderr.strip() or "EMPTY")
    
    # 1. Verify the process exit code status (0 means success)
    assert result.returncode == 0, f"Script execution failed with error: {result.stdout or result.stderr}"
    
    # 2. Verify dynamic database connection string format output log via regex match
    db_conn_pattern = r"Opened db connection: '\S+@\S+:\d+/\S+'"
    assert re.search(db_conn_pattern, result.stdout) is not None, \
        f"Database connection log string was not found or has an invalid format: {result.stdout}"

    # 3. Verify target schema connection log statements
    assert f"Set session search path to: '{cfg.TARGET_SCHEMA}'." in result.stdout
    assert "Target schema environment ID matches the scripts directory ID:" in result.stdout
    assert "Closed db connection." in result.stdout
    
    # 4. Verify baseline and versioned block summary messages
    assert "Baseline scripts to install:" in result.stdout
    assert "00_esbdb_schema.sql" in result.stdout
    assert "01_esbdb_data.sql" in result.stdout
