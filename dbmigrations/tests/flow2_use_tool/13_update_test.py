import subprocess
import re
import pytest

def test_dbmigration_update_baseline_with_dump_success(cfg):
    """Test checks the successful execution of the update subcommand for a baseline with a dump."""
    
    # Path to the specific valid sample folder from the log
    target_sample_path = cfg.SCRIPTS_PATH
    
    # Construct the CLI command based on the successful run
    command = [
        cfg.PYTHON_EXE,
        str(cfg.DBMIGRATION_PY_PATH),
        "update",
        cfg.TARGET_SCHEMA,        # 'esbdb'
        str(target_sample_path),  # './samples/test1_baseline_with_dump'
        "--skip-confirmation"
    ]
    
    # Run the database migration script in update mode
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace"
    )
    
    # Print outputs to console (pytest shows this only if the test fails)
    print("\n=== STDOUT ===")
    print(result.stdout.strip() or "EMPTY")
    print("=== STDERR ===")
    print(result.stderr.strip() or "EMPTY")
    
    # 1. Verify the process exit code status (0 means success)
    assert result.returncode == 0, (
        f"Script execution failed with exit code {result.returncode}.\n"
        f"=== STDERR ===\n{result.stderr}"
    )
    
    # 2. Verify database connection and search path logs
    db_conn_pattern = r"Opened db connection: '\S+@\S+:\d+/\S+'"
    assert re.search(db_conn_pattern, result.stdout) is not None, "Database connection log missing."
    assert f"Set session search path to: '{cfg.TARGET_SCHEMA}'." in result.stdout
    assert "Target schema environment ID matches the scripts directory ID:" in result.stdout
    
    # 3. Verify that the correct external psql tool was picked up and used
    assert "Running baseline scripts with external tool" in result.stdout
    assert "psql" in result.stdout
    
    # 4. Verify that specific SQL dump steps and scripts were processed
    assert "Running script: [test1_baseline_with_dump/baseline/V000/00_esbdb_schema.sql" in result.stdout
    assert "Running script: [test1_baseline_with_dump/baseline/V000/01_esbdb_data.sql" in result.stdout
    assert "CREATE TABLE" in result.stdout
    assert "COPY " in result.stdout
    
    # 5. Verify the final successful lifecycle states
    assert "Setting the baseline version 'V000'..." in result.stdout
    assert "Committed." in result.stdout
    assert "Baseline scripts applied." in result.stdout
    assert "Updated." in result.stdout
    assert "Closed db connection." in result.stdout
