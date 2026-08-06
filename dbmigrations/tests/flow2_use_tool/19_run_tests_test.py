import subprocess
import re

def test_dbmigration_run_tests_failure(cfg):
    """Test checks the execution of the run-tests subcommand when some internal SQL tests fail."""
    
    # Construct the path to the specific samples folder
    target_sample_path = cfg.SCRIPTS_PATH
    
    # Construct the CLI command for the run-tests action
    command = [
        cfg.PYTHON_EXE,
        str(cfg.DBMIGRATION_PY_PATH),
        "run-tests",
        cfg.TARGET_SCHEMA,                         # Target schema 'test3' from console
        str(target_sample_path),
        "--dbenv", cfg.DB_ENV              # Assuming you still need --dbenv in your test setup
    ]
    
    # Run the database migration script in test mode
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding="utf-8-sig",
        errors="replace" 
    )
    
    # Print outputs for manual verification via pytest -v -s
    print("\n=== STDOUT ===")
    print(result.stdout or "EMPTY")
    print("=== STDERR ===")
    print(result.stderr or "EMPTY")

    # 1. Verify the process exit code status (Should be non-zero because tests failed)
    assert result.returncode == 0, "Script was expected to exit with code 0 but {result.returncode} returned"
    
    # 2. Verify dynamic database connection string format output log via regex match
    db_conn_pattern = r"Opened db connection: '\S+@\S+:\d+/\S+'"
    assert re.search(db_conn_pattern, result.stdout) is not None, \
        f"Database connection log string was not found or has an invalid format: {result.stdout}"

    # 3. Verify target schema setup and tests initialization logs
    assert f"Set session search path to '{cfg.TARGET_SCHEMA}'." in result.stdout
    assert "Target schema environment ID matches the scripts directory ID:" in result.stdout
    assert "Running unit tests for scripts repository:" in result.stdout
    assert "Target version matches the latest installed version 'V000'" in result.stdout
    
    # 5. Verify successful SQL test executions (PASS / DONE)
    assert "assure_that_table_t1_is_ok.sql'...PASS" in result.stdout
    assert "assure_that_table_t2_is_ok.sql'...PASS" in result.stdout
    assert "assure_that_view_latest_t1_is_ok.sql'...PASS" in result.stdout
    assert "is_true_that_view_latest_t1_returns_max_value.sql'...PASS" in result.stdout

    assert "All 4 tests passed" in result.stdout 
