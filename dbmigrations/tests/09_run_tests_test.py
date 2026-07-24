import subprocess
import re

def test_dbmigration_run_tests_failure(cfg):
    """Test checks the execution of the run-tests subcommand when some internal SQL tests fail."""
    
    # Construct the path to the specific samples folder
    target_sample_path = cfg.SAMPLES_PATH.joinpath("test1")
    
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
    print("=== STDOUT ===")
    print(result.stdout)
    print("=== STDERR ===")
    print(result.stderr)

    # 1. Verify the process exit code status (Should be non-zero because tests failed)
    assert result.returncode != 0, "Script was expected to fail, but exit code is 0"
    
    # 2. Verify dynamic database connection string format output log via regex match
    db_conn_pattern = r"Opened db connection: '\S+@\S+:\d+/\S+'"
    assert re.search(db_conn_pattern, result.stdout) is not None, \
        f"Database connection log string was not found or has an invalid format: {result.stdout}"

    # 3. Verify target schema setup and tests initialization logs
    assert "Set session search path to 'test3'." in result.stdout
    assert "Target schema environment ID matches the scripts directory ID:" in result.stdout
    assert "Running unit tests for scripts repository:" in result.stdout
    assert "Target version matches the latest installed version 'V002'" in result.stdout
    
    # 4. Verify savepoint and transaction isolation lifecycle logs
    assert "Make savepoint..." in result.stdout
    assert "Rolled back to savepoint." in result.stdout
    assert "Rolled back transaction." in result.stdout
    assert "Closed db connection." in result.stdout

    # 5. Verify successful SQL test executions (PASS / DONE)
    assert "Running setup: 'test1/tests/_setup.sql'...DONE" in result.stdout
    assert "is_true_that_setup_data_is_populated.sql'...PASS" in result.stdout
    assert "assure_that_t1_exists.sql'...PASS" in result.stdout
    assert "assure_that_t2_is_ok.sql'...PASS" in result.stdout

    # 6. Verify the specific test failures (FAIL)
    # Failure 1: Data validation mismatch
    assert "detect_missing_t1_records.sql'...FAIL. (2) Missing records:" in result.stdout
    assert "id: 33" in result.stdout
    assert "FAIL. Expected no results!" in result.stdout
    
    # Failure 2: Naming convention validation mismatch
    assert "test_view_latest_t1.sql'...FAIL. Unable to detect test type from script name" in result.stdout
    assert "It should start with one of the following prefixes:" in result.stdout

    # 7. Verify the final error summary in stderr or stdout (depending on where your CLI writes it)
    # Check stdout first, if it's in stderr, move this assert to result.stderr
    assert "Command error: Tests failed: 2, passed: 9." in result.stdout 
