import subprocess
import re

def test_dbmigration_force_reapply_latest_success(cfg):
    """Test checks the successful execution of the update subcommand with --force-reapply-latest-version flag."""
    
    # Construct the path to the specific samples folder
    target_sample_path = cfg.SAMPLES_PATH.joinpath("test1")
    
    # Construct the CLI command using the exact parameter structure from your console output
    command = [
        cfg.PYTHON_EXE,
        str(cfg.DBMIGRATION_PY_PATH),
        "update",
        cfg.TARGET_SCHEMA,                         # Using target schema 'test3' from console
        str(target_sample_path),
        "--dbenv", cfg.DB_ENV,              # Assuming you still need --dbenv in your test setup
        "--skip-confirmation",
        "--force-reapply-latest-version" # New flag under test
    ]
    
    # Run the database migration script
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

    # 1. Verify the process exit code status
    assert result.returncode == 0, f"Script execution failed with error: {result.stderr}"
    
    # 2. Verify dynamic database connection string format output log via regex match
    db_conn_pattern = r"Opened db connection: '\S+@\S+:\d+/\S+'"
    assert re.search(db_conn_pattern, result.stdout) is not None, \
        f"Database connection log string was not found or has an invalid format: {result.stdout}"

    # 3. Verify target schema setup and reapply initialization logs
    assert "Set session search path to 'test3'." in result.stdout
    assert "Performing reapply latest version from scripts repository:" in result.stdout
    assert "Target schema environment ID matches the scripts directory ID:" in result.stdout
    
    # 4. Verify versioned phase reapply execution logs
    assert "The latest installed version is V002." in result.stdout
    assert "Reapply version V002..." in result.stdout
    assert "Running script: [test1/versions/V002/_cleanup.sql" in result.stdout
    assert "Running script: [test1/versions/V002/dummy.sql" in result.stdout
    assert "Committed." in result.stdout
    
    # 5. Verify repeatable phase execution logs during reapply
    assert "Check repeatable scripts..." in result.stdout
    assert "Target version matches the latest installed version 'V002'" in result.stdout
    assert "Found 2 scripts to re-run" in result.stdout
    assert "Apply repeatable scripts..." in result.stdout
    assert "Running script: [test1/repeatable/00_create_view_latest_t1.sql" in result.stdout
    assert "Running script: [test1/repeatable/01_create_view_max_t2_kk.sql" in result.stdout
    assert "The repeatable scripts were applied." in result.stdout
    
    # 6. Verify successful completion logs for reapply action and connection closing
    assert "Reapplied." in result.stdout
    assert "Closed db connection." in result.stdout
