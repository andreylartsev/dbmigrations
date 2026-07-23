import subprocess
import re
import pytest
# Import predefined paths and variables from the private configuration module
from _config import DBMIGRATION_PY_PATH, SAMPLES_PATH, PYTHON_EXE, TARGET_SCHEMA, DB_ENV

def test_dbmigration_update_success():
    """Test checks the successful execution of the update subcommand with database modifications."""
    
    # Construct the path to the specific samples folder
    target_sample_path = SAMPLES_PATH.joinpath("test1")
    
    # Construct the CLI command for the update action using your exact parameter structure
    command = [
        PYTHON_EXE,
        str(DBMIGRATION_PY_PATH),
        "update",
        TARGET_SCHEMA,
        str(target_sample_path),
        "--dbenv", DB_ENV,
        "--skip-confirmation"
    ]
    
    # Run the database migration script in update mode
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding="utf-8-sig",  # Handles Windows console BOM character matching properly
        errors="replace" 
    )
    
    # Print tool output execution logs for manual check inside pytest -v -s
    # Print outputs for manual verification via pytest -v -s
    print("=== STDOUT ===")
    print(result.stdout)
    print("=== STDERR ===")
    print(result.stderr)

    
    # 1. Verify the process exit code status (0 means success)
    assert result.returncode == 0, f"Script execution failed with error: {result.stderr}"
    
    # 2. Verify dynamic database connection string format output log via regex match
    db_conn_pattern = r"Opened db connection: '\S+@\S+:\d+/\S+'"
    assert re.search(db_conn_pattern, result.stdout) is not None, \
        f"Database connection log string was not found or has an invalid format: {result.stdout}"

    # 3. Verify target schema setup and initial consistency checks logs
    assert f"Set session search path to '{TARGET_SCHEMA}'." in result.stdout
    assert "Performing updates from scripts repository:" in result.stdout
    assert "Target schema environment ID matches the scripts directory ID:" in result.stdout
    assert "Completed." in result.stdout
    
    # 4. Verify baseline phase execution logs
    assert "The baseline version to install V000." in result.stdout
    assert "Apply baseline scripts..." in result.stdout
    assert "00_create_t1.sql" in result.stdout
    assert "01_insert_into_t1.sql" in result.stdout
    assert "Setting the baseline version as 'V000'." in result.stdout
    assert "The baseline scripts were applied." in result.stdout
    
    # 5. Verify versioned phase execution logs
    assert "The latest installed version is V000." in result.stdout
    assert "Found 2 new versions for installation." in result.stdout
    assert "Apply versioned scripts..." in result.stdout
    assert "Apply version V001..." in result.stdout
    assert "00_create_t2.sql" in result.stdout
    assert "01_insert_into_t2.sql" in result.stdout
    assert "Apply version V002..." in result.stdout
    assert "dummy.sql" in result.stdout
    assert "The versioned scripts were applied." in result.stdout
    
    # 6. Verify repeatable phase execution logs
    assert "Check repeatable scripts..." in result.stdout
    assert "Target version matches the latest installed version 'V002'" in result.stdout
    assert "Found 2 scripts to re-run" in result.stdout
    assert "Apply repeatable scripts..." in result.stdout
    assert "00_create_view_latest_t1.sql" in result.stdout
    assert "01_create_view_max_t2_kk.sql" in result.stdout
    assert "The repeatable scripts were applied." in result.stdout
    
    # 7. Verify successful completion logs and connection closing
    assert "Updated." in result.stdout
    assert "Closed db connection." in result.stdout
