import subprocess
import re
import pytest
# Import predefined paths and variables from the private configuration module
from _config import DBMIGRATION_PATH, SAMPLES_PATH, PYTHON_EXE, TARGET_SCHEMA, DB_ENV

def test_dbmigration_verify_success():
    """Test checks the successful verification of the migration scripts via the verify subcommand."""
    
    # Construct the path to the specific samples folder
    target_sample_path = SAMPLES_PATH.joinpath("test1")
    
    # Construct the CLI command using your exact parameter structure
    command = [
        PYTHON_EXE,
        str(DBMIGRATION_PATH),
        "verify",
        TARGET_SCHEMA,
        str(target_sample_path),
        "--dbenv", DB_ENV,
        "--skip-git-checks"
    ]
    
    # Run the database migration script in verification mode
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding="utf-8-sig"  # Handles Windows console BOM character matching properly
    )
    
    # Print tool output execution logs for manual check inside pytest -v -s
    print("\n=== MIGRATION VERIFY OUTPUT ===")
    print("STDOUT:", result.stdout)
    print("STDERR:", result.stderr)
    
    # 1. Verify the process exit code status (0 means success)
    assert result.returncode == 0, f"Script execution failed with error: {result.stderr}"
    
    # 2. Verify dynamic database connection string format output log via regex match
    db_conn_pattern = r"Opened db connection: '\S+@\S+:\d+/\S+'"
    assert re.search(db_conn_pattern, result.stdout) is not None, \
        f"Database connection log string was not found or has an invalid format: {result.stdout}"

    # 3. Verify target schema connection log statements
    assert f"Set session search path to '{TARGET_SCHEMA}'." in result.stdout
    assert "Target schema environment ID matches the scripts directory ID:" in result.stdout
    assert "Closed db connection." in result.stdout
    
    # 4. Verify baseline and versioned block summary messages
    assert "The baseline scripts to install:" in result.stdout
    assert "00_create_t1.sql" in result.stdout
    assert "01_insert_into_t1.sql" in result.stdout
    
    assert "The versioned scripts to install:" in result.stdout
    assert "00_create_t2.sql" in result.stdout
    assert "01_insert_into_t2.sql" in result.stdout
    assert "dummy.sql" in result.stdout
    
    # 5. Verify database schema status logs
    assert "No versions were installed in the database schema." in result.stdout
    assert "The target version for repeatable scripts is V002." in result.stdout
    
    # 6. Verify repeatable scripts block summary messages
    assert "The repeatable scripts to (re)install:" in result.stdout
    assert "00_create_view_latest_t1.sql" in result.stdout
    assert "01_create_view_max_t2_kk.sql" in result.stdout
