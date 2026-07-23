import subprocess
import re
import pytest
# Import predefined paths and variables from the private configuration module
from _config import DBMIGRATION_PY_PATH, SAMPLES_PATH, PYTHON_EXE, TARGET_SCHEMA, DB_ENV

def test_dbmigration_verify_success():
    """Test checks the successful execution of the verify subcommand and its changelog output structure."""
    
    # Construct the path to the specific samples folder
    target_sample_path = SAMPLES_PATH.joinpath("test1")
    
    # Construct the CLI command for the verify action
    command = [
        PYTHON_EXE,
        str(DBMIGRATION_PY_PATH),
        "verify",
        "test3",                         # Target schema 'test3' from console
        str(target_sample_path)
    ]
    
    # Run the database migration script in verify mode
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

    # 1. Verify the process exit code status (0 means success)
    assert result.returncode == 0, f"Script execution failed with error: {result.stderr}"
    
    # 2. Verify dynamic database connection string format output log via regex match
    db_conn_pattern = r"Opened db connection: '\S+@\S+:\d+/\S+'"
    assert re.search(db_conn_pattern, result.stdout) is not None, \
        f"Database connection log string was not found or has an invalid format: {result.stdout}"

    # 3. Verify target schema setup and verification initialization logs
    assert "Set session search path to 'test3'." in result.stdout
    assert "Target schema environment ID matches the scripts directory ID:" in result.stdout
    assert "Performing a cross-check for consistency" in result.stdout
    assert "Completed." in result.stdout
    
    # 4. Verify version and script status summary logs
    assert "The target schema has the baseline version installed: V000" in result.stdout
    assert "The latest version installed is V002. No newer scripts were found for installation." in result.stdout
    assert "The target version for repeatable scripts is V002." in result.stdout
    assert "No changed repeatable scripts found for (re)installation." in result.stdout
    assert "The list of recent changes were applied to the target schema:" in result.stdout

    # 5. Verify the audit log / history entries via strict substring checks
    # Check block authors
    assert re.search(r"Author: \S+", result.stdout) is not None
    
    # Check baseline scripts audit
    assert "test1/baseline/V000/00_create_t1.sql" in result.stdout
    assert "test1/baseline/V000/01_insert_into_t1.sql" in result.stdout
    
    # Check versioned scripts audit
    assert "test1/versions/V001/00_create_t2.sql" in result.stdout
    assert "test1/versions/V001/01_insert_into_t2.sql" in result.stdout
    assert "test1/versions/V002/_cleanup.sql" in result.stdout
    assert "test1/versions/V002/dummy.sql" in result.stdout
    
    # Check repeatable scripts audit
    assert "test1/repeatable/00_create_view_latest_t1.sql" in result.stdout
    assert "test1/repeatable/01_create_view_max_t2_kk.sql" in result.stdout

    # 6. Verify specific git commit hashes and metadata patterns using regex
    # Matches commit hashes like [563cf87e], dates like 2026-07-22, and the commit message
    assert re.search(r"\[[0-9a-f]{8}\] \d{4}-\d{2}-\d{2} - .+", result.stdout) is not None
    
    # Matches log execution metadata format: [2026-07-23 22:51:52 | V002   | ... (OID: 1504cd9a)]
    assert re.search(r"\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \| \S+ | \S+ \(OID: [0-9a-f]+\)\]", result.stdout) is not None

    # 7. Verify connection closing
    assert "Closed db connection." in result.stdout
