import subprocess
import re
import pytest
# Import predefined paths and variables from the private configuration module
from _config import DBMIGRATION_PY_PATH, SAMPLES_PATH, PYTHON_EXE, TARGET_SCHEMA, DB_ENV

def test_dbmigration_init_success():
    """Test checks the successful migration structure initialization using the --dbenv flag."""
    
    # Construct the path to the specific samples folder
    target_sample_path = SAMPLES_PATH.joinpath("test1")
    
    # Construct the CLI command using variables from the configuration file
    command = [
        PYTHON_EXE,
        str(DBMIGRATION_PY_PATH),
        "init",
        TARGET_SCHEMA,
        str(target_sample_path),
        "--dbenv", DB_ENV
    ]
    
    # Run the database migration script
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding="utf-8-sig"  # Handles Windows console BOM character matching properly
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

    # 3. Verify standard static output application log statements
    assert f"Set session search path to '{TARGET_SCHEMA}'." in result.stdout
    assert "Created." in result.stdout
    assert "Closed db connection." in result.stdout
    
    # 4. Verify version control environment ID dynamically using the UUID pattern structure
    uuid_pattern = r"Creating the version control tables with environment ID: '[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}'"
    assert re.search(uuid_pattern, result.stdout) is not None, \
        "Environment tracking ID was not found or its UUID syntax format is invalid"
