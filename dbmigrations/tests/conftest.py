import pytest
import subprocess
# Import predefined paths and variables from the private configuration module
from _config import DB_USER, DB_NAME, DB_HOST, DB_PORT, TARGET_SCHEMA, PSQL_EXE

@pytest.fixture(scope="session", autouse=True)
def setup_database_session():
    """Recreates the target database schema exactly ONCE before the entire test suite starts."""
    
    # Construct the psql command using variables from configuration
    command = [
        str(PSQL_EXE),
        "-U", DB_USER,
        "-h", DB_HOST,
        "-p", str(DB_PORT),
        "-d", DB_NAME,
        "-c", f"DROP SCHEMA IF EXISTS {TARGET_SCHEMA} CASCADE; CREATE SCHEMA {TARGET_SCHEMA};"
    ]
    
    # Run the psql command to reset the schema environment
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding="utf-8-sig"
    )
    
    # Verify that the schema was recreated successfully before running any test dependencies
    assert result.returncode == 0, f"Database schema preparation failed: {result.stderr}"
    assert "DROP SCHEMA" in result.stdout
    assert "CREATE SCHEMA" in result.stdout
    
    # Optional print for manual log verification inside pytest -v -s
    print(f"\n[SESSION SETUP] Target schema '{TARGET_SCHEMA}' has been successfully recreated via psql.")
    
    # Yield control to allow dependent test sequence execution
    yield
