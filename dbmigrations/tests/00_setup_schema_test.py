import subprocess
import pytest
import shutil
from _config import DB_USER, DB_NAME, DB_HOST, DB_PORT, TARGET_SCHEMA

# Check if the psql utility is available in the system PATH
PSQL_PATH = shutil.which("psql")

@pytest.mark.skipif(not PSQL_PATH, reason="The psql utility was not found in the system PATH")
def test_recreate_schema_via_psql():
    """Test checks the successful recreation of the target schema via psql."""
    
    # Construct the command using variables
    command = [
        "psql",
        "-U", DB_USER,
        "-h", DB_HOST,
        "-p", DB_PORT,
        "-d", DB_NAME,
        "-c", f"DROP SCHEMA IF EXISTS {TARGET_SCHEMA} CASCADE; CREATE SCHEMA {TARGET_SCHEMA};"
    ]
    
    # Run the psql command
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding="utf-8-sig"
    )
    
    # 1. Verify the return code (0 means success)
    assert result.returncode == 0, f"psql execution failed: {result.stderr}"
    
    # 2. Verify the standard output (stdout)
    assert "DROP SCHEMA" in result.stdout
    assert "CREATE SCHEMA" in result.stdout
    
    # Print outputs for manual verification via pytest -v -s
    print("\n=== PSQL OUTPUT ===")
    print("STDOUT:", result.stdout)
    print("STDERR:", result.stderr)
    
