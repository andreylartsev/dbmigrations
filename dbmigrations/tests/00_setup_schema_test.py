import subprocess
from _config import DB_USER, DB_NAME, DB_HOST, DB_PORT, TARGET_SCHEMA, PSQL_EXE

def test_recreate_schema_via_psql():
    """Test checks the successful recreation of the target schema via psql."""

    # Construct the command using variables
    command = [
        str(PSQL_EXE),
        "-U", DB_USER,
        "-h", DB_HOST,
        "-p", str(DB_PORT),
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
    
    # Print outputs for manual verification via pytest -v -s
    print("=== STDOUT ===")
    print(result.stdout)
    print("=== STDERR ===")
    print(result.stderr)
    
    # 1. Verify the return code (0 means success)
    assert result.returncode == 0, f"psql execution failed: {result.stderr}"
    
    # 2. Verify the standard output (stdout)
    assert "DROP SCHEMA" in result.stdout
    assert "CREATE SCHEMA" in result.stdout
    
