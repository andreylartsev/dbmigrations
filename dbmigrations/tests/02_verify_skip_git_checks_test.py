import subprocess
import re
import uuid
import pytest

def test_dbmigration_verify_skip_git_checks_success(cfg):
    """Test checks the successful verification of the migration scripts via the verify subcommand."""
    
    # Construct the path to the specific samples folder
    target_sample_path = cfg.SAMPLES_PATH.joinpath("test1")
    
    # Generate a completely unique filename using UUID v4 to avoid any file handle conflicts on Windows
    unique_filename = f"verify_test_{uuid.uuid4().hex}.sql"
    output_sql_file = cfg.TESTS_DIR.joinpath(unique_filename)
    
    # Construct the CLI command including the new script generation flag
    command = [
        cfg.PYTHON_EXE,
        str(cfg.DBMIGRATION_PY_PATH),
        "verify",
        cfg.TARGET_SCHEMA,
        str(target_sample_path),
        "--dbenv", cfg.DB_ENV,
        "--skip-git-checks",
        "--build-update-script", str(output_sql_file)
    ]
    
    try:
        # Run the database migration script in verification mode
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            encoding="utf-8-sig"  # Handles Windows console BOM character matching properly
        )
        
        # Print outputs with a fallback to "EMPTY" if stdout/stderr are empty or contain only whitespaces
        print("\n=== STDOUT ===")
        print(result.stdout.strip() or "EMPTY")
        print("=== STDERR ===")
        print(result.stderr.strip() or "EMPTY")
        
        # 1. Verify the process exit code status (0 means success)
        assert result.returncode == 0, f"Script execution failed with error: {result.stdout or result.stderr}"
        
        # 2. Verify dynamic database connection string format output log via regex match
        db_conn_pattern = r"Opened db connection: '\S+@\S+:\d+/\S+'"
        assert re.search(db_conn_pattern, result.stdout) is not None, \
            f"Database connection log string was not found or has an invalid format: {result.stdout}"
    
        # 3. Verify target schema connection log statements
        assert f"Set session search path to '{cfg.TARGET_SCHEMA}'." in result.stdout
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
    
        # 7. Verify that the console logs the successful generation of the update script
        assert f"The update script is written to '{output_sql_file}'." in result.stdout
    
        # 8. Verify the physical existence and contents of the generated .sql file
        assert output_sql_file.exists(), "The generated update script file does not exist on disk"
        
        sql_content = output_sql_file.read_text(encoding="utf-8")

        # Print the generated SQL script content to console for manual verification via pytest -v -s
        print("=== GENERATED SQL SCRIPT ===")
        print(sql_content.strip() or "EMPTY")
        
        # Cross-check key blocks inside the generated SQL structure
        assert f"set_config('search_path', '{cfg.TARGET_SCHEMA}', false);" in sql_content
        assert "create table t1" in sql_content
        assert "insert into t1 values (1);" in sql_content
        assert f"INSERT INTO \"{cfg.TARGET_SCHEMA}\".dbmigration_versions" in sql_content
        assert "create table t2" in sql_content
        assert "drop view if exists latest_t1;" in sql_content

    finally:
        # CRITICAL: Clean up the generated file after assertions complete or if an assertion fails
        if output_sql_file.exists():
            output_sql_file.unlink()
