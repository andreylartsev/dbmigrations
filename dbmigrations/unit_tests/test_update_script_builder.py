from pathlib import Path
import uuid
import pytest

from dbmigration import CommandError, UpdateScriptBuilder


def test_successful_script_building_lifecycle() -> None:
    """Tests the full successful lifecycle: check -> open -> write -> finalize."""
    unique_prefix = f"dbmigration_{uuid.uuid4()}_"
    target_path = Path(".") / f"{unique_prefix}update_migration.sql"
    builder = UpdateScriptBuilder(target_path)
    
    try:
        # 1. Run validation and path initialization
        builder.check()
        
        # 2. Write SQL content using the context manager stream
        with builder as b:
            b.write_header("-- Automatically generated migration package\n")
            b.write_header("BEGIN;\n\n")
            b.write_body("-- Step 1: Create core tables\n")
            b.write_body("CREATE TABLE users (id SERIAL PRIMARY KEY, email TEXT);\n")
            b.write_body_lines([
                "CREATE INDEX idx_users_email ON users(email);\n",
                "COMMIT;\n"
            ])
        
        # Verify body bytes were tracked correctly
        assert builder.get_written_body_bytes() > 0

        # 3. Finalize and swap files
        builder.finalize()
        
        # Target file must now exist and temp file must be cleaned up
        assert target_path.exists()
        assert not builder.temp_script_path.exists()
        
        # Validate the written SQL content match
        content = target_path.read_text(encoding="utf-8")
        assert "BEGIN;" in content
        assert "CREATE TABLE users" in content
        assert "COMMIT;" in content

    finally:
        # First, let the builder clear its internal state and tracked files
        builder.cleanup()
        # Then, ensure the finalized production SQL script is purged from the root directory
        target_path.unlink(missing_ok=True)


def test_check_raises_error_if_target_already_exists() -> None:
    """Tests that check() triggers CommandError due to the predefined touch file lock."""
    unique_prefix = f"dbmigration_{uuid.uuid4()}_"
    target_path = Path(".") / f"{unique_prefix}existing_script.sql"
    target_path.touch()  # Simulate the path already being locked/occupied
    
    builder = UpdateScriptBuilder(target_path)
    
    try:
        with pytest.raises(CommandError) as exc_info:
            builder.check()
            
        assert "already exists" in str(exc_info.value)
    finally:
        builder.cleanup()
        target_path.unlink(missing_ok=True)


def test_check_raises_error_if_parent_directory_does_not_exist() -> None:
    """Tests that check() validation catches a missing base path configuration."""
    invalid_path = Path("/non/existent/dir/structure/script.sql")
    builder = UpdateScriptBuilder(invalid_path)
    
    with pytest.raises(CommandError) as exc_info:
        builder.check()
        
    assert "parent directory" in str(exc_info.value)


def test_context_manager_cleanup_on_exception() -> None:
    """Tests that __exit__ removes files if an exception is raised inside the 'with' block."""
    unique_prefix = f"dbmigration_{uuid.uuid4()}_"
    target_path = Path(".") / f"{unique_prefix}failed_script.sql"
    builder = UpdateScriptBuilder(target_path)
    
    try:
        builder.check()
        
        # Ensure temporary structure is present initially
        assert builder.temp_script_path.exists()

        with pytest.raises(ValueError):
            with builder:
                builder.write_header("BEGIN;\n")
                raise ValueError("Interrupted process simulation")
                
        # __exit__ should roll back changes and remove target files on error
        assert not builder.temp_script_path.exists()
        assert not target_path.exists()
    finally:
        builder.cleanup()
        target_path.unlink(missing_ok=True)


def test_explicit_cleanup_removes_all_files() -> None:
    """Tests that a manual call to cleanup() clears both target and temporary files."""
    unique_prefix = f"dbmigration_{uuid.uuid4()}_"
    target_path = Path(".") / f"{unique_prefix}cleanup_script.sql"
    builder = UpdateScriptBuilder(target_path)
    
    try:
        builder.check()
        
        with builder:
            builder.write_body("SELECT 1;")
            
        assert builder.temp_script_path.exists()
        
        builder.cleanup()
        
        # All paths must be completely unlinked post-cleanup
        assert not builder.temp_script_path.exists()
        assert not target_path.exists()
    finally:
        builder.cleanup()
        target_path.unlink(missing_ok=True)


def test_finalize_handles_existing_target_file_unlinking() -> None:
    """Tests that finalize safely clears down target path checks during rename operations."""
    unique_prefix = f"dbmigration_{uuid.uuid4()}_"
    target_path = Path(".") / f"{unique_prefix}replace_script.sql"
    builder = UpdateScriptBuilder(target_path)
    
    try:
        builder.check()
        
        with builder:
            builder.write_body("ALTER TABLE users ADD COLUMN age INT;")
        
        # Re-verify target path existence handling edge cases directly before finalize
        target_path.touch(exist_ok=True) 

        builder.finalize()
        
        assert target_path.exists()
        assert target_path.read_text(encoding="utf-8") == "ALTER TABLE users ADD COLUMN age INT;"
    finally:
        builder.cleanup()
        target_path.unlink(missing_ok=True)
