import sys
import shutil
import tempfile
import time
import pytest
import uuid
from pathlib import Path
from typing import Generator
from dbmigration import CommandError, UpdateScriptBuilder

@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    """
    Creates an isolated temporary directory using Python's standard tempfile 
    module combined with a unique UUID4 to prevent any OS file handle locking 
    or path collisions on Windows.
    """
    # Generate a completely unique folder name using a random GUID
    unique_prefix = f"dbmigration_{uuid.uuid4()}_"
    
    test_dir = tempfile.mkdtemp(prefix=unique_prefix)
    yield Path(test_dir)
    
    # Clean up the directory and all files inside after the test finishes
    #try:
    #    shutil.rmtree(test_dir)
    #except Exception:
    #    pass


def test_successful_script_building_lifecycle(temp_dir: Path) -> None:
    """Tests the full successful lifecycle: check -> open -> write -> finalize."""
    target_path = temp_dir / "update_migration.sql"
    builder = UpdateScriptBuilder(target_path)
    
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

    # Give Windows OS enough time to flush and release internal file handles
    if sys.platform == "win32":
        time.sleep(1)  # 50 milliseconds delay to mitigate WinError 6

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


def test_check_raises_error_if_target_already_exists(temp_dir: Path) -> None:
    """Tests that check() triggers CommandError due to the predefined touch file lock."""
    target_path = temp_dir / "existing_script.sql"
    target_path.touch()  # Simulate the path already being locked/occupied
    
    builder = UpdateScriptBuilder(target_path)
    
    with pytest.raises(CommandError) as exc_info:
        builder.check()
        
    assert "already exists" in str(exc_info.value)


def test_check_raises_error_if_parent_directory_does_not_exist() -> None:
    """Tests that check() validation catches a missing base path configuration."""
    invalid_path = Path("/non/existent/dir/structure/script.sql")
    builder = UpdateScriptBuilder(invalid_path)
    
    with pytest.raises(CommandError) as exc_info:
        builder.check()
        
    assert "parent directory" in str(exc_info.value)


def test_context_manager_cleanup_on_exception(temp_dir: Path) -> None:
    """Tests that __exit__ removes files if an exception is raised inside the 'with' block."""
    target_path = temp_dir / "failed_script.sql"
    builder = UpdateScriptBuilder(target_path)
    builder.check()
    
    # Ensure temporary structure is present initially
    assert builder.temp_script_path.exists()

    with pytest.raises(ValueError):
        with builder:
            builder.write_header("BEGIN;\n")
            raise ValueError("Interrupted process simulation")
            
    # Give Windows OS time to drop handles before pytest attempts to inspect unlinked paths
    if sys.platform == "win32":
        time.sleep(1)

    # __exit__ should roll back changes and remove target files on error
    assert not builder.temp_script_path.exists()
    assert not target_path.exists()


def test_explicit_cleanup_removes_all_files(temp_dir: Path) -> None:
    """Tests that a manual call to cleanup() clears both target and temporary files."""
    target_path = temp_dir / "cleanup_script.sql"
    builder = UpdateScriptBuilder(target_path)
    builder.check()
    
    with builder:
        builder.write_body("SELECT 1;")
        
    assert builder.temp_script_path.exists()
    
    builder.cleanup()
    
    # All paths must be completely unlinked post-cleanup
    assert not builder.temp_script_path.exists()
    assert not target_path.exists()


def test_finalize_handles_existing_target_file_unlinking(temp_dir: Path) -> None:
    """Tests that finalize safely clears down target path checks during rename operations."""
    target_path = temp_dir / "replace_script.sql"
    builder = UpdateScriptBuilder(target_path)
    builder.check()
    
    with builder:
        builder.write_body("ALTER TABLE users ADD COLUMN age INT;")
    
    # Re-verify target path existence handling edge cases directly before finalize
    target_path.touch(exist_ok=True) 

    # Give Windows OS time to handle file locking states between touch and rename
    if sys.platform == "win32":
        time.sleep(1)

    builder.finalize()
    
    assert target_path.exists()
    assert target_path.read_text(encoding="utf-8") == "ALTER TABLE users ADD COLUMN age INT;"
