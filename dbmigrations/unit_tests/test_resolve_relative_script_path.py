import pytest
from pathlib import Path
# Replace 'your_module' with the actual name of your file containing the functions and exception
from dbmigration import resolve_relative_script_path, SCRIPT_LIST_FILE_NAME, CommandError 

def test_resolve_relative_path_success():
    """Verifies successful path resolution preserving '..' upward directory transitions."""
    start_path = Path("/base/dir/project/sub")
    depth = 2
    path_str = "@other_env/tools/run.py"
    
    result = resolve_relative_script_path(start_path, depth, path_str)
    
    # Expected path must contain exactly 3 '..' steps (depth + 1)
    # and accurately mirror the last 2 segments of the source path (project, sub)
    expected = Path("/base/dir/project/sub/../../../other_env/project/sub/tools/run.py")
    
    assert result == expected


def test_resolve_relative_path_zero_depth():
    """Validates the function behavior when the base directory nesting depth is 0."""
    start_path = Path("/base/dir")
    depth = 0
    path_str = "@prod_env/main.py"
    
    result = resolve_relative_script_path(start_path, depth, path_str)
    
    # 1 step back (depth + 1), trailing_parts should remain empty
    expected = Path("/base/dir/../prod_env/main.py")
    
    assert result == expected


def test_resolve_relative_path_windows_separators():
    """Ensures Windows-style backslashes are properly normalized during parsing."""
    start_path = Path("/base/dir")
    depth = 0
    path_str = "@prod_env\\subfolder\\script.py"  # Windows-style path string
    
    result = resolve_relative_script_path(start_path, depth, path_str)
    expected = Path("/base/dir/../prod_env/subfolder/script.py")
    
    assert result == expected


def test_raise_error_missing_at_symbol():
    """Confirms an error is raised if the path does not originate with the '@' prefix."""
    start_path = Path("/base/dir")
    
    with pytest.raises(CommandError) as exc_info:
        resolve_relative_script_path(start_path, 1, "invalid_env/script.py")
        
    assert "must start with @ symbol" in str(exc_info.value)


def test_raise_error_no_separator_after_env():
    """Validates exception raising when no slash follows the target environment name."""
    start_path = Path("/base/dir")
    
    with pytest.raises(CommandError) as exc_info:
        resolve_relative_script_path(start_path, 1, "@only_env_name")
        
    assert "No path separator found after environment name" in str(exc_info.value)
    assert "script_list.txt" in str(exc_info.value)  # Checks for the injected filename
