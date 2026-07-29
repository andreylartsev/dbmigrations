import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path
import subprocess

# Replace 'dbmigration' with your actual module name
from dbmigration import GitChecker, CommandError, GIT_CMD_CONFIG_ATTRIBUTE

# =========================================================================
# TESTS FOR EXECUTABLE PATH RESOLUTION (_try_get_git_cmd_path)
# =========================================================================

def test_git_cmd_path_missing_in_system_shows_warning(capsys):
    """
    Checks that a clear warning is printed to stdout and None is returned
    if Git is completely missing from the system environments and TOML config.
    """
    toml_config = {}  # No explicit path in TOML
    
    # Mock shutil.which to simulate that 'git' executable cannot be found in PATH
    with patch("shutil.which", return_value=None):
        result = GitChecker._try_get_git_cmd_path(toml_config)
        
        assert result is None
        
        # Capture stdout/stderr to verify the user-friendly warning
        captured = capsys.readouterr()
        assert "Warning: Git executable was not found in system PATH" in captured.out


def test_git_cmd_path_invalid_in_toml_raises_command_error():
    """
    Checks that CommandError is raised if the user explicitly provided 
    a path in the TOML config, but that file does not exist on disk.
    """
    toml_config = {GIT_CMD_CONFIG_ATTRIBUTE: "C:/invalid/path/to/git.exe"}
    
    # Path.exists() will return False for this non-existent route
    with pytest.raises(CommandError) as exc_info:
        GitChecker._try_get_git_cmd_path(toml_config)
        
    assert "The git cmd specified in" in str(exc_info.value)
    assert "does not exist!" in str(exc_info.value)


def test_git_repo_not_found_shows_warning(capsys, tmp_path):
    """
    Checks that a clear warning is printed and None is returned if the directory
    is valid but it is not located inside any Git repository structure.
    """
    tmp_path.mkdir(parents=True, exist_ok=True)
    mock_git_cmd = Path("git")

    # Simulate subprocess.run raising CalledProcessError when git rev-parse fails
    with patch("subprocess.run", side_effect=subprocess.CalledProcessError(1, "git")):
        result = GitChecker._try_get_git_repo_root(mock_git_cmd, tmp_path)

        assert result is None

        captured = capsys.readouterr()
        assert "Warning: A valid Git repository root was not found" in captured.out


def test_git_repo_invalid_directory_path_raises_command_error():
    """
    Checks that CommandError is immediately raised if the target scripts directory 
    path is not a folder or doesn't exist at all.
    """
    fake_path = Path("C:/totally/fake/and/missing/dir")
    mock_git_cmd = Path("git")
    
    with pytest.raises(CommandError) as exc_info:
        GitChecker._try_get_git_repo_root(mock_git_cmd, fake_path)
        
    assert "is invalid or not a directory!" in str(exc_info.value)


# =========================================================================
# INTEGRATION TESTS FOR THE FACTORY METHOD (try_get)
# =========================================================================

def test_try_get_returns_none_if_executable_resolution_fails():
    """
    Ensures that the top-level factory method returns None without breaking 
    if the internal executable path lookup returns None.
    """
    toml_config = {}
    scripts_dir = Path(".")
    
    # Stub the sub-method to return None directly
    with patch.object(GitChecker, "_try_get_git_cmd_path", return_value=None):
        result = GitChecker.try_get(toml_config, scripts_dir)
        assert result is None


def test_try_get_initializes_successfully_on_happy_path(tmp_path):
    """
    Verifies that the factory returns a valid GitChecker instance when both 
    executable and repository targets are correctly resolved.
    """
    toml_config = {}
    mock_path = Path("/usr/bin/git")
    mock_repo_root = Path("/projects/my_project")
    
    # Stub both internal routines to simulate a pristine working state
    with patch.object(GitChecker, "_try_get_git_cmd_path", return_value=mock_path), \
         patch.object(GitChecker, "_try_get_git_repo_root", return_value=mock_repo_root):
         
        checker = GitChecker.try_get(toml_config, tmp_path)
        
        # Verify the wrapper object state and dependencies
        assert isinstance(checker, GitChecker)
        assert checker.git_cmd == mock_path
        assert checker.repo_root == mock_repo_root
