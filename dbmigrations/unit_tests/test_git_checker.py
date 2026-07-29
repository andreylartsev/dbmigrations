import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path
import subprocess
from datetime import datetime

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

# =========================================================================
# TESTS FOR LOGIC METHODS (get_latest_commit & get_commit_by_file_oid)
# =========================================================================

def test_get_latest_commit_file_is_untracked():
    """
    Ensures get_latest_commit detects an untracked file via 'git status' output
    and returns a properly flagged uncommitted CommitInfo object.
    """
    # Create checker with dummy paths
    checker = GitChecker(git_cmd=Path("git"), repo_root=Path("/repo"))
    target_file = Path("new_script.py")

    # Simulate 'git status --porcelain -z' output for an untracked file
    # The output format for untracked is '?? filename\x00'
    mock_status_output = "?? new_script.py\x00"

    with patch.object(checker, "_run_git", return_value=mock_status_output) as mock_run:
        result = checker.get_latest_commit(target_file)

        # Verify right Git command arguments were passed
        mock_run.assert_called_once_with(["status", "--porcelain", "-z", "--", "new_script.py"])
        
        # Verify the returned CommitInfo object state
        assert result.oid is None
        assert "untracked" in result.message


def test_get_latest_commit_file_is_clean_returns_log_data():
    """
    Ensures get_latest_commit parses 'git log' stdout properly when the file
    is clean and returns a CommitInfo object filled with real commit metadata.
    """
    checker = GitChecker(git_cmd=Path("git"), repo_root=Path("/repo"))
    target_file = Path("existing_script.py")

    # Mock responses: empty status (clean file) and a structured log line
    # Format: SHA|AUTHOR|TIMESTAMP|SUBJECT
    mock_log_output = "a1b2c3d4e5f6|John Doe|1711800000|Fix minor database connection leak"

    def side_effect(args):
        if "status" in args:
            return "" # No uncommitted changes
        if "log" in args:
            return mock_log_output
        return ""

    with patch.object(checker, "_run_git", side_effect=side_effect):
        result = checker.get_latest_commit(target_file)

        assert result.oid == "a1b2c3d4e5f6"
        assert result.author == "John Doe"
        assert result.date == datetime.fromtimestamp(1711800000)
        assert result.message == "Fix minor database connection leak"


def test_get_commit_by_file_oid_happy_path():
    """
    Verifies get_commit_by_file_oid successfully finds a commit by blob OID
    and maps the structured git log response into a CommitInfo instance.
    """
    checker = GitChecker(git_cmd=Path("git"), repo_root=Path("/repo"))
    target_blob_oid = "7f8e9d1c"

    # Simulated output from 'git log -1 --find-object=...'
    mock_log_output = "f1e2d3c4b5a6|Jane Smith|1711900000|Add core migration logic"

    with patch.object(checker, "_run_git", return_value=mock_log_output) as mock_run:
        result = checker.get_commit_by_file_oid(target_blob_oid)

        # Ensure --find-object argument was correctly formatted and passed
        mock_run.assert_called_once_with(["log", "-1", f"--find-object={target_blob_oid}", "--format=%H|%an|%ct|%s"])

        assert result.oid == "f1e2d3c4b5a6"
        assert result.author == "Jane Smith"
        assert result.date == datetime.fromtimestamp(1711900000)
        assert result.message == "Add core migration logic"


def test_get_commit_by_file_oid_untracked_or_missing_hash():
    """
    Checks that get_commit_by_file_oid returns an 'unknown' state CommitInfo
    if the log command output is completely empty (object is not tracked in history).
    """
    checker = GitChecker(git_cmd=Path("git"), repo_root=Path("/repo"))
    target_blob_oid = "000000000000000000000000"

    # Empty log output means Git could not find this object hash in any commit
    with patch.object(checker, "_run_git", return_value=""):
        result = checker.get_commit_by_file_oid(target_blob_oid)

        assert result.oid == target_blob_oid
        assert result.author is None
        assert result.date is None
        assert "untracked or modified locally" in result.message


def test_get_commit_by_file_oid_empty_argument_raises_value_error():
    """
    Ensures that passing an empty string or whitespace to get_commit_by_file_oid
    immediately raises a ValueError before executing any Git processes.
    """
    checker = GitChecker(git_cmd=Path("git"), repo_root=Path("/repo"))

    with pytest.raises(ValueError) as exc_info:
        checker.get_commit_by_file_oid("   ")

    assert "must not be empty" in str(exc_info.value)