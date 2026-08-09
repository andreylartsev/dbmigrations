import pytest
from unittest.mock import MagicMock

from dbmigration import CommandError, VerifyCommand

@pytest.fixture
def cross_check():
    """Fixture to isolate the method and bind it to a MagicMock of VerifyCommand."""
    cmd = MagicMock()
    return VerifyCommand.cross_check_of_the_target_version_for_repeatable_scripts.__get__(cmd)


@pytest.mark.parametrize(
    "target, scripts, installed",
    [
        ("1.0.0", None, "1.0.0"),     # Branch 2: No scripts provided, versions match
        ("1.0.0", "1.0.0", None),     # Branch 3: Empty database, versions match
        ("1.1.0", "1.1.0", "1.0.0"),  # Branch 4: Scripts version is newer, versions match
        ("1.2.0", "1.0.0", "1.2.0"),  # Branch 5: Installed version is newer/equal, versions match
        ("1.0.0", "1.0.0", "1.0.0"),  # Branch 5: Both versions are completely equal, versions match
    ]
)
def test_cross_check_success_cases(cross_check, target, scripts, installed):
    """Verify all successful scenarios where no exception should be raised."""
    cross_check(target, scripts, installed)


@pytest.mark.parametrize(
    "target, scripts, installed, expected_msg",
    [
        # Branch 1: Both versions are None
        (
            "1.0.0", None, None, 
            "Failed to check target version '1.0.0' because no version is installed and no versioned scripts were provided in the scripts directory."
        ),
        
        # Branch 2: Scripts version is None, target does not match installed version
        (
            "2.0.0", None, "1.0.0", 
            "The target version '2.0.0' does not match the latest installed version '1.0.0'."
        ),
        
        # Branch 3: Installed version is None, target does not match scripts version
        (
            "2.0.0", "1.0.0", None, 
            "The target version '2.0.0' does not match the latest scripts version '1.0.0'."
        ),
        
        # Branch 4: Scripts version is newer, but target is stuck on old installed version
        (
            "1.0.0", "1.1.0", "1.0.0", 
            "The target version '1.0.0' does not match the latest scripts version '1.1.0'."
        ),
        
        # Branch 5: Installed version is newer/equal, but target is stuck on old scripts version
        (
            "1.0.0", "1.0.0", "1.2.0", 
            "The target version '1.0.0' does not match the latest installed version '1.2.0'."
        ),
    ]
)
def test_cross_check_error_cases(cross_check, target, scripts, installed, expected_msg):
    """Verify all five original error branches for an exact string match."""
    with pytest.raises(CommandError) as exc_info:
        cross_check(target, scripts, installed)
    
    # Strict character-by-character comparison to pin down original text behavior
    assert str(exc_info.value) == expected_msg
