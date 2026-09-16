import pytest
from unittest.mock import MagicMock

from dbmigration import CommandError, ScriptFsInfo, VerifyCommand

@pytest.fixture
def cross_check():
    """Fixture to isolate the method and bind it to a MagicMock of VerifyCommand."""
    cmd = MagicMock()
    return VerifyCommand.cross_check_of_the_target_version_for_repeatable_scripts.__get__(cmd)


def make_script_info(tmp_path, content: str, name: str = "repeat.sql") -> ScriptFsInfo:
    """Creates a real ScriptFsInfo object backed by a file in tmp_path."""
    target_file = tmp_path / name
    target_file.write_text(content)
    return ScriptFsInfo.get_info_with_text(tmp_path, target_file)


class TestDisplayScriptDiffs:
    """Unit tests for VerifyCommand.display_script_diffs."""

    def test_happy_path_renders_unified_diff(self, capsys, tmp_path):
        cmd = MagicMock()
        cmd.git = MagicMock()
        cmd.git.get_blob_content_by_oid.return_value = "SELECT 1;\n"
        cmd.get_db_oid_for_repeatable_script.return_value = "oldoid"

        script_infos = [make_script_info(tmp_path, "SELECT 2;\n")]

        VerifyCommand.display_script_diffs.__get__(cmd)(
            script_infos, version="V000", scripts_table="repeatable"
        )

        captured = capsys.readouterr()
        assert "Script text differences" in captured.out
        assert "-SELECT 1;" in captured.out
        assert "+SELECT 2;" in captured.out

    def test_new_script_uses_empty_old_text(self, capsys, tmp_path):
        cmd = MagicMock()
        cmd.git = MagicMock()
        cmd.get_db_oid_for_repeatable_script.return_value = None

        script_infos = [make_script_info(tmp_path, "SELECT 1;\n")]

        VerifyCommand.display_script_diffs.__get__(cmd)(
            script_infos, version="V000", scripts_table="repeatable"
        )

        captured = capsys.readouterr()
        assert "Script text differences" in captured.out
        assert "+SELECT 1;" in captured.out

    def test_missing_blob_prints_notice_and_skips_file(self, capsys, tmp_path):
        cmd = MagicMock()
        cmd.git = MagicMock()
        cmd.git.get_blob_content_by_oid.return_value = None
        cmd.get_db_oid_for_repeatable_script.return_value = "oldoid"

        script_infos = [make_script_info(tmp_path, "SELECT 2;\n")]

        VerifyCommand.display_script_diffs.__get__(cmd)(
            script_infos, version="V000", scripts_table="repeatable"
        )

        captured = capsys.readouterr()
        assert "was not found in the local repository" in captured.out

    def test_identical_texts_produce_no_file_diff(self, capsys, tmp_path):
        cmd = MagicMock()
        cmd.git = MagicMock()
        cmd.git.get_blob_content_by_oid.return_value = "SELECT 2;\n"
        cmd.get_db_oid_for_repeatable_script.return_value = "oldoid"

        script_infos = [make_script_info(tmp_path, "SELECT 2;\n")]

        VerifyCommand.display_script_diffs.__get__(cmd)(
            script_infos, version="V000", scripts_table="repeatable"
        )

        captured = capsys.readouterr()
        assert "Script text differences" in captured.out
        assert "SELECT 2;" not in captured.out

    def test_versioned_table_lookup_is_used_by_default(self, capsys, tmp_path):
        cmd = MagicMock()
        cmd.git = MagicMock()
        cmd.git.get_blob_content_by_oid.return_value = "OLD;\n"
        cmd.get_db_oid_for_versioned_script.return_value = "oldoid"

        script_infos = [make_script_info(tmp_path, "NEW;\n")]

        VerifyCommand.display_script_diffs.__get__(cmd)(
            script_infos, version="V001", scripts_table="versioned"
        )

        call_args = cmd.get_db_oid_for_versioned_script.call_args
        assert call_args[0][0].endswith("repeat.sql")
        assert call_args[0][1] == "V001"
        captured = capsys.readouterr()
        assert "-OLD;" in captured.out
        assert "+NEW;" in captured.out


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
