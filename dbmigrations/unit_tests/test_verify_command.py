import pytest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

from dbmigration import CommandError, CommitInfo, ScriptFsInfo, VerifyCommand

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


class TestGetStoredOidForScript:
    """Unit tests for VerifyCommand.get_stored_oid_for_script."""

    def test_repeatable_table_lookup_for_repeatable(self, tmp_path):
        cmd = MagicMock()
        cmd.get_db_oid_for_repeatable_script.return_value = "repevailid"
        cmd.get_db_oid_for_versioned_script.return_value = "veroid"

        script_info = make_script_info(tmp_path, "SELECT 1;\n")

        result = VerifyCommand.get_stored_oid_for_script.__get__(cmd)(
            script_info, version="V000", scripts_table="repeatable"
        )

        assert result == "repevailid"
        cmd.get_db_oid_for_repeatable_script.assert_called_once()
        cmd.get_db_oid_for_versioned_script.assert_not_called()
        call_args = cmd.get_db_oid_for_repeatable_script.call_args
        assert call_args[0][0].endswith("repeat.sql")
        assert call_args[0][1] == "V000"

    def test_versioned_table_lookup_is_used_by_default(self, tmp_path):
        cmd = MagicMock()
        cmd.get_db_oid_for_repeatable_script.return_value = "repevailid"
        cmd.get_db_oid_for_versioned_script.return_value = "veroid"

        script_info = make_script_info(tmp_path, "SELECT 1;\n")

        result = VerifyCommand.get_stored_oid_for_script.__get__(cmd)(
            script_info, version="V001", scripts_table="versioned"
        )

        assert result == "veroid"
        cmd.get_db_oid_for_versioned_script.assert_called_once()
        cmd.get_db_oid_for_repeatable_script.assert_not_called()
        call_args = cmd.get_db_oid_for_versioned_script.call_args
        assert call_args[0][0].endswith("repeat.sql")
        assert call_args[0][1] == "V001"


class TestBuildScriptDiffText:
    """Unit tests for VerifyCommand.build_script_diff_text."""

    def _make_cmd(self, stored_oid) -> MagicMock:
        cmd = MagicMock()
        cmd.git = MagicMock()
        cmd.get_stored_oid_for_script = MagicMock(return_value=stored_oid)
        return cmd

    def test_happy_path_builds_unified_diff(self, tmp_path):
        cmd = self._make_cmd(stored_oid="oldoid")
        cmd.git.get_blob_content_by_oid.return_value = "SELECT 1;\n"
        script_info = make_script_info(tmp_path, "SELECT 2;\n")

        diff_text = VerifyCommand.build_script_diff_text.__get__(cmd)(
            script_info, version="V000", scripts_table="repeatable"
        )

        assert "-SELECT 1;" in diff_text
        assert "+SELECT 2;" in diff_text

    def test_new_script_returns_collapsed_marker(self, tmp_path):
        cmd = self._make_cmd(stored_oid=None)
        script_info = make_script_info(tmp_path, "SELECT 1;\n")

        diff_text = VerifyCommand.build_script_diff_text.__get__(cmd)(
            script_info, version="V000", scripts_table="repeatable"
        )

        assert diff_text == "+ New file, will be applied in full."

    def test_missing_blob_prints_notice_and_returns_none(self, capsys, tmp_path):
        cmd = self._make_cmd(stored_oid="oldoid")
        cmd.git.get_blob_content_by_oid.return_value = None
        script_info = make_script_info(tmp_path, "SELECT 2;\n")

        diff_text = VerifyCommand.build_script_diff_text.__get__(cmd)(
            script_info, version="V000", scripts_table="repeatable"
        )

        captured = capsys.readouterr()
        assert "was not found in the local repository" in captured.out
        assert diff_text is None

    def test_identical_texts_return_empty_diff(self, tmp_path):
        cmd = self._make_cmd(stored_oid="oldoid")
        cmd.git.get_blob_content_by_oid.return_value = "SELECT 2;\n"
        script_info = make_script_info(tmp_path, "SELECT 2;\n")

        diff_text = VerifyCommand.build_script_diff_text.__get__(cmd)(
            script_info, version="V000", scripts_table="repeatable"
        )

        assert diff_text == ""


class TestDisplayScriptEntry:
    """Unit tests for the inline script file entry + diff rendering."""

    def _make_cmd(self, skip_diffs: bool = False) -> MagicMock:
        cmd = MagicMock()
        cmd.git = MagicMock()
        cmd.opts = SimpleNamespace(skip_diffs=skip_diffs)
        cmd._print_diff = VerifyCommand._print_diff.__get__(cmd)
        return cmd

    def test_happy_path_renders_inline_unified_diff(self, capsys, tmp_path):
        cmd = self._make_cmd()
        cmd.build_script_diff_text = MagicMock(return_value="-SELECT 1;\n+SELECT 2;")
        script_info = make_script_info(tmp_path, "SELECT 2;\n")

        VerifyCommand.display_script_entry.__get__(cmd)(
            script_info, version="V000", scripts_table="repeatable"
        )

        captured = capsys.readouterr()
        assert "Script text differences" not in captured.out
        assert f"{script_info!r}" in captured.out
        assert "-SELECT 1;" in captured.out
        assert "+SELECT 2;" in captured.out
        cmd.build_script_diff_text.assert_called_once_with(script_info, "V000", "repeatable")

    def test_new_script_renders_collapsed_marker(self, capsys, tmp_path):
        cmd = self._make_cmd()
        cmd.build_script_diff_text = MagicMock(return_value="+ New file, will be applied in full.")
        script_info = make_script_info(tmp_path, "SELECT 1;\n")

        VerifyCommand.display_script_entry.__get__(cmd)(
            script_info, version="V000", scripts_table="repeatable"
        )

        captured = capsys.readouterr()
        assert "+ New file, will be applied in full." in captured.out
        assert "+SELECT 1;" not in captured.out

    def test_missing_blob_prints_notice_and_skips_diff(self, capsys, tmp_path):
        cmd = self._make_cmd()
        cmd.get_stored_oid_for_script = MagicMock(return_value="oldoid")
        cmd.git.get_blob_content_by_oid.return_value = None
        cmd.build_script_diff_text = VerifyCommand.build_script_diff_text.__get__(cmd)
        script_info = make_script_info(tmp_path, "SELECT 2;\n")

        VerifyCommand.display_script_entry.__get__(cmd)(
            script_info, version="V000", scripts_table="repeatable"
        )

        captured = capsys.readouterr()
        assert "was not found in the local repository" in captured.out
        assert "-SELECT 2;" not in captured.out
        assert "+SELECT 2;" not in captured.out

    def test_identical_texts_produce_no_file_diff(self, capsys, tmp_path):
        cmd = self._make_cmd()
        cmd.build_script_diff_text = MagicMock(return_value="")
        script_info = make_script_info(tmp_path, "SELECT 2;\n")

        VerifyCommand.display_script_entry.__get__(cmd)(
            script_info, version="V000", scripts_table="repeatable"
        )

        captured = capsys.readouterr()
        assert f"{script_info!r}" in captured.out
        assert "-SELECT 2;" not in captured.out
        assert "+SELECT 2;" not in captured.out

    def test_skip_diffs_hides_diff_but_keeps_entry(self, capsys, tmp_path):
        cmd = self._make_cmd(skip_diffs=True)
        cmd.build_script_diff_text = MagicMock(return_value="-X\n+Y")
        script_info = make_script_info(tmp_path, "SELECT 2;\n")

        VerifyCommand.display_script_entry.__get__(cmd)(
            script_info, version="V000", scripts_table="repeatable"
        )

        cmd.build_script_diff_text.assert_not_called()
        captured = capsys.readouterr()
        assert f"{script_info!r}" in captured.out
        assert "-X" not in captured.out
        assert "+Y" not in captured.out

    def test_no_git_prints_entry_without_diff(self, capsys, tmp_path):
        cmd = self._make_cmd()
        cmd.git = None
        script_info = make_script_info(tmp_path, "SELECT 2;\n")

        VerifyCommand.display_script_entry.__get__(cmd)(
            script_info, version="V000", scripts_table="repeatable"
        )

        captured = capsys.readouterr()
        assert f"{script_info!r}" in captured.out
        assert "SELECT 2;" not in captured.out

    def test_empty_script_diffs_do_not_print(self, capsys, tmp_path):
        cmd = MagicMock()
        script_info = make_script_info(tmp_path, "SELECT 2;\n")

        VerifyCommand._print_diff.__get__(cmd)("", "    ")
        VerifyCommand._print_diff.__get__(cmd)(None, "    ")

        captured = capsys.readouterr()
        assert captured.out == ""


class TestBuildRecentChangesDiffMap:
    """Unit tests for VerifyCommand.build_recent_changes_diff_map."""

    def _make_cmd(self, pending_changes=None) -> MagicMock:
        cmd = MagicMock()
        cmd.git = MagicMock()
        cmd.pending_changes = [] if pending_changes is None else pending_changes
        return cmd

    def test_repeatable_row_renders_prev_vs_applied_diff(self, capsys):
        cmd = self._make_cmd()
        cmd.git.get_blob_content_by_oid.side_effect = ["SELECT 1;\n", "SELECT 2;\n"]
        cmd.get_previous_db_oid_for_repeatable_script.return_value = "prevoid"

        rows = [("2026-01-01", "repeatable", "V000", "dir/r.sql", "curoid")]

        diff_map = VerifyCommand.build_recent_changes_diff_map.__get__(cmd)(rows)

        lookup_args, _ = cmd.get_previous_db_oid_for_repeatable_script.call_args
        assert lookup_args == ("dir/r.sql", "V000", "2026-01-01")
        assert "-SELECT 1;" in diff_map["dir/r.sql"]
        assert "+SELECT 2;" in diff_map["dir/r.sql"]

    def test_versioned_row_without_prev_renders_collapsed_marker(self, capsys):
        cmd = self._make_cmd()
        cmd.git.get_blob_content_by_oid.side_effect = ["SELECT 42;\n"]
        cmd.get_previous_db_oid_for_versioned_script.return_value = None

        rows = [("2026-01-01 12:00:00", "versioned", "V001", "dir/v.sql", "curoid")]

        diff_map = VerifyCommand.build_recent_changes_diff_map.__get__(cmd)(rows)

        lookup_args, _ = cmd.get_previous_db_oid_for_versioned_script.call_args
        assert lookup_args == ("dir/v.sql", "V001")
        assert diff_map["dir/v.sql"] == "+ New file, will be applied in full."

    def test_prev_blob_not_found_prints_notice_and_skips(self, capsys):
        cmd = self._make_cmd()
        cmd.git.get_blob_content_by_oid.side_effect = [None, "SELECT 2;\n"]
        cmd.get_previous_db_oid_for_repeatable_script.return_value = "prevoid"

        rows = [("2026-01-01", "repeatable", "V000", "dir/r.sql", "curoid")]

        diff_map = VerifyCommand.build_recent_changes_diff_map.__get__(cmd)(rows)

        captured = capsys.readouterr()
        assert "was not found in the local repository" in captured.out
        assert diff_map["dir/r.sql"] is None

    def test_applied_blob_not_found_prints_notice_and_skips(self, capsys):
        cmd = self._make_cmd()
        cmd.git.get_blob_content_by_oid.side_effect = ["SELECT 1;\n", None]
        cmd.get_previous_db_oid_for_repeatable_script.return_value = "prevoid"

        rows = [("2026-01-01", "repeatable", "V000", "dir/r.sql", "curoid")]

        diff_map = VerifyCommand.build_recent_changes_diff_map.__get__(cmd)(rows)

        captured = capsys.readouterr()
        assert "was not found in the local repository" in captured.out
        assert diff_map["dir/r.sql"] is None

    def test_identical_texts_produce_empty_diff(self, capsys):
        cmd = self._make_cmd()
        cmd.git.get_blob_content_by_oid.side_effect = ["SELECT 1;\n", "SELECT 1;\n"]
        cmd.get_previous_db_oid_for_repeatable_script.return_value = "prevoid"

        rows = [("2026-01-01", "repeatable", "V000", "dir/r.sql", "curoid")]

        diff_map = VerifyCommand.build_recent_changes_diff_map.__get__(cmd)(rows)

        captured = capsys.readouterr()
        assert diff_map["dir/r.sql"] == ""

    def test_empty_rows_produce_empty_map(self, capsys):
        cmd = self._make_cmd()

        diff_map = VerifyCommand.build_recent_changes_diff_map.__get__(cmd)([])

        captured = capsys.readouterr()
        assert diff_map == {}

    def test_only_most_recent_application_per_file_is_diffed(self, capsys):
        cmd = self._make_cmd()
        cmd.git.get_blob_content_by_oid.side_effect = ["SELECT 1;\n", "SELECT 42;\n"]
        cmd.get_previous_db_oid_for_repeatable_script.return_value = "prevoid"

        rows = [
            ("2026-01-01 12:02:00", "repeatable", "V000", "dir/r.sql", "v2oid"),
            ("2026-01-01 12:01:00", "repeatable", "V000", "dir/r.sql", "v1oid"),
        ]

        diff_map = VerifyCommand.build_recent_changes_diff_map.__get__(cmd)(rows)

        cmd.get_previous_db_oid_for_repeatable_script.assert_called_once()
        assert "-SELECT 1;" in diff_map["dir/r.sql"]
        assert "+SELECT 42;" in diff_map["dir/r.sql"]

    def test_pending_changes_skip_their_own_diff(self, capsys):
        cmd = self._make_cmd(pending_changes=["dir/r.sql"])
        cmd.git.get_blob_content_by_oid.side_effect = ["SELECT 1;\n", "SELECT 2;\n"]
        cmd.get_previous_db_oid_for_repeatable_script.return_value = "prevoid"

        rows = [("2026-01-01", "repeatable", "V000", "dir/r.sql", "curoid")]

        diff_map = VerifyCommand.build_recent_changes_diff_map.__get__(cmd)(rows)

        assert "dir/r.sql" not in diff_map


class TestDisplayRecentChanges:
    """Unit tests for VerifyCommand.display_recent_changes wiring."""

    def _make_cmd(self, skip_diffs: bool, pending_changes=None) -> MagicMock:
        cmd = MagicMock()
        cmd.git = MagicMock()
        cmd.opts = SimpleNamespace(skip_diffs=skip_diffs)
        cmd.pending_changes = [] if pending_changes is None else pending_changes
        rows = [("2026-01-01 12:00:00", "versioned", "V001", "dir/v.sql", "curoid")]
        cmd.get_recent_changes_from_db.return_value = rows
        return cmd

    def test_diffs_on_by_default(self, capsys):
        cmd = self._make_cmd(skip_diffs=False)
        expected_rows = cmd.get_recent_changes_from_db.return_value

        VerifyCommand.display_recent_changes.__get__(cmd)(limit=10, window_minutes=30)

        cmd.build_recent_changes_diff_map.assert_called_once_with(expected_rows)
        cmd.display_recent_changes_grouped_by_git_commits.assert_called_once()
        call_args, call_kwargs = cmd.display_recent_changes_grouped_by_git_commits.call_args
        assert call_args[0] == expected_rows
        assert call_kwargs["diff_map"] == cmd.build_recent_changes_diff_map.return_value

    def test_skip_diffs_skips_building_diff_map(self, capsys):
        cmd = self._make_cmd(skip_diffs=True)
        expected_rows = cmd.get_recent_changes_from_db.return_value

        VerifyCommand.display_recent_changes.__get__(cmd)(limit=10, window_minutes=30)

        cmd.build_recent_changes_diff_map.assert_not_called()
        cmd.display_recent_changes_grouped_by_git_commits.assert_called_once()
        call_args, call_kwargs = cmd.display_recent_changes_grouped_by_git_commits.call_args
        assert call_args[0] == expected_rows
        assert call_kwargs["diff_map"] is None

    def test_pending_changes_still_build_diff_map_for_other_files(self, capsys):
        cmd = self._make_cmd(skip_diffs=False, pending_changes=["repeatable/00_current_value.sql"])

        VerifyCommand.display_recent_changes.__get__(cmd)(limit=10, window_minutes=30)

        cmd.build_recent_changes_diff_map.assert_called_once()
        cmd.display_recent_changes_grouped_by_git_commits.assert_called_once()

    def test_grouped_display_attaches_diff_only_to_most_recent_occurrence(self, capsys):
        cmd = MagicMock()
        cmd.git = MagicMock()
        commit = CommitInfo(
            oid="abcd1234", author="Tester",
            date=datetime(2026, 1, 1, 12, 0, 0), message="commit",
        )
        cmd.git.get_commit_by_file_oid.return_value = commit
        cmd._print_diff = VerifyCommand._print_diff.__get__(cmd)

        rows = [
            (datetime(2026, 1, 1, 12, 2, 0), "repeatable", "V000", "dir/r.sql", "v2oid"),
            (datetime(2026, 1, 1, 12, 1, 0), "repeatable", "V000", "dir/r.sql", "v1oid"),
        ]
        diff_map = {"dir/r.sql": "-OLD\n+NEW"}

        VerifyCommand.display_recent_changes_grouped_by_git_commits.__get__(cmd)(
            rows, diff_map=diff_map
        )

        captured = capsys.readouterr()
        assert captured.out.count("-OLD") == 1
        assert captured.out.count("+NEW") == 1
        assert "[2026-01-01 12:02:00" in captured.out
        assert "[2026-01-01 12:01:00" in captured.out


class TestDisplayRequiredChanges:
    """Unit tests for the pending changes recording in VerifyCommand.display_required_changes."""

    def test_records_pending_changes(self, tmp_path):
        cmd = MagicMock()
        cmd.git = None
        cmd.pending_changes = []

        script_infos = [make_script_info(tmp_path, "SELECT 1;\n", name="a.sql")]
        script_infos.append(make_script_info(tmp_path, "SELECT 2;\n", name="b.sql"))

        VerifyCommand.display_required_changes.__get__(cmd)(script_infos)

        assert [i.relative_path for i in script_infos] == cmd.pending_changes


class TestGetPreviousDbOid:
    """Unit tests for the previous OID lookup methods."""

    def test_repeatable_lookup_uses_created_at_cutoff(self):
        cmd = MagicMock()
        cmd.format_sql = lambda sql, **kwargs: sql
        cmd.dbconn_get_single_value.return_value = "prevoid"
        applied_at = datetime(2026, 1, 1, 12, 0, 0)

        result = VerifyCommand.get_previous_db_oid_for_repeatable_script.__get__(cmd)(
            "dir/r.sql", "V000", applied_at
        )

        args, kwargs = cmd.dbconn_get_single_value.call_args
        assert "created_at < %s" in args[0]
        assert args[1] == ("dir/r.sql", "V000", applied_at)
        assert result == "prevoid"

    def test_versioned_lookup_uses_earlier_version_cutoff(self):
        cmd = MagicMock()
        cmd.format_sql = lambda sql, **kwargs: sql
        cmd.dbconn_get_single_value.return_value = None

        result = VerifyCommand.get_previous_db_oid_for_versioned_script.__get__(cmd)(
            "dir/v.sql", "V001"
        )

        args, kwargs = cmd.dbconn_get_single_value.call_args
        assert "version_id < %s" in args[0]
        assert args[1] == ("dir/v.sql", "V001")
        assert result is None


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
