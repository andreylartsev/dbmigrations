import pytest

from dbmigration import render_script_diff_text


def test_new_script_diff_shows_additions():
    """
    A script that has never been applied (no DB OID) must be rendered as
    a whole-file addition with explicit 'new script' OID label.
    """
    diff = render_script_diff_text("", "SELECT 1;\n", "test/repeat.sql", None, "oidnew")

    assert "a/test/repeat.sql (DB OID: new script)" in diff
    assert "b/test/repeat.sql (REPO OID: oidnew)" in diff
    assert "+SELECT 1;" in diff


def test_modified_script_diff_shows_changed_lines():
    """
    A modified script must contain both removed ('-') and added ('+') lines
    together with a hunk header ('@@').
    """
    old_text = "CREATE VIEW v AS SELECT 1 AS a;\n"
    new_text = "CREATE VIEW v AS SELECT 2 AS a; -- changed\n"

    diff = render_script_diff_text(old_text, new_text, "path/r.sql", "oidold", "oidnew")

    assert "a/path/r.sql (DB OID: oidold)" in diff
    assert "b/path/r.sql (REPO OID: oidnew)" in diff
    assert "-CREATE VIEW v AS SELECT 1 AS a;" in diff
    assert "+CREATE VIEW v AS SELECT 2 AS a; -- changed" in diff
    assert "@@" in diff


def test_equal_scripts_produce_no_diff():
    """
    When the applied (DB) and the repository script texts are equal,
    the unified diff must be empty so that no noise is printed.
    """
    diff = render_script_diff_text("same\n", "same\n", "path/r.sql", "oidold", "oidnew")
    assert diff == ""


def test_diff_handles_crlf_line_endings():
    """
    The diff must tolerate Windows-style CRLF line endings in both inputs.
    """
    diff = render_script_diff_text("SELECT 1;\r\n", "SELECT 2;\r\n", "p/r.sql", "o1", "o2")
    assert "-SELECT 1;" in diff
    assert "+SELECT 2;" in diff


def test_full_oid_labels_are_shortened_to_eight_chars():
    """
    Long git blob OIDs must be abbreviated to their short 8-char prefix in the
    diff labels, matching the rest of the CLI output.
    """
    old_oid = "2d03ba9d47a60d9ddc48f4f4f843227971910641"
    new_oid = "2f3fd6c54a24340f89d81155344f832325ffbc4a"

    diff = render_script_diff_text("old;\n", "new;\n", "path/r.sql", old_oid, new_oid)

    assert "a/path/r.sql (DB OID: 2d03ba9d)" in diff
    assert old_oid not in diff
    assert "b/path/r.sql (REPO OID: 2f3fd6c5)" in diff
    assert new_oid not in diff