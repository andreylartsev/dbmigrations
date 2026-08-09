import pytest
from unittest.mock import MagicMock

from dbmigration import VerifyCommand

@pytest.fixture
def comment_formatter():
    """Fixture to isolate the format_sql_comment method using a MagicMock."""
    cmd = MagicMock()
    return VerifyCommand.format_sql_comment.__get__(cmd)


def test_format_comment_adds_prefix(comment_formatter):
    """Verify that the SQL comment prefix '-- ' is automatically added if missing."""
    result = comment_formatter("Standard text comment")
    assert result == "-- Standard text comment\n"


def test_format_comment_keeps_existing_prefix(comment_formatter):
    """Verify that an existing '-- ' prefix is not duplicated."""
    result = comment_formatter("-- Already a comment")
    assert result == "-- Already a comment\n"


def test_format_comment_strips_whitespace(comment_formatter):
    """Verify that leading and trailing whitespaces are properly stripped before formatting."""
    result = comment_formatter("   Clean me up   ")
    assert result == "-- Clean me up\n"


def test_format_comment_neutralizes_sql_injection(comment_formatter):
    """
    Verify that newline characters are replaced with spaces to completely
    neutralize CRLF SQL Injection attempts within comment blocks.
    """
    malicious_input = "BAD VERSION\n;TRUNCATE dbmigration_environment_id CASCADE; --"
    expected_output = "-- BAD VERSION ;TRUNCATE dbmigration_environment_id CASCADE; --\n"
    
    result = comment_formatter(malicious_input)
    assert result == expected_output
