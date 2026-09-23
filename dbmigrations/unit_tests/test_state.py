"""Unit tests for StateProbe (no database, DbConnection is stubbed)."""

from collections import deque

import pytest

from _errors import CommandError
from _state import DbConnection as _real_DbConnection
from _state import State
from dbmigration import StateProbe


class CtxCursor:
    def __init__(self, results):
        self._results = results

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params):
        pass

    def fetchone(self):
        return self._results.popleft() if self._results else None


class FakeDb:
    """Stands in for DbConnection; feed rows via FakeDb.results in query order."""

    results = []
    settings = None

    def __init__(self, dbconn_settings, out=None, quiet=False):
        self.settings = dbconn_settings
        self.out = out
        self.quiet = quiet
        self._results = deque(type(self).results)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def cursor(self):
        return CtxCursor(self._results)


class FailingDb:
    def __init__(self, dbconn_settings, out=None, quiet=False):
        pass

    def __enter__(self):
        raise CommandError("Unable to establish connection to database server. Inner error: boom")

    def __exit__(self, *args):
        return False


# query order: schema_exists, schema_is_empty,
# then 4 control-table checks, then latest/baseline version
INITIALIZED = [
    (True,),       # schema exists
    (False,),      # not empty
    (True,),       # dbmigration_environment_id
    (True,),       # dbmigration_versions
    (True,),       # dbmigration_version_scripts
    (True,),       # dbmigration_repeatable_scripts
    ("V003",),     # latest version installed
    ("V001",),     # baseline version installed
]

EMPTY_SCHEMA = [
    (True,),   # schema exists
    (True,),   # empty -> control tables not probed
]


def make_repo(tmp_path, with_tests=False, with_git=False, with_target_version=True):
    baseline = tmp_path / "baseline" / "V001"
    baseline.mkdir(parents=True)
    versions = tmp_path / "versions"
    versions.mkdir()
    repeatable = tmp_path / "repeatable"
    repeatable.mkdir()
    if with_tests:
        (tmp_path / "tests").mkdir()
    if with_target_version:
        (repeatable / "target_version.txt").write_text("V003\n")
    (tmp_path / "target_environment_id.txt").write_text("env-1\n")
    (tmp_path / "set_search_path.txt").write_text("schema_one\n")
    if with_git:
        (tmp_path / ".git").mkdir()
    return tmp_path


def probe_with(results, scripts_path, monkeypatch, schema_name="myschema"):
    FakeDb.results = results
    monkeypatch.setattr("_state.DbConnection", FakeDb)
    settings = {"dbname": "test1"}
    return StateProbe(
        dbconn_settings=settings,
        scripts_path=str(scripts_path),
        schema_name=schema_name,
    ).probe()


class TestProbeDbState:
    def test_initialized_schema(self, tmp_path, monkeypatch):
        state = probe_with(INITIALIZED, make_repo(tmp_path), monkeypatch)

        assert state.connection_ok
        assert state.schema_exists is True
        assert state.schema_is_empty is False
        assert state.control_tables_exist is True
        assert state.latest_version_installed == "V003"
        assert state.baseline_version_installed == "V001"

    def test_empty_schema(self, tmp_path, monkeypatch):
        state = probe_with(EMPTY_SCHEMA, make_repo(tmp_path), monkeypatch)

        assert state.schema_exists is True
        assert state.schema_is_empty is True
        assert state.control_tables_exist is False
        assert state.latest_version_installed is None

    def test_missing_schema(self, tmp_path, monkeypatch):
        state = probe_with([(False,)], make_repo(tmp_path), monkeypatch)

        assert state.schema_exists is False
        assert state.control_tables_exist is False

    def test_connection_error_is_recorded(self, tmp_path, monkeypatch):
        monkeypatch.setattr("_state.DbConnection", FailingDb)
        state = StateProbe(
            dbconn_settings={"dbname": "nope"},
            scripts_path=str(tmp_path),
            schema_name="myschema",
        ).probe()

        assert state.connection_error is not None
        assert "boom" in state.connection_error
        assert state.connection_ok is False
        assert state.schema_exists is False
        assert state.control_tables_exist is False


class TestCommandAvailability:
    def test_can_init_on_empty_schema(self, tmp_path, monkeypatch):
        state = probe_with(EMPTY_SCHEMA, make_repo(tmp_path), monkeypatch)

        assert state.can_init is True
        assert state.can_update is False
        assert state.can_verify is False
        assert state.can_run_tests is False

    def test_can_update_and_verify_when_initialized(self, tmp_path, monkeypatch):
        state = probe_with(
            INITIALIZED, make_repo(tmp_path, with_tests=True), monkeypatch
        )

        assert state.can_init is False
        assert state.can_update is True
        assert state.can_verify is True
        assert state.can_run_tests is True

    def test_run_tests_requires_tests_dir(self, tmp_path, monkeypatch):
        state = probe_with(INITIALIZED, make_repo(tmp_path, with_tests=False), monkeypatch)

        assert state.can_update is True
        assert state.can_run_tests is False

    def test_no_commands_when_connection_failed(self, tmp_path, monkeypatch):
        monkeypatch.setattr("_state.DbConnection", FailingDb)
        state = StateProbe(
            dbconn_settings={"dbname": "nope"},
            scripts_path=str(tmp_path),
            schema_name="myschema",
        ).probe()

        assert state.can_init is False
        assert state.can_update is False
        assert state.can_verify is False
        assert state.can_run_tests is False


class TestProbeRepository:
    def test_missing_scripts_path(self, tmp_path, monkeypatch):
        state = probe_with(INITIALIZED, tmp_path / "no_such_dir", monkeypatch)

        assert state.scripts_path_exists is False
        assert state.baseline_dir_exists is False
        assert state.is_git_repo is False

    def test_repo_layout_flags(self, tmp_path, monkeypatch):
        state = probe_with(
            INITIALIZED, make_repo(tmp_path, with_git=True), monkeypatch
        )

        assert state.scripts_path_exists is True
        assert state.baseline_dir_exists is True
        assert state.versions_dir_exists is True
        assert state.repeatable_dir_exists is True
        assert state.tests_dir_exists is False
        assert state.target_version_file_exists is True
        assert state.target_version == "V003"
        assert state.target_environment_id_file_exists is True
        assert state.set_search_path_file_exists is True
        assert state.is_git_repo is True

    def test_not_a_git_repo(self, tmp_path, monkeypatch):
        state = probe_with(INITIALIZED, make_repo(tmp_path, with_git=False), monkeypatch)

        assert state.is_git_repo is False

    def test_git_marker_in_parent_directory(self, tmp_path, monkeypatch):
        parent_git = tmp_path / ".git"
        parent_git.mkdir()
        repo = tmp_path / "sub" / "repo"
        repo.mkdir(parents=True)
        state = probe_with(INITIALIZED, repo, monkeypatch)

        assert state.is_git_repo is True


class TestStateDataclass:
    def test_defaults(self):
        state = State(schema_name="s", scripts_path="/tmp")

        assert state.connection_ok is True
        assert state.initialized is False
        assert state.can_init is False
        assert state.can_update is False
        assert state.can_verify is False
        assert state.can_run_tests is False