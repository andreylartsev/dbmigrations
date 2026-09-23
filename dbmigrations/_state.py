"""Read-only state introspection of the target schema and scripts repository.

Used by the TUI to decide which commands are available and what to render.
All checks are non-raising: connection/query problems are recorded in
`State.connection_error` instead of propagating exceptions.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

import psycopg
from psycopg import sql as psycopg_sql

from _constants import (
    BASELINE_DIR_NAME,
    REPEATABLE_DIR_NAME,
    SEARCH_PATH_FILE_NAME,
    TARGET_ENVIRONMENT_ID_FILE_NAME,
    TARGET_VERSION_FILE,
    TESTS_DIR_NAME,
    VERSION_CONTROL_TABLE_NAMES,
    VERSIONED_DIR_NAME,
)
from _db import DbConnection
from _errors import CommandError
from _i18n import _
from _output import Output
from _scripts import read_as_trimmed_string


@dataclass
class State:
    """Snapshot of everything the UI needs to know about schema and repository."""

    schema_name: str
    scripts_path: str

    connection_error: str | None = None

    schema_exists: bool = False
    schema_is_empty: bool = True
    control_tables_exist: bool = False
    latest_version_installed: str | None = None
    baseline_version_installed: str | None = None

    scripts_path_exists: bool = False
    is_git_repo: bool = False
    baseline_dir_exists: bool = False
    versions_dir_exists: bool = False
    repeatable_dir_exists: bool = False
    tests_dir_exists: bool = False
    target_version_file_exists: bool = False
    target_version: str | None = None
    target_environment_id_file_exists: bool = False
    set_search_path_file_exists: bool = False

    @property
    def connection_ok(self) -> bool:
        return self.connection_error is None

    @property
    def initialized(self) -> bool:
        return self.control_tables_exist

    @property
    def can_init(self) -> bool:
        return self.connection_ok and self.schema_exists and not self.control_tables_exist

    @property
    def can_update(self) -> bool:
        return self.connection_ok and self.control_tables_exist

    @property
    def can_verify(self) -> bool:
        return self.connection_ok and self.control_tables_exist

    @property
    def can_run_tests(self) -> bool:
        return self.connection_ok and self.control_tables_exist and self.tests_dir_exists


class StateProbe:
    """Collects a read-only `State` snapshot without raising."""

    def __init__(
        self,
        dbconn_settings: dict[str, Any],
        scripts_path: str | Path,
        schema_name: str = "",
        out: Output | None = None,
        quiet: bool = True,
    ) -> None:
        self.dbconn_settings = dbconn_settings
        self.scripts_path = Path(scripts_path)
        self.schema_name = schema_name
        self.out = out if out is not None else Output()
        self.quiet = quiet

    def probe(self) -> State:
        state = State(schema_name=self.schema_name, scripts_path=str(self.scripts_path))
        state.scripts_path_exists = self.scripts_path.is_dir()
        self._probe_repository(state)
        self._probe_db(state)
        return state

    def _probe_repository(self, state: State) -> None:
        scripts_dir = self.scripts_path if state.scripts_path_exists else None
        if scripts_dir is None:
            return

        state.is_git_repo = self._is_git_repo(scripts_dir)
        for attr_name, subdir_name in (
            ("baseline_dir_exists", BASELINE_DIR_NAME),
            ("versions_dir_exists", VERSIONED_DIR_NAME),
            ("repeatable_dir_exists", REPEATABLE_DIR_NAME),
            ("tests_dir_exists", TESTS_DIR_NAME),
        ):
            setattr(state, attr_name, scripts_dir.joinpath(subdir_name).is_dir())

        target_version_file = scripts_dir.joinpath(REPEATABLE_DIR_NAME, TARGET_VERSION_FILE)
        state.target_version_file_exists = target_version_file.is_file()
        if state.target_version_file_exists:
            state.target_version = read_as_trimmed_string(target_version_file)

        state.target_environment_id_file_exists = scripts_dir.joinpath(
            TARGET_ENVIRONMENT_ID_FILE_NAME
        ).is_file()
        state.set_search_path_file_exists = scripts_dir.joinpath(SEARCH_PATH_FILE_NAME).is_file()

    @staticmethod
    def _is_git_repo(scripts_dir: Path) -> bool:
        current = Path(scripts_dir).resolve()
        while True:
            if current.joinpath(".git").exists():
                return True
            if current.parent == current:
                return False
            current = current.parent

    def _probe_db(self, state: State) -> None:
        try:
            with DbConnection(
                self.dbconn_settings, out=self.out, quiet=self.quiet
            ) as dbconn:
                state.schema_exists = self._query_bool(
                    dbconn,
                    "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = %s)",
                    (self.schema_name,),
                )
                if not state.schema_exists:
                    return

                state.schema_is_empty = self._query_bool(
                    dbconn,
                    """
                    SELECT NOT EXISTS (
                        SELECT 1
                        FROM pg_class c
                        JOIN pg_namespace s ON s.oid = c.relnamespace
                        WHERE s.nspname = %s
                    )
                    """,
                    (self.schema_name,),
                )

                state.control_tables_exist = all(
                    self._query_bool(
                        dbconn,
                        """
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.tables
                            WHERE table_schema = %s AND table_name = %s
                        )
                        """,
                        (self.schema_name, table_name),
                    )
                    for table_name in VERSION_CONTROL_TABLE_NAMES
                )
                if not state.control_tables_exist:
                    return

                versions_table = psycopg_sql.Identifier(
                    self.schema_name, "dbmigration_versions"
                )
                state.latest_version_installed = self._query_value(
                    dbconn,
                    psycopg_sql.SQL("SELECT MAX(version_id) FROM {}").format(versions_table),
                    (),
                )
                state.baseline_version_installed = self._query_value(
                    dbconn,
                    psycopg_sql.SQL(
                        "SELECT version_id FROM {} WHERE is_baseline IS TRUE "
                        "ORDER BY version_id DESC LIMIT 1"
                    ).format(versions_table),
                    (),
                )
        except (CommandError, psycopg.Error) as error:
            state.connection_error = str(error)
            state.schema_exists = False
            state.control_tables_exist = False

    @staticmethod
    def _query_bool(
        dbconn: DbConnection, sql: str | psycopg_sql.Composed, params: Sequence[Any]
    ) -> bool:
        return bool(StateProbe._query_value(dbconn, sql, params))

    @staticmethod
    def _query_value(
        dbconn: DbConnection, sql: str | psycopg_sql.Composed, params: Sequence[Any]
    ) -> Any:
        with dbconn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return next(iter(row), None) if row is not None else None