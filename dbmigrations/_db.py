from __future__ import annotations

import psycopg
from psycopg import Cursor
from psycopg.rows import TupleRow
from types import TracebackType
from typing import Any, Mapping, Sequence, Self

from _errors import CommandError
from _i18n import _
from _output import Output



def log_server_notices(diag, out: Output | None = None):
    (out if out is not None else Output()).print(
        _("Server: {severity} - {message_primary}")
        .format(severity=diag.severity, message_primary=diag.message_primary)
    )

class DbConnection:
    """
    Owns the lifecycle of a psycopg connection: opening, rollback, and closing.

    Used as a context manager so that the connection is released even if the
    command raises an exception. All SQL access helpers used across commands
    (cursor, transaction, get_single_value, ...) are exposed here, keeping
    database plumbing out of the command classes.
    """

    def __init__(
        self,
        dbconn_settings: dict[str, Any],
        out: Output | None = None,
        quiet: bool = False,
    ) -> None:
        self.settings = dbconn_settings
        self.out = out if out is not None else Output()
        self.quiet = quiet
        self.conn: psycopg.Connection[Any] | None = None

    def __enter__(self) -> Self:
        try:
            self.conn = psycopg.connect(**self.settings)
        except psycopg.Error as pg_error:
            error_message = str(pg_error)
            raise CommandError(
                _("Unable to establish connection to database server. Inner error: {error_message}")
                .format(error_message=error_message)
            )
        if not self.quiet:
            (self.__dict__.get('out') or Output()).print(
                _("Opened db connection: '{connection_string}'").format(
                    connection_string=self.get_connection_string(self.conn)
                )
            )
        self.conn.add_notice_handler(lambda diag: log_server_notices(diag, self.out))
        self.conn.autocommit = True
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool | None:
        if not self.quiet:
            if exc_type is not None and self.conn is not None:
                self.conn.rollback()
                (self.__dict__.get('out') or Output()).print(_("Rolled back transaction."))
            if self.conn is not None:
                self.conn.close()
                (self.__dict__.get('out') or Output()).print(_("Closed db connection."))
        else:
            if self.conn is not None:
                if exc_type is not None and not self.conn.closed:
                    self.conn.rollback()
                self.conn.close()
        return False  # propagate the exception

    def cursor(self) -> Cursor[TupleRow]:
        assert self.conn is not None, _("DB connection is not initialized yet")
        return self.conn.cursor()

    def transaction(self) -> Any:
        assert self.conn is not None, _("DB connection is not initialized yet")
        return self.conn.transaction()

    def get_single_value(
        self,
        sql: str | psycopg.sql.Composed,
        params: Sequence[Any] | Mapping[str, Any],
    ) -> Any | None:
        with self.cursor() as cur:
            cur.execute(sql, params)
            try:
                row = cur.fetchone()
            except psycopg.ProgrammingError:  # thrown in case of DDL or anonymous PL/pgSQL block
                return None
            return next(iter(row), None) if row is not None else None

    def exec_with_no_result_in_tran(
        self,
        sql: str | psycopg.sql.Composed,
        params: Sequence[Any] | Mapping[str, Any],
    ) -> None:
        with self.conn:
            with self.cursor() as cur:
                cur.execute(sql, params)

    @staticmethod
    def get_connection_string(dbconn: psycopg.Connection[Any]) -> str:
        info = dbconn.info
        host_val = getattr(info, "host", None)
        host = host_val if host_val else "[local_socket]"

        port_val = getattr(info, "port", None)
        port = f":{port_val}" if port_val else ""

        return f"{info.user}@{host}{port}/{info.dbname}"
