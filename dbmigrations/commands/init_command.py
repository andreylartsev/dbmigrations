"""InitCommand: creates control tables."""

from _confirmation import Confirmation
from _errors import CommandError
from _i18n import _
from _options import Deps, InitOptions
from _output import Output

from commands.base import BaseCommand


class InitCommand (BaseCommand):
    """Creates version control tables in an empty database schema."""

    def check_if_schema_is_empty(self) -> bool:
        sql = """
            SELECT NOT EXISTS (
                SELECT 1
                FROM pg_class c
                JOIN pg_namespace s ON s.oid = c.relnamespace
                WHERE s.nspname = %s
            )
        """
        schema_name = self.get_schema_name_arg()        
        value = self.dbconn_get_single_value(sql, (schema_name,))
        return bool(value)
    
    def create_version_tracking_tables(self, environment_id: str) -> None:
        ddl = """
            CREATE TABLE {schema_name}.dbmigration_environment_id (
                id VARCHAR(64) NOT NULL,
                is_singleton BOOL NOT NULL DEFAULT TRUE, 
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                created_by VARCHAR(64) NOT NULL DEFAULT SESSION_USER,
                created_from INET DEFAULT INET_CLIENT_ADDR(),
                CONSTRAINT dbmigration_environment_primary_key PRIMARY KEY(id),
                -- restricts insertion of any secondary records
                CONSTRAINT dbmigration_is_singleton_must_be_true CHECK (is_singleton = TRUE),
                CONSTRAINT dbmigration_table_must_contain_only_one_environment UNIQUE (is_singleton)
            );
            GRANT SELECT ON TABLE {schema_name}.dbmigration_environment_id TO PUBLIC;
                    
            CREATE TABLE {schema_name}.dbmigration_versions (
                version_id VARCHAR(64) NOT NULL,
                is_baseline BOOL NOT NULL DEFAULT FALSE,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                created_by VARCHAR(64) NOT NULL DEFAULT SESSION_USER,
                created_from INET DEFAULT INET_CLIENT_ADDR(),
                CONSTRAINT dbmigration_versions_primary_key PRIMARY KEY(version_id) 
            );
            GRANT SELECT ON TABLE {schema_name}.dbmigration_versions TO PUBLIC;

            CREATE TABLE {schema_name}.dbmigration_version_scripts (
                version_id VARCHAR(64) NOT NULL,
                relative_path VARCHAR(2048) NOT NULL,
                git_blob_sha1 VARCHAR(64) NOT NULL,
                CONSTRAINT dbmigration_version_scripts_primary_key PRIMARY KEY(version_id, relative_path),
                CONSTRAINT dbmigration_version_scripts_version_foreign_key FOREIGN KEY (version_id)
                    REFERENCES {schema_name}.dbmigration_versions (version_id)
                    ON DELETE CASCADE
            );
            GRANT SELECT ON TABLE {schema_name}.dbmigration_version_scripts TO PUBLIC;

            CREATE TABLE {schema_name}.dbmigration_repeatable_scripts (
                version_id VARCHAR(64) NOT NULL,
                relative_path VARCHAR(2048) NOT NULL,
                created_at TIMESTAMP(6) WITH TIME ZONE NOT NULL DEFAULT CLOCK_TIMESTAMP(),
                git_blob_sha1 VARCHAR(64) NOT NULL,
                created_by VARCHAR(64) NOT NULL DEFAULT SESSION_USER,
                created_from INET DEFAULT INET_CLIENT_ADDR(),
                CONSTRAINT dbmigration_repeatable_scripts_primary_key PRIMARY KEY(version_id, relative_path, created_at),
                CONSTRAINT dbmigration_repeatable_scripts_version_foreign_key FOREIGN KEY (version_id)
                    REFERENCES {schema_name}.dbmigration_versions (version_id)
                    ON DELETE CASCADE
            );
            GRANT SELECT ON TABLE {schema_name}.dbmigration_repeatable_scripts TO PUBLIC;
        """
        dml = """
            INSERT INTO {schema_name}.dbmigration_environment_id (id, is_singleton) VALUES (%s, TRUE);
        """
        schema_id = self.get_schema_name()
        with self.dbconn.transaction():        
            with self.dbconn.cursor() as cur:
                formatted_ddl = self.format_sql(ddl, schema_name=schema_id)
                cur.execute(formatted_ddl, [])
                formatted_dml = self.format_sql(dml, schema_name=schema_id)
                cur.execute(formatted_dml, (environment_id,))

    def __init__(
        self,
        opts: InitOptions,
        deps: Deps,
        out: Output | None = None,
        confirm: Confirmation | None = None,
    ) -> None:
        super().__init__(opts, deps, out, confirm)

    def _run(self) -> int:
        schema_name = self.get_schema_name_arg()
        if not self.check_if_schema_exists():
            raise CommandError(
                _("The target schema '{schema_name}' is not accessible")
                .format(schema_name=schema_name)
            )
        self.set_session_search_path(schema_name)

        force_init = self.opts.force_init
        if not self.check_if_schema_is_empty():
            if not force_init:
                raise CommandError(
                    _("The target schema '{schema_name}' must be empty")
                    .format(schema_name=schema_name)
                )
            self.check_if_all_version_control_tables_do_not_exist()
            (self.__dict__.get('out') or Output()).print(_("WARNING: Schema is not empty!"))

        environment_id = self.get_scripts_environment_id()

        (self.__dict__.get('out') or Output()).print(
            _("Creating the version control tables with environment ID: '{environment_id}'")
            .format(environment_id=environment_id)
        )
        self.create_version_tracking_tables(environment_id)
        (self.__dict__.get('out') or Output()).print(_("Created."))
        return 0
