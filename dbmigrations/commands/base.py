"""Base command abstraction shared by all subcommands."""

import collections
import pathlib
import re
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Mapping, Sequence

import psycopg

from _confirmation import Confirmation, ConsoleConfirm
from _constants import (
    BASELINE_DIR_NAME,
    DEFAULT_SEARCH_PATH,
    DEPENDS_ON_PATTERN,
    NAME_LENGTH_LIMIT,
    OPTIONS_CONFIG_GROUP,
    OPTIONS_DEFAULT_FILE_GLOB_FILTERS,
    OPTIONS_DEFAULT_FILE_READ_ENCODING,
    OPTIONS_DEFAULT_FILE_READ_ENCODING_ERRORS,
    REPEATABLE_DIR_NAME,
    SCRIPT_LIST_FILE_NAME,
    SEARCH_PATH_FILE_NAME,
    TARGET_ENVIRONMENT_ID_FILE_NAME,
    TARGET_VERSION_FILE,
    TOML_CONFIG_FILE,
    USE_TOOL_NAME_FILE_NAME,
    VERSION_CONTROL_TABLE_NAMES,
    VERSIONED_DIR_NAME,
    VERSION_CLEANUP_FILE_NAME,
)
from _db import DbConnection
from _errors import CommandError
from _i18n import _
from _migrations import MigrationCheckForOlderVersionControlTables, OwnMigration
from _options import CommonCliOptions, Deps
from _output import Output
from _scripts import read_as_trimmed_string, resolve_relative_script_path


class BaseCommand(ABC):

    _all_own_migrations: list[OwnMigration] = [
       MigrationCheckForOlderVersionControlTables() 
    ]

    def apply_all_own_migrations(self) -> int:
        applied_count = 0
        for m in self._all_own_migrations:
            if not isinstance(m, OwnMigration):
                raise CommandError(_("Not a 'Migration' object found within the migrations collection"))
            sql = m.get_sql_to_check_if_need_migration()
            formatted_sql = self.format_sql(sql, schema_name_identity=self.get_schema_name(), schema_name_str=self.opts.schema_name)
            result = self.dbconn_get_single_value(formatted_sql, [])
            if result:
                ddl = m.get_migration_ddl()
                desc = m.get_migration_desc()
                formatted_ddl = self.format_sql(ddl, schema_name_identity=self.get_schema_name(), schema_name_str=self.opts.schema_name)
                (self.__dict__.get('out') or Output()).print(f"Run migration: {desc}...", flush=True, end="")
                self.dbconn_exec_with_no_result_in_tran(formatted_ddl, [])
                (self.__dict__.get('out') or Output()).print(f"Done.")
                applied_count += 1
        return applied_count

    def check_if_all_own_migrations_are_applied(self) -> None:
        for m in self._all_own_migrations:
            if not isinstance(m, OwnMigration):
                raise CommandError(_("Not a 'Migration' object found within the migrations collection"))
            sql = m.get_sql_to_check_if_need_migration()
            formatted_sql = self.format_sql(sql, schema_name_identity=self.get_schema_name(), schema_name_str=self.opts.schema_name)
            result = self.dbconn_get_single_value(formatted_sql, [])
            if result:
                desc = m.get_migration_desc()
                raise CommandError(
                    _(
                        "Run 'update' subcommand to update version control tables within the schema. "
                        "The following migration need to be applied: {desc}"
                    ).format(desc=desc)
                )

    def get_script_dependencies(self, base_dir:Path, depth_within_base_dir:int, script_path:Path)->list[Path]:
        if not script_path.exists():
            raise CommandError(
                _("The path {script_path} does not exists.").format(script_path=script_path)
            )
        if not script_path.is_file():
            raise CommandError(
                _("The path {script_path} is not a file.").format(script_path=script_path)
            )
        start_path = pathlib.Path(base_dir)
        result_list = []
        with script_path.open("r", encoding="utf-8-sig", errors="replace") as script_file:
            lines = script_file.readlines()
            for line in lines:
                match = re.search(DEPENDS_ON_PATTERN, line)
                if match: 
                    found_match = match.group(1)
                    if found_match.startswith("@"):
                        dependency_path = resolve_relative_script_path(base_dir, depth_within_base_dir, found_match)
                        result_list.append(dependency_path)
                    else:
                        dependency_path = start_path.joinpath(found_match)
                        result_list.append(dependency_path)
        return result_list
    
    def resolve_scripts_dependencies_inner_recursive_loop(self, reversed_deps: dict[Path, list[Path]], script_to_add: Path, visited: list[Path] | None = None) -> list[Path]:
        # print(visited)
        if visited is None:
            visited = []
        if script_to_add in visited:
            cycle_path = " -> ".join([f"'{p.name}'" for p in visited]) + f" -> '{script_to_add.name}'"
            raise CommandError(
                _("Circular dependency detected! Path loop: {cycle_path}").format(cycle_path=cycle_path)
            )
        result_list = [script_to_add]
        if script_to_add in reversed_deps:
            deps = reversed_deps[script_to_add]
            for dependency in deps:
                l = self.resolve_scripts_dependencies_inner_recursive_loop(
                    reversed_deps, 
                    dependency, 
                    [*visited, script_to_add]
                )
                result_list = [*result_list, *l]
        return result_list

    def resolve_scripts_dependencies(self, base_dir:Path, depth_within_base_dir:int, orig_script_list:list[Path], changed_scripts:list[Path]) -> list[Path]:
        assert depth_within_base_dir > 0

        resolved_changed_scripts = [p.resolve() for p in changed_scripts]     
        resolved_orig_script_list = [p.resolve() for p in orig_script_list]     
        reversed_deps = collections.defaultdict(list)
        for script_path in resolved_orig_script_list:
            script_deps = self.get_script_dependencies(base_dir, depth_within_base_dir, script_path)
            for dependency in script_deps:
                resolved_dependency = dependency.resolve()
                if not resolved_dependency.exists():
                    raise CommandError(
                        _(
                            "The script '{dependency}' specified in '{script_path}' "
                            "as a dependency does not exist."
                        ).format(dependency=dependency, script_path=script_path)
                    )
                if not resolved_dependency.is_file():
                    raise CommandError(
                        _(
                            "The script '{dependency}' specified in '{script_path}' "
                            "as a dependency is not a valid file."
                        ).format(dependency=dependency, script_path=script_path)
                    )
                if resolved_dependency not in resolved_orig_script_list:
                    raise CommandError(
                        _(
                            "The script '{dependency}' (specified in '{script_path}') "
                            "was not found in '{script_list_file_name}' or in the origin scripts folder."
                        ).format(
                            dependency=dependency,
                            script_path=script_path,
                            script_list_file_name=SCRIPT_LIST_FILE_NAME,
                        )
                    )
                reversed_deps[resolved_dependency].append(script_path)
        # print(reversed_deps)     
        result_list = []
        for changed in resolved_changed_scripts:
            l = self.resolve_scripts_dependencies_inner_recursive_loop(reversed_deps, changed)
            result_list = [*result_list, *l]
        # print(result_list)
        # make the list unique
        result_list = list(dict.fromkeys(result_list)) 
        return result_list

    def get_sorted_scripts_from_dir(self, base_dir: Path, depth_within_base_dir: int, force_run_cleanup: bool = False, recursion_depth: int = 0) -> list[Path]:
        MAX_RECURSION_DEPTH = 25
        if recursion_depth > MAX_RECURSION_DEPTH:
            raise CommandError(
                _(
                    "Maximum recursion depth ({recursion_depth}) exceeded at '{base_dir}' "
                    "due to circular path references."
                ).format(recursion_depth=recursion_depth, base_dir=base_dir)
            )
        start_path = Path(base_dir) 
        if not start_path.exists():
            raise CommandError(
                _("The folder '{base_dir}' does not exists").format(base_dir=base_dir)
            )
        if not start_path.is_dir():
            raise CommandError(
                _("The path '{base_dir}' is not a directory").format(base_dir=base_dir)
            )                
        script_list_file_path = start_path.joinpath(SCRIPT_LIST_FILE_NAME)
        sorted_files = []
        if script_list_file_path.exists():
            with script_list_file_path.open("r", encoding=self.file_read_encoding, errors=self.file_read_encoding_errors) as script_list_file:
                lines = script_list_file.readlines()
                for line in lines:
                    trimmed_str = line.strip()
                    if len(trimmed_str) == 0 or trimmed_str.startswith("#"):
                        continue              
                    if trimmed_str.startswith("!"):
                        (self.__dict__.get('out') or Output()).print(f"Skip: {trimmed_str}")
                        continue
                    if trimmed_str.startswith("@"):
                        script_path = resolve_relative_script_path(start_path, depth_within_base_dir, trimmed_str)
                    else:
                        script_path = start_path.joinpath(trimmed_str)
                    script_name = script_path.name
                    if script_name == '*':
                        new_base_path = script_path.parent
                        scripts_to_add = self.get_sorted_scripts_from_dir(new_base_path, depth_within_base_dir, force_run_cleanup, recursion_depth + 1)
                        sorted_files = [*sorted_files, *scripts_to_add]
                    else:
                        if force_run_cleanup:
                            if len(sorted_files) == 0 and (not script_name == VERSION_CLEANUP_FILE_NAME):
                                raise CommandError(
                                    _(
                                        "The list of scripts '{script_list_file_path}' must start with "
                                        "'{version_cleanup_file_name}', but '{script_name}' was given."
                                    ).format(
                                        script_list_file_path=script_list_file_path,
                                        version_cleanup_file_name=VERSION_CLEANUP_FILE_NAME,
                                        script_name=script_name,
                                    )
                                )
                        else:
                            if (script_name == VERSION_CLEANUP_FILE_NAME):
                                continue
                        if not script_path.exists():
                            raise CommandError(
                                _(
                                    "The file '{trimmed_str}' specified in script list file "
                                    "'{script_list_file_path}' does not exists"
                                ).format(trimmed_str=trimmed_str, script_list_file_path=script_list_file_path)
                            )
                        if not script_path.is_file():
                            raise CommandError(
                                _(
                                    "The file '{trimmed_str}' specified in script list file "
                                    "'{script_list_file_path}' is not a file"
                                ).format(trimmed_str=trimmed_str, script_list_file_path=script_list_file_path)
                            )
                        sorted_files.append(script_path)
        else:
            all_files = []
            exclusions = [
                USE_TOOL_NAME_FILE_NAME, 
                TARGET_VERSION_FILE,
                VERSION_CLEANUP_FILE_NAME]
            exclusions_set = set(exclusions)
            for glob_filter in self.file_glob_filters:
                all_items = start_path.rglob(glob_filter)
                for item in all_items: 
                    if item.is_file() and not item.name in exclusions_set:
                        all_files.append(item)
            sorted_files = sorted(all_files)
            if force_run_cleanup:
                cleanup_file_path = start_path.joinpath(VERSION_CLEANUP_FILE_NAME)
                if not cleanup_file_path.exists():
                    raise CommandError(
                        _("The file '{cleanup_file_path}' does not exists").format(cleanup_file_path=cleanup_file_path)
                    )
                if not cleanup_file_path.is_file():
                    raise CommandError(
                        _("The path '{cleanup_file_path}' is not a file").format(cleanup_file_path=cleanup_file_path)
                    )
                sorted_files.insert(0, cleanup_file_path)
        return sorted_files

    def format_sql_comment(self, comment : str) -> str:
        """
        PostgreSQL's format() function ignores lines that start with a comment.
        This can lead to SQL injection vulnerability if a formatted placeholder 
        contains a newline character, for example:
        'BAD VERSION\n;TRUNCATE dbmigration_environment_id CASCADE; --'
        """
        result_str = comment.replace("\n", " ").replace("\r", "").strip() + "\n"
        if not result_str.startswith("--"):
            result_str = f"-- {result_str}"
        return result_str

    def format_sql_text(self, sql : str, **params) -> str:
        if self.dbconn is None or self.dbconn.conn is None:
            raise CommandError(_("DB connection is not initialized yet"))
        composed_query = psycopg.sql.SQL(sql).format(**params)
        result_str = composed_query.as_string(self.dbconn.conn)
        return result_str        

    def format_sql(self, sql: str, **params) -> psycopg.sql.Composed:
        result_query = psycopg.sql.SQL(sql).format(**params)
        return result_query

    def dbconn_get_single_value(
        self, 
        sql : str | psycopg.sql.Composed, 
        params : Sequence[Any] | Mapping[str, Any]
    ) -> Any | None:
        return self.dbconn.get_single_value(sql, params)
        
    def dbconn_exec_with_no_result_in_tran(
        self, 
        sql: str | psycopg.sql.Composed, 
        params: Sequence[Any] | Mapping[str, Any]
    ) -> None:
        self.dbconn.exec_with_no_result_in_tran(sql, params)

    def dbconn_get_connection_string(self, dbconn: psycopg.Connection[Any]) -> str:
        return DbConnection.get_connection_string(dbconn)
    
    def get_schema_name_arg(self) -> str:
        schema_name = self.opts.schema_name
        if not schema_name:
            raise CommandError(_("The attribute opts.schema_name must not be empty"))
        return schema_name

    def get_schema_name(self) -> psycopg.sql.Identifier:
        schema_name = self.get_schema_name_arg()        
        return psycopg.sql.Identifier(schema_name)

    def check_if_schema_exists(self) -> bool:
        schema_name = self.get_schema_name_arg()
        sql = """
            SELECT EXISTS (
                SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = %s)"""
        value = self.dbconn_get_single_value(sql, (schema_name,))
        return bool(value)

    def get_scripts_path_arg(self) -> Path:
        if not self.opts.scripts_path:
            raise CommandError(_("The path specified by 'scripts_path' must not be an empty string"))
            
        scripts_path = Path(self.opts.scripts_path)
        if not scripts_path.exists():
            raise CommandError(
                _("The path specified by 'scripts_path' argument does not exist: {scripts_path}")
                .format(scripts_path=str(scripts_path))
            )
        if not scripts_path.is_dir():
            raise CommandError(
                _("The path specified by 'scripts_path' argument is not a valid directory: {scripts_path}")
                .format(scripts_path=str(scripts_path))
            )        
        return scripts_path

    def get_resolved_scripts_dir(self) -> Path:
        return self.get_scripts_path_arg().resolve()
    
    def get_scripts_environment_id(self) -> str:            
        scripts_path = self.get_scripts_path_arg()
            
        target_environment_id_file_name = scripts_path.joinpath(TARGET_ENVIRONMENT_ID_FILE_NAME)
        
        if target_environment_id_file_name.exists():
            environment_id = read_as_trimmed_string(target_environment_id_file_name)
            if not environment_id:
                raise CommandError(_("The environment ID must not be an empty string"))
            if len(environment_id) > NAME_LENGTH_LIMIT:
                raise CommandError(
                    _(
                        "The length of the environment ID taken from '{target_environment_id_file_name}' "
                        "exceeds the limit: {name_length_limit}"
                    ).format(
                        target_environment_id_file_name=target_environment_id_file_name,
                        name_length_limit=NAME_LENGTH_LIMIT,
                    )
                )
        else: 
            # Considering directory name as environment ID
            environment_id = scripts_path.resolve().name
            if not environment_id:
                raise CommandError(_("The environment ID must not be an empty string"))
            if len(environment_id) > NAME_LENGTH_LIMIT:
                raise CommandError(
                    _(
                        "The length of the directory name specified by 'scripts_path' argument "
                        "exceeds the limit: {name_length_limit}"
                    ).format(name_length_limit=NAME_LENGTH_LIMIT)
                )

        return environment_id

    def get_stored_environment_id(self) -> str:
        schema_id = self.get_schema_name()
        sql = """
                SELECT id FROM {schema_name_identity}.dbmigration_environment_id ORDER BY created_at ASC LIMIT 1"""        
        formatted_sql = self.format_sql(sql, schema_name_identity=schema_id) 
        value = self.dbconn_get_single_value(formatted_sql, [])
        if value is None:
            raise CommandError(_("Schema consistency check failed: environment ID not found in table 'dbmigration_environment_id'."))           
        return value
    
    def get_search_path_for_scripts(self) -> str:            
        scripts_path = self.get_scripts_path_arg()   
        set_search_path_file = scripts_path.joinpath(SEARCH_PATH_FILE_NAME)
        if not set_search_path_file.exists():
            return self.get_schema_name_arg()
        if not set_search_path_file.is_file():
            raise CommandError(
                _(
                    "The search path file '{search_path_file_name}' "
                    "within scripts directory '{scripts_path}' is not a valid file"
                ).format(search_path_file_name=SEARCH_PATH_FILE_NAME, scripts_path=scripts_path)
            )
        trimmed_str = read_as_trimmed_string(set_search_path_file)
        return trimmed_str
    
    def set_session_search_path(self, search_path : str) -> None:
        (self.__dict__.get('out') or Output()).print(_("Set session search path to: '{search_path}'.").format(search_path=search_path))
        sql = f"""
            SELECT pg_catalog.set_config('search_path', %s, false)"""
        result = self.dbconn_get_single_value(sql, (search_path,))
        if result != search_path:
            raise CommandError(
                _("Unexpected value '{result}' returned on attempt to set the search path").format(result=result)
            )

    def check_if_table_exists(self, table_name : str) -> bool:
        schema_name = self.get_schema_name_arg()
        sql = """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s
            );
        """
        value = self.dbconn_get_single_value(sql, (schema_name, table_name))
        return bool(value)
    
    def check_if_version_table_include_baseline_version(self) -> bool:
        schema_id = self.get_schema_name()
        sql = """
            SELECT EXISTS (
                SELECT 1
                FROM {schema_name}.dbmigration_versions
                WHERE is_baseline IS TRUE
            );
        """
        formatted_sql = self.format_sql(sql, schema_name=schema_id)
        value = self.dbconn_get_single_value(formatted_sql, [])
        return bool(value)
    
    def get_latest_version_installed(self) -> str|None:
        schema_id = self.get_schema_name()
        sql = """
            SELECT MAX(version_id) FROM {schema_identity}.dbmigration_versions"""
        formatted_sql = self.format_sql(sql, schema_identity=schema_id)
        value = self.dbconn_get_single_value(formatted_sql, [])
        return value

    def check_if_any_latest_version_installed(self) -> str:
        value = self.get_latest_version_installed()
        if value is None:
            raise CommandError(_("Unable to get latest installed version"))
        return value

    def check_if_repeatable_script_installed(self, git_blob_sha1: str, version: str, relative_path: str) -> bool:
        sql = """
            SELECT EXISTS (
                SELECT 1 FROM (
                    SELECT git_blob_sha1 
                    FROM {schema_name}.dbmigration_repeatable_scripts
                    WHERE relative_path = %s 
                      AND version_id = %s
                    ORDER BY created_at DESC
                    LIMIT 1
                ) latest
                WHERE latest.git_blob_sha1 = %s
            );
        """                
        formatted_sql = self.format_sql(sql, schema_name=self.get_schema_name())        
        params = (relative_path, version, git_blob_sha1)        
        value = self.dbconn_get_single_value(formatted_sql, params)
        return bool(value)


    def check_if_max_version_of_versioned_scripts_matches_repeatable_target(self) -> None:
        scripts_dir = self.get_resolved_scripts_dir()
        (self.__dict__.get('out') or Output()).print(_("Performing a cross-check for consistency between the target version's repeatable scripts and the versioned scripts..."))
        latest_version_in_baseline = None
        baseline_dir = scripts_dir.joinpath(BASELINE_DIR_NAME)
        if baseline_dir.exists():
            baseline_subdirs = [item.name for item in baseline_dir.iterdir() if item.is_dir()]
            baseline_subdirs_len = len(baseline_subdirs)
            if baseline_subdirs_len != 1:
                raise CommandError(
                    _(
                        "The baseline directory must include exactly one subdirectory with version scripts, "
                        "but {baseline_subdirs_len} present."
                    ).format(baseline_subdirs_len=baseline_subdirs_len)
                )
            latest_version_in_baseline = baseline_subdirs[0]

        latest_version_in_versioned = None
        versioned_dir = scripts_dir.joinpath(VERSIONED_DIR_NAME)
        if versioned_dir.exists():
            latest_version_in_versioned = max((item.name for item in versioned_dir.iterdir() if item.is_dir()), default=None)

        if latest_version_in_versioned and latest_version_in_baseline and latest_version_in_versioned <= latest_version_in_baseline:
            raise CommandError(
                _(
                    "The latest version of the subdirectory with the versions '{latest_version_in_versioned}' "
                    "must be greater than the version of the baseline scripts '{latest_version_in_baseline}'."
                ).format(
                    latest_version_in_versioned=latest_version_in_versioned,
                    latest_version_in_baseline=latest_version_in_baseline
                )
            )
    
        latest_version_in_scripts = max(
            filter(None, [latest_version_in_versioned, latest_version_in_baseline]), default=None)

        if latest_version_in_scripts is None:
            (self.__dict__.get('out') or Output()).print(
                _("No baseline or versioned scripts found in scripts directory: '{scripts_dir}'").format(
                    scripts_dir=scripts_dir
                )
            )
            return
                
        target_version_in_repeatable = None
        repeatable_dir = scripts_dir.joinpath(REPEATABLE_DIR_NAME)
        if repeatable_dir.exists():
            target_version_file_path = repeatable_dir.joinpath(TARGET_VERSION_FILE)
            if target_version_file_path.exists():
                target_version_in_repeatable = read_as_trimmed_string(target_version_file_path)

        if target_version_in_repeatable is None:
            (self.__dict__.get('out') or Output()).print(
                _("No repeatable scripts found in scripts directory: '{scripts_dir}'").format(
                    scripts_dir=scripts_dir
                )
            )
            return 

        if latest_version_in_scripts != target_version_in_repeatable:
            raise CommandError(
                _(
                    "The target version for repeatable scripts '{target_version_in_repeatable}' "
                    "does not match the latest version in versioned scripts '{latest_version_in_scripts}'"
                ).format(
                    target_version_in_repeatable=target_version_in_repeatable,
                    latest_version_in_scripts=latest_version_in_scripts
                )
            )
        
        (self.__dict__.get('out') or Output()).print(_("Completed."))

    def do_initial_cross_checks(self) -> None:
        if not self.check_if_schema_exists():
            raise CommandError(
                _("The target schema '{schema_name}' is not accessible").format(
                    schema_name=self.opts.schema_name
                )
            )
        search_path = self.get_search_path_for_scripts()
        if search_path != DEFAULT_SEARCH_PATH:
            self.set_session_search_path(search_path)
        else:
            (self.__dict__.get('out') or Output()).print(_("Use the default users search path"))

    def check_if_stored_environment_id_matches_to_scripts_dir(self) -> None:
        stored_environment_id = self.get_stored_environment_id()
        scripts_environment_id = self.get_scripts_environment_id()
        if stored_environment_id != scripts_environment_id:
            scripts_path = self.get_scripts_path_arg()
            raise CommandError(
                _(
                    "The stored environment ID '{stored_environment_id}' in the target schema "
                    "does not match the environment ID of the scripts directory '{scripts_path}'"
                ).format(
                    stored_environment_id=stored_environment_id,
                    scripts_path=scripts_path
                )
            )
        (self.__dict__.get('out') or Output()).print(
            _("Target schema environment ID matches the scripts directory ID: {stored_environment_id}").format(
                stored_environment_id=stored_environment_id
            )
        )

    _required_version_control_tables = VERSION_CONTROL_TABLE_NAMES

    def check_if_all_version_control_tables_exist(self) -> None:
        schema_name = self.get_schema_name_arg()    
        for table_name in self._required_version_control_tables:
            if not self.check_if_table_exists(table_name):
                raise CommandError(
                    _("The schema '{schema_name}' is missing the version control table '{table_name}'").format(
                        schema_name=schema_name,
                        table_name=table_name
                    )
                )
            
    def check_if_all_version_control_tables_do_not_exist(self) -> None:
        schema_name = self.get_schema_name_arg()    
        for table_name in self._required_version_control_tables:
            if self.check_if_table_exists(table_name):
                raise CommandError(
                    _("The schema '{schema_name}' already contains the version control table '{table_name}'").format(
                        schema_name=schema_name,
                        table_name=table_name
                    )
                )

    def __init__(
        self,
        opts: CommonCliOptions,
        deps: Deps,
        out: Output | None = None,
        confirm: Confirmation | None = None,
    ) -> None:
        self.opts = opts
        self.deps = deps
        self.out = out if out is not None else Output()
        self.confirm = confirm if confirm is not None else ConsoleConfirm(self.out)
        self.config = deps.config
        self.dbconn_settings = deps.db_settings
        self.dbconn: DbConnection | None = None
        self.git = None

        if OPTIONS_CONFIG_GROUP not in self.config:
            raise CommandError(
                _(
                    "Missing configuration group '{config_group}' "
                    "in configuration file '{config_file}'."
                ).format(config_group=OPTIONS_CONFIG_GROUP, config_file=TOML_CONFIG_FILE)
            )
        self.options = self.config[OPTIONS_CONFIG_GROUP]

        self.file_read_encoding = self.options.get("file_read_encoding", OPTIONS_DEFAULT_FILE_READ_ENCODING)
        self.file_read_encoding_errors = self.options.get("file_read_encoding_errors", OPTIONS_DEFAULT_FILE_READ_ENCODING_ERRORS)
        self.file_glob_filters = self.options.get("file_glob_filters", OPTIONS_DEFAULT_FILE_GLOB_FILTERS)

    def run(self, db: DbConnection) -> int:
        """Binds the database connection and delegates to the command implementation."""
        self.dbconn = db
        return self._run()

    @abstractmethod
    def _run(self) -> int:
        """Main command pipeline. Returns the process exit code."""
        pass
