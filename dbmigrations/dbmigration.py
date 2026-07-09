"""
Simple database migrations tool
"""

#
# prerequire packages listed in requirements.txt
# 

import sys
import argparse
import tomllib
import os
import pathlib 
import hashlib
import psycopg
import subprocess
import copy
import re
import collections
from abc import ABC, abstractmethod
import shutil

TOML_CONFIG_FILE = 'dbmigration.toml'
OPTIONS_CONFIG_GROUP = "options"

GIT_CMD_CONFIG_ATTRIBUTE = "git_cmd_path"

DBENVS_CONFIG_GROUP = "dbenvs"
DEFAULT_DBENV_CONFIG_ATTRIBUTE = "default_dbenv"

RUN_TESTS_BY_ATTRIBUTE = "run_tests_by"
DBCONN_CONFIG_USER_ATTRIBUTE = "user"
NO_PASSWORD_ATTRIBUTE = "no_password"

OPTIONS_DEFAULT_FILE_GLOB_FILTERS = ["*.sql", "*.dump"]
OPTIONS_DEFAULT_FILE_READ_ENCODING = "utf-8"
OPTIONS_DEFAULT_FILE_READ_ENCODING_ERRORS = "ignore"

DBCONN_USER_PASSWORD_ENVVAR_NAME = "USER_PASSWORD"
DBCONN_TESTER_PASSWORD_ENVVAR_NAME = "TESTER_PASSWORD"

BASELINE_DIR_NAME = "baseline"
VERSIONED_DIR_NAME = "versions"
REPEATABLE_DIR_NAME = "repeatable"
TESTS_DIR_NAME = "tests"
TARGET_VERSION_FILE = "target_version.txt"
SCRIPT_LIST_FILE_NAME = "script_list.txt"

TOOLS_CONFIG_GROUP = "tools"
TOOL_EXEC_ATTRIBUTE = "executable"
TOOL_ARGS_ATTRIBUTE = "args"
TOOL_SUCCESS_RESULT_CODE_ATTRIBUTE = "success_result_code"
USE_TOOL_NAME_FILE_NAME = "use_tool.txt"

VERSION_CLEANUP_FILE_NAME = "_cleanup.sql"

SEARCH_PATH_FILE_NAME = "set_search_path.txt"
DEFAULT_SEARCH_PATH = ":default"

TARGET_ENVIRONMENT_ID_FILE_NAME = "target_environment_id.txt"

BASELINE_FILES_DEPTH = 2
VERSIONED_FILES_DEPTH = 2
REPEATABLE_FILES_DEPTH = 1
TESTS_FILES_DEPTH = 1

NAME_LENGTH_LIMIT=64

DEPENDS_ON_PATTERN = r'(?<=@depends_on)\s*(\S+)'

IS_TRUE_THAT_TEST_PREFIX = "is_true_that_"
DETECT_MISSING_TEST_PREFIX = "detect_missing_"
ASSURE_THAT_TEST_PREFIX = "assure_that_"

SETUP_TESTS_FILE_NAME = "_setup.sql"

UNCOMMITTED_SHA_LABEL = "UNCOMMITTED"
UNCOMMITTED_AUTHOR_LABEL = "Local Changes"
UNCOMMITTED_DATE_LABEL = "-------"


class CommandError(Exception):
    """A critical command error terminated the command execution."""
    def __init__(self, message):
        super().__init__(message)

def get_git_blob_sha1_for_bytes(script_bytes):
    content = script_bytes.replace(b'\r\n', b'\n')
    header = f"blob {len(content)}\x00".encode('utf-8')
    sha1 = hashlib.sha1()
    sha1.update(header)
    sha1.update(content)
    return sha1.hexdigest()

def read_as_trimmed_string(file_path):
    with open(file_path, 'rb') as f:
        for binary_line in f:
            decoded_str = binary_line.decode("utf-8-sig", "ignore")
            trimmed_str = decoded_str.strip()
            if trimmed_str:
                return trimmed_str
    raise CommandError(f"The file '{file_path}' must not be empty")

def log_server_notices(diag):
    print(f"Server: {diag.severity} - {diag.message_primary}")

import sys

def get_char():
    if sys.platform == "win32":
        import msvcrt
        char = msvcrt.getche().decode("utf-8", errors="ignore")
        print()
        return char
    else:
        import termios
        import tty
        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        try:
            tty.setcbreak(fd)
            char = sys.stdin.read(1)
            print(char) 
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
        return char

class ExternalTool:
    def make_variables_dict_from_config_and_script_path(self, script_path):
        result = {}
        for key, value in self.dbconn_config.items():
            variable_key = "${" + key.strip() + "}"  
            result[variable_key] = value
        result["${file}"] = script_path
        result["${schema_name}"] = self.schema_name
        return result

    def match_variables_to_args(self, variables, args):
        result = []
        for arg in args:
            variable_key = arg.strip() 
            if variable_key in variables:
                value_str = str(variables[variable_key])
                result.append(value_str)
            else:
                result.append(arg)
        return result

    def __init__(self, tool_name, schema_name, dbconn_config, toml_config):
        self.tool_name = tool_name
        self.schema_name = schema_name
        self.dbconn_config = dbconn_config

        if not TOOLS_CONFIG_GROUP in toml_config:
            raise CommandError(f"There is no configuration group '{TOOLS_CONFIG_GROUP}' in the configuration file '{TOML_CONFIG_FILE}'.")
        tools_config = toml_config[TOOLS_CONFIG_GROUP]
        
        if not tool_name in tools_config:
            raise CommandError(f"Unable find the specified external tool name '{tool_name}' in configuration group '{TOOLS_CONFIG_GROUP}'.")
        tool_config = tools_config[tool_name]

        if not TOOL_EXEC_ATTRIBUTE in tool_config:
            raise CommandError(f"There is no attribute '{TOOL_EXEC_ATTRIBUTE}' in the tool configuration '{tool_name}'.")
        exec_attribute = tool_config[TOOL_EXEC_ATTRIBUTE]
        exec_path = pathlib.Path(exec_attribute)
        if not exec_path.exists():
            raise CommandError(f"There path specified by attribute '{TOOL_EXEC_ATTRIBUTE}' in the tool configuration '{tool_name}' does not exists.")
        if not exec_path.is_file():
            raise CommandError(f"There path specified by attribute '{TOOL_EXEC_ATTRIBUTE}' in the tool configuration '{tool_name}' is not a file.")
        self.exec_path = exec_path

        if not TOOL_ARGS_ATTRIBUTE in tool_config:
            raise CommandError(f"There is no attribute '{TOOL_ARGS_ATTRIBUTE}' in the tool configuration '{tool_name}'.")
        self.args = tool_config[TOOL_ARGS_ATTRIBUTE]

        if not TOOL_SUCCESS_RESULT_CODE_ATTRIBUTE in tool_config:
            raise CommandError(f"There is no attribute '{TOOL_SUCCESS_RESULT_CODE_ATTRIBUTE}' in the tool configuration '{tool_name}'.")
        self.success_result_code = tool_config[TOOL_SUCCESS_RESULT_CODE_ATTRIBUTE]

    def run(self, script_path):
        tool_absolute_path = self.exec_path.absolute()
        tool_args = self.args
        variables = self.make_variables_dict_from_config_and_script_path(script_path)
        tool_args_with_matched_variables = self.match_variables_to_args(variables, tool_args)
        command_line = [str(tool_absolute_path), *tool_args_with_matched_variables]
        process = subprocess.Popen(
            args=command_line, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.STDOUT, 
            text=True,
            encoding='utf-8')
        # Read and print output line by line as it happens
        for line in iter(process.stdout.readline, ''):
            print(line, end='') 

        remaining_stdout, _ = process.communicate()
        if remaining_stdout:
            print(remaining_stdout, end='')

        result_code = process.returncode 

        if result_code != self.success_result_code:
            raise CommandError(f"The tool '{self.tool_name}' returned unsuccessful result code {result_code}!")

class OwnMigration(ABC):    
    @abstractmethod
    def get_sql_to_check_if_need_migration(self):
        pass
    @abstractmethod
    def get_migration_ddl(self):
        pass
    @abstractmethod
    def get_migration_desc(self):
        pass

class MigrationCheckForOlderVersionControlTables (OwnMigration):
    def get_sql_to_check_if_need_migration(self):
        sql = """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = {schema_name_str} AND table_name = 'dbmigration_versions'
            ) AND NOT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = {schema_name_str} AND table_name = 'dbmigration_version_scripts'
            ) AS conditions_met;     
        """
        return sql
    def get_migration_ddl(self):
        raise CommandError(f"This version of dbmigration tools is incompatible with this schema.\n" 
                            "Please use the previous version available by tag 0.9.x or upgrade the current schema by deleting dbmigration_version and dbmigration_repeatable tables and running the update subcommand with --force-run-cleanup flag: \n" 
                            "i.e. dbmigration.py update <schema_name> <scripts_folder> --force-run-cleanup")
                           
    def get_migration_desc(self):
        desc = "Check for older version control tables"
        return desc

class BaseCommand:

    all_own_migrations: list[OwnMigration]

    def apply_all_own_migrations(self):
        applied_count = 0
        for m in self.all_own_migrations:
            if not isinstance(m, OwnMigration):
                raise CommandError(f"Not a 'Migration' object found within the migrations collection")
            sql = m.get_sql_to_check_if_need_migration()
            formatted_sql = self.format_sql(sql, schema_name_identity=self.get_schema_name(), schema_name_str=self.args.schema_name)
            result = self.dbconn_get_single_value(formatted_sql, [])
            if result:
                ddl = m.get_migration_ddl()
                desc = m.get_migration_desc()
                formatted_ddl = self.format_sql(ddl, schema_name_identity=self.get_schema_name(), schema_name_str=self.args.schema_name)
                print(f"Run migration: {desc}...", flush=True, end="")
                self.dbconn_exec_with_no_result_in_tran(formatted_ddl, [])
                print(f"Done.")
                applied_count += 1
        return applied_count

    def check_if_all_own_migrations_are_applied(self):
        for m in self.all_own_migrations:
            if not isinstance(m, OwnMigration):
                raise CommandError(f"Not a 'Migration' object found within the migrations collection")
            sql = m.get_sql_to_check_if_need_migration()
            formatted_sql = self.format_sql(sql, schema_name_identity=self.get_schema_name(), schema_name_str=self.args.schema_name)
            result = self.dbconn_get_single_value(formatted_sql, [])
            if result:
                desc = m.get_migration_desc()
                raise CommandError(f"Run 'update' subcommand to update version control tables within the schema. The following migration need to be applied: {desc}")

    def get_default_dbenv(self, toml_config):
        if not DEFAULT_DBENV_CONFIG_ATTRIBUTE in toml_config:
            raise CommandError(f"There is no '{DEFAULT_DBENV_CONFIG_ATTRIBUTE}' within the configuration file '{TOML_CONFIG_FILE}'.")
        default_dbenv = toml_config[DEFAULT_DBENV_CONFIG_ATTRIBUTE]
        return default_dbenv

    def get_dbenv_config(self, toml_config, dbenv_param):
        if not DBENVS_CONFIG_GROUP in toml_config:
            raise CommandError(f"There is no configuration group'{DBENVS_CONFIG_GROUP}' within the configuration file '{TOML_CONFIG_FILE}'.")
        dbenvs_config = toml_config[DBENVS_CONFIG_GROUP]
        if not dbenv_param in dbenvs_config:
            raise CommandError(f"There is no configuration group '{DBENVS_CONFIG_GROUP}.{dbenv_param}' within the configuration file '{TOML_CONFIG_FILE}'.")
        config = copy.deepcopy(dbenvs_config[dbenv_param])
        run_tests_by = config.pop(RUN_TESTS_BY_ATTRIBUTE, None)
        no_password = config.pop(NO_PASSWORD_ATTRIBUTE, False)
        return config, run_tests_by, no_password

    def try_get_external_tool_name(self, dir):
        start_path = pathlib.Path(dir) 
        if not start_path.exists():
            raise CommandError(f"The folder '{dir}' does not exists")
        if not start_path.is_dir():
            raise CommandError(f"The path '{dir}' is not a directory")        
        use_tool_file_name = start_path.joinpath(USE_TOOL_NAME_FILE_NAME)
        if not use_tool_file_name.exists():
            return None
        tool_name = read_as_trimmed_string(use_tool_file_name)
        if not TOOLS_CONFIG_GROUP in self.config:
            raise CommandError(f"There is no configuration group '{TOOLS_CONFIG_GROUP}' in the configuration file '{TOML_CONFIG_FILE}'.")
        tools_config = self.config[TOOLS_CONFIG_GROUP]        
        if not tool_name in tools_config:
            raise CommandError(f"Unable find the specified external tool name '{tool_name}' in configuration group '{TOOLS_CONFIG_GROUP}'.")
        return tool_name
    
    def script_path_for_log(self, scripts_dir, script_path):
        dir = pathlib.Path(scripts_dir).parent.resolve()
        file = pathlib.Path(script_path).resolve()
        if file.is_relative_to(dir):
            result = file.relative_to(dir)
        else:
            result = script_path
        return result.as_posix()

    def resolve_relative_script_path(self, start_path, depth_within_base_dir, path_str):
        if not path_str.startswith("@"):
            raise CommandError(f"The relative environment path must start with @ symbol, but '{path_str}' was found")
        start = path_str.find("@") + 1
        end = path_str.find(os.path.sep, start)
        if end == -1:
            end = path_str.find("/", start)
            if end == -1:
                script_list_file_path = start_path.joinpath(SCRIPT_LIST_FILE_NAME)
                raise CommandError(f"No path separator found after environment name in path '{path_str}' specified in file '{script_list_file_path}'.")
        env_name = path_str[start:end]
        script_sub_path = path_str[end + 1:]
        result = start_path
        # walk back
        for i in range(depth_within_base_dir + 1):
            result = result.joinpath("..")
        # add a referencing env name
        result = result.joinpath(env_name)
        # walk forward
        last_parts = start_path.parts[-depth_within_base_dir:]
        for part in last_parts:
            result = result.joinpath(part)
        # add extra path specified after env name
        result = result.joinpath(script_sub_path)
        return result

    def get_script_dependencies(self, base_dir, depth_within_base_dir, script_path):
        if not script_path.exists():
            raise CommandError(f"The path {script_path} does not exists.")
        if not script_path.is_file():
            raise CommandError(f"The path {script_path} is not a file.")
        start_path = pathlib.Path(base_dir)
        result_list = []
        with script_path.open("r", encoding="utf-8-sig", errors="ignore") as script_file:
            lines = script_file.readlines()
            for line in lines:
                match = re.search(DEPENDS_ON_PATTERN, line)
                if match: 
                    found_match = match.group(1)
                    if found_match.startswith("@"):
                        dependency_path = self.resolve_relative_script_path(base_dir, depth_within_base_dir, found_match)
                        result_list.append(dependency_path)
                    else:
                        dependency_path = start_path.joinpath(found_match)
                        result_list.append(dependency_path)
        return result_list
    
    def resolve_scripts_dependencies_inner_recursive_loop(self, reversed_deps, script_to_add, visited=None):
        # print(visited)
        if visited is None:
            visited = []
        if script_to_add in visited:
            cycle_path = " -> ".join([f"'{p.name}'" for p in visited]) + f" -> '{script_to_add.name}'"
            raise CommandError(f"Circular dependency detected! Path loop: {cycle_path}")
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

    def resolve_scripts_dependencies(self, base_dir, depth_within_base_dir, orig_script_list, changed_scripts):
        resolved_changed_scripts = [p.resolve() for p in changed_scripts]     
        resolved_orig_script_list = [p.resolve() for p in orig_script_list]     
        reversed_deps = collections.defaultdict(list)
        for script_path in resolved_orig_script_list:
            script_deps = self.get_script_dependencies(base_dir, depth_within_base_dir, script_path)
            for dependency in script_deps:
                resolved_dependency = dependency.resolve()
                if not resolved_dependency.exists():
                    raise CommandError(f"The script '{dependency}' specified in '{script_path}' as a dependency does not exist.")
                if not resolved_dependency.is_file():
                    raise CommandError(f"The script '{dependency}' specified in '{script_path}' as a dependency is not a valid file.")
                if not resolved_dependency in resolved_orig_script_list:
                    raise CommandError(f"The script '{dependency}' (specified in '{script_path}') was not found in '{SCRIPT_LIST_FILE_NAME}' or in the origin scripts folder.")
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

    def get_sorted_scripts_from_dir(self, base_dir, depth_within_base_dir, force_run_cleanup = False, recursion_depth=0):
        MAX_RECURSION_DEPTH = 25
        if recursion_depth > MAX_RECURSION_DEPTH:
            raise CommandError(f"Maximum recursion depth ({recursion_depth}) exceeded at '{base_dir}' due to circular path references.")
        start_path = pathlib.Path(base_dir) 
        if not start_path.exists():
            raise CommandError(f"The folder '{base_dir}' does not exists")
        if not start_path.is_dir():
            raise CommandError(f"The path '{base_dir}' is not a directory")        
        script_list_file_path = start_path.joinpath(SCRIPT_LIST_FILE_NAME)
        sorted_files = []
        if script_list_file_path.exists():
            with script_list_file_path.open("r", encoding="utf-8-sig", errors="ignore") as script_list_file:
                lines = script_list_file.readlines()
                for line in lines:
                    trimmed_str = line.strip()
                    if len(trimmed_str) == 0 or trimmed_str.startswith("#"):
                        continue              
                    if trimmed_str.startswith("!"):
                        print(f"Skip: {trimmed_str}")
                        continue
                    if trimmed_str.startswith("@"):
                        script_path = self.resolve_relative_script_path(base_dir, depth_within_base_dir, trimmed_str)
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
                                raise CommandError(f"The list of scripts '{script_list_file_path}' must start with '{VERSION_CLEANUP_FILE_NAME}', but '{script_name}' was given.")
                        else:
                            if (script_name == VERSION_CLEANUP_FILE_NAME):
                                continue
                        if not script_path.exists():
                            raise CommandError(f"The file '{trimmed_str}' specified in script list file '{script_list_file_path}' does not exists") 
                        if not script_path.is_file():
                            raise CommandError(f"The file '{trimmed_str}' specified in script list file '{script_list_file_path}' is not a file")
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
                    raise CommandError(f"The file '{cleanup_file_path}' does not exists")
                if not cleanup_file_path.is_file():
                    raise CommandError(f"The path '{cleanup_file_path}' is not a file")
                sorted_files.insert(0, cleanup_file_path)
        return sorted_files

    def format_sql(self, sql, **params):
        if self.dbconn is None:
            raise CommandError(f"DB connection is not initialized yet")
        formatted_sql = psycopg.sql.SQL(sql).format(**params)
        formatted_text = formatted_sql.as_string(self.dbconn)
        return formatted_text        

    def dbconn_get_single_value(self, sql, params):
        with self.dbconn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return row[0] if not row is None else None
        
    def dbconn_exec_with_no_result_in_tran(self, sql, params):
        with self.dbconn.cursor() as cur:
            cur.execute("BEGIN")
            cur.execute(sql, params)
            cur.execute("COMMIT")

    def dbconn_attr_as_utf_8(self, attr):
        if attr is None:
            return "None"
        else:
            return attr.decode('utf-8')

    def dbconn_get_connection_str(self, dbconn):
        info = dbconn.pgconn
        result = f"{self.dbconn_attr_as_utf_8(info.user)}@{self.dbconn_attr_as_utf_8(info.host)}:{self.dbconn_attr_as_utf_8(info.port)}/{self.dbconn_attr_as_utf_8(info.db)}"
        return result

    def get_schema_name(self):
         if self.args is None:
             raise CommandError(f"The attribute self.args must not be None")
         if self.args.schema_name is None:
             raise CommandError(f"The attribute self.args.schema_name must not be None")
         return psycopg.sql.Identifier(self.args.schema_name)

    def check_if_schema_exists(self):
        sql = """
            SELECT EXISTS (
                SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = %s)"""
        value = self.dbconn_get_single_value(sql, (self.args.schema_name,))
        if value is None:
            raise CommandError(f"Unable to check whether target schema exists because the query returned nothing: '{sql}' ")
        return value
    
    def get_scripts_path_environment_id(self):
        environment_id = None
        if not hasattr(self.args, 'scripts_path'):
            raise CommandError("The argument 'scripts_path' is undefined")
        if len(self.args.scripts_path) == 0:
            raise CommandError("The path that is specified by 'scripts_path' must not be empty string")
        scripts_path = pathlib.Path(self.args.scripts_path)
        if not scripts_path.exists():
            raise CommandError("The path that is specified by 'scripts_path' argument does not exist")
        if not scripts_path.is_dir():
            raise CommandError("The path that is specified by 'scripts_path' argument is not a valid directory")        
        target_environment_id_file_name = scripts_path.joinpath(TARGET_ENVIRONMENT_ID_FILE_NAME)
        if target_environment_id_file_name.exists():
            environment_id = read_as_trimmed_string(target_environment_id_file_name)
            if len(environment_id) == 0:
                raise CommandError(f"The length of the environment ID must not be empty string")
            if len(environment_id) > NAME_LENGTH_LIMIT:
                raise CommandError(f"The length of the environment ID taken from '{target_environment_id_file_name}' exceeds the limit: {NAME_LENGTH_LIMIT} ")
        # considering dir name as env id
        else: 
            resolved_path = scripts_path.resolve()
            environment_id = resolved_path.name
            if len(environment_id) == 0:
                raise CommandError(f"The length of the environment ID must not be empty string")
            if len(environment_id) > NAME_LENGTH_LIMIT:
                raise CommandError(f"The length of the directory name specified by 'scripts_path' argument is more than {NAME_LENGTH_LIMIT} characters")
        return environment_id

    def get_stored_environment_id(self):
        sql = """
                SELECT id FROM {schema_name_identity}.dbmigration_environment_id ORDER BY created_at ASC LIMIT 1"""        
        formatted_sql = self.format_sql(sql, schema_name_identity=self.get_schema_name()) 
        value = self.dbconn_get_single_value(formatted_sql, [])
        if value is None:
            raise CommandError(f"Unable to get stored environment id from the database schema")
        return value
    
    def get_search_path_for_scripts(self):
        scripts_dir = pathlib.Path(self.args.scripts_path)
        set_search_path_file = scripts_dir.joinpath(SEARCH_PATH_FILE_NAME)
        if not set_search_path_file.exists():
            return self.args.schema_name
        if not set_search_path_file.is_file():
            raise CommandError(f"The search path file '{SEARCH_PATH_FILE_NAME}' within scripts directory '{self.args.scripts_path}' is not a file")
        trimmed_str = read_as_trimmed_string(set_search_path_file)
        return trimmed_str
    
    def set_session_search_path(self, search_path):
        print(f"Set session search path to '{search_path}'.")
        sql = f"""
            SELECT pg_catalog.set_config('search_path', %s, false)"""
        result = self.dbconn_get_single_value(sql, (search_path,))
        if result != search_path:
            raise CommandError(f"Unexpected value '{result}' returned on attempt to set the search path")

    def check_if_table_exists(self, table_name):
        sql = """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s)"""
        value = self.dbconn_get_single_value(sql, (self.args.schema_name, table_name))
        if value is None:
            raise CommandError(f"Unable to check whether table {table_name} exists in target schema")
        return value
    def check_if_version_table_include_baseline_version(self):
        sql = """
            SELECT EXISTS (
                SELECT 1
                FROM {schema_name}.dbmigration_versions
                WHERE is_baseline IS TRUE)"""
        formatted_sql = self.format_sql(sql, schema_name=self.get_schema_name())
        value = self.dbconn_get_single_value(formatted_sql, [])
        if value is None:
            raise CommandError(f"Unable to check whether the baseline scripts were applied in the target schema")
        return value
    
    def get_latest_version_installed(self):
        sql = """
            SELECT MAX(version_id) FROM {schema_name}.dbmigration_versions"""
        formatted_sql = self.format_sql(sql, schema_name=self.get_schema_name())
        value = self.dbconn_get_single_value(formatted_sql, [])
        if value is None:
            raise CommandError(f"Unable to get latest installed version")
        return value

    def check_if_repeatable_script_installed(self, git_blob_sha1, version, relative_path):
        sql = """
            SELECT EXISTS (
                SELECT 1 
                FROM {schema_name}.dbmigration_repeatable_scripts newer
                WHERE newer.relative_path = {relative_path}
                AND newer.version_id = {version_id}
                AND newer.git_blob_sha1 = {git_blob_sha1}
                AND newer.created_at = (
                    SELECT current_rec.created_at 
                    FROM {schema_name}.dbmigration_repeatable_scripts current_rec
                    WHERE current_rec.relative_path = {relative_path}
                        AND current_rec.version_id = {version_id}
                    ORDER BY current_rec.created_at DESC
                    LIMIT 1
                )
            )
        """        
        formatted_sql = self.format_sql(sql, 
                                        schema_name=self.get_schema_name(), 
                                        version_id=version, 
                                        relative_path=relative_path, 
                                        git_blob_sha1=git_blob_sha1)
        value = self.dbconn_get_single_value(formatted_sql, [])
        if value is None:
            raise CommandError(f"Unable to check if repeatable script was installed")
        return value

    def check_if_max_version_of_versioned_scripts_matches_repeatable_target(self, scripts_dir):
        print(f"Performing a cross-check for consistency between the target version's repeatable scripts and the versioned scripts in: {scripts_dir}")
        
        latest_version_in_baseline = None
        baseline_dir = scripts_dir.joinpath(BASELINE_DIR_NAME)
        if baseline_dir.exists():
            baseline_subdirs = [item for item in baseline_dir.iterdir() if item.is_dir()]
            if len(baseline_subdirs) == 1:
                baseline_version_subdir = baseline_subdirs[0]
                latest_version_in_baseline = baseline_version_subdir.name

        latest_version_in_versioned = None
        versioned_dir = scripts_dir.joinpath(VERSIONED_DIR_NAME)
        if versioned_dir.exists():
            latest_version_in_versioned = max((item.name for item in versioned_dir.iterdir() if item.is_dir()), default=None)

        latest_version_in_scripts = None
        if latest_version_in_versioned is None:
            latest_version_in_scripts = latest_version_in_baseline
        elif latest_version_in_baseline is None:
            latest_version_in_scripts = latest_version_in_versioned
        elif latest_version_in_versioned > latest_version_in_baseline:
            latest_version_in_scripts = latest_version_in_versioned
        else:
            raise CommandError(f"The latest version of the subdirectory with the versions'{latest_version_in_versioned}' must be greater than the version of the baseline scripts '{latest_version_in_scripts}'.")

        if latest_version_in_scripts is None:
            print(f"No either baseline or versioned script updates were found in scripts dir: '{scripts_dir}'")
            return        
        target_version_in_repeatable = None
        repeatable_dir = scripts_dir.joinpath(REPEATABLE_DIR_NAME)
        if repeatable_dir.exists():
            target_version_file_path = repeatable_dir.joinpath(TARGET_VERSION_FILE)
            if target_version_file_path.exists():
                target_version_in_repeatable = read_as_trimmed_string(target_version_file_path)
        if target_version_in_repeatable is None:
            print(f"No repeatable scripts were found in scripts dir: '{scripts_dir}'")
            return 
        if latest_version_in_scripts != target_version_in_repeatable:
            raise CommandError(f"The target version for repeatable scripts '{target_version_in_repeatable}' does not match the latest version in versioned scripts '{latest_version_in_scripts}'")
        print(f"Completed.")

    def do_initial_cross_checks(self):
        self.scripts_dir = pathlib.Path(self.args.scripts_path).resolve()        
        if not self.scripts_dir.exists():
            raise CommandError(f"The scripts repository path '{self.args.scripts_path}' does not exists")       
        if not self.check_if_schema_exists():
            raise CommandError(f"The target schema '{self.args.schema_name}' is not accessible")
        search_path = self.get_search_path_for_scripts()
        if search_path != DEFAULT_SEARCH_PATH:
            self.set_session_search_path(search_path)
        else:
            print(f"Use the default users search path")     

    def check_if_stored_environment_id_matches_to_scripts_dir(self):
        stored_environment_id = self.get_stored_environment_id()
        scripts_environment_id = self.get_scripts_path_environment_id()
        if stored_environment_id != scripts_environment_id:
            raise CommandError(f"The stored environment ID '{stored_environment_id}' in the target schema does not match the scripts directory '{self.scripts_dir}'")
        print(f"Target schema environment ID matches the scripts directory ID: {stored_environment_id}")

    def check_if_all_version_control_tables_exists(self):
        if not self.check_if_table_exists("dbmigration_environment_id"):
            raise CommandError(f"The schema '{self.args.schema_name}' does not include version control table 'dbmigration_environment_id'")
        if not self.check_if_table_exists("dbmigration_versions"):
            raise CommandError(f"The schema '{self.args.schema_name}' does not include version control table 'dbmigration_versions'")
        if not self.check_if_table_exists("dbmigration_version_scripts"):
            raise CommandError(f"The schema '{self.args.schema_name}' does not include version control table 'dbmigration_version_scripts'")
        if not self.check_if_table_exists("dbmigration_repeatable_scripts"):
            raise CommandError(f"The schema '{self.args.schema_name}' does not include repeatable scripts control table 'dbmigration_repeatable_scripts'")
    
    def check_if_all_version_control_tables_does_not_exists(self):
        if self.check_if_table_exists("dbmigration_environment_id"):
            raise CommandError(f"The schema '{self.args.schema_name}' already include version control table 'dbmigration_environment_id'")
        if self.check_if_table_exists("dbmigration_versions"):
            raise CommandError(f"The schema '{self.args.schema_name}' already include version control table 'dbmigration_versions'")
        if self.check_if_table_exists("dbmigration_version_scripts"):
            raise CommandError(f"The schema '{self.args.schema_name}' already include version control table 'dbmigration_version_scripts'")
        if self.check_if_table_exists("dbmigration_repeatable_scripts"):
            raise CommandError(f"The schema '{self.args.schema_name}' already include repeatable scripts control table 'dbmigration_repeatable_scripts'")

    def __init__(self, config, subparsers, command_name, command_help):

        self.all_own_migrations = [
            MigrationCheckForOlderVersionControlTables()
        ]

        self.config = config
        self.default_dbenv = self.get_default_dbenv(config)
        self.dbconn_settings, self.run_tests_by, self.no_password = self.get_dbenv_config(config, self.default_dbenv)
        self.use_run_tests_by_user = False
        try:        
            self.options = config[OPTIONS_CONFIG_GROUP]
        except:
            raise CommandError(f"Configuration file {TOML_CONFIG_FILE} does not contain configuration group '{OPTIONS_CONFIG_GROUP}'")  
    
        self.file_read_encoding =  self.options.get("file_read_encoding", OPTIONS_DEFAULT_FILE_READ_ENCODING)
        self.file_read_encoding_errors =  self.options.get("file_read_encoding_errors", OPTIONS_DEFAULT_FILE_READ_ENCODING_ERRORS)
        self.file_glob_filters =  self.options.get("file_glob_filters", OPTIONS_DEFAULT_FILE_GLOB_FILTERS)
        
        self.parser = subparsers.add_parser(command_name, help=command_help)
        self.parser.add_argument("schema_name", type=str, help="the name of target database schema")
        self.parser.add_argument("--dbenv", type=str, default=self.default_dbenv, help="db environment name within TOML config")
        self.parser.add_argument("--host", type=str, default=None, help="db server host name")
        self.parser.add_argument("--port", type=int, default=None, help="db server port")
        self.parser.add_argument("--dbname", type=str, default=None, help="database name")
        self.parser.add_argument("--user", type=str, default=None, help="user name")
        self.parser.add_argument("-n","--no-password",  action="store_true", default=self.no_password, help="dont ask user password")
        self.parser.set_defaults(call=self) 
    def __enter__(self):
        if not self.args.dbenv is None:
            self.dbconn_settings, self.run_tests_by, self.no_password = self.get_dbenv_config(self.config, self.args.dbenv)  
        if not self.args.host is None:
            self.dbconn_settings["host"]=self.args.host
        if not self.args.port is None:
            self.dbconn_settings["port"]=self.args.port
        
        if not self.args.user is None:
            self.dbconn_settings["user"]=self.args.user
        elif not self.run_tests_by is None and self.use_run_tests_by_user:
            self.dbconn_settings["user"]=self.run_tests_by
        
        if not self.args.dbname is None:
            self.dbconn_settings["dbname"]=self.args.dbname
        if not self.args.no_password and not self.no_password:
            password = None
            if self.use_run_tests_by_user:
                password = os.getenv(DBCONN_TESTER_PASSWORD_ENVVAR_NAME)
                if password is None:
                    password = os.getenv(DBCONN_USER_PASSWORD_ENVVAR_NAME)
                    if password is None:
                        raise CommandError(f"The database user password must be specified via the environment variable '{DBCONN_USER_PASSWORD_ENVVAR_NAME}'.")
            self.dbconn_settings["password"]=password
        else:
            self.dbconn_settings.pop("password", None)
        self.dbconn = psycopg.connect(**self.dbconn_settings)
        print(f"Opened db connection: '{self.dbconn_get_connection_str(self.dbconn)}'")
        self.dbconn.add_notice_handler(log_server_notices)
        self.dbconn.autocommit = True 

    def __exit__(self, exc_type, exc_value, traceback):
        if not exc_type is None:
            self.dbconn.rollback()
            print(f"Rolled back transaction.")
        if not self.dbconn is None:
            self.dbconn.close()
            print(f"Closed db connection.")
        return False # propagate the exception
    def run(self):
        pass
    def __call__(self, args):
        self.args = args
        with self:
            self.run()

class UpdateCommand (BaseCommand):
    """Applies base, versioned, and repeatable scripts to the target database schema."""

    def run_baseline_scripts_with_external_tool(self, version, scripts_dir, scripts, tool):
         print(f"Running baseline scripts with external tool '{tool.exec_path}'")
         with self.dbconn.cursor() as cur:
            for script_path in scripts:
                script_path_for_log = self.script_path_for_log(scripts_dir, script_path)
                print(f"Running script: [{script_path_for_log}]...")
                tool.run(script_path)
            print(f"Setting the baseline version '{version}'...")
            cur.execute("BEGIN")
            formatted_sql = self.format_sql("INSERT INTO {schema_name}.dbmigration_versions (version_id, is_baseline) VALUES (%s, %s)", 
                                            schema_name=self.get_schema_name())                                  
            cur.execute(formatted_sql, (version, True))
            for script_path in scripts:
                with open(script_path, 'rb') as f:
                    script_bytes = f.read()
                    relative_script_path = self.script_path_for_log(scripts_dir, script_path)
                    git_blob_sha1 = get_git_blob_sha1_for_bytes(script_bytes)
                    formatted_sql = self.format_sql("INSERT INTO {schema_name}.dbmigration_version_scripts (version_id, relative_path, git_blob_sha1) VALUES ({version_id}, {relative_path},{git_blob_sha1});\n", 
                                                        schema_name=self.get_schema_name(), version_id=version,relative_path=relative_script_path,git_blob_sha1=git_blob_sha1)
                    cur.execute(formatted_sql, [])
            cur.execute("COMMIT")       
            print(f"Committed.")

    def run_baseline_scripts_each_in_own_tran(self, version, scripts_dir, scripts):
         with self.dbconn.cursor() as cur:
            for script_path in scripts:
                with open(script_path, 'rt', encoding=self.file_read_encoding, errors=self.file_read_encoding_errors) as f:
                    script_path_for_log = self.script_path_for_log(scripts_dir, script_path)
                    print(f"Running script: [{script_path_for_log}]...")
                    script_text = f.read()
                    cur.execute("BEGIN")
                    cur.execute(script_text)                                  
                    cur.execute("COMMIT")       
                    print(f"Committed.")
            print(f"Setting the baseline version as '{version}'.")
            cur.execute("BEGIN")
            formatted_sql = self.format_sql("INSERT INTO {schema_name}.dbmigration_versions (version_id, is_baseline) VALUES (%s, %s)", 
                                            schema_name=self.get_schema_name())                                  
            cur.execute(formatted_sql, (version, True))
            for script_path in scripts:
                with open(script_path, 'rb') as f:
                    script_bytes = f.read()
                    relative_script_path = self.script_path_for_log(scripts_dir, script_path)
                    git_blob_sha1 = get_git_blob_sha1_for_bytes(script_bytes)
                    formatted_sql = self.format_sql("INSERT INTO {schema_name}.dbmigration_version_scripts (version_id, relative_path, git_blob_sha1) VALUES ({version_id}, {relative_path},{git_blob_sha1});\n", 
                                                        schema_name=self.get_schema_name(), version_id=version,relative_path=relative_script_path,git_blob_sha1=git_blob_sha1)
                    cur.execute(formatted_sql, [])        
            cur.execute("COMMIT")       
            print(f"Committed.")

    def rerun_versioned_scripts(self, version, scripts_dir, scripts):
        with self.dbconn.cursor() as cur:
            print(f"Reapply version {version}...")
            cur.execute("BEGIN")
            formatted_sql = self.format_sql("DELETE FROM {schema_name}.dbmigration_version_scripts WHERE version_id=%s", schema_name=self.get_schema_name())
            cur.execute(formatted_sql, (version,))    
            formatted_sql = self.format_sql("DELETE FROM {schema_name}.dbmigration_versions WHERE version_id=%s", schema_name=self.get_schema_name())
            cur.execute(formatted_sql, (version,))    
            for script_path in scripts:
                with open(script_path, 'rt', encoding=self.file_read_encoding, errors=self.file_read_encoding_errors) as f:
                    script_path_for_log = self.script_path_for_log(scripts_dir, script_path)
                    print(f"Running script: [{script_path_for_log}]...")
                    script_text = f.read()
                    cur.execute(script_text)                                  
            formatted_sql = self.format_sql("INSERT INTO {schema_name}.dbmigration_versions (version_id, is_baseline) VALUES (%s, %s)", 
                                            schema_name=self.get_schema_name())                                  
            cur.execute(formatted_sql, (version, False))
            for script_path in scripts:
                with open(script_path, 'rb') as f:
                    script_bytes = f.read()
                    relative_script_path = self.script_path_for_log(scripts_dir, script_path)
                    git_blob_sha1 = get_git_blob_sha1_for_bytes(script_bytes)
                    formatted_sql = self.format_sql("INSERT INTO {schema_name}.dbmigration_version_scripts (version_id, relative_path, git_blob_sha1) VALUES ({version_id}, {relative_path},{git_blob_sha1});\n", 
                                                        schema_name=self.get_schema_name(), version_id=version,relative_path=relative_script_path,git_blob_sha1=git_blob_sha1)
                    cur.execute(formatted_sql, [])        
            cur.execute("COMMIT")       
            print(f"Committed.")

    def run_versioned_scripts_in_tran(self, version, scripts_dir, scripts):
        with self.dbconn.cursor() as cur:
            print(f"Apply version {version}...")
            cur.execute("BEGIN")
            for script_path in scripts:
                with open(script_path, 'rt', encoding=self.file_read_encoding, errors=self.file_read_encoding_errors) as f:
                    script_path_for_log = self.script_path_for_log(scripts_dir, script_path)
                    print(f"Running script: [{script_path_for_log}]...")
                    script_text = f.read()
                    cur.execute(script_text)
            formatted_sql = self.format_sql("INSERT INTO {schema_name}.dbmigration_versions (version_id, is_baseline) VALUES (%s, %s)", 
                                            schema_name=self.get_schema_name())
            cur.execute(formatted_sql, (version, False))       
            for script_path in scripts:
                with open(script_path, 'rb') as f:
                    script_bytes = f.read()
                    relative_script_path = self.script_path_for_log(scripts_dir, script_path)
                    git_blob_sha1 = get_git_blob_sha1_for_bytes(script_bytes)
                    formatted_sql = self.format_sql("INSERT INTO {schema_name}.dbmigration_version_scripts (version_id, relative_path, git_blob_sha1) VALUES ({version_id}, {relative_path},{git_blob_sha1});\n", 
                                                        schema_name=self.get_schema_name(), version_id=version,relative_path=relative_script_path,git_blob_sha1=git_blob_sha1)
                    cur.execute(formatted_sql, [])       
            cur.execute("COMMIT")       
            print(f"Committed.")


    def __init__(self, config, subparsers): 
        super().__init__(config, subparsers, "update", UpdateCommand.__doc__)
        self.parser.add_argument("--force-reapply-latest-version",  action="store_true", default=False, help="clean up the latest version within the database and reapply the included *.sql scripts.")
        self.parser.add_argument("--force-reapply-all-repeatable",  action="store_true", default=False, help="reapply all repeatable scripts, regardless of changes.")
        self.parser.add_argument("--force-run-cleanup",  action="store_true", default=False, help="run the cleanup script before executing version-specific scripts.")
        self.parser.add_argument("--skip-confirmation",  action="store_true", default=False, help="skip confirmation before executing updates.")
        self.parser.add_argument("scripts_path", type=str, help="source scripts repository path")


    def apply_baseline_scripts(self, scripts_dir):
        baseline_dir = scripts_dir.joinpath(BASELINE_DIR_NAME)
        if not baseline_dir.exists():
            print(f"The scripts path '{scripts_dir}' does not include '{BASELINE_DIR_NAME}' subdirectory.")
            return
        if self.check_if_version_table_include_baseline_version():
            print(f"The target schema already has the baseline version installed. Baseline scripts will be skipped.")
            return
        baseline_subdirs = [item for item in baseline_dir.iterdir() if item.is_dir()]
        if len(baseline_subdirs) != 1:
            raise CommandError(f"The baseline path {baseline_dir} must have single subdirectory with the baseline scripts but {len(baseline_subdirs)} was found")
        baseline_version_subdir = baseline_subdirs[0]
        baseline_version = baseline_version_subdir.name
        print(f"The baseline version to install {baseline_version}.")       
        print(f"Apply baseline scripts...")
        scripts_sorted = self.get_sorted_scripts_from_dir(baseline_version_subdir, BASELINE_FILES_DEPTH, force_run_cleanup = self.args.force_run_cleanup)
        
        external_tool_name = self.try_get_external_tool_name(baseline_version_subdir);
        if not external_tool_name is None:
            tool = ExternalTool(external_tool_name, self.args.schema_name, self.dbconn_settings, self.config)
            self.run_baseline_scripts_with_external_tool(baseline_version, scripts_dir, scripts_sorted, tool)
        else:
            self.run_baseline_scripts_each_in_own_tran(baseline_version, scripts_dir, scripts_sorted)
        print(f"The baseline scripts were applied.")       

    def reapply_the_latest_version(self, scripts_dir):
        versioned_dir = scripts_dir.joinpath(VERSIONED_DIR_NAME)
        if not versioned_dir.exists():
            print(f"The scripts path '{scripts_dir}' does not include '{VERSIONED_DIR_NAME}' subdirectory.")
            return
        latest_installed_version = self.get_latest_version_installed()
        print(f"The latest installed version is {latest_installed_version}.")
        version_subdirs = [item for item in versioned_dir.iterdir() if item.is_dir() and item.name == latest_installed_version]
        if len(version_subdirs) != 1:
            raise CommandError(f"There is no subdirectory with scripts that matched to the latest installed version '{latest_installed_version}'")
        latest_version_dir = version_subdirs[0]
        scripts_sorted = self.get_sorted_scripts_from_dir(latest_version_dir, VERSIONED_FILES_DEPTH, force_run_cleanup=True)
        if len(scripts_sorted) == 0:
            filters_str = ",".join(self.file_glob_filters)
            raise CommandError(f"The scripts subdirectory '{latest_version_dir}' does not include any '{filters_str}' scripts")
        self.rerun_versioned_scripts(latest_installed_version, scripts_dir, scripts_sorted)       


    def apply_versioned_scripts(self, scripts_dir):
        versioned_dir = scripts_dir.joinpath(VERSIONED_DIR_NAME)
        if not versioned_dir.exists():
            print(f"The scripts path '{scripts_dir}' does not include '{VERSIONED_DIR_NAME}' subdirectory.")
            return
        versioned_subdirs = [item for item in versioned_dir.iterdir() if item.is_dir()]
        if len(versioned_subdirs) == 0:
            raise CommandError(f"The versioned scripts path {versioned_dir} must have at least one subdirectory but nothing was found")
        if not self.check_if_version_table_include_baseline_version():
            raise CommandError(f"The baseline version must be installed before running versioned scripts")
        latest_installed_version = self.get_latest_version_installed()
        print(f"The latest installed version is {latest_installed_version}.")       
        newer_version_subdirs = [x for x in versioned_subdirs if x.name > latest_installed_version]
        if len(newer_version_subdirs) == 0:
            print(f"No newer versions found for installation.")       
            return
        newer_version_subdirs_sorted = sorted(newer_version_subdirs)
        print(f"Found {len(newer_version_subdirs)} new versions for installation.")       
        print(f"Apply versioned scripts...")
        for script_version_dir in newer_version_subdirs_sorted:        
            version_id = script_version_dir.name
            scripts_sorted = self.get_sorted_scripts_from_dir(script_version_dir, VERSIONED_FILES_DEPTH, force_run_cleanup = self.args.force_run_cleanup)
            if len(scripts_sorted) == 0:
                filters_str = ",".join(self.file_glob_filters)
                raise CommandError(f"The scripts subdirectory '{script_version_dir}' does not include any '{filters_str}' scripts")
            self.run_versioned_scripts_in_tran(version_id, scripts_dir, scripts_sorted)       
        print(f"The versioned scripts were applied.")

    def apply_repeatable_scripts(self, scripts_dir, force_reapply = False):
        
        repeatable_dir = scripts_dir.joinpath(REPEATABLE_DIR_NAME)
        if not repeatable_dir.exists():
            print(f"The scripts path '{scripts_dir}' does not include '{REPEATABLE_DIR_NAME}' subdirectory.")
            return
        print(f"Check repeatable scripts...")       
        target_version_file_path = repeatable_dir.joinpath(TARGET_VERSION_FILE)
        if not target_version_file_path.exists():
            raise CommandError(f"The file with target version '{TARGET_VERSION_FILE}' does not exists in repeatable scripts subdirectory '{repeatable_dir}'.")
        target_version = read_as_trimmed_string(target_version_file_path)
        latest_installed_version = self.get_latest_version_installed() 
        if latest_installed_version != target_version:
            raise CommandError(f"The target version {target_version} for repeatable scripts does not match the latest installed version {latest_installed_version}.")                  
        print(f"Target version matches the latest installed version '{target_version}'")
        repeatable_scripts_sorted = self.get_sorted_scripts_from_dir(repeatable_dir, REPEATABLE_FILES_DEPTH)
        scripts_to_repeat = [] 
        for script_path in repeatable_scripts_sorted:
            with open(script_path, 'rb') as f:
                script_bytes = f.read()
                git_blob_sha1 = get_git_blob_sha1_for_bytes(script_bytes)
                script_text = script_bytes.decode(self.file_read_encoding, errors=self.file_read_encoding_errors)
                relative_script_path = self.script_path_for_log(scripts_dir, script_path)
                if force_reapply:
                    scripts_to_repeat.append(script_path)
                elif not self.check_if_repeatable_script_installed(git_blob_sha1, target_version, relative_script_path):
                    scripts_to_repeat.append(script_path)
        if len(scripts_to_repeat) == 0:
            print(f"No changed repeatable scripts found for (re)installation.")       
            return
        scripts_to_repeat = self.resolve_scripts_dependencies(repeatable_dir, REPEATABLE_FILES_DEPTH, repeatable_scripts_sorted, scripts_to_repeat)
        print(f"Found {len(scripts_to_repeat)} scripts to re-run")
        print(f"Apply repeatable scripts...")       
        with self.dbconn.cursor() as cur:
            for script_path in scripts_to_repeat:
                with open(script_path, 'rb') as f:
                    script_bytes = f.read()
                    git_blob_sha1 = get_git_blob_sha1_for_bytes(script_bytes)
                    script_text = script_bytes.decode(self.file_read_encoding, errors=self.file_read_encoding_errors)
                    relative_script_path = self.script_path_for_log(scripts_dir, script_path)
                    print(f"Running script: [{relative_script_path}]...")
                    cur.execute("BEGIN")
                    cur.execute(script_text)
                    formatted_sql = self.format_sql("INSERT INTO {schema_name}.dbmigration_repeatable_scripts (git_blob_sha1, version_id, relative_path) VALUES (%s, %s, %s)", 
                                                    schema_name=self.get_schema_name())                                  
                    cur.execute(formatted_sql, (git_blob_sha1, target_version, str(relative_script_path)))     
                    cur.execute("COMMIT")
                    print(f"Committed.")
        print(f"The repeatable scripts were applied.")       

    def run(self):
        if not self.args.skip_confirmation:
            print("You are going to run updates. Would you like to continue? [y/N]: ", end="", flush=True)
            answer = get_char().lower()
            if answer != 'y':
                raise CommandError("Cancelled by user");
        
        self.do_initial_cross_checks()        
        
        applied_count = self.apply_all_own_migrations()
        if applied_count > 0:
            print(f"The version control tables were updated. Please rerun the tool to update the schema using your scripts.")
            return
        
        if self.args.force_reapply_latest_version:
            print(f"Performing reapply latest version from scripts repository: '{self.scripts_dir}'")
            self.check_if_all_version_control_tables_exists()
            self.check_if_stored_environment_id_matches_to_scripts_dir() 
            self.reapply_the_latest_version(self.scripts_dir)
            self.apply_repeatable_scripts(self.scripts_dir, force_reapply=True)
            print(f"Reapplied.")
        else:
            print(f"Performing updates from scripts repository: '{self.scripts_dir}'")
            self.check_if_all_version_control_tables_exists() 
            self.check_if_stored_environment_id_matches_to_scripts_dir() 
            self.check_if_max_version_of_versioned_scripts_matches_repeatable_target(self.scripts_dir)
            self.apply_baseline_scripts(self.scripts_dir)
            self.apply_versioned_scripts(self.scripts_dir)
            self.apply_repeatable_scripts(self.scripts_dir, force_reapply=self.args.force_reapply_all_repeatable)
            print(f"Updated.")

class VerifyCommand (BaseCommand):
    """Validates the target schema and lists versioned and reproducible scripts to apply if the 'update' command is executed."""
    
    def make_dbconn_session_readonly(self):
        sql = """
            SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY"""
        with self.dbconn.cursor() as cur:
            cur.execute(sql)

    def get_baseline_version_installed(self):
        sql = """
                SELECT version_id FROM {schema_name}.dbmigration_versions WHERE is_baseline IS TRUE ORDER BY version_id DESC LIMIT 1"""
        formatted_sql = self.format_sql(sql, schema_name=self.get_schema_name())
        value = self.dbconn_get_single_value(formatted_sql, [])
        return value

    def cross_check_of_the_target_version_for_repeatable_scripts(self, target_version, latest_version_in_scripts, latest_installed_version):
        if latest_version_in_scripts is None and latest_installed_version is None:
            raise CommandError(f"Failed to check target version '{target_version}' because no version is installed and no versioned scripts were provided in the scripts directory.")
        elif latest_version_in_scripts is None:
            if target_version != latest_installed_version:
                raise CommandError(f"The target version '{target_version}' does not match to the latest installed version '{latest_installed_version}'.")
        elif latest_installed_version is None:
            if target_version != latest_version_in_scripts:
                raise CommandError(f"The target version '{target_version}' does not match to the latest version in scripts '{latest_version_in_scripts}'.")
        elif latest_version_in_scripts > latest_installed_version:
            if target_version != latest_version_in_scripts:
                raise CommandError(f"The target version '{target_version}' does not match to the latest version in scripts '{latest_version_in_scripts}'.")
        elif latest_version_in_scripts <= latest_installed_version:
            if target_version != latest_installed_version:
                raise CommandError(f"The target version '{target_version}' does not match to the latest installed version '{latest_installed_version}'.")
    
    def check_if_target_script_file_path_accessible_for_write(self, script_path):
        path = pathlib.Path(script_path)
        try:
            path.touch(exist_ok=False)
            path.unlink()
        except FileExistsError as e:
            raise CommandError(f"The specified script file '{script_path}' already exists")
        except Exception as e:
            raise CommandError(f"The specified script file '{script_path}' is not accessible for write")

    def try_get_git_cmd_path(self, toml_config):
        if GIT_CMD_CONFIG_ATTRIBUTE in toml_config:
            cmd_path_str = toml_config[GIT_CMD_CONFIG_ATTRIBUTE]
            cmd_path = pathlib.Path(cmd_path_str)
            if not cmd_path.exists():
                raise CommandError(f"The git cmd specified in {GIT_CMD_CONFIG_ATTRIBUTE} of TOML config does not exist! Comment it out if you are not sure where it is in the system")
            return cmd_path
        else:
            cmd_path_str = shutil.which("git")
            if cmd_path_str is None:
                return None
            cmd_path = pathlib.Path(cmd_path_str)
            return cmd_path

    def try_get_git_repo_root(self, git_cmd_path, scripts_dir):
        if git_cmd_path is None:
            raise CommandError("try_get_git_repo_root(): The argument 'git_cmd_path' must be provided")
        if scripts_dir is None:
            raise CommandError("try_get_git_repo_root(): The argument 'scripts_dir' must be provided")

        resolved_scripts_dir = pathlib.Path(scripts_dir).resolve()
        if not resolved_scripts_dir.exists():
            raise CommandError(f"The specified directory '{scripts_dir}' does not exist!")
        if not resolved_scripts_dir.is_dir():
            raise CommandError(f"The specified path '{scripts_dir}' is not a directory!")
        
        completed_process = subprocess.run(
            [str(git_cmd_path), "rev-parse", "--show-toplevel"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding='utf-8-sig',
            cwd=str(resolved_scripts_dir)
        )
        if completed_process.returncode != 0:
            return None
                
        stdout_text = completed_process.stdout.strip() 
        resolved_path = pathlib.Path(stdout_text).resolve()
        return resolved_path
    
    def get_file_commit_history(self, git_cmd_path, repo_root_dir, relative_file_path):

        if git_cmd_path is None:
            raise CommandError("get_file_commit_history(): The argument 'git_cmd_path' must be provided")
        if repo_root_dir is None:
            raise CommandError("get_file_commit_history(): The argument 'repo_root_dir' must be provided")

        completed_status_process = subprocess.run(
            [str(git_cmd_path), "status", "--porcelain", str(relative_file_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding='utf-8-sig',
            cwd=str(repo_root_dir)
        )
        if completed_status_process.returncode != 0:
            raise CommandError(f"Warning: Unable to get git status for file '{relative_file_path}'")

        status_output = completed_status_process.stdout.strip()
        # the file have local changes
        if status_output:
            status_code = status_output.lstrip()[:1]            
            if status_code == "?":
                return {
                    "sha": UNCOMMITTED_SHA_LABEL,
                    "author": UNCOMMITTED_AUTHOR_LABEL,
                    "date": UNCOMMITTED_DATE_LABEL,
                    "message": "File is completely untracked by Git"
                }
            elif status_code == "M":
                return {
                    "sha": UNCOMMITTED_SHA_LABEL,
                    "author": UNCOMMITTED_AUTHOR_LABEL,
                    "date": UNCOMMITTED_DATE_LABEL,
                    "message": "File is modified"
                }
            elif status_code == "A":
                return {
                    "sha": UNCOMMITTED_SHA_LABEL,
                    "author": UNCOMMITTED_AUTHOR_LABEL,
                    "date": UNCOMMITTED_DATE_LABEL,
                    "message": "File is added"
                }
            elif status_code == "D":
                return {
                    "sha": UNCOMMITTED_SHA_LABEL,
                    "author": UNCOMMITTED_AUTHOR_LABEL,
                    "date": UNCOMMITTED_DATE_LABEL,
                    "message": "File is deleted"
                }
            else:
                return {
                    "sha": UNCOMMITTED_SHA_LABEL,
                    "author": UNCOMMITTED_AUTHOR_LABEL,
                    "date": UNCOMMITTED_DATE_LABEL,
                    "message": f"The status is UNKNOWN : {status_code}"
                }

        completed_log_process = subprocess.run(
            [str(git_cmd_path), "log", "-1", "--format=%H|%an|%ad|%s", "--date=short", "--", str(relative_file_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding='utf-8-sig',
            cwd=str(repo_root_dir)
        )        
        if completed_log_process.returncode != 0:
            raise CommandError(f"Warning: Unable to get git log for file '{relative_file_path}'")
            
        log_output = completed_log_process.stdout.strip()
        if not log_output:
            return {
                "sha": UNCOMMITTED_SHA_LABEL,
                "author": UNCOMMITTED_AUTHOR_LABEL,
                "date": UNCOMMITTED_DATE_LABEL,
                "message": "File is completely untracked by Git"
            }
            
        sha, author, date, message = log_output.split('|', 3)
        return {
            "sha": sha[:8],
            "author": author,
            "date": date,
            "message": message
        }

    def display_verification_changes_by_commits(self, git_cmd_path, git_root_path, files_sorted):
        resolved_repo_root = pathlib.Path(git_root_path).resolve()
        commits_group = collections.defaultdict(list)        
        for file_path in files_sorted:
            abs_path = pathlib.Path(file_path).resolve()            
            try:
                rel_path = abs_path.relative_to(resolved_repo_root)
            except ValueError:
                rel_path = abs_path                
            commit_info = self.get_file_commit_history(git_cmd_path, resolved_repo_root, rel_path)            
            if commit_info:
                msg_first_line = commit_info["message"].split('\n')[0].strip()
                commit_key = (commit_info["date"], commit_info["author"], commit_info["sha"], msg_first_line)
            else:
                commit_key = ("----------", "Local Changes", "UNCOMMITTED", "File has modifications not yet committed to Git")                
            commits_group[commit_key].append(rel_path)
        
        sorted_commits = sorted(
            commits_group.items(), 
            key=lambda x: x[0], 
            reverse=True
        )

        for (date, author, sha, message), files in sorted_commits:
            print(f"[{sha}] {date} — {message}")
            print(f"  Author: {author}")
            for f in files:
                print(f"    [{str(f).replace('\\', '/')}]")                

    def display_verification_changes(self, scripts_dir, git_cmd_path, git_root_path, scripts_sorted):
        if git_root_path is None: 
            for item in scripts_sorted:
                relative_script_path = self.script_path_for_log(scripts_dir, item)
                print(f"   [{relative_script_path}]")
        else:
            self.display_verification_changes_by_commits(git_cmd_path, git_root_path, scripts_sorted)

    def get_oid_commit_history(self, git_cmd_path, repo_root_dir, target_oid):

        if git_cmd_path is None:
            raise CommandError("get_oid_commit_history(): The argument 'git_cmd_path' must be provided")
        if repo_root_dir is None:
            raise CommandError("get_oid_commit_history(): The argument 'repo_root_dir' must be provided")
        if target_oid is None or not str(target_oid).strip():
            raise CommandError("get_oid_commit_history(): The argument 'target_oid' must be provided and not empty")

        completed_log_process = subprocess.run(
            [str(git_cmd_path), "log", "-1", f"--find-object={target_oid}", "--format=%H|%an|%ad|%s", "--date=short"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding='utf-8-sig',
            cwd=str(repo_root_dir)
        )        
        if completed_log_process.returncode != 0:
            raise CommandError(f"Unable to get git log for OID '{target_oid}'")
            
        log_output = completed_log_process.stdout.strip()
        
        if not log_output:
            return {
                "sha": UNCOMMITTED_SHA_LABEL,
                "author": UNCOMMITTED_AUTHOR_LABEL,
                "date": UNCOMMITTED_DATE_LABEL,
                "message": "Content hash (OID) is completely untracked or modified locally"
            }
            
        sha, author, date, message = log_output.split('|', 3)
        return {
            "sha": sha[:8],
            "author": author,
            "date": date,
            "message": message
        }

    def display_recent_changes_grouped_by_git_commits(self, git_cmd_path, git_root_path, limit=10, window_minutes=30):

        if git_cmd_path is None:
            raise CommandError("get_oid_commit_history(): The argument 'git_cmd_path' must be provided")
        if git_root_path is None:
            raise CommandError("get_oid_commit_history(): The argument 'git_root_path' must be provided")

        resolved_git_root_path = pathlib.Path(git_root_path).resolve()
        commits_group = collections.defaultdict(list)
        
        sql = """
            WITH latest_time AS (
                SELECT COALESCE(MAX(applied_at), NOW()) AS max_at
                FROM (
                    SELECT MAX(v.created_at) AS applied_at FROM {schema_name}.dbmigration_versions v
                    UNION ALL
                    SELECT MAX(r.created_at) AS applied_at FROM {schema_name}.dbmigration_repeatable_scripts r
                ) t
            )
            SELECT 
                v.created_at AS applied_at,
                'versioned' AS script_type,
                s.version_id,
                s.relative_path,
                s.git_blob_sha1
            FROM {schema_name}.dbmigration_version_scripts s
            JOIN {schema_name}.dbmigration_versions v ON s.version_id = v.version_id
            CROSS JOIN latest_time
            WHERE v.created_at >= latest_time.max_at - ({window_minutes} || ' minutes')::interval

            UNION ALL

            SELECT 
                r.created_at AS applied_at,
                'repeatable' AS script_type,
                r.version_id,
                r.relative_path,
                r.git_blob_sha1
            FROM {schema_name}.dbmigration_repeatable_scripts r
            CROSS JOIN latest_time
            WHERE r.created_at >= latest_time.max_at - ({window_minutes} || ' minutes')::interval

            ORDER BY applied_at DESC
            LIMIT {limit};

        """

        formatted_sql = self.format_sql(sql, schema_name=self.get_schema_name(), limit=limit, window_minutes=window_minutes)
        
        cursor = self.dbconn.cursor()
        cursor.execute(formatted_sql, [])
        rows = cursor.fetchall()
        
        if not rows:
            return

        print(f"The list of recent changes were applied to the target schema:")

        for applied_at, script_type, version_id, relative_path, git_blob_sha1 in rows:

            clean_oid = git_blob_sha1.strip()
            commit_info = self.get_oid_commit_history(git_cmd_path, resolved_git_root_path, clean_oid)

            if not commit_info:
                raise CommandError(f"Unable to find commit history for git sha1 {clean_oid}")
            
            msg_first_line = commit_info["message"].split('\n')[0].strip()
            commit_key = (commit_info["date"], commit_info["author"], commit_info["sha"], msg_first_line)
                
            commits_group[commit_key].append({
                "path": relative_path,
                "type": script_type,
                "version": version_id,
                "oid": clean_oid[:8],
                "applied_at": applied_at.strftime("%Y-%m-%d %H:%M:%S")
            })
        
        sorted_commits = sorted(
            commits_group.items(), 
            key=lambda x: x[0], 
            reverse=True
        )
        for (date, author, sha, message), scripts in sorted_commits:
            print(f"[{sha}] {date} — {message}")
            print(f"  Author: {author}")
            for s in scripts:
                print(f"     [{s['applied_at']:<19} | {s['version']:<6} | {s['path']} (OID: {s['oid']})]")


    def __init__(self, config, subparsers): 
        super().__init__(config, subparsers, "verify", VerifyCommand.__doc__)
        self.parser.add_argument("--skip-git-checks",  action="store_true", default=False, help="skip grouping changes by git commits")
        self.parser.add_argument("--build-update-script", type=str, default=None, help="the update script path if you want one as an additional result of the verify command")
        self.parser.add_argument("scripts_path", type=str, help="source scripts repository path")
        self.latest_version_in_scripts = None

    def write_search_path(self, search_path, target_script_path):
        with pathlib.Path(target_script_path).open("a") as target_file:
            formatted_sql_text = self.format_sql("SELECT pg_catalog.set_config('search_path', {search_path}, false);\n", search_path=search_path)
            target_file.write(formatted_sql_text)

    def write_baseline_scripts(self, version, scripts_dir, scripts, target_script_path):
        with pathlib.Path(target_script_path).open("a") as target_file:
            formatted_sql_text = self.format_sql("-- Baseline scripts for version {version_id}\n", version_id=version)
            target_file.write(formatted_sql_text)
            for script_path in scripts:
                with script_path.open("r", encoding="utf-8-sig", errors="ignore") as source_file:
                    lines = source_file.readlines()
                    relative_script_path = self.script_path_for_log(scripts_dir, script_path)
                    formatted_sql_text = self.format_sql("--{script_path}\n", script_path=str(relative_script_path))                    
                    target_file.write(formatted_sql_text)
                    target_file.write(f"BEGIN;\n")
                    target_file.writelines(lines)
                    target_file.write(f"\n")
                    target_file.write(f"COMMIT;\n")
            target_file.write(f"BEGIN;\n")
            formatted_sql_text = self.format_sql("INSERT INTO {schema_name}.dbmigration_versions (version_id, is_baseline) VALUES ({version_id}, TRUE);\n", 
                                                 schema_name=self.get_schema_name(), version_id=version)
            target_file.write(formatted_sql_text)
            for script_path in scripts:
                with open(script_path, 'rb') as f:
                    script_bytes = f.read()
                    relative_script_path = self.script_path_for_log(scripts_dir, script_path)
                    git_blob_sha1 = get_git_blob_sha1_for_bytes(script_bytes)
                    formatted_sql_text = self.format_sql("INSERT INTO {schema_name}.dbmigration_version_scripts (version_id, relative_path, git_blob_sha1) VALUES ({version_id}, {relative_path},{git_blob_sha1});\n", 
                                                        schema_name=self.get_schema_name(), version_id=version,relative_path=relative_script_path,git_blob_sha1=git_blob_sha1)
                    target_file.write(formatted_sql_text)
            target_file.write(f"COMMIT;\n")

    def verify_baseline_scripts(self, scripts_dir, git_cmd_path, git_root_path, target_script_path):
        baseline_dir = scripts_dir.joinpath(BASELINE_DIR_NAME)
        if not baseline_dir.exists():
            print(f"The scripts path '{scripts_dir}' does not include '{BASELINE_DIR_NAME}' subdirectory. ")
            return
        if self.check_if_version_table_include_baseline_version():
            installed_baseline_version = self.get_baseline_version_installed()
            print(f"The target schema has the baseline version installed: {installed_baseline_version}")
            return
        baseline_subdirs = [item for item in baseline_dir.iterdir() if item.is_dir()]
        if len(baseline_subdirs) != 1:
            raise CommandError(f"The baseline path {baseline_dir} must have single subdirectory with the baseline scripts but {len(baseline_subdirs)} was found")
        baseline_version_subdir = baseline_subdirs[0]
        baseline_version = baseline_version_subdir.name

        scripts_sorted = self.get_sorted_scripts_from_dir(baseline_version_subdir, BASELINE_FILES_DEPTH)
        print(f"The baseline scripts to install: ")
        self.display_verification_changes(scripts_dir, git_cmd_path, git_root_path, scripts_sorted)

        if not target_script_path is None:
            self.write_baseline_scripts(baseline_version, scripts_dir, scripts_sorted, target_script_path)
        
        # remember latest version in scripts for the further use in verify_repeatable()
        self.latest_version_in_scripts = baseline_version

    def write_versioned_scripts(self, version, scripts_dir, scripts, target_script_path):
        with pathlib.Path(target_script_path).open("a") as target_file:
            formatted_sql_text = self.format_sql("-- Versioned scripts for version {version_id}\n", version_id=version)
            target_file.write(formatted_sql_text)
            target_file.write(f"BEGIN;\n")
            for script_path in scripts:
                with script_path.open("r", encoding="utf-8-sig", errors="ignore") as source_file:
                    lines = source_file.readlines()
                    relative_script_path = self.script_path_for_log(scripts_dir, script_path)
                    formatted_sql_text = self.format_sql("--{script_path}\n", script_path=str(relative_script_path))
                    target_file.write(formatted_sql_text)
                    target_file.writelines(lines)
                    target_file.write(f"\n")
            formatted_sql_text = self.format_sql("INSERT INTO {schema_name}.dbmigration_versions (version_id, is_baseline) VALUES ({version_id}, FALSE);\n", 
                                                 schema_name=self.get_schema_name(), version_id=version)
            target_file.write(formatted_sql_text)
            for script_path in scripts:
                with open(script_path, 'rb') as f:
                    script_bytes = f.read()
                    relative_script_path = self.script_path_for_log(scripts_dir, script_path)
                    git_blob_sha1 = get_git_blob_sha1_for_bytes(script_bytes)
                    formatted_sql_text = self.format_sql("INSERT INTO {schema_name}.dbmigration_version_scripts (version_id, relative_path, git_blob_sha1) VALUES ({version_id}, {relative_path},{git_blob_sha1});\n", 
                                                        schema_name=self.get_schema_name(), version_id=version,relative_path=relative_script_path,git_blob_sha1=git_blob_sha1)
                    target_file.write(formatted_sql_text)
            target_file.write(f"COMMIT;\n")

    def verify_versioned_scripts(self, scripts_dir, git_cmd_path, git_root_path, target_script_path):
        versioned_dir = scripts_dir.joinpath(VERSIONED_DIR_NAME)
        if not versioned_dir.exists():
            print(f"The scripts path '{scripts_dir}' does not include '{VERSIONED_DIR_NAME}' subdirectory.")
            return
        versioned_subdirs = [item for item in versioned_dir.iterdir() if item.is_dir()]
        if len(versioned_subdirs) == 0:
            raise CommandError(f"The versioned scripts path {versioned_dir} must have at least one subdirectory but nothing was found")
        latest_installed_version = None 
        newer_version_subdirs = []
        try:
            latest_installed_version = self.get_latest_version_installed()
            newer_version_subdirs = [x for x in versioned_subdirs if x.name > latest_installed_version]
        except CommandError:
            newer_version_subdirs = versioned_subdirs

        if len(newer_version_subdirs) == 0:
            print(f"The latest version installed is {latest_installed_version}. No newer scripts were found for installation.")       
            return
        newer_version_subdirs_sorted = sorted(newer_version_subdirs)
        
        # remember latest version in scripts for the further use in verify_repeatable()
        latest_version = newer_version_subdirs_sorted[-1].name

        if self.latest_version_in_scripts is None:
            self.latest_version_in_scripts = latest_version
        elif latest_version > self.latest_version_in_scripts:
            self.latest_version_in_scripts = latest_version
        else:
            raise CommandError(f"The latest version of the subdirectory with the versions'{latest_version}' must be greater than the version of the baseline scripts '{self.latest_version_in_scripts}'.")

        print(f"The versioned scripts to install: ")    
        for script_version_dir in newer_version_subdirs_sorted:    
            scripts_sorted = self.get_sorted_scripts_from_dir(script_version_dir, VERSIONED_FILES_DEPTH)
            if len(scripts_sorted) == 0:
                filters_str = ",".join(self.file_glob_filters)
                raise CommandError(f"The scripts subdirectory '{script_version_dir}' does not include any {filters_str} scripts")
            self.display_verification_changes(scripts_dir, git_cmd_path, git_root_path, scripts_sorted)
            if not target_script_path is None:
                version_id = script_version_dir.name
                self.write_versioned_scripts(version_id, scripts_dir, scripts_sorted, target_script_path)   

    def write_repeatable_scripts(self, target_version, scripts_dict, scripts_dir, target_script_path):
        with pathlib.Path(target_script_path).open("a") as target_file:
            formatted_sql_text = self.format_sql("-- Repeatable scripts for version {version_id}\n", version_id=target_version)
            target_file.write(formatted_sql_text)
            for git_blob_sha1, script_path in scripts_dict.items():
                with script_path.open("r", encoding="utf-8-sig", errors="ignore") as source_file:
                    lines = source_file.readlines()
                    relative_script_path = self.script_path_for_log(scripts_dir, script_path)
                    formatted_sql_text = self.format_sql("--{script_path}\n", script_path=str(relative_script_path))
                    target_file.write(formatted_sql_text)
                    target_file.write(f"BEGIN;\n")
                    target_file.writelines(lines)
                    target_file.write(f"\n")
                    formatted_sql_text = self.format_sql("INSERT INTO {schema_name}.dbmigration_repeatable_scripts (git_blob_sha1, version_id, relative_path) VALUES ({git_blob_sha1}, {version_id}, {relative_path});\n", 
                                                         schema_name=self.get_schema_name(), git_blob_sha1=git_blob_sha1, version_id=target_version, relative_path=str(relative_script_path))
                    target_file.write(formatted_sql_text)
                    target_file.write(f"COMMIT;\n")

    def verify_repeatable_scripts(self, scripts_dir, git_cmd_path, git_root_path, target_script_path):
        repeatable_dir = scripts_dir.joinpath(REPEATABLE_DIR_NAME)
        if not repeatable_dir.exists():
            print(f"The scripts path '{scripts_dir}' does not include '{REPEATABLE_DIR_NAME}' subdirectory.")
            return
        target_version_file_path = repeatable_dir.joinpath(TARGET_VERSION_FILE)
        if not target_version_file_path.exists():
            raise CommandError(f"The file with target version '{TARGET_VERSION_FILE}' does not exists in repeatable scripts subdirectory '{repeatable_dir}'.")
        target_version = read_as_trimmed_string(target_version_file_path)
        latest_installed_version = None 
        try:
            latest_installed_version = self.get_latest_version_installed()
        except CommandError:
            print(f"No versions were installed in the database schema.")

        self.cross_check_of_the_target_version_for_repeatable_scripts(target_version, self.latest_version_in_scripts, latest_installed_version)

        repeatable_scripts_sorted = self.get_sorted_scripts_from_dir(repeatable_dir, REPEATABLE_FILES_DEPTH)
        print(f"The target version for repeatable scripts is {target_version}.")
        scripts_to_repeat = []
        for script_path in repeatable_scripts_sorted:
            with open(script_path, 'rb') as f:
                script_bytes = f.read()
                git_blob_sha1 = get_git_blob_sha1_for_bytes(script_bytes)
                relative_script_path = self.script_path_for_log(scripts_dir, script_path)
                if not self.check_if_repeatable_script_installed(git_blob_sha1, target_version, relative_script_path):
                    scripts_to_repeat.append(script_path)
        if len(scripts_to_repeat) == 0:
            print(f"No changed repeatable scripts found for (re)installation.")
            return
        print(f"The repeatable scripts to (re)install: ")
        scripts_to_repeat = self.resolve_scripts_dependencies(repeatable_dir, REPEATABLE_FILES_DEPTH, repeatable_scripts_sorted, scripts_to_repeat)
        self.display_verification_changes(scripts_dir, git_cmd_path, git_root_path, scripts_to_repeat)
        if not target_script_path is None:
            scripts_to_repeat_dict = {}
            for script_path in scripts_to_repeat:
                with open(script_path, 'rb') as f:
                    script_bytes = f.read()
                    git_blob_sha1 = get_git_blob_sha1_for_bytes(script_bytes)
                    scripts_to_repeat_dict[git_blob_sha1] = script_path
            self.write_repeatable_scripts(target_version, scripts_to_repeat_dict, scripts_dir, target_script_path)

    def run(self):
        self.make_dbconn_session_readonly()
        self.do_initial_cross_checks()        
        self.check_if_all_own_migrations_are_applied()
        self.check_if_all_version_control_tables_exists();
        self.check_if_stored_environment_id_matches_to_scripts_dir() 
        self.check_if_max_version_of_versioned_scripts_matches_repeatable_target(self.scripts_dir)

        git_root_path = None
        git_cmd_path = None
        if not self.args.skip_git_checks:
            git_cmd_path = self.try_get_git_cmd_path(self.config)
            if not git_cmd_path is None:
                git_root_path = self.try_get_git_repo_root(git_cmd_path, self.scripts_dir)

        script_path = None
        temp_script_path = None
        if not self.args.build_update_script is None:
            self.check_if_target_script_file_path_accessible_for_write(self.args.build_update_script)
            script_path = pathlib.Path(self.args.build_update_script)
            temp_script_path = script_path.with_suffix(script_path.suffix + ".tmp")
            search_path = self.get_search_path_for_scripts()
            self.write_search_path(search_path, temp_script_path)      
        try:            
            self.verify_baseline_scripts(self.scripts_dir, git_cmd_path, git_root_path, temp_script_path)
            self.verify_versioned_scripts(self.scripts_dir, git_cmd_path, git_root_path, temp_script_path)
            self.verify_repeatable_scripts(self.scripts_dir, git_cmd_path, git_root_path, temp_script_path)
            if git_root_path:
                self.display_recent_changes_grouped_by_git_commits(git_cmd_path, git_root_path, 100, 30)
            # finalize writing update script
            if not temp_script_path is None:
                if temp_script_path.exists():
                    temp_script_path.replace(script_path)
                    print(f"The update script is written to '{script_path}'.")
                else:
                    print(f"Nothing to write to update script '{script_path}'")
                
        except Exception:
            if not script_path is None and script_path.exists():
                script_path.unlink()
            if not temp_script_path is None and temp_script_path.exists():
                temp_script_path.unlink()
            raise

class InitCommand (BaseCommand):
    """Creates version control tables in an empty database schema."""

    def check_if_schema_is_empty(self):
        sql = """
            SELECT count(*)
            FROM pg_class c
            JOIN pg_namespace s ON s.oid = c.relnamespace
            WHERE s.nspname = %s
            AND s.nspname NOT IN ('pg_catalog', 'information_schema')
            AND s.nspname NOT LIKE 'pg_toast%%'
            AND s.nspname NOT LIKE 'pg_temp%%'
        """
        value = self.dbconn_get_single_value(sql, (self.args.schema_name,))
        if value is None:
            raise CommandError(f"Unable to check whether target schema exists because the query returned nothing: '{sql}' ")
        return (value == 0)
    
    def create_version_tracking_tables(self, environment_id):
        sql_script = """
            BEGIN;

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

            -- insert environment id
            INSERT INTO {schema_name}.dbmigration_environment_id (id, is_singleton) VALUES ({environment_id_str}, TRUE);

            COMMIT;
        """        
        with self.dbconn.cursor() as cur:
            formatted_sql = self.format_sql(sql_script, schema_name=self.get_schema_name(), environment_id_str=environment_id)
            cur.execute(formatted_sql)

    def __init__(self, config, subparsers): 
        super().__init__(config, subparsers, "init", InitCommand.__doc__)
        self.parser.add_argument("scripts_path", type=str, help="source scripts repository path")
        self.parser.add_argument("--force-init",  action="store_true", default=False, help="Force create version control tables even on non empty schema")

    def run(self):
        if not self.check_if_schema_exists():
            raise CommandError(f"The target schema '{self.args.schema_name}' is not accessible")
        self.set_session_search_path(self.args.schema_name)

        if not self.check_if_schema_is_empty():
            if not self.args.force_init:
                raise CommandError(f"The target schema '{self.args.schema_name}' must be empty")
            self.check_if_all_version_control_tables_does_not_exists()
            print(f"WARNING: Schema is not empty!")

        environment_id = self.get_scripts_path_environment_id()

        print(f"Creating the version control tables with environment ID: '{environment_id}'")
        self.create_version_tracking_tables(environment_id)
        print(f"Created.")

class TestFailed(Exception):
    """A unit test error."""
    def __init__(self, message):
        super().__init__(message)

class RunTestsCommand (BaseCommand):
    """Runs db unit test scripts to the target database schema."""

    def run_conditional(self, cursor, scripts_dir, script_path, script_text):
        path = pathlib.Path(script_path)
        file_name = path.name
        relative_script_path = self.script_path_for_log(scripts_dir, script_path)
        print(f"Running test: '{relative_script_path}'...", end="", flush=True)
        if file_name.startswith(IS_TRUE_THAT_TEST_PREFIX):
            cursor.execute(script_text)
            result_number = 0
            for results in cursor.results():
                result_number += 1
                if cursor.rowcount > 0:
                    row = cursor.fetchone()
                    value = row[0] if not row is None else False
                    if not value:
                        raise TestFailed(f"({result_number}) Expected true, got {value}!") 
        elif file_name.startswith(DETECT_MISSING_TEST_PREFIX):
            has_failed = False
            cursor.execute(script_text)
            result_number = 0
            for results in cursor.results():
                result_number += 1
                if cursor.rowcount > 0:
                    columns = [desc[0] for desc in cursor.description]
                    print(f"FAIL. ({result_number}) Missing records:")
                    print("=================================")
                    for row in cursor:
                        items = [f"{k}: {v}" for k, v in zip(columns, row)]
                        line = ", ".join(items)
                        print(line)
                    has_failed = True
            if has_failed:
                raise TestFailed(f"Expected no results!")
        elif file_name.startswith(ASSURE_THAT_TEST_PREFIX):
            cursor.execute(script_text)
        else:
            raise TestFailed(f"Unable to detect test type from script name '{file_name}'. It should start with one of the following prefixes: '{IS_TRUE_THAT_TEST_PREFIX}','{DETECT_MISSING_TEST_PREFIX}','{ASSURE_THAT_TEST_PREFIX}'")
        print(f"PASS")

    def is_subpath_of(self, child, parent):
        child_parts = pathlib.Path(child).absolute().parts
        parent_parts = pathlib.Path(parent).absolute().parts        
        return child_parts[:len(parent_parts)] == parent_parts
    
    def make_savepoint_id(self, folder):
        hash_str = str(hash(folder))
        return psycopg.sql.Identifier("savepoint_" + hash_str)

    def run_test_scripts_each_in_own_tran(self, scripts_dir, scripts):
        self.fail_count = 0
        self.pass_count = 0
        with self.dbconn.cursor() as cur:
            cur.execute("BEGIN") # start global tran for tests
            setup_folder_stack = []            
            for script_path in scripts:
                with open(script_path, 'rt', encoding=self.file_read_encoding, errors=self.file_read_encoding_errors) as f:
                    script_text = f.read()
                    script_name = script_path.name

                    if len(setup_folder_stack) > 0:
                        script_folder = str(script_path.absolute().parent)
                        latest_item = setup_folder_stack[-1]
                        if not self.is_subpath_of(script_folder, latest_item):
                            setup_folder_stack.pop()
                            savepoint_id = self.make_savepoint_id(setup_folder)
                            formatted_sql = self.format_sql("ROLLBACK TO SAVEPOINT {savepoint_id}", savepoint_id=savepoint_id)
                            cur.execute(formatted_sql)
                            print(f"Rolled back to savepoint.")
                    
                    if script_name == SETUP_TESTS_FILE_NAME:
                        setup_folder = str(script_path.absolute().parent)
                        setup_folder_stack.append(setup_folder)
                        savepoint_id = self.make_savepoint_id(setup_folder)
                        formatted_sql = self.format_sql("SAVEPOINT {savepoint_id}", savepoint_id=savepoint_id)
                        print(f"Make savepoint...")
                        cur.execute(formatted_sql)
                        relative_script_path = self.script_path_for_log(scripts_dir, script_path)
                        print(f"Running setup: '{relative_script_path}'...", end="", flush=True)
                        cur.execute(script_text)
                        print(f"DONE")
                        continue
                    else:
                        cur.execute("SAVEPOINT savepoint_test_boundary")
                        try:
                            self.run_conditional(cur, scripts_dir, script_path, script_text)
                            self.pass_count += 1
                        except TestFailed as e:
                            self.fail_count += 1
                            print(f"FAIL.", e)
                        except Exception as e:
                            self.fail_count += 1
                            error_type_name = type(e).__name__ 
                            print(f"FAIL. {error_type_name}:", e)
                        cur.execute("ROLLBACK TO SAVEPOINT savepoint_test_boundary")

            cur.execute("ROLLBACK") # rollback global tran for tests

    def __init__(self, config, subparsers):       
        super().__init__(config, subparsers, "run-tests", RunTestsCommand.__doc__)
        self.parser.add_argument("scripts_path", type=str, help="source scripts repository path")
        self.parser.add_argument("--skip-env-checks",  action="store_true", default=False, help="Skip version and environment ID checks to run tests in any plain environment not made by the tool itself")
    
    def __enter__(self):
        self.use_run_tests_by_user = True
        super().__enter__()

    def run_unit_test_scripts(self, scripts_dir):
        unit_tests_dir = scripts_dir.joinpath(TESTS_DIR_NAME)
        if not unit_tests_dir.exists():
            raise CommandError(f"The scripts path '{scripts_dir}' does not include '{TESTS_DIR_NAME}' subdirectory.")

        if not self.args.skip_env_checks:
            target_version_file_path = unit_tests_dir.joinpath(TARGET_VERSION_FILE)
            if not target_version_file_path.exists():
                raise CommandError(f"The file with target version '{TARGET_VERSION_FILE}' does not exists in unit tests scripts subdirectory '{unit_tests_dir}'.")
            target_version = read_as_trimmed_string(target_version_file_path)
            latest_installed_version = self.get_latest_version_installed() 
            if latest_installed_version != target_version:
                raise CommandError(f"The target version {target_version} for unit test scripts does not match the latest installed version {latest_installed_version}.")                  
            print(f"Target version matches the latest installed version '{target_version}'")

        scripts_sorted = self.get_sorted_scripts_from_dir(unit_tests_dir, TESTS_FILES_DEPTH)        
        self.run_test_scripts_each_in_own_tran(scripts_dir, scripts_sorted)
        if self.fail_count > 0:
            raise CommandError(f"Tests failed: {self.fail_count}, passed: {self.pass_count}.")
        else:
            print(f"All {self.pass_count} tests passed.")
            

    def run(self):
        self.do_initial_cross_checks()
        if not self.args.skip_env_checks:
            self.check_if_all_own_migrations_are_applied()
            self.check_if_all_version_control_tables_exists() 
            self.check_if_stored_environment_id_matches_to_scripts_dir()    
        print(f"Running unit tests for scripts repository: '{self.scripts_dir}'")
        self.run_unit_test_scripts(self.scripts_dir)

def read_toml_config():
    script_dir = pathlib.Path(__file__).absolute().parent
    target_path = script_dir.joinpath(TOML_CONFIG_FILE)
    with open(target_path, 'rb') as f:
        config = tomllib.load(f)
        return config

def main():
    try:
        config = read_toml_config()

        parser = argparse.ArgumentParser(description=__doc__)    
        subparsers = parser.add_subparsers(dest="cmd", help="Available subcommands")

        UpdateCommand(config, subparsers)
        VerifyCommand(config, subparsers)
        InitCommand(config, subparsers)
        RunTestsCommand(config, subparsers)

        # Parse arguments
        args = parser.parse_args()

        # Call the function associated with the subcommand
        if hasattr(args, 'call'):
            args.call(args)
        else:
            # If no subcommand is given, print help (or handle as needed)
            parser.print_help()
        return 0
    except CommandError as e:    
        error_type_name = type(e).__name__ 
        print(f"Command error:", e)
        return 1
    except Exception as e:
        error_type_name = type(e).__name__ 
        print(f"Error: {error_type_name}:", e)
        return 1

if __name__ == "__main__":
    sys.exit(main())

