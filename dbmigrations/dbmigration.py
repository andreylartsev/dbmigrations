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
import psycopg;
import subprocess;

TOML_CONFIG_FILE = 'dbmigration.toml'
DBCONN_CONFIG_GROUP = 'dbconnection'
OPTIONS_CONFIG_GROUP = "options"

OPTIONS_DEFAULT_FILE_GLOB_FILTERS = ["*.sql", "*.dump"]
OPTIONS_DEFAULT_FILE_READ_ENCODING = "utf-8"
OPTIONS_DEFAULT_FILE_READ_ENCODING_ERRORS = "ignore"

DBCONN_DEFAULT_HOST = 'localhost'
DBCONN_DEFAULT_PORT = 5432
DBCONN_DEFAULT_USER = None
DBCONN_DEFAULT_DBNAME = 'postgres'
DBCONN_USER_PASSWORD_ENVVAR_NAME = "USER_PASSWORD"


BASELINE_DIR_NAME = "baseline"
VERSIONED_DIR_NAME = "versions"
REPEATABLE_DIR_NAME = "repeatable"
REPEATABLE_SCRIPTS_TARGET_VERSION_FILE = "target_version.txt"
SCRIPT_LIST_FILE_NAME = "script_list.txt"

TOOLS_CONFIG_GROUP = "tools"
TOOL_EXEC_ATTRIBUTE = "executable"
TOOL_ARGS_ATTRIBUTE = "args"
TOOL_SUCCESS_RESULT_CODE_ATTRIBUTE = "success_result_code"
USE_TOOL_NAME_FILE_NAME = "use_tool.txt"

VERSION_CLEANUP_FILE_NAME = "_cleanup.sql"

SEARCH_PATH_FILE_NAME = "set_search_path.txt"

class CommandError(Exception):
    """A critical command error terminated the command execution."""
    def __init__(self, message):
        super().__init__(message)

def get_sha256sum_for_bytes(script_bytes):
    hash_object = hashlib.sha256(script_bytes)
    hex_dig = hash_object.hexdigest()
    return hex_dig


def read_as_trimmed_string(file_path):
    with open(file_path, 'rb') as f:
        bytes = f.read()
        str = bytes.decode("utf-8", "ignore")
        trimmed_str = str.strip()
        return trimmed_str

def log_server_notices(diag):
    print(f"Server notice: {diag.severity} - {diag.message_primary}")

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

    def __init__(self, tool_name, schema_name, toml_config):
        self.tool_name = tool_name
        self.schema_name = schema_name
        if not DBCONN_CONFIG_GROUP in toml_config:
            raise CommandError(f"There is no configuration group '{DBCONN_CONFIG_GROUP}' in the configuration file '{TOML_CONFIG_FILE}'.")
        self.dbconn_config = toml_config[DBCONN_CONFIG_GROUP]
        
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
            text=True)
        # Read and print output line by line as it happens
        for line in iter(process.stdout.readline, ''):
            print(line, end='') 
        result_code = process.wait() # Ensure process finishes

        if result_code != self.success_result_code:
            raise CommandError(f"The tool '{self.tool_name}' returned unsuccessful result code {result_code}!")

class BaseCommand:

    def check_if_migration_to_add_version_id_to_repeatable_table_is_required(self):
        sql = """
            SELECT NOT EXISTS (
                SELECT 1 
                FROM information_schema.columns 
                WHERE table_schema = %s 
                AND table_name   = 'dbmigration_repeatable' 
                AND column_name  = 'version_id' 
            ); 
        """
        result = self.dbconn_get_single_value(sql, (self.args.schema_name,))
        return result


    def migration_to_add_version_id_to_repeatable_table(self):
        sql = """
            DO $$
            DECLARE 
                pk_name VARCHAR(128);
            BEGIN
                RAISE NOTICE 'Start migration of table dbmigration_repeatable to add column version_id.';

                ALTER TABLE {schema_name_identity}.dbmigration_repeatable ADD COLUMN version_id VARCHAR(64) NOT NULL DEFAULT '0';
                
                UPDATE {schema_name_identity}.dbmigration_repeatable SET version_id = (SELECT MAX(version_id) FROM {schema_name_identity}.dbmigration_versions);
                
                SELECT constraint_name INTO pk_name
                FROM information_schema.table_constraints 
                WHERE table_schema = {schema_name_str}
                    AND table_name = 'dbmigration_repeatable' 
                    AND constraint_type = 'PRIMARY KEY';

                IF pk_name IS NOT NULL THEN
                    EXECUTE 'ALTER TABLE {schema_name_identity}.dbmigration_repeatable DROP CONSTRAINT ' || quote_ident(pk_name);
                END IF;

                ALTER TABLE {schema_name_identity}.dbmigration_repeatable ADD PRIMARY KEY (sha256sum, version_id);
                
                RAISE NOTICE 'The column version_id has been added.';
            END
            $$;
        """
        formatted_sql = self.format_sql(sql, schema_name_identity=self.get_schema_name(), schema_name_str=self.args.schema_name)
        self.dbconn_exec_with_no_result(formatted_sql, [])

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

    def walk_through_dir_sorted(self, dir):
        exclusions = [
            VERSION_CLEANUP_FILE_NAME, 
            USE_TOOL_NAME_FILE_NAME, 
            REPEATABLE_SCRIPTS_TARGET_VERSION_FILE]
        exclusions_set = set(exclusions)
        start_path = pathlib.Path(dir) 
        if not start_path.exists():
            raise CommandError(f"The folder '{dir}' does not exists")
        if not start_path.is_dir():
            raise CommandError(f"The path '{dir}' is not a directory")        
        script_list_file_path = start_path.joinpath(SCRIPT_LIST_FILE_NAME)
        sorted_files = []
        if script_list_file_path.exists():
            sorted_files = []
            with script_list_file_path.open("r") as script_list_file:
                lines = script_list_file.readlines()
                for line in lines:
                    trimmed_str = line.strip()
                    script_path = start_path.joinpath(trimmed_str)
                    if not script_path.exists():
                        raise CommandError(f"The file '{trimmed_str}' specified in script list file '{script_list_file_path}' does not exists") 
                    if not script_path.is_file():
                        raise CommandError(f"The file '{trimmed_str}' specified in script list file '{script_list_file_path}' is not a file") 
                    sorted_files.append(script_path)
        else:
            all_files = []
            for glob_filter in self.file_glob_filters:
                all_items = start_path.rglob(glob_filter)
                for item in all_items: 
                    if item.is_file() and not item.name in exclusions_set:
                        all_files.append(item)
            sorted_files = sorted(all_files)
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
    def dbconn_exec_with_no_result(self, sql, params):
        with self.dbconn.cursor() as cur:
            cur.execute(sql, params)

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
    
    def get_search_path_for_scripts(self):
        scripts_dir = pathlib.Path(self.args.scripts_path)
        set_search_path_file = scripts_dir.joinpath(SEARCH_PATH_FILE_NAME)
        if not set_search_path_file.exists():
            print(f"No file '{SEARCH_PATH_FILE_NAME}' within scripts directory '{self.args.scripts_path}', using current schema name '{self.args.schema_name}'")
            return self.args.schema_name
        if not set_search_path_file.is_file():
            raise CommandError(f"The search path file '{SEARCH_PATH_FILE_NAME}' within scripts directory '{self.args.scripts_path}' is not a file")
        trimmed_str = read_as_trimmed_string(set_search_path_file)
        print(f"Using '{trimmed_str}' as a session 'search_path'")
        return trimmed_str
    
    def set_session_search_path(self, search_path):
        sql = f"""
            SELECT pg_catalog.set_config('search_path', %s, false)"""
        result = self.dbconn_get_single_value(sql, (search_path,))
        if result != search_path:
            raise CommandError(f"Unexpected value '{result}' returned on attempt to set the search path")

    def check_if_version_table_exists(self, table_name):
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

    def check_if_repeatable_script_installed(self, sha256sum, version):
        sql = """
            SELECT EXISTS (
                SELECT 1
                FROM {schema_name}.dbmigration_repeatable
                WHERE sha256sum = %s 
                  AND version_id = %s)"""        
        formatted_sql = self.format_sql(sql, schema_name=self.get_schema_name())
        value = self.dbconn_get_single_value(formatted_sql, (sha256sum, version))
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
            target_version_file_path = repeatable_dir.joinpath(REPEATABLE_SCRIPTS_TARGET_VERSION_FILE)
            if target_version_file_path.exists():
                target_version_in_repeatable = read_as_trimmed_string(target_version_file_path)
        if target_version_in_repeatable is None:
            print(f"No any repeatable scripts were found in scripts dir: '{scripts_dir}'")
            return 
        if latest_version_in_scripts != target_version_in_repeatable:
            raise CommandError(f"The target version for repeatable scripts '{target_version_in_repeatable}' does not match the latest version in versioned scripts '{latest_version_in_scripts}'")
        print(f"Completed.")

    def do_initial_cross_checks(self):
        self.scripts_dir = pathlib.Path(self.args.scripts_path)        
        if not self.scripts_dir.exists():
            raise CommandError(f"The scripts repository path '{self.args.scripts_path}' does not exists")       
        if not self.check_if_schema_exists():
            raise CommandError(f"The target schema '{self.args.schema_name}' is not accessible")
        search_path = self.get_search_path_for_scripts()
        self.set_session_search_path(search_path)     
        if not self.check_if_version_table_exists("dbmigration_versions"):
            raise CommandError(f"The schema '{self.args.schema_name}' does not include version control table 'dbmigration_versions'")
        if not self.check_if_version_table_exists("dbmigration_repeatable"):
            raise CommandError(f"The schema '{self.args.schema_name}' does not include repeatable scripts control table 'dbmigration_repeatable'")

    def __init__(self, config, subparsers, command_name, command_help):
        self.config = config        
        try:        
            self.dbconn_settings = config[DBCONN_CONFIG_GROUP]
        except:
            raise CommandError(f"Configuration file {TOML_CONFIG_FILE} does not contain configuration group '{DBCONN_CONFIG_GROUP}'")
        try:        
            self.options = config[OPTIONS_CONFIG_GROUP]
        except:
            raise CommandError(f"Configuration file {TOML_CONFIG_FILE} does not contain configuration group '{OPTIONS_CONFIG_GROUP}'")  
    
        self.file_read_encoding =  self.options.get("file_read_encoding", OPTIONS_DEFAULT_FILE_READ_ENCODING)
        self.file_read_encoding_errors =  self.options.get("file_read_encoding_errors", OPTIONS_DEFAULT_FILE_READ_ENCODING_ERRORS)
        self.file_glob_filters =  self.options.get("file_glob_filters", OPTIONS_DEFAULT_FILE_GLOB_FILTERS)
        
        self.parser = subparsers.add_parser(command_name, help=command_help)
        self.parser.add_argument("schema_name", type=str, help="the name of target database schema")
        self.parser.add_argument("--host", type=str, default=self.dbconn_settings.get("host", DBCONN_DEFAULT_HOST), help="db server host name")
        self.parser.add_argument("--port", type=int, default=self.dbconn_settings.get("port", DBCONN_DEFAULT_PORT), help="db server port")
        self.parser.add_argument("--dbname", type=str, default=self.dbconn_settings.get("dbname", DBCONN_DEFAULT_DBNAME), help="database name")
        self.parser.add_argument("--user", type=str, default=self.dbconn_settings.get("user", DBCONN_DEFAULT_USER), help="user name")
        self.parser.add_argument("-n","--no-password",  action="store_true", default=self.options.get("no_password", False), help="dont ask user password")
        self.parser.set_defaults(call=self) 
    def __enter__(self):
        if not self.args.host is None:
            self.dbconn_settings["host"]=self.args.host
        if not self.args.port is None:
            self.dbconn_settings["port"]=self.args.port
        if not self.args.user is None:
            self.dbconn_settings["user"]=self.args.user
        if not self.args.dbname is None:
            self.dbconn_settings["dbname"]=self.args.dbname
        if not self.args.no_password:
            password = os.getenv(DBCONN_USER_PASSWORD_ENVVAR_NAME)
            if password is None:
                raise CommandError(f"The database user password must be specified via the environment variable '{DBCONN_USER_PASSWORD_ENVVAR_NAME}'.")
            self.dbconn_settings["password"]=password
        else:
            self.dbconn_settings.pop("password", None)
        self.dbconn = psycopg.connect(**self.dbconn_settings)
        print(f"Opened db connection")
        self.dbconn.add_notice_handler(log_server_notices)
        self.dbconn.autocommit = True 

    def __exit__(self, exc_type, exc_value, traceback):
        if not exc_type is None:
            self.dbconn.rollback()
            print(f"Rolled back transaction")
        if not self.dbconn is None:
            self.dbconn.close()
            print(f"Closed db connection")
        return False # propagate the exception
    def run(self):
        pass
    def __call__(self, args):
        self.args = args
        with self:
            self.run()

class UpdateCommand (BaseCommand):
    """Applies base, versioned, and repeatable scripts to the target database schema."""

    def run_baseline_scripts_with_external_tool(self, version, scripts, tool):
         print(f"Running baseline scripts with external tool '{tool.exec_path}'")
         with self.dbconn.cursor() as cur:
            for script_path in scripts:
                print(f"Running script: '{script_path}'...")
                tool.run(script_path)
            print(f"Setting the baseline version '{version}'...")
            cur.execute("BEGIN")
            formatted_sql = self.format_sql("INSERT INTO {schema_name}.dbmigration_versions (version_id, is_baseline) VALUES (%s, %s)", 
                                            schema_name=self.get_schema_name())                                  
            cur.execute(formatted_sql, (version, True))       
            cur.execute("COMMIT")       
            print(f"Committed transaction")

    def run_baseline_scripts_each_in_own_tran(self, version, scripts):
         with self.dbconn.cursor() as cur:
            for script_path in scripts:
                with open(script_path, 'rt', encoding=self.file_read_encoding, errors=self.file_read_encoding_errors) as f:
                    print(f"Running script: '{script_path}'...")
                    script_text = f.read()
                    cur.execute("BEGIN")
                    print(f"Begin transaction")
                    cur.execute(script_text)                                  
                    cur.execute("COMMIT")       
                    print(f"Committed transaction")
            print(f"Setting the baseline version '{version}'...")
            cur.execute("BEGIN")
            formatted_sql = self.format_sql("INSERT INTO {schema_name}.dbmigration_versions (version_id, is_baseline) VALUES (%s, %s)", 
                                            schema_name=self.get_schema_name())                                  
            cur.execute(formatted_sql, (version, True))       
            cur.execute("COMMIT")       
            print(f"Committed transaction")

    def rerun_versioned_scripts(self, version, scripts):
        with self.dbconn.cursor() as cur:
            cur.execute("BEGIN")
            print(f"Begin transaction")
            formatted_sql = self.format_sql("DELETE FROM {schema_name}.dbmigration_versions WHERE version_id=%s", schema_name=self.get_schema_name())
            cur.execute(formatted_sql, (version,))    
            for script_path in scripts:
                with open(script_path, 'rt', encoding=self.file_read_encoding, errors=self.file_read_encoding_errors) as f:
                    print(f"Running script: '{script_path}'...")
                    script_text = f.read()
                    cur.execute(script_text)                                  
            formatted_sql = self.format_sql("INSERT INTO {schema_name}.dbmigration_versions (version_id, is_baseline) VALUES (%s, %s)", 
                                            schema_name=self.get_schema_name())                                  
            cur.execute(formatted_sql, (version, False))       
            cur.execute("COMMIT")       
            print(f"Committed transaction")

    def run_versioned_scripts_in_tran(self, version, scripts):
        with self.dbconn.cursor() as cur:
            cur.execute("BEGIN")
            print(f"Begin transaction")
            for script_path in scripts:
                with open(script_path, 'rt', encoding=self.file_read_encoding, errors=self.file_read_encoding_errors) as f:
                    print(f"Running script: '{script_path}'...")
                    script_text = f.read()
                    cur.execute(script_text)
            formatted_sql = self.format_sql("INSERT INTO {schema_name}.dbmigration_versions (version_id, is_baseline) VALUES (%s, %s)", 
                                            schema_name=self.get_schema_name())                                  
            cur.execute(formatted_sql, (version, False))       
            cur.execute("COMMIT")       
            print(f"Committed transaction")


    def __init__(self, config, subparsers): 
        super().__init__(config, subparsers, "update", UpdateCommand.__doc__)
        self.parser.add_argument("--force-reapply-latest-version",  action="store_true", default=False, help="cleanup the latest version within database and reapply the included *.sql scripts")
        self.parser.add_argument("scripts_path", type=str, help="source scripts repository path")


    def apply_baseline_scripts(self, scripts_dir):
        baseline_dir = scripts_dir.joinpath(BASELINE_DIR_NAME)
        if not baseline_dir.exists():
            print(f"The scripts path '{scripts_dir}' does not include '{BASELINE_DIR_NAME}' subdirectory. Skip running the baseline scripts.")
            return
        if self.check_if_version_table_include_baseline_version():
            print(f"The target schema already has the baseline version installed. Skip running the baseline scripts.")
            return
        baseline_subdirs = [item for item in baseline_dir.iterdir() if item.is_dir()]
        if len(baseline_subdirs) != 1:
            raise CommandError(f"The baseline path {baseline_dir} must have single subdirectory with the baseline scripts but {len(baseline_subdirs)} was found")
        baseline_version_subdir = baseline_subdirs[0]
        baseline_version = baseline_version_subdir.name
        print(f"The baseline version to install {baseline_version}.")       
        print(f"Apply baseline scripts...")
        scripts_sorted = self.walk_through_dir_sorted(baseline_version_subdir)
        
        external_tool_name = self.try_get_external_tool_name(baseline_version_subdir);
        if not external_tool_name is None:
            tool = ExternalTool(external_tool_name, self.args.schema_name, self.config)
            self.run_baseline_scripts_with_external_tool(baseline_version, scripts_sorted, tool)
        else:
            self.run_baseline_scripts_each_in_own_tran(baseline_version, scripts_sorted)
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
        clean_version_file_path = latest_version_dir.joinpath(VERSION_CLEANUP_FILE_NAME)
        if not clean_version_file_path.exists():
            raise CommandError(f"The version cleanup file '{VERSION_CLEANUP_FILE_NAME}' does not exists in folder {str(latest_version_dir)}")
        if not clean_version_file_path.is_file():
            raise CommandError(f"The version cleanup file '{VERSION_CLEANUP_FILE_NAME}' is not a file")
        scripts_sorted = self.walk_through_dir_sorted(latest_version_dir)
        if len(scripts_sorted) == 0:
            filters_str = ",".join(self.file_glob_filters)
            raise CommandError(f"The scripts subdirectory '{latest_version_dir}' does not include any '{filters_str}' scripts")
        cleanup_and_reapply_scripts = [clean_version_file_path, *scripts_sorted]
        self.rerun_versioned_scripts(latest_installed_version, cleanup_and_reapply_scripts)       


    def apply_versioned_scripts(self, scripts_dir):
        versioned_dir = scripts_dir.joinpath(VERSIONED_DIR_NAME)
        if not versioned_dir.exists():
            print(f"The scripts path '{scripts_dir}' does not include '{VERSIONED_DIR_NAME}' subdirectory. Skip running the versioned scrips")
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
            print(f"No newer versions were found for installation.")       
            return
        newer_version_subdirs_sorted = sorted(newer_version_subdirs)
        print(f"{len(newer_version_subdirs)} new versions were found for installation.")       
        print(f"Apply versioned scripts...")
        for script_version_dir in newer_version_subdirs_sorted:        
            version_id = script_version_dir.name
            scripts_sorted = self.walk_through_dir_sorted(script_version_dir)
            if len(scripts_sorted) == 0:
                filters_str = ",".join(self.file_glob_filters)
                raise CommandError(f"The scripts subdirectory '{script_version_dir}' does not include any '{filters_str}' scripts")
            self.run_versioned_scripts_in_tran(version_id, scripts_sorted)       
        print(f"The versioned scripts were applied.")

    def apply_repeatable_scripts(self, scripts_dir, force_reapply = False):
        
        repeatable_dir = scripts_dir.joinpath(REPEATABLE_DIR_NAME)
        if not repeatable_dir.exists():
            print(f"The scripts path '{scripts_dir}' does not include '{REPEATABLE_DIR_NAME}' subdirectory. Skip running the repeatable updates")
            return
        print(f"Check repeatable scripts...")       
        target_version_file_path = repeatable_dir.joinpath(REPEATABLE_SCRIPTS_TARGET_VERSION_FILE)
        if not target_version_file_path.exists():
            raise CommandError(f"The file with target version '{REPEATABLE_SCRIPTS_TARGET_VERSION_FILE}' does not exists in repeatable scripts subdirectory '{repeatable_dir}'.")
        target_version = read_as_trimmed_string(target_version_file_path)
        latest_installed_version = self.get_latest_version_installed() 
        if latest_installed_version != target_version:
            raise CommandError(f"The target version {target_version} for repeatable scripts does not match the latest installed version {latest_installed_version}.")                  
        print(f"Target version matches the latest installed version '{target_version}'")
        repeatable_scripts_sorted = self.walk_through_dir_sorted(repeatable_dir)
        scripts_to_repeat = [] 
        for script_path in repeatable_scripts_sorted:
            with open(script_path, 'rb') as f:
                print(f"Checking script '{script_path}' checksum...")
                script_bytes = f.read()
                sha256sum = get_sha256sum_for_bytes(script_bytes)
                script_text = script_bytes.decode(self.file_read_encoding, errors=self.file_read_encoding_errors)
                if force_reapply:
                    scripts_to_repeat.append(script_path)
                    print(f"Script '{script_path}' with checksum '{sha256sum}' will be (re)installed")
                elif not self.check_if_repeatable_script_installed(sha256sum, target_version):
                    scripts_to_repeat.append(script_path)
                    print(f"Script '{script_path}' with checksum '{sha256sum}' will be (re)installed")
                else:
                    print(f"Script with checksum '{sha256sum}' is already installed")        
        if len(scripts_to_repeat) == 0:
            print(f"No any repeatable scripts found for (re)installation")       
            return
        print(f"{len(scripts_to_repeat)} scripts were found to repeat")
        print(f"Apply repeatable scripts...")       
        with self.dbconn.cursor() as cur:
            for script_path in scripts_to_repeat:
                with open(script_path, 'rb') as f:
                    script_bytes = f.read()
                    sha256sum = get_sha256sum_for_bytes(script_bytes)
                    script_text = script_bytes.decode(self.file_read_encoding, errors=self.file_read_encoding_errors)
                    relative_script_path = script_path.relative_to(scripts_dir)
                    print(f"Running script '{script_path}'...")
                    cur.execute("BEGIN")
                    print(f"Begin transaction")
                    if force_reapply:
                        formatted_sql = self.format_sql("DELETE FROM {schema_name}.dbmigration_repeatable WHERE sha256sum = %s AND version_id = %s", 
                                                        schema_name=self.get_schema_name())                                  
                        cur.execute(formatted_sql, (sha256sum, target_version))     
                    cur.execute(script_text)
                    formatted_sql = self.format_sql("INSERT INTO {schema_name}.dbmigration_repeatable (sha256sum, version_id, relative_path) VALUES (%s, %s, %s)", 
                                                    schema_name=self.get_schema_name())                                  
                    cur.execute(formatted_sql, (sha256sum, target_version, str(relative_script_path)))     
                    cur.execute("COMMIT")
                    print(f"Committed transaction.")
        print(f"The repeatable scripts were applied.")       

    def run(self):
        self.do_initial_cross_checks()
        if self.check_if_migration_to_add_version_id_to_repeatable_table_is_required():
            self.migration_to_add_version_id_to_repeatable_table()
        if self.args.force_reapply_latest_version:
            print(f"Performing reapply latest version from scripts repository: '{self.scripts_dir}'")
            self.reapply_the_latest_version(self.scripts_dir)
            self.apply_repeatable_scripts(self.scripts_dir, force_reapply=True)
            print(f"Reapplied.")
        else:
            print(f"Performing updates from scripts repository: '{self.scripts_dir}'")
            self.check_if_max_version_of_versioned_scripts_matches_repeatable_target(self.scripts_dir)
            self.apply_baseline_scripts(self.scripts_dir)
            self.apply_versioned_scripts(self.scripts_dir)
            self.apply_repeatable_scripts(self.scripts_dir, force_reapply=False)
            print(f"Updated.")

class VerifyCommand (BaseCommand):
    """Validates the target schema and lists versioned and reproducible scripts to apply if the 'update' command is executed."""
    
    def make_dbconn_session_readonly(self):
        sql = """
            SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY"""
        self.dbconn_exec_with_no_result(sql, [])

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

    def __init__(self, config, subparsers): 
        super().__init__(config, subparsers, "verify", VerifyCommand.__doc__)
        self.parser.add_argument("--build-update-script", type=str, default=None, help="the update script path if you want one as an additional result of the verify command")
        self.parser.add_argument("scripts_path", type=str, help="source scripts repository path")
        self.latest_version_in_scripts = None

    def write_search_path(self, search_path, target_script_path):
        with pathlib.Path(target_script_path).open("a") as target_file:
            formatted_sql_text = self.format_sql("SELECT pg_catalog.set_config('search_path', {search_path}, false);\n", search_path=search_path)
            target_file.write(formatted_sql_text)

    def write_baseline_scripts(self, version, scripts, target_script_path):
        with pathlib.Path(target_script_path).open("a") as target_file:
            formatted_sql_text = self.format_sql("-- Baseline scripts for version {version_id}\n", version_id=version)
            target_file.write(formatted_sql_text)
            for script_path in scripts:
                with script_path.open("r") as source_file:
                    lines = source_file.readlines()
                    formatted_sql_text = self.format_sql("--{script_path}\n", script_path=str(script_path))
                    target_file.write(formatted_sql_text)
                    target_file.write(f"BEGIN;\n")
                    target_file.writelines(lines)
                    target_file.write(f"\n")
                    target_file.write(f"COMMIT;\n")
            target_file.write(f"BEGIN;\n")
            formatted_sql_text = self.format_sql("INSERT INTO {schema_name}.dbmigration_versions (version_id, is_baseline) VALUES ({version_id}, TRUE);\n", 
                                                 schema_name=self.get_schema_name(), version_id=version)
            target_file.write(formatted_sql_text)
            target_file.write(f"COMMIT;\n")

    def verify_baseline_scripts(self, scripts_dir, target_script_path):
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

        scripts_sorted = self.walk_through_dir_sorted(baseline_version_subdir)
        print(f"The baseline scripts to install: ")       
        for item in scripts_sorted:
            print(f"[{item}]")
        if not target_script_path is None:
            self.write_baseline_scripts(baseline_version, scripts_sorted, target_script_path)
        
        # remember latest version in scripts for the further use in verify_repeatable()
        self.latest_version_in_scripts = baseline_version

    def write_versioned_scripts(self, version, scripts, target_script_path):
        with pathlib.Path(target_script_path).open("a") as target_file:
            formatted_sql_text = self.format_sql("-- Versioned scripts for version {version_id}\n", version_id=version)
            target_file.write(formatted_sql_text)
            target_file.write(f"BEGIN;\n")
            for script_path in scripts:
                with script_path.open("r") as source_file:
                    lines = source_file.readlines()
                    formatted_sql_text = self.format_sql("--{script_path}\n", script_path=str(script_path))
                    target_file.write(formatted_sql_text)
                    target_file.writelines(lines)
                    target_file.write(f"\n")
            formatted_sql_text = self.format_sql("INSERT INTO {schema_name}.dbmigration_versions (version_id, is_baseline) VALUES ({version_id}, FALSE);\n", 
                                                 schema_name=self.get_schema_name(), version_id=version)
            target_file.write(formatted_sql_text)
            target_file.write(f"COMMIT;\n")

    def verify_versioned_scripts(self, scripts_dir, target_script_path):
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
            scripts_sorted = self.walk_through_dir_sorted(script_version_dir)
            if len(scripts_sorted) == 0:
                filters_str = ",".join(self.file_glob_filters)
                raise CommandError(f"The scripts subdirectory '{script_version_dir}' does not include any {filters_str} scripts")
            for item in scripts_sorted:
                print(f"[{item}]")
            if not target_script_path is None:
                version_id = script_version_dir.name
                self.write_versioned_scripts(version_id, scripts_sorted, target_script_path)   

    def write_repeatable_scripts(self, target_version, scripts_dict, target_script_path):
        with pathlib.Path(target_script_path).open("a") as target_file:
            formatted_sql_text = self.format_sql("-- Repeatable scripts for version {version_id}\n", version_id=target_version)
            target_file.write(formatted_sql_text)
            for sha256sum, script_path in scripts_dict.items():
                with script_path.open("r") as source_file:
                    lines = source_file.readlines()
                    formatted_sql_text = self.format_sql("--{script_path}\n", script_path=str(script_path))
                    target_file.write(formatted_sql_text)
                    target_file.write(f"BEGIN;\n")
                    target_file.writelines(lines)
                    target_file.write(f"\n")
                    formatted_sql_text = self.format_sql("INSERT INTO {schema_name}.dbmigration_repeatable (sha256sum, version_id, relative_path) VALUES ({sha256sum}, {version_id}, {relative_path});\n", 
                                                         schema_name=self.get_schema_name(), sha256sum=sha256sum, version_id=target_version, relative_path=str(script_path))
                    target_file.write(formatted_sql_text)
                    target_file.write(f"COMMIT;\n")

    def verify_repeatable_scripts(self, scripts_dir, target_script_path):
        repeatable_dir = scripts_dir.joinpath(REPEATABLE_DIR_NAME)
        if not repeatable_dir.exists():
            print(f"The scripts path '{scripts_dir}' does not include '{REPEATABLE_DIR_NAME}' subdirectory.")
            return
        target_version_file_path = repeatable_dir.joinpath(REPEATABLE_SCRIPTS_TARGET_VERSION_FILE)
        if not target_version_file_path.exists():
            raise CommandError(f"The file with target version '{REPEATABLE_SCRIPTS_TARGET_VERSION_FILE}' does not exists in repeatable scripts subdirectory '{repeatable_dir}'.")
        target_version = read_as_trimmed_string(target_version_file_path)
        latest_installed_version = None 
        try:
            latest_installed_version = self.get_latest_version_installed()
        except CommandError:
            print(f"No any versions are installed in the database schema.")

        self.cross_check_of_the_target_version_for_repeatable_scripts(target_version, self.latest_version_in_scripts, latest_installed_version)

        repeatable_scripts_sorted = self.walk_through_dir_sorted(repeatable_dir)
        print(f"The target version for repeatable scripts is {target_version}.")
        scripts_to_repeat = []
        scripts_to_repeat_dict = {}
        for script_path in repeatable_scripts_sorted:
            with open(script_path, 'rb') as f:
                script_bytes = f.read()
                sha256sum = get_sha256sum_for_bytes(script_bytes)
                if not self.check_if_repeatable_script_installed(sha256sum, target_version):
                    scripts_to_repeat.append(script_path)
                    if not target_script_path is None:
                        scripts_to_repeat_dict[sha256sum] = script_path
        if len(scripts_to_repeat) == 0:
            print(f"No any new repeatable scripts were found for installation")
            return
        print(f"The repeatable scripts to (re)install: ")
        for item in scripts_to_repeat:
            print(f"[{item}]")
        if not target_script_path is None:
            self.write_repeatable_scripts(target_version, scripts_to_repeat_dict, target_script_path)

    def run(self):
        self.make_dbconn_session_readonly()
        self.do_initial_cross_checks()
        if self.check_if_migration_to_add_version_id_to_repeatable_table_is_required():
            raise CommandError(f"It is required to update dbmigration_repeatable table. To apply automatic migration please execute either 'update' of 'init' subcommand with same schema name.")
        self.check_if_max_version_of_versioned_scripts_matches_repeatable_target(self.scripts_dir)

        script_path = None
        temp_script_path = None
        if not self.args.build_update_script is None:
            self.check_if_target_script_file_path_accessible_for_write(self.args.build_update_script)
            script_path = pathlib.Path(self.args.build_update_script)
            temp_script_path = script_path.with_suffix(script_path.suffix + ".tmp")
            search_path = self.get_search_path_for_scripts()
            self.write_search_path(search_path, temp_script_path)      
        try:            
            self.verify_baseline_scripts(self.scripts_dir, temp_script_path)
            self.verify_versioned_scripts(self.scripts_dir, temp_script_path)
            self.verify_repeatable_scripts(self.scripts_dir, temp_script_path)

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
    
    def create_version_tracking_tables(self):
        sql_script = """
            BEGIN;            
            CREATE TABLE {schema_name}.dbmigration_versions (
                version_id VARCHAR(64) NOT NULL,
                is_baseline BOOL NOT NULL DEFAULT FALSE,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                created_by VARCHAR(64) NOT NULL DEFAULT SESSION_USER,
                created_from INET DEFAULT INET_CLIENT_ADDR(),
                CONSTRAINT dbmigration_versions_primary_key PRIMARY KEY(version_id) 
            );
            INSERT INTO {schema_name}.dbmigration_versions (version_id, is_baseline) VALUES ('0', true);
            DELETE FROM {schema_name}.dbmigration_versions WHERE version_id = '0';
            CREATE TABLE {schema_name}.dbmigration_repeatable (
                sha256sum VARCHAR(128) NOT NULL,
                version_id VARCHAR(64) NOT NULL,
                relative_path VARCHAR(2048) NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                created_by VARCHAR(64) NOT NULL DEFAULT SESSION_USER,
                created_from INET DEFAULT INET_CLIENT_ADDR(),
                CONSTRAINT dbmigration_repeatable_primary_key PRIMARY KEY(sha256sum, version_id)
            );
            INSERT INTO {schema_name}.dbmigration_repeatable (sha256sum, version_id, relative_path) VALUES ('0', '0', 'test.sql');
            DELETE FROM {schema_name}.dbmigration_repeatable WHERE sha256sum = '0' and version_id = '0';
            COMMIT;
        """        
        with self.dbconn.cursor() as cur:
            formatted_sql = self.format_sql(sql_script, schema_name=self.get_schema_name())
            cur.execute(formatted_sql)

    def __init__(self, config, subparsers): 
        super().__init__(config, subparsers, "init", InitCommand.__doc__)
        self.parser.add_argument("--force-init",  action="store_true", default=False, help="Force create version control tables even on non empty schema")

    def run(self):
        if not self.check_if_schema_exists():
            raise CommandError(f"The target schema '{self.args.schema_name}' is not accessible")
        self.set_session_search_path(self.args.schema_name)

        if not self.check_if_schema_is_empty():
            if self.check_if_migration_to_add_version_id_to_repeatable_table_is_required():
                self.migration_to_add_version_id_to_repeatable_table()
            if not self.args.force_init:
                raise CommandError(f"The target schema '{self.args.schema_name}' must be empty")
            if self.check_if_version_table_exists("dbmigration_versions"):
                raise CommandError(f"The version control table 'dbmigration_versions' already exists")
            if self.check_if_version_table_exists("dbmigration_repeatable"):
                raise CommandError(f"The version control table 'dbmigration_repeatable' already exists")
            print(f"WARNING: Schema is not empty!")

        print(f"Creating the version control tables...")
        self.create_version_tracking_tables()
        print(f"Created.")

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

        # Parse arguments
        args = parser.parse_args()

        # Call the function associated with the subcommand
        if hasattr(args, 'call'):
            args.call(args)
        else:
            # If no subcommand is given, print help (or handle as needed)
            parser.print_help()
        return 0
    except Exception as e:
        error_type_name = type(e).__name__ 
        print(f"Error: {error_type_name}:", e)
        return 1

if __name__ == "__main__":
    sys.exit(main())

