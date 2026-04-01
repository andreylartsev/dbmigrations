"""
Simple database migrations tool
"""

import sys
import argparse
import tomllib
import os
import pathlib 
import hashlib

#
# prerequire packages listed in requirements.txt
# 
# Use pip to install all required packages
#  
# $ python3 -m venv .venv
# $ source .venv/bin/activate
#
# $ python3 -m pip install --upgrade pip
# $ python3 -m pip install -r requirements.txt

#  brew services start postgresql@18
#
# LC_ALL="en_US.UTF-8" /opt/homebrew/opt/postgresql@18/bin/postgres -D /opt/homebrew/var/postgresql@18
import psycopg;

TOML_CONFIG_FILE = 'dbmigration.toml'

DBCONN_DEFAULT_HOST = 'localhost'
DBCONN_DEFAULT_PORT = 5432
DBCONN_DEFAULT_USER = None
DBCONN_DEFAULT_DBNAME = 'postgres'
DBCONN_USER_PASSWORD_ENVVAR_NAME = "USER_PASSWORD"

BASELINE_DIR_NAME = "baseline"
VERSIONED_DIR_NAME = "versions"
REPEATABLE_DIR_NAME = "repeatable"
REPEATABLE_SCRIPTS_TARGET_VERSION_FILE = "target_version.txt"
SQL_SCRIPTS_RGLOB_FILTER = "*.sql"

class CommandError(Exception):
    """A critical command error terminated the command execution."""
    def __init__(self, message):
        super().__init__(message)

def walk_through_dir_sorted(dir, rglob_filter):
    start_path = pathlib.Path(dir) 
    if not start_path.exists():
        raise CommandError(f"The folder '{dir}' does not exists")
    all_items = start_path.rglob(rglob_filter)
    files = [item for item in all_items if item.is_file()]
    sorted_files = sorted(files)
    return sorted_files

def get_sha256sum_for_bytes(script_bytes):
    hash_object = hashlib.sha256(script_bytes)
    hex_dig = hash_object.hexdigest()
    return hex_dig


def read_as_trimmed_string(file_path):
    with open(file_path, 'rb') as f:
        bytes = f.read()
        str = bytes.decode("utf-8")
        trimmed_str = str.strip()
        return trimmed_str

class BaseCommand:
    def dbconn_get_single_value(self, sql, params):
        with self.dbconn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return row[0] if not row is None else None
    def dbconn_exec_with_no_result(self, sql, params):
        with self.dbconn.cursor() as cur:
            cur.execute(sql, params)
        
    def check_if_schema_exists(self):
        sql = """
            SELECT EXISTS (
                SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = %s)"""
        value = self.dbconn_get_single_value(sql, (self.args.schema_name,))
        if value is None:
            raise CommandError(f"Unable to check whether target schema exists because the query returned nothing: '{sql}' ")
        return value
    
    def set_session_search_path(self):
        sql = f"""
            SET search_path TO {self.args.schema_name}, public"""
        self.dbconn_exec_with_no_result(sql, [])

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
                FROM dbmigration_versions
                WHERE is_baseline IS TRUE)"""
        value = self.dbconn_get_single_value(sql, [])
        if value is None:
            raise CommandError(f"Unable to check whether the baseline scripts were applied in the target schema")
        return value
    
    def get_latest_version_installed(self):
        sql = """
            SELECT MAX(version_id) FROM dbmigration_versions"""
        value = self.dbconn_get_single_value(sql, [])
        if value is None:
            raise CommandError(f"Unable to get latest installed version")
        return value

    def check_if_repeatable_script_installed(self, sha256sum):
        sql = """
            SELECT EXISTS (
                SELECT 1
                FROM dbmigration_repeatable
                WHERE sha256sum = %s)"""        
        value = self.dbconn_get_single_value(sql, (sha256sum,))
        if value is None:
            raise CommandError(f"Unable to check if repeatable script was installed")
        return value

    def check_if_max_version_versioned_scripts_corresponds_to_repeatable_target(self, scripts_dir):
        print(f"Performing a cross-check for consistency between the target version's repeatable scripts and the versioned scripts in: {scripts_dir}")
        latest_version_in_scripts = None
        baseline_dir = scripts_dir.joinpath(BASELINE_DIR_NAME)
        if baseline_dir.exists():
            baseline_subdirs = [item for item in baseline_dir.iterdir() if item.is_dir()]
            if len(baseline_subdirs) == 1:
                baseline_version_subdir = baseline_subdirs[0]
                latest_version_in_scripts = baseline_version_subdir.name
        versioned_dir = scripts_dir.joinpath(VERSIONED_DIR_NAME)
        if versioned_dir.exists():
            for item in versioned_dir.iterdir():
                if item.is_dir():
                    if latest_version_in_scripts is None or item.name > latest_version_in_scripts:
                        latest_version_in_scripts = item.name
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
            raise CommandError(f"The target version for repeatable scripts '{target_version_in_repeatable}' does not corresponds to the latest version in versioned scripts '{latest_version_in_scripts}'")
        print(f"Completed.")

    def do_initial_cross_checks(self):
        self.scripts_dir = pathlib.Path(self.args.scripts_path)        
        if not self.scripts_dir.exists():
            raise CommandError(f"The scripts repository path '{self.args.scripts_path}' does not exists")       
        self.check_if_max_version_versioned_scripts_corresponds_to_repeatable_target(self.scripts_dir)
        if not self.check_if_schema_exists():
            raise CommandError(f"The target schema '{self.args.schema_name}' is not accessible")
        self.set_session_search_path()     
        if not self.check_if_version_table_exists("dbmigration_versions"):
            raise CommandError(f"The schema '{self.args.schema_name}' does not include version control table 'dbmigration_versions'")
        if not self.check_if_version_table_exists("dbmigration_repeatable"):
            raise CommandError(f"The schema '{self.args.schema_name}' does not include repeatable scripts control table 'dbmigration_repeatable'")

    def __init__(self, config, subparsers, command_name, command_help):        
        DBCONNECTION = 'dbconnection'
        try:        
            self.dbconn_settings = config[DBCONNECTION]
        except:
            raise ValueError(f"config file {TOML_CONFIG_FILE} does not include configuration group '{DBCONNECTION}'")
        OPTIONS = "options"
        try:        
            self.options = config[OPTIONS]
        except:
            raise ValueError(f"config file {TOML_CONFIG_FILE} does not include configuration group '{OPTIONS}'")
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
                raise ValueError(f"db user password must be provided via environment variable {DBCONN_USER_PASSWORD_ENVVAR_NAME}")
            self.dbconn_settings["password"]=password
        self.dbconn = psycopg.connect(**self.dbconn_settings)
        print(f"Opened db connection")
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

    def run_versioned_scripts_in_tran(self, version, is_baseline, scripts):
         with self.dbconn.cursor() as cur:
            cur.execute("BEGIN")
            print(f"Begin transaction")
            for script_path in scripts:
                with open(script_path, 'rb') as f:
                    print(f"Running script: '{script_path}'...")
                    script_text = f.read()
                    cur.execute(script_text)                                  
            cur.execute("INSERT INTO dbmigration_versions (version_id, is_baseline) VALUES (%s, %s)", (version, is_baseline))       
            cur.execute("COMMIT")       
            print(f"Committed transaction")


    def __init__(self, config, subparsers): 
        super().__init__(config, subparsers, "update", UpdateCommand.__doc__)
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
        scripts_sorted = walk_through_dir_sorted(baseline_version_subdir, SQL_SCRIPTS_RGLOB_FILTER)
        self.run_versioned_scripts_in_tran(baseline_version, True, scripts_sorted)
        print(f"The baseline scripts were applied.")       

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
            scripts_sorted = walk_through_dir_sorted(script_version_dir, SQL_SCRIPTS_RGLOB_FILTER)
            if len(scripts_sorted) == 0:
                raise CommandError(f"The scripts subdirectory '{script_version_dir}' does not include any {SQL_SCRIPTS_RGLOB_FILTER} scripts")
            self.run_versioned_scripts_in_tran(version_id, False, scripts_sorted)       
        print(f"The versioned scripts were applied.")

    def apply_repeatable_scripts(self, scripts_dir):
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
            raise CommandError(f"The target version {target_version} for repeatable scripts does not corresponds to the latest installed version {latest_installed_version}.")                  
        print(f"Target version matches the latest installed version '{target_version}'")
        repeatable_scripts_sorted = walk_through_dir_sorted(repeatable_dir, SQL_SCRIPTS_RGLOB_FILTER)
        scripts_to_repeat = [] 
        for script_path in repeatable_scripts_sorted:
            with open(script_path, 'rb') as f:
                print(f"Checking script '{script_path}' checksum...")
                script_text = f.read()
                sha256sum = get_sha256sum_for_bytes(script_text)
                if not self.check_if_repeatable_script_installed(sha256sum):
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
                    script_text = f.read()
                    sha256sum = get_sha256sum_for_bytes(script_text)
                    relative_script_path = script_path.relative_to(scripts_dir)
                    print(f"Running script '{script_path}'...")
                    cur.execute("BEGIN")
                    print(f"Begin transaction")
                    cur.execute(script_text)                                  
                    cur.execute("INSERT INTO dbmigration_repeatable (sha256sum, relative_path) VALUES (%s, %s)", (sha256sum, str(relative_script_path)))       
                    cur.execute("COMMIT")
                    print(f"Committed transaction.")
        print(f"The repeatable scripts were applied.")       

    def run(self):
        self.do_initial_cross_checks()
        print(f"Performing updates from scripts repository: '{self.scripts_dir}'")
        self.apply_baseline_scripts(self.scripts_dir)
        self.apply_versioned_scripts(self.scripts_dir)
        self.apply_repeatable_scripts(self.scripts_dir)
        print(f"Updated.")

class VerifyCommand (BaseCommand):
    """Validates the target schema and lists versioned and reproducible scripts to apply if the 'update' command is executed."""
    
    def get_baseline_version_installed(self):
        sql = """
                SELECT version_id FROM dbmigration_versions WHERE is_baseline IS TRUE ORDER BY version_id DESC LIMIT 1"""
        value = self.dbconn_get_single_value(sql, [])
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


    def __init__(self, config, subparsers): 
        super().__init__(config, subparsers, "verify", VerifyCommand.__doc__)
        self.parser.add_argument("scripts_path", type=str, help="source scripts repository path")
        self.latest_version_in_scripts = None

    def verify_baseline_scripts(self, scripts_dir):
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
        print(f"The baseline scripts to install: ")       
        scripts_sorted = walk_through_dir_sorted(baseline_version_subdir, SQL_SCRIPTS_RGLOB_FILTER)
        for item in scripts_sorted:
            print(f"[{item}]")
    
    def verify_versioned_scripts(self, scripts_dir):
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
        self.latest_version_in_scripts = newer_version_subdirs_sorted[-1].name
        
        print(f"The versioned scripts to install: ")    
        for script_version_dir in newer_version_subdirs_sorted:        
            scripts_sorted = walk_through_dir_sorted(script_version_dir, SQL_SCRIPTS_RGLOB_FILTER)
            if len(scripts_sorted) == 0:
                raise CommandError(f"The scripts subdirectory '{script_version_dir}' does not include any {SQL_SCRIPTS_RGLOB_FILTER} scripts")
            for item in scripts_sorted:
                print(f"[{item}]")
            
    def verify_repeatable_scripts(self, scripts_dir):
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

        repeatable_scripts_sorted = walk_through_dir_sorted(repeatable_dir, SQL_SCRIPTS_RGLOB_FILTER)
        print(f"The target version for repeatable scripts is {target_version}.")
        scripts_to_repeat = []
        for script_path in repeatable_scripts_sorted:
            with open(script_path, 'rb') as f:
                script_text = f.read()
                sha256sum = get_sha256sum_for_bytes(script_text)
                if not self.check_if_repeatable_script_installed(sha256sum):
                    scripts_to_repeat.append(script_path)
        if len(scripts_to_repeat) == 0:
            print(f"No any new repeatable scripts were found for installation")
            return
        print(f"The repeatable scripts to (re)install: ")
        for item in scripts_to_repeat:
            print(f"[{item}]")

    def run(self):
        self.do_initial_cross_checks()
        self.verify_baseline_scripts(self.scripts_dir)
        self.verify_versioned_scripts(self.scripts_dir)
        self.verify_repeatable_scripts(self.scripts_dir)

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
            CREATE TABLE dbmigration_versions (
                version_id VARCHAR(64) NOT NULL PRIMARY KEY,
                is_baseline BOOL NOT NULL DEFAULT FALSE,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                created_by VARCHAR(64) NOT NULL DEFAULT SESSION_USER,
                created_from INET DEFAULT INET_CLIENT_ADDR()
            );
            INSERT INTO dbmigration_versions (version_id, is_baseline) VALUES ('0', true);
            DELETE FROM dbmigration_versions WHERE version_id = '0';
            CREATE TABLE dbmigration_repeatable (
                sha256sum VARCHAR(128) NOT NULL PRIMARY KEY,
                relative_path VARCHAR(2048) NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                created_by VARCHAR(64) NOT NULL DEFAULT SESSION_USER,
                created_from INET DEFAULT INET_CLIENT_ADDR()
            );
            INSERT INTO dbmigration_repeatable (sha256sum, relative_path) VALUES ('0', 'test.sql');
            DELETE FROM dbmigration_repeatable WHERE sha256sum = '0';
            COMMIT;
        """
        with self.dbconn.cursor() as cur:
            cur.execute(sql_script);

    def __init__(self, config, subparsers): 
        super().__init__(config, subparsers, "init", InitCommand.__doc__)

    def run(self):
        if not self.check_if_schema_exists():
            raise CommandError(f"The target schema '{self.args.schema_name}' is not accessible")
        if not self.check_if_schema_is_empty():
            raise CommandError(f"The target schema '{self.args.schema_name}' must be empty")
        self.set_session_search_path()        
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

