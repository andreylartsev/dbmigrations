"""
Simple database migrations tool
"""

import argparse
import tomllib
import os
import pathlib 

#
# prerequired packages listed in requirements.txt
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

class FatalError(Exception):
    """A critical error terminated the command execution."""
    def __init__(self, message):
        super().__init__(message)

def read_toml_config():
    script_dir = pathlib.Path(__file__).absolute().parent
    target_path = script_dir.joinpath(TOML_CONFIG_FILE)
    with open(target_path, 'rb') as f:
        config = tomllib.load(f)
        return config

def walk_through_dir_sorted(dir):
    start_path = pathlib.Path(dir) 
    if not start_path.exists():
        raise FatalError(f"The folder '{dir}' does not exists")
    all_items = start_path.rglob('*')
    files = [item for item in all_items if item.is_file()]
    sorted_files = sorted(files)
    return sorted_files

def dbconn_exec(dbconn, sql):
    with dbconn.cursor() as cur:
        cur.execute(sql)

def dbconn_get_single_value(dbconn, sql, params):
    with dbconn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        return row[0] if not row is None else None

class BaseCommand:
    def check_if_schema_exists(self):
        sql = """
            SELECT EXISTS (
                SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = %s)"""
        params = (self.args.schema_name,)
        value = dbconn_get_single_value(self.dbconn, sql, params)
        if value is None:
            raise FatalError(f"Unable to check whether target schema exists because the query returned nothing: '{sql}' ")
        return value

    def check_if_schema_is_empty(self):
        sql = """
            SELECT count(*)
            FROM pg_class c
            JOIN pg_namespace s ON s.oid = c.relnamespace
            WHERE s.nspname = %s
            AND s.nspname NOT IN ('pg_catalog', 'information_schema')
            AND s.nspname NOT LIKE 'pg_toast%%'
            AND s.nspname NOT LIKE 'pg_temp%%';
        """
        value = dbconn_get_single_value(self.dbconn, sql, (self.args.schema_name,))
        if value is None:
            raise FatalError(f"Unable to check whether target schema exists because the query returned nothing: '{sql}' ")
        return (value == 0)

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
        self.parser.add_argument("scripts_path", type=str, help="source scripts repository path")
        self.parser.add_argument("--host", type=str, default=self.dbconn_settings.get("host", DBCONN_DEFAULT_HOST), help="db server host name")
        self.parser.add_argument("--port", type=int, default=self.dbconn_settings.get("port", DBCONN_DEFAULT_PORT), help="db server port")
        self.parser.add_argument("--dbname", type=str, default=self.dbconn_settings.get("dbname", DBCONN_DEFAULT_DBNAME), help="database name")
        self.parser.add_argument("--user", type=str, default=self.dbconn_settings.get("user", DBCONN_DEFAULT_USER), help="user name")
        self.parser.add_argument("-n","--no-password",  action="store_true", default=self.options.get("no_password", False), help="dont ask user password")
        self.parser.add_argument("-s","--skip-signature-check", action="store_true", default=False, help="to skip the signature check")
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
        print(f"Opened connection")
    def __exit__(self, exc_type, exc_value, traceback):
        self.dbconn.close()
        print(f"Closed connection")
        return False # propagate the exception
    def run(self):
        pass
    def __call__(self, args):
        self.args = args
        with self:
            self.run()

class UpdateCommand (BaseCommand):
    """Applies baseline, versioned and repeatable scripts within target database schema"""
    def __init__(self, config, subparsers): 
        super().__init__(config, subparsers, "update", UpdateCommand.__doc__)
    def run(self):
        print(f"Apply updates scripts_path={self.args.scripts_path}, schema_name={self.args.schema_name}, skip_signature_check={self.args.skip_signature_check}")
        [print(item) for item in walk_through_dir_sorted(self.args.scripts_path)]

class VerifyCommand (BaseCommand):
    """Verifies target schema and lists versioned and repatable scripts to be applied within"""
    def __init__(self, config, subparsers): 
        super().__init__(config, subparsers, "verify", VerifyCommand.__doc__)
    def run(self):
        print(f"Verify database schema scripts_path={self.args.scripts_path}, schema={self.args.schema_name}, {self.args.build_update_script}, skip_signature_check={self.args.skip_signature_check}")

class InitCommand (BaseCommand):
    """Creates version control tables in the empty database schema"""
    def __init__(self, config, subparsers): 
        super().__init__(config, subparsers, "init", InitCommand.__doc__)
        self.parser.add_argument("-f","--force-init", action="store_true", default=False, help="Force init schema versioning tables even if the target schema is not empty")
    def run(self):
        if not self.check_if_schema_exists():
            raise FatalError(f"The target schema '{self.args.schema_name}' is not accessible")
        if not self.check_if_schema_is_empty():
            raise FatalError(f"The target schema '{self.args.schema_name}' must be empty")
        print(f"Schema is empty")
        print(f"Creates version control tables scripts_path={self.args.scripts_path}, schema={self.args.schema_name}, force_init={self.args.force_init}, skip_signature_check={self.args.skip_signature_check}")

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
    except Exception as e:
        print("Error:", e)


if __name__ == "__main__":
    main()

