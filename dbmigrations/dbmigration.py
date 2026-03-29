"""
Simple database migrations tool
"""

import argparse
import tomllib
import os

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
DBCONN_DEFAULT_USER = 'postgres'
DBCONN_DEFAULT_DBNAME = 'postgres'
DBCONN_USER_PASSWORD_ENVVAR_NAME = "USER_PASSWORD"

def read_toml_config():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    target_path = os.path.join(script_dir, TOML_CONFIG_FILE)
    with open(target_path, 'rb') as f:
        config = tomllib.load(f)
        return config

class BaseCommand:        
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
        self.parser.set_defaults(exec=self) 
    def __enter__(self):
        self.dbconn_settings["host"]=self.args.host
        self.dbconn_settings["port"]=self.args.port
        self.dbconn_settings["user"]=self.args.user
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
        print(f"Creates version control tables scripts_path={self.args.scripts_path}, schema={self.args.schema_name}, force_init={self.args.force_init}, skip_signature_check={self.args.skip_signature_check}")

def main():
    config = read_toml_config()
    parser = argparse.ArgumentParser(description=__doc__)    
    subparsers = parser.add_subparsers(dest="cmd", help="Available subcommands")

    UpdateCommand(config, subparsers)
    VerifyCommand(config, subparsers)
    InitCommand(config, subparsers)

    # Parse arguments
    args = parser.parse_args()

    # Call the function associated with the subcommand
    if hasattr(args, 'exec'):
        args.exec(args)
    else:
        # If no subcommand is given, print help (or handle as needed)
        parser.print_help()

if __name__ == "__main__":
    main()

