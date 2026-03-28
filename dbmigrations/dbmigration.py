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
DBCONN_DEFAULT_DB = 'postgres'
DBCONN_USER_PASSWORD_ENVVAR_NAME = "USER_PASSWORD"

def read_toml_config():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    target_path = os.path.join(script_dir, TOML_CONFIG_FILE)
    with open(target_path, 'rb') as f:
        config = tomllib.load(f)
        return config

def get_value_or_default(key, dict, default_value):
    return dict.get(key, default_value)

def make_db_connection(host, port, db, user, password):  
    dict = {}      
    dict["host"]=host
    dict["port"]=port
    dict["dbname"]=db
    dict["user"]=user
    if not password is None: 
        dict["password"]=password
    conn = psycopg.connect(**dict)

class BaseCommand:
        
    def __init__(self, config, subparsers, command_name, command_help):        
        DBCONNECTION = 'dbconnection'
        try:        
            dbconn_settings = config[DBCONNECTION]
        except:
            raise ValueError(f"config file does not include configuration group '{DBCONNECTION}'")
        parser = subparsers.add_parser(command_name, help=command_help)
        parser.add_argument("schema_name", type=str, help="the name of target database schema")
        parser.add_argument("scripts_path", type=str, help="source scripts repository path")
        parser.add_argument("--host", type=str, default=get_value_or_default("host", dbconn_settings, DBCONN_DEFAULT_HOST), help="db server host name")
        parser.add_argument("--port", type=int, default=get_value_or_default("port", dbconn_settings, DBCONN_DEFAULT_PORT), help="db server port")
        parser.add_argument("--db", type=str, default=get_value_or_default("db", dbconn_settings, DBCONN_DEFAULT_DB), help="database name")
        parser.add_argument("--user", type=str, default=get_value_or_default("user", dbconn_settings, DBCONN_DEFAULT_USER), help="user name")
        parser.add_argument("-n","--no-password",  action="store_true", default=False, help="dont ask user password")
        parser.add_argument("-s","--skip-signature-check", action="store_true", default=False, help="to skip the signature check")
        parser.set_defaults(exec=self) 

class UpdateCommand (BaseCommand):
    """Applies baseline, versioned and repeatable scripts within target database schema"""
    def __init__(self, config, subparsers): 
        super().__init__(config, subparsers, "update", UpdateCommand.__doc__)
    def __call__(self, args):
        password = None
        if not args.no_password:
            password = os.getenv(DBCONN_USER_PASSWORD_ENVVAR_NAME)
            if password is None:
                raise ValueError(f"db user password must be provided via environment variable {DBCONN_USER_PASSWORD_ENVVAR_NAME}")
        conn = make_db_connection(args.host, args.port, args.db, args.user, password)
        print(f"Apply updates scripts_path={args.scripts_path}, schema_name={args.schema_name}, skip_signature_check={args.skip_signature_check}")

class VerifyCommand (BaseCommand):
    """Verifies target schema and lists versioned and repatable scripts to be applied within"""
    def __init__(self, config, subparsers): 
        super().__init__(config, subparsers, "verify", VerifyCommand.__doc__)
    def __call__(self, args):
        print(f"Verify database schema scripts_path={args.scripts_path}, schema={args.schema_name}, {args.build_update_script}, skip_signature_check={args.skip_signature_check}")

class InitCommand (BaseCommand):
    """Creates version control tables in the empty database schema"""
    def __init__(self, config, subparsers): 
        super().__init__(config, subparsers, "init", InitCommand.__doc__)
    def __call__(self, args):
        print(f"Creates version control tables scripts_path={args.scripts_path}, schema={args.schema_name}, force_init={args.force_init}, skip_signature_check={args.skip_signature_check}")

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

