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

def __test_reading_toml():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    relative_file_path = 'dbmigration.toml'
    target_path = os.path.join(script_dir, relative_file_path)
    try:
        with open(target_path, 'rb') as f:
            data = tomllib.load(f)
            
        print(f"TOML data loaded as a Python dictionary: {data}")

        # Accessing specific values using dictionary syntax
        #print(f"\nTitle: {data['title']}")
        #print(f"Database server: {data['database']['server']}")
        #print(f"First port: {data['database']['ports'][0]}")
        #print(f"Owner's name: {data['owner']['name']}")

    except FileNotFoundError:
        print(f"Error: The file '{target_path}' was not found.")
    except tomllib.TOMLDecodeError as e:
        print(f"Error decoding TOML file: {e}")

def __test_connect_to_db():
    conn = psycopg.connect("dbname=postgres host=localhost user=postgres password=1234561")

class UpdateCommand:
    """Applies baseline, versioned and repeatable scripts within target database schema"""
    def __init__(self, subparsers):
        parser = subparsers.add_parser("update", help=UpdateCommand.__doc__)
        parser.add_argument("schema_name", type=str, help="the name of target database schema")
        parser.add_argument("scripts_path", type=str, help="source scripts repository path")
        parser.add_argument("-s","--skip-signature-check", action="store_true", default=False, help="to skip the signature check")
        parser.set_defaults(exec=self) 
    def __call__(self, args):
        print(f"Apply updates scripts_path={args.scripts_path}, schema_name={args.schema_name}, skip_signature_check={args.skip_signature_check}")

class VerifyCommand:
    """Verifies target schema and lists versioned and repatable scripts to be applied within"""
    def __init__(self, subparsers):
        parser = subparsers.add_parser("verify", help=VerifyCommand.__doc__)
        parser.add_argument("schema_name", type=str, help="the name of target database schema")
        parser.add_argument("scripts_path", type=str, help="source scripts repository path")
        parser.add_argument("-b","--build-update-script", action="store_true", default=False, help="to build and list the whole SQL script instead of just listing script files")
        parser.add_argument("-s","--skip-signature-check", action="store_true", default=False, help="to skip the signature check")
        parser.set_defaults(exec=self) 
    def __call__(self, args):
        print(f"Verify database schema scripts_path={args.scripts_path}, schema={args.schema_name}, {args.build_update_script}, skip_signature_check={args.skip_signature_check}")

class InitCommand:
    """Creates version control tables in the empty database schema"""
    def __init__(self, subparsers):
        parser = subparsers.add_parser("init", help=InitCommand.__doc__)
        parser.add_argument("schema_name", type=str, help="the name of target database schema")
        parser.add_argument("scripts_path", type=str, help="source scripts repository path")
        parser.add_argument("-f","--force-init", action="store_true", default=False, help="to create version control tables even if schema is NOT empty")
        parser.add_argument("-s","--skip-signature-check", action="store_true", default=False, help="to skip the signature check")
        parser.set_defaults(exec=self) 
    def __call__(self, args):
        print(f"Creates version control tables scripts_path={args.scripts_path}, schema={args.schema_name}, force_init={args.force_init}, skip_signature_check={args.skip_signature_check}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)    
    subparsers = parser.add_subparsers(dest="cmd", help="Available subcommands")

    UpdateCommand(subparsers)

    VerifyCommand(subparsers)

    InitCommand(subparsers)

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
    __test_connect_to_db()
    __test_reading_toml()

