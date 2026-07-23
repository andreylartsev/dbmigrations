import sys
import shutil
import tomllib
from pathlib import *

PYTHON_EXE = sys.executable

psql_exe_str = shutil.which("psql")

assert psql_exe_str, "psql utility not found"

PSQL_EXE = Path(psql_exe_str)

assert PSQL_EXE.exists(), "psql utility does not exists"

TESTS_DIR = Path(__file__).parent

DBMIGRATION_PY = "dbmigration.py"
DBMIGRATION_PY_PATH = TESTS_DIR.parent / DBMIGRATION_PY

TOML_CONFIG_FILE="dbmigration.toml"
TOML_CONFIG_PATH = TESTS_DIR.parent / TOML_CONFIG_FILE

with open(TOML_CONFIG_PATH, 'rb') as f:
    toml_config = tomllib.load(f)

DEFAULT_DBENV_CONFIG_ATTRIBUTE="default_dbenv"
DBENVS_CONFIG_GROUP = "dbenvs"
RUN_TESTS_BY_ATTRIBUTE = "run_tests_by"
NO_PASSWORD_ATTRIBUTE = "no_password"

assert DEFAULT_DBENV_CONFIG_ATTRIBUTE in toml_config, f"There is no '{DEFAULT_DBENV_CONFIG_ATTRIBUTE}' within the configuration file '{TOML_CONFIG_FILE}'."

DB_ENV = toml_config[DEFAULT_DBENV_CONFIG_ATTRIBUTE]

assert DBENVS_CONFIG_GROUP in toml_config, f"There is no configuration group '{DBENVS_CONFIG_GROUP}' within the configuration file '{TOML_CONFIG_FILE}'."

dbenvs = toml_config[DBENVS_CONFIG_GROUP]

assert DB_ENV in dbenvs, f"There is no configuration group '{DBENVS_CONFIG_GROUP}.{DB_ENV}' within the configuration file '{TOML_CONFIG_FILE}'."

dbenv_config = dbenvs[DB_ENV]

# Optional attributes
RUN_TESTS_BY = dbenv_config.get(RUN_TESTS_BY_ATTRIBUTE, None)
NO_PASSWORD = dbenv_config.get(NO_PASSWORD_ATTRIBUTE, False)

SAMPLES_PATH = TESTS_DIR.parent.joinpath("samples")

# Database connection settings
DB_USER = dbenv_config["user"]
DB_NAME = dbenv_config["dbname"]
DB_HOST = dbenv_config["host"]
DB_PORT = dbenv_config["port"]

# Target schema to recreate
TARGET_SCHEMA = "test3"

