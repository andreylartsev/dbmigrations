import sys
from pathlib import *
import shutil

# Database connection settings
DB_USER = "postgres"
DB_NAME = "test1"
DB_HOST = "localhost"  # Can be changed to any host
DB_PORT = "5432"       # Can be changed to any port

# Target schema to recreate
TARGET_SCHEMA = "test3"

# DB env to work with
DB_ENV = "local_windows"

TESTS_DIR = Path(__file__).resolve().parent

DBMIGRATION_PATH = TESTS_DIR.joinpath("..").joinpath("dbmigration.py")
SAMPLES_PATH = TESTS_DIR.joinpath("..").joinpath("samples")
PYTHON_EXE = sys.executable

# Check if the psql utility is available in the system PATH
PSQL_PATH = shutil.which("psql")

