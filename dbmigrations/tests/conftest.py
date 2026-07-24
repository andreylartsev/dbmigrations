import sys
import shutil
import tomllib
from pathlib import Path
from typing import NamedTuple, Any, Dict
import subprocess
import pytest

# 1. Define the Immutable Configuration Structure
class TestConfig(NamedTuple):
    DB_USER: str
    DB_NAME: str
    DB_HOST: str
    DB_PORT: int
    TARGET_SCHEMA: str
    DB_ENV: str
    PYTHON_EXE: str
    PSQL_EXE: Path
    DBMIGRATION_PY_PATH: Path
    TESTS_DIR: Path
    SAMPLES_PATH: Path
    DBCONN_CONFIG: Dict[str, Any]  # Ready to be used as psycopg.connect(**config.DBCONN_CONFIG)
    RUN_TESTS_BY: Any
    NO_PASSWORD: bool


# 2. Config Loader Function
def load_config_parameters() -> TestConfig:
    """Reads the TOML file and runtime environment to build the AppConfig named tuple."""
    python_exe = sys.executable

    psql_exe_str = shutil.which("psql")
    assert psql_exe_str, "psql utility not found"
    psql_exe = Path(psql_exe_str)
    assert psql_exe.exists(), "psql utility does not exist"

    tests_dir = Path(__file__).parent
    dbmigration_py = "dbmigration.py"
    dbmigration_py_path = tests_dir.parent / dbmigration_py

    toml_config_file = "dbmigration.toml"
    toml_config_path = tests_dir.parent / toml_config_file

    with open(toml_config_path, 'rb') as f:
        toml_config = tomllib.load(f)

    DEFAULT_DBENV_CONFIG_ATTRIBUTE = "default_dbenv"
    DBENVS_CONFIG_GROUP = "dbenvs"
    RUN_TESTS_BY_ATTRIBUTE = "run_tests_by"
    NO_PASSWORD_ATTRIBUTE = "no_password"

    assert DEFAULT_DBENV_CONFIG_ATTRIBUTE in toml_config, \
        f"There is no '{DEFAULT_DBENV_CONFIG_ATTRIBUTE}' within the configuration file '{toml_config_file}'."

    db_env = toml_config[DEFAULT_DBENV_CONFIG_ATTRIBUTE]

    assert DBENVS_CONFIG_GROUP in toml_config, \
        f"There is no configuration group '{DBENVS_CONFIG_GROUP}' within the configuration file '{toml_config_file}'."

    dbenvs = toml_config[DBENVS_CONFIG_GROUP]

    assert db_env in dbenvs, \
        f"There is no configuration group '{DBENVS_CONFIG_GROUP}.{db_env}' within the configuration file '{toml_config_file}'."

    # Shallow copy the dict to prevent mutating the original TOML structure on subsequent calls
    dbconn_config = dbenvs[db_env].copy()

    # Get and remove optional attributes so that DBCONN_CONFIG is usable with psycopg.connect()
    run_tests_by = dbconn_config.pop(RUN_TESTS_BY_ATTRIBUTE, None)
    no_password = dbconn_config.pop(NO_PASSWORD_ATTRIBUTE, False)

    samples_path = tests_dir.parent.joinpath("samples")

    # Database connection settings
    db_user = dbconn_config["user"]
    db_name = dbconn_config["dbname"]
    db_host = dbconn_config["host"]
    db_port = dbconn_config["port"]

    # Target schema to recreate
    target_schema = "test3"

    return TestConfig(
        DB_USER=db_user,
        DB_NAME=db_name,
        DB_HOST=db_host,
        DB_PORT=int(db_port),
        TARGET_SCHEMA=target_schema,
        DB_ENV=db_env,
        PYTHON_EXE=python_exe,
        PSQL_EXE=psql_exe,
        DBMIGRATION_PY_PATH=dbmigration_py_path,
        TESTS_DIR=tests_dir,
        SAMPLES_PATH=samples_path,
        DBCONN_CONFIG=dbconn_config,
        RUN_TESTS_BY=run_tests_by,
        NO_PASSWORD=no_password
    )


# 3. Pytest Fixture providing the NamedTuple to tests
@pytest.fixture(scope="session")
def cfg() -> TestConfig:
    """Provides a session-wide named tuple instance containing all settings."""
    return load_config_parameters()


# 4. Session Setup Fixture
@pytest.fixture(scope="session", autouse=True)
def setup_database_session():
    """Recreates the target database schema exactly ONCE before the entire test suite starts."""
    cfg = load_config_parameters()
    
    command = [
        str(cfg.PSQL_EXE),
        "-U", cfg.DB_USER,
        "-h", cfg.DB_HOST,
        "-p", str(cfg.DB_PORT),
        "-d", cfg.DB_NAME,
        "-c", f"DROP SCHEMA IF EXISTS {cfg.TARGET_SCHEMA} CASCADE; CREATE SCHEMA {cfg.TARGET_SCHEMA};"
    ]
    
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding="utf-8-sig"
    )
    
    assert result.returncode == 0, f"Database schema preparation failed: {result.stderr}"
    assert "DROP SCHEMA" in result.stdout
    assert "CREATE SCHEMA" in result.stdout
    
    print(f"\n[SESSION SETUP] Target schema '{cfg.TARGET_SCHEMA}' has been successfully recreated via psql.")
    yield
