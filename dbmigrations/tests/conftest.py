import sys
import tomllib
from pathlib import Path
from typing import NamedTuple, Any, Dict
import pytest
import psycopg  

DEFAULT_TARGET_SCHEMA_NAME = "test3"

# 1. Define the Immutable Configuration Structure
class TestConfig(NamedTuple):
    DB_USER: str | None
    DB_NAME: str
    DB_HOST: str | None
    DB_PORT: int | None
    TARGET_SCHEMA: str
    DB_ENV: str
    PYTHON_EXE: str
    DBMIGRATION_PY_PATH: Path
    TESTS_DIR: Path
    SAMPLES_PATH: Path
    DBCONN_CONFIG: Dict[str, Any]  # Ready to be used as psycopg.connect(**config.DBCONN_CONFIG)
    RUN_TESTS_BY: Any
    NO_PASSWORD: bool

def pytest_addoption(parser):
    """Add custom named option to pytest."""
    parser.addoption(
        "--schema",
        action="store",
        default=DEFAULT_TARGET_SCHEMA_NAME, 
        help="Target database schema to recreate and test"
    )

# 2. Config Loader Function
def load_config_parameters(target_schema) -> TestConfig:
    """Reads the TOML file and runtime environment to build the AppConfig named tuple."""
    python_exe = sys.executable

    tests_dir = Path(__file__).parent
    dbmigration_py = "dbmigration.py"
    dbmigration_py_path = tests_dir.parent / dbmigration_py

    toml_config_file = "dbmigration.toml"
    toml_config_path = tests_dir.parent / toml_config_file

    assert toml_config_path.exists(), f"TOML {toml_config_path} configuration file does not exists"

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

    #print(f"load_config_parameters: {dbconn_config=}")

    # Get and remove optional attributes so that DBCONN_CONFIG is usable with psycopg.connect()
    run_tests_by = dbconn_config.pop(RUN_TESTS_BY_ATTRIBUTE, None)
    no_password = dbconn_config.pop(NO_PASSWORD_ATTRIBUTE, False)

    samples_path = tests_dir.parent.joinpath("samples")

    # Database connection settings
    db_user = dbconn_config.get("user", None)
    db_name = dbconn_config.get("dbname", "postgres")
    db_host = dbconn_config.get("host", None)
    db_port = dbconn_config.get("port", None)

    return TestConfig(
        DB_USER=db_user,
        DB_NAME=db_name,
        DB_HOST=db_host,
        DB_PORT=int(db_port) if db_port is not None else None,
        TARGET_SCHEMA=target_schema,
        DB_ENV=db_env,
        PYTHON_EXE=python_exe,
        DBMIGRATION_PY_PATH=dbmigration_py_path,
        TESTS_DIR=tests_dir,
        SAMPLES_PATH=samples_path,
        DBCONN_CONFIG=dbconn_config,
        RUN_TESTS_BY=run_tests_by,
        NO_PASSWORD=no_password
    )


# 3. Pytest Fixture providing the NamedTuple to tests
@pytest.fixture(scope="session")
def cfg(request) -> TestConfig:
    """Provides a session-wide named tuple instance containing all settings."""
    schema_from_cli = request.config.getoption("--schema")
    return load_config_parameters(target_schema=schema_from_cli)
 

# 4. Session Setup Fixture
@pytest.fixture(scope="session", autouse=True)
def setup_database_session(cfg: TestConfig):
    """Recreates the target database schema exactly ONCE before the entire test suite starts."""
    
    with psycopg.connect(**cfg.DBCONN_CONFIG) as conn:
        conn.autocommit = True  
        with conn.cursor() as cur:
            cur.execute(
                psycopg.sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE;").format(psycopg.sql.Identifier(cfg.TARGET_SCHEMA))
            )
            cur.execute(
                psycopg.sql.SQL("CREATE SCHEMA {};").format(psycopg.sql.Identifier(cfg.TARGET_SCHEMA))
            )
            
    print(f"\n[SESSION SETUP] Target schema '{cfg.TARGET_SCHEMA}' has been successfully recreated via psycopg.")
    yield
