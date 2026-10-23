import os
import pytest
import psycopg
from typing import NamedTuple

@pytest.fixture(scope="package")
def cfg(session_cfg) -> NamedTuple:
    """Provides a package-wide named tuple instance containing all settings."""
    result = session_cfg._replace(
        SCRIPTS_PATH=session_cfg.SAMPLES_PATH.joinpath("test1")
    )
    return result

@pytest.fixture(scope="package", autouse=True)
def setup_database_session(cfg):
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

def pytest_configure(config):
    # use messages from the code
    os.environ["LC_MESSAGES"] = "C"
    os.environ["LC_ALL"] = "C"
    os.environ["LANG"] = "C"    
    os.environ["LANGUAGE"] = "en_US:en"