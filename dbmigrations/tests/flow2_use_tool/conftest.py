import pytest  
import psycopg
from typing import NamedTuple

@pytest.fixture(scope="package")
def esbdb_cfg(cfg) -> NamedTuple:
    """Provides a session-wide named tuple instance containing all settings."""
    result = cfg._replace(
        TARGET_SCHEMA="esbdb",
        SCRIPTS_PATH=cfg.SAMPLES_PATH.joinpath("test1")
    )
    return result

@pytest.fixture(scope="package", autouse=True)
def setup_database_session(esbdb_cfg):
    """Recreates the target database schema exactly ONCE before the entire test suite starts."""

    with psycopg.connect(**esbdb_cfg.DBCONN_CONFIG) as conn:
        conn.autocommit = True  
        with conn.cursor() as cur:
            cur.execute(
                psycopg.sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE;").format(psycopg.sql.Identifier(esbdb_cfg.TARGET_SCHEMA))
            )
            cur.execute(
                psycopg.sql.SQL("CREATE SCHEMA {};").format(psycopg.sql.Identifier(esbdb_cfg.TARGET_SCHEMA))
            )
            
    print(f"\n[SESSION SETUP] Target schema '{esbdb_cfg.TARGET_SCHEMA}' has been successfully recreated via psycopg.")
    yield