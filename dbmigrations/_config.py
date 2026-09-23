from __future__ import annotations

import copy
import os
import pathlib
import tomllib
from pathlib import Path
from typing import Any

from _constants import (
    DBCONN_TESTER_PASSWORD_ENVVAR_NAME,
    DBCONN_USER_PASSWORD_ENVVAR_NAME,
    DBENVS_CONFIG_GROUP,
    DEFAULT_DBENV_CONFIG_ATTRIBUTE,
    NO_PASSWORD_ATTRIBUTE,
    RUN_TESTS_BY_ATTRIBUTE,
    TOML_CONFIG_FILE,
)
from _errors import CommandError
from _i18n import _
from _options import CommonCliOptions

# ============================================================================
# CLI layer helpers: TOML config loading and connection settings.
# ============================================================================



def read_toml_config() -> dict[str, Any]:
    script_dir = pathlib.Path(__file__).absolute().parent
    target_path = script_dir.joinpath(TOML_CONFIG_FILE)
    if not target_path.exists():
        raise CommandError(_("The configuration file '{TOML_CONFIG_FILE}' is not found at path '{target_path}'").format(TOML_CONFIG_FILE=TOML_CONFIG_FILE, target_path=target_path))
    if not target_path.is_file():
        raise CommandError(_("The configuration file '{target_path}' is not a regular file").format(target_path=target_path))
    with open(target_path, 'rb') as f:
        config = tomllib.load(f)
        return config

def get_default_dbenv(toml_config: dict[str, Any]) -> str:
    if DEFAULT_DBENV_CONFIG_ATTRIBUTE not in toml_config:
        raise CommandError(
            _(
                "Missing required key '{default_dbenv_config_attribute}' "
                "in configuration file '{toml_config_file}'."
            ).format(
                default_dbenv_config_attribute=DEFAULT_DBENV_CONFIG_ATTRIBUTE,
                toml_config_file=TOML_CONFIG_FILE,
            )
        )
    default_dbenv = toml_config[DEFAULT_DBENV_CONFIG_ATTRIBUTE]
    return str(default_dbenv)

def get_dbenv_config(
        toml_config: dict[str, Any],
        dbenv_param: str
) -> tuple[dict[str, Any], str | None, bool]:
    if DBENVS_CONFIG_GROUP not in toml_config:
        raise CommandError(
            _(
                "Missing required configuration group '{dbenvs_config_group}' "
                "in configuration file '{toml_config_file}'."
            ).format(
                dbenvs_config_group=DBENVS_CONFIG_GROUP,
                toml_config_file=TOML_CONFIG_FILE,
            )
        )
    dbenvs_config = toml_config[DBENVS_CONFIG_GROUP]
    if dbenv_param not in dbenvs_config:
        raise CommandError(
            _(
                "Missing configuration group '{dbenvs_config_group}.{dbenv_param}' "
                "in configuration file '{toml_config_file}'."
            ).format(
                dbenvs_config_group=DBENVS_CONFIG_GROUP,
                dbenv_param=dbenv_param,
                toml_config_file=TOML_CONFIG_FILE,
            )
        )
    config_copy = copy.deepcopy(dbenvs_config[dbenv_param])
    run_tests_by = config_copy.pop(RUN_TESTS_BY_ATTRIBUTE, None)
    no_password = config_copy.pop(NO_PASSWORD_ATTRIBUTE, False)
    return config_copy, run_tests_by, no_password

def build_connection_settings(
    config: dict[str, Any],
    opts: CommonCliOptions,
    use_run_tests_by_user: bool = False,
) -> dict[str, Any]:
    dbconn_settings, run_tests_by, no_password = get_dbenv_config(config, opts.dbenv)
    if opts.host is not None:
        dbconn_settings["host"] = opts.host
    if opts.port is not None:
        dbconn_settings["port"] = opts.port

    if opts.user is not None:
        dbconn_settings["user"] = opts.user
    elif run_tests_by is not None and use_run_tests_by_user:
        dbconn_settings["user"] = run_tests_by

    if opts.dbname is not None:
        dbconn_settings["dbname"] = opts.dbname

    if not opts.no_password and not no_password:
        password = None
        if use_run_tests_by_user:
            password = os.getenv(
                DBCONN_TESTER_PASSWORD_ENVVAR_NAME,
                os.getenv(DBCONN_USER_PASSWORD_ENVVAR_NAME))
        else:
            password = os.getenv(DBCONN_USER_PASSWORD_ENVVAR_NAME)
        if password is None:
            raise CommandError(
                _("The database user password must be specified via the environment variable '{env_var_name}'.")
                .format(env_var_name=DBCONN_USER_PASSWORD_ENVVAR_NAME)
            )
        dbconn_settings["password"] = password
    else:
        dbconn_settings["password"] = None
    return dbconn_settings

def add_common_db_arguments(sp: Any, default_dbenv: str) -> None:
    sp.add_argument("--dbenv", type=str, default=default_dbenv, help=_("db environment name within TOML config"))
    sp.add_argument("--host", type=str, default=None, help=_("db server host name"))
    sp.add_argument("--port", type=int, default=None, help=_("db server port"))
    sp.add_argument("--dbname", type=str, default=None, help=_("database name"))
    sp.add_argument("--user", type=str, default=None, help=_("user name"))
    sp.add_argument("-n", "--no-password", dest="no_password", action="store_true", default=False, help=_("don't ask user password"))
