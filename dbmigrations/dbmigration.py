"""
Simple database migrations tool
"""

import argparse
import collections
import copy
import difflib
import getpass
import gettext
import hashlib
import mmap
import os
import pathlib
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tomllib
import traceback
import locale
import functools
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from types import TracebackType
from typing import NamedTuple, Self, Any, TextIO, Iterable, Type, List, Dict, Sequence, Mapping

#
# prerequire packages listed in requirements.txt
# 
import os
import psycopg
from psycopg.rows import TupleRow
from psycopg import Cursor

from _config import (
    add_common_db_arguments,
    build_connection_settings,
    get_default_dbenv,
    read_toml_config,
)
from _confirmation import Confirmation, ConsoleConfirm, get_char
from _constants import *
from _db import DbConnection, log_server_notices
from _errors import CommandError
from _git import CommitInfo, GitChecker
from _i18n import _, setup_translations
from _launch import launch_command, run_command
from _migrations import MigrationCheckForOlderVersionControlTables, OwnMigration
from _options import (
    CommonCliOptions,
    Deps,
    InitOptions,
    RunTestsOptions,
    UpdateOptions,
    VerifyOptions,
)
from _output import Output
from _scripts import (
    ScriptDbInfo,
    ScriptFsInfo,
    get_git_blob_sha1_for_bytes,
    get_git_blob_sha1_for_file_path,
    get_script_path_for_log,
    read_as_trimmed_string,
    render_script_diff_text,
    resolve_relative_script_path,
)
from _state import State, StateProbe

if __name__ == "__main__":
    sys.modules.setdefault("dbmigration", sys.modules["__main__"])

from _tool import ExternalTool
from commands import (
    BaseCommand,
    InitCommand,
    RunTestsCommand,
    TestFailed,
    UpdateCommand,
    UpdateScriptBuilder,
    VerifyCommand,
)

def main() -> int:
    try:
        setup_translations(None)

        config = read_toml_config()

        lang = os.environ.get(LANGUAGE_OVERRIDE_ENV)
        if lang is None and OPTIONS_CONFIG_GROUP in config:
            options = config[OPTIONS_CONFIG_GROUP]
            lang = options.get(LANGUAGE_ATTR_NAME, None)
        if lang is not None:
            setup_translations(lang)

        parser = build_parser(config)

        args = parser.parse_args()

        if hasattr(args, 'handler'):
            return args.handler(args)
        parser.print_help()
        return 0
    except CommandError as e:    
        Output().print(_("Command error:"), e)
        return 1
    except psycopg.Error as e:    
        Output().print(_("Server error:"), e)
        return 1
    except Exception as e:
        error_type_name = type(e).__name__ 
        Output().print(_("Error: {error_type_name}:").format(error_type_name=error_type_name), e)
        traceback.print_exc()
        return 1


def _update_handler(args: Any, config: dict[str, Any]) -> int:
    opts = UpdateOptions(
        schema_name=args.schema_name,
        dbenv=args.dbenv,
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.user,
        no_password=args.no_password,
        scripts_path=args.scripts_path,
        force_reapply_latest_version=args.force_reapply_latest_version,
        force_reapply_all_repeatable=args.force_reapply_all_repeatable,
        force_run_cleanup=args.force_run_cleanup,
        skip_confirmation=args.skip_confirmation,
    )
    return run_command(UpdateCommand, opts, config)

def _verify_handler(args: Any, config: dict[str, Any]) -> int:
    opts = VerifyOptions(
        schema_name=args.schema_name,
        dbenv=args.dbenv,
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.user,
        no_password=args.no_password,
        scripts_path=args.scripts_path,
        skip_git_checks=args.skip_git_checks,
        skip_diffs=args.skip_diffs,
        skip_display_recent_changes=args.skip_display_recent_changes,
        build_update_script=args.build_update_script,
    )
    return run_command(VerifyCommand, opts, config)

def _init_handler(args: Any, config: dict[str, Any]) -> int:
    opts = InitOptions(
        schema_name=args.schema_name,
        dbenv=args.dbenv,
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.user,
        no_password=args.no_password,
        scripts_path=args.scripts_path,
        force_init=args.force_init,
    )
    return run_command(InitCommand, opts, config)

def _run_tests_handler(args: Any, config: dict[str, Any]) -> int:
    opts = RunTestsOptions(
        schema_name=args.schema_name,
        dbenv=args.dbenv,
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.user,
        no_password=args.no_password,
        scripts_path=args.scripts_path,
        skip_env_checks=args.skip_env_checks,
    )
    return run_command(RunTestsCommand, opts, config, use_run_tests_by_user=True)

def _tui_handler(args: Any, config: dict[str, Any]) -> int:
    from tui.app import run_tui

    opts = CommonCliOptions(
        schema_name=args.schema_name,
        dbenv=args.dbenv,
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.user,
        no_password=args.no_password,
        scripts_path=args.scripts_path,
    )
    return run_tui(config, opts)

def build_parser(config: dict[str, Any]) -> argparse.ArgumentParser:
    default_dbenv = get_default_dbenv(config)
    parser = argparse.ArgumentParser(description=_("Simple database migrations tool"))
    subparsers = parser.add_subparsers(dest="cmd", help=_("Available subcommands"))

    sp = subparsers.add_parser("update", help=_("Applies base, versioned, and repeatable scripts to the target database schema."))
    add_common_db_arguments(sp, default_dbenv)
    sp.add_argument("schema_name", type=str, help=_("the name of target database schema"))
    sp.add_argument("scripts_path", type=str, help=_("source scripts repository path"))
    sp.add_argument("--force-reapply-latest-version", action="store_true", help=_("clean up the latest version within the database and reapply the included *.sql scripts."))
    sp.add_argument("--force-reapply-all-repeatable", action="store_true", help=_("reapply all repeatable scripts, regardless of changes."))
    sp.add_argument("--force-run-cleanup", action="store_true", help=_("run the cleanup script before executing version-specific scripts."))
    sp.add_argument("--skip-confirmation", action="store_true", help=_("skip confirmation before executing updates."))
    sp.set_defaults(handler=functools.partial(_update_handler, config=config))

    sp = subparsers.add_parser("verify", help=_("Validates the target schema and lists versioned and reproducible scripts to apply if the 'update' command is executed."))
    add_common_db_arguments(sp, default_dbenv)
    sp.add_argument("schema_name", type=str, help=_("the name of target database schema"))
    sp.add_argument("scripts_path", type=str, help=_("source scripts repository path"))
    sp.add_argument("--skip-git-checks", action="store_true", help=_("skip grouping changes by git commits"))
    sp.add_argument(
        "--skip-diffs",
        action="store_true",
        help=_("skip unified text diffs between scripts applied in the database "
               "(by git OID) and the current script files in the repository")
    )
    sp.add_argument("--skip-display-recent-changes", action="store_true", help=_("skip display recent changes stored within target db schema"))
    sp.add_argument("--build-update-script", type=str, help=_("the update script path if you want one as an additional result of the verify command"))
    sp.set_defaults(handler=functools.partial(_verify_handler, config=config))

    sp = subparsers.add_parser("init", help=_("Creates version control tables in an empty database schema."))
    add_common_db_arguments(sp, default_dbenv)
    sp.add_argument("schema_name", type=str, help=_("the name of target database schema"))
    sp.add_argument("scripts_path", type=str, help=_("source scripts repository path"))
    sp.add_argument("--force-init", action="store_true", default=False, help=_("Force create version control tables even on non empty schema"))
    sp.set_defaults(handler=functools.partial(_init_handler, config=config))

    sp = subparsers.add_parser("run-tests", help=_("Runs db unit test scripts to the target database schema."))
    add_common_db_arguments(sp, default_dbenv)
    sp.add_argument("schema_name", type=str, help=_("the name of target database schema"))
    sp.add_argument("scripts_path", type=str, help=_("source scripts repository path"))
    sp.add_argument("--skip-env-checks", action="store_true", help=_("Skip version and environment ID checks to run tests in any plain environment not made by the tool itself"))
    sp.set_defaults(handler=functools.partial(_run_tests_handler, config=config))

    sp = subparsers.add_parser("tui", help=_("Open the interactive Textual user interface"))
    add_common_db_arguments(sp, default_dbenv)
    sp.add_argument("schema_name", type=str, help=_("the name of target database schema"))
    sp.add_argument("scripts_path", type=str, help=_("source scripts repository path"))
    sp.set_defaults(handler=functools.partial(_tui_handler, config=config))

    return parser

if __name__ == "__main__":
    sys.exit(main())
