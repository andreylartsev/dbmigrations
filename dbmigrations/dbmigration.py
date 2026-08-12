"""
Simple database migrations tool
"""

import argparse
import collections
import copy
import getpass
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
from abc import ABC, abstractmethod
from datetime import datetime
from types import TracebackType
from typing import NamedTuple, Self, Any, TextIO, Iterable, Type, List, Dict, Sequence, Mapping

#
# prerequire packages listed in requirements.txt
# 
import psycopg
from psycopg.rows import TupleRow
from psycopg import Cursor

TOML_CONFIG_FILE = 'dbmigration.toml'
OPTIONS_CONFIG_GROUP = "options"

GIT_CMD_CONFIG_ATTRIBUTE = "git_cmd_path"

DBENVS_CONFIG_GROUP = "dbenvs"
DEFAULT_DBENV_CONFIG_ATTRIBUTE = "default_dbenv"

RUN_TESTS_BY_ATTRIBUTE = "run_tests_by"
DBCONN_CONFIG_USER_ATTRIBUTE = "user"
NO_PASSWORD_ATTRIBUTE = "no_password"

OPTIONS_DEFAULT_FILE_GLOB_FILTERS = ["*.sql", "*.dump"]
OPTIONS_DEFAULT_FILE_READ_ENCODING = "utf-8"
OPTIONS_DEFAULT_FILE_READ_ENCODING_ERRORS = "ignore"

DBCONN_USER_PASSWORD_ENVVAR_NAME = "USER_PASSWORD"
DBCONN_TESTER_PASSWORD_ENVVAR_NAME = "TESTER_PASSWORD"

BASELINE_DIR_NAME = "baseline"
VERSIONED_DIR_NAME = "versions"
REPEATABLE_DIR_NAME = "repeatable"
TESTS_DIR_NAME = "tests"
TARGET_VERSION_FILE = "target_version.txt"
SCRIPT_LIST_FILE_NAME = "script_list.txt"

TOOLS_CONFIG_GROUP = "tools"
TOOL_EXEC_ATTRIBUTE = "executable"
TOOL_ARGS_ATTRIBUTE = "args"
TOOL_SUCCESS_RESULT_CODE_ATTRIBUTE = "success_result_code"
USE_TOOL_NAME_FILE_NAME = "use_tool.txt"

VERSION_CLEANUP_FILE_NAME = "_cleanup.sql"

SEARCH_PATH_FILE_NAME = "set_search_path.txt"
DEFAULT_SEARCH_PATH = ":default"

TARGET_ENVIRONMENT_ID_FILE_NAME = "target_environment_id.txt"

BASELINE_FILES_DEPTH = 2
VERSIONED_FILES_DEPTH = 2
REPEATABLE_FILES_DEPTH = 1
TESTS_FILES_DEPTH = 1

NAME_LENGTH_LIMIT=64

DEPENDS_ON_PATTERN = r'(?<=@depends_on)\s*(\S+)'

IS_TRUE_THAT_TEST_PREFIX = "is_true_that_"
DETECT_MISSING_TEST_PREFIX = "detect_missing_"
ASSURE_THAT_TEST_PREFIX = "assure_that_"

SETUP_TESTS_FILE_NAME = "_setup.sql"

RECENT_CHANGES_WINDOW_MINUTES = 30
RECENT_CHANGES_LIMIT = 1000

UNKNOWN_SHA_LABEL = "UNKNOWN"
UNKNOWN_AUTHOR_LABEL = "Unknown"
UNKNOWN_MESSAGE_LABEL = "Content hash (OID) is completely untracked or modified locally"
UNCOMMITTED_SHA_LABEL = "UNCOMMITTED"
UNCOMMITTED_AUTHOR_LABEL = "Local Changes"
UNCOMMITTED_DATE_LABEL = "-------"
UNCOMMITTED_MESSAGE_LABEL = "Uncommitted changes"


class CommandError(Exception):
    """A critical command error terminated the command execution."""

def get_git_blob_sha1_for_bytes(script_bytes : bytes) -> str:
    content = script_bytes.replace(b'\r\n', b'\n')
    header = f"blob {len(content)}\x00".encode('utf-8')
    sha1 = hashlib.sha1(header)
    sha1.update(content)
    result = sha1.hexdigest()
    return result

def get_git_blob_sha1_for_file_path(file_path: str | Path) -> str:
    path = Path(file_path)
    chunk_size = 1048576  

    def bytes_stream():
        if path.stat().st_size == 0:
            return                
        with open(path, "rb") as f:
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                while chunk := mm.read(chunk_size):
                    yield chunk.replace(b"\r", b"")

    total_bytes = sum(len(chunk) for chunk in bytes_stream())

    header = f"blob {total_bytes}\x00".encode("utf-8")
    sha1 = hashlib.sha1(header)
    
    for chunk in bytes_stream():
        sha1.update(chunk)
        
    result = sha1.hexdigest()
    return result

def get_script_path_for_log(scripts_dir: str|Path, script_path: str|Path) -> str:
    base_dir = Path(scripts_dir).parent.resolve()
    target_file = Path(script_path).resolve()
    if target_file.is_relative_to(base_dir):
        result = target_file.relative_to(base_dir).as_posix()
    else:
        result = target_file.as_posix()
    return result

def read_as_trimmed_string(file_path : str|Path) -> str:
    with open(file_path, 'rb') as f:
        for binary_line in f:
            decoded_str = binary_line.decode("utf-8-sig", "ignore")
            trimmed_str = decoded_str.strip()
            if trimmed_str:
                return trimmed_str
    raise CommandError(f"The file '{file_path}' contains no valid text data")

def resolve_relative_script_path(start_path: Path, depth_within_base_dir: int, path_str : str) -> Path:
    if not path_str.startswith("@"):
        raise CommandError(f"The relative environment path must start with @ symbol, but '{path_str}' was found")
    # path normalization for windows style paths
    path_str = path_str.replace("\\", "/")
    start = path_str.find("@") + 1
    end = path_str.find("/", start)
    if end == -1:
        script_list_file_path = start_path.joinpath(SCRIPT_LIST_FILE_NAME)
        raise CommandError(f"No path separator found after environment name in path '{path_str}' specified in file '{script_list_file_path}'.")
    env_name = path_str[start:end]
    script_sub_path = path_str[end + 1:]
    result = start_path
    # walk back
    for i in range(depth_within_base_dir + 1):
        result = result.joinpath("..")
    # add a referencing env name
    result = result.joinpath(env_name)
    # walk forward
    last_parts = start_path.parts[-depth_within_base_dir:] if depth_within_base_dir > 0 else ()
    for part in last_parts:
        result = result.joinpath(part)
    # add extra path specified after env name
    result = result.joinpath(script_sub_path)
    return result

def log_server_notices(diag):
    print(f"Server: {diag.severity} - {diag.message_primary}")

def get_char() -> str:
    result = ""    
    if sys.platform == "win32":
        import msvcrt        
        char_bytes = msvcrt.getch()        
        # Handle special/function keys (arrows, F1-F12) which emit a prefix byte
        if char_bytes in (b"\x00", b"\xe0"):
            msvcrt.getch()  # Consume the second trailing byte of the special key
            result = ""
        else:
            result = char_bytes.decode("utf-8", "ignore")
            print(result, flush=True)            
    else:
        import termios
        import tty        
        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        try:
            tty.setcbreak(fd)
            result = sys.stdin.read(1)
            print(result, flush=True)
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)            
    return result 

class ScriptFsInfo(NamedTuple):
    script_path : Path
    relative_path : str
    oid : str
    text : str

    def __repr__(self) -> str:
        short_oid = self.oid[:8]
        return f"[{self.relative_path} (OID: {short_oid})]"

    @classmethod
    def get_info(
        cls, 
        scripts_dir: str | Path, 
        script_path: str | Path
    ) -> Self:
        """
        Factory method for potentially large files. 
        Calculates Git SHA-1 efficiently in chunks without loading text into memory.
        """
        relative_script_path = get_script_path_for_log(scripts_dir, script_path)
        git_blob_sha1 = get_git_blob_sha1_for_file_path(script_path)
        
        result = cls(
            script_path=script_path, 
            relative_path=relative_script_path,
            oid=git_blob_sha1,
            text=""
        )
        return result 

    @classmethod
    def get_info_with_text(
        cls, 
        scripts_dir: str | Path, 
        script_path: str | Path, 
        encoding: str = "utf-8-sig", 
        encoding_errors: str = "ignore"
    ) -> Self:
        """
        Factory method for small/medium files where full text content is needed.
        Loads file bytes to calculate SHA-1 and decodes them into the 'text' field.
        """
        relative_script_path = get_script_path_for_log(scripts_dir, script_path)
        
        with open(script_path, 'rb') as f:
            script_bytes = f.read()
            
        git_blob_sha1 = get_git_blob_sha1_for_bytes(script_bytes)
        text = script_bytes.decode(encoding, encoding_errors)
        
        result = cls(
            script_path=script_path, 
            relative_path=relative_script_path,
            oid=git_blob_sha1,
            text=text
        )
        return result



class ScriptDbInfo(NamedTuple):
    applied_at : datetime
    script_type : str
    version_id : str
    relative_path : str
    git_blob_sha1 : str
    def __repr__(self) -> str:
        date_str = self.applied_at.strftime("%Y-%m-%d %H:%M:%S")
        clean_oid = self.git_blob_sha1.strip()[:8]
        return f"  [{date_str} | {self.script_type:<10} | {self.version_id:<6} | {self.relative_path} (OID: {clean_oid})]"

class CommitInfo(NamedTuple):
    oid : str | None
    author : str | None    
    date : datetime | None
    message : str | None

    @classmethod
    def uncommitted(cls, message : str | None = None) -> Self:
        return cls(
            oid=None,
            author=getpass.getuser(),
            date=datetime.now(),
            message=message or UNCOMMITTED_MESSAGE_LABEL
        )

    @classmethod
    def unknown(cls, message: str | None = None) -> Self:
        return cls(
            oid=None,
            author=None,
            date=None,
            message=message or UNKNOWN_MESSAGE_LABEL
        )

    @property
    def is_uncommitted(self):
        return self.oid is None 

    def __repr__(self) -> str:
        date_label = self.date.strftime("%Y-%m-%d") if self.date is not None else UNCOMMITTED_DATE_LABEL
        oid_label = self.oid[:8] if self.oid else UNCOMMITTED_SHA_LABEL
        message_label = self.message if self.message else UNCOMMITTED_MESSAGE_LABEL
        author_label = self.author if self.author else UNKNOWN_AUTHOR_LABEL
        return f"[{oid_label}] {date_label} - {message_label}\n  Author: {author_label}"
    
    def sort_key(self) -> tuple[datetime, str, str]:
        if self.date is not None:
            sort_date = self.date.replace(tzinfo=None)
        else:
            sort_date = datetime.min
            
        sort_author = self.author if self.author else ""
        sort_oid = self.oid if self.oid else ""        
        return (sort_date, sort_author, sort_oid)

class GitChecker:

    def __init__(self, git_cmd: Path, repo_root: Path):
        self.git_cmd = git_cmd
        self.repo_root = repo_root

    @classmethod
    def try_get(cls, toml_config: dict[str, Any], scripts_dir: Path) -> Self | None:
        # 1. Look up the git executable path
        git_cmd = cls._try_get_git_cmd_path(toml_config)
        if git_cmd is None:
            return None

        # 2. Locate the root directory of the Git repository
        repo_root = cls._try_get_git_repo_root(git_cmd, scripts_dir)
        if repo_root is None:
            return None
                    
        return cls(git_cmd, repo_root)
    
    @classmethod
    def _try_get_git_cmd_path(cls, toml_config: dict[str, Any]) -> Path | None:
        if GIT_CMD_CONFIG_ATTRIBUTE in toml_config:
            cmd_path_str = toml_config[GIT_CMD_CONFIG_ATTRIBUTE]
            cmd_path = Path(cmd_path_str)
            if not cmd_path.exists():
                raise CommandError(
                    f"The git cmd specified in {GIT_CMD_CONFIG_ATTRIBUTE} of TOML config does not exist!"
                )
            return cmd_path        
        
        cmd_path_str = shutil.which("git")
        if cmd_path_str is None:
            print("Warning: Git executable was not found in system PATH. Git features are disabled.")
            return None
            
        return Path(cmd_path_str)
        
    @classmethod
    def _try_get_git_repo_root(cls, git_cmd: Path, scripts_dir: Path) -> Path | None:
        resolved_dir = Path(scripts_dir).resolve()
        if not resolved_dir.is_dir():
            raise CommandError(f"The specified path '{scripts_dir}' is invalid or not a directory!")        
        
        try:
            # Find the .git root directory using the rev-parse command
            res = subprocess.run(
                [str(git_cmd), "-C", str(resolved_dir), "rev-parse", "--show-toplevel"],
                capture_output=True, text=True, check=True
            )
            return Path(res.stdout.strip())
        except (subprocess.CalledProcessError, FileNotFoundError):
            print(f"Warning: A valid Git repository root was not found for '{scripts_dir}'. Git features are disabled.")
            return None

    def _run_git(self, args: list[str]) -> str:
        """Helper method to safely execute Git commands."""
        cmd = [str(self.git_cmd), "-C", str(self.repo_root)] + args
        res = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
        if res.returncode != 0:
            return ""
        return res.stdout

    def get_latest_commit(self, relative_file_path: Path) -> CommitInfo:
        """Fetches the status or the latest commit information for a single specific file."""
        posix_path = relative_file_path.as_posix()

        # =========================================================================
        # STEP 1: Check for uncommitted changes in the specific file
        # =========================================================================
        # The --porcelain flag guarantees a stable, machine-readable output format.
        # Passing the specific path optimizes the lookup on large repositories.
        status_output = self._run_git(["status", "--porcelain", "-z", "--", posix_path])
        if status_output:
            # Split by \x00 due to the -z flag (protects against spaces in file paths)
            entry = status_output.split("\x00")[0]
            if len(entry) >= 4:
                status_code = entry[:2]
                if "??" in status_code:
                    return CommitInfo.uncommitted("File is untracked by Git")
                else:
                    return CommitInfo.uncommitted(f"File is modified ({status_code.strip()})")

        # =========================================================================
        # STEP 2: Fetch the latest commit for a clean file via Git Log
        # =========================================================================
        # Output format: SHA | AUTHOR | TIMESTAMP | COMMIT_SUBJECT
        log_format = "--format=%H|%an|%ct|%s"
        log_output = self._run_git(["log", "-1", log_format, "--", posix_path])
        
        if log_output:
            parts = log_output.strip().split("|", 3)
            if len(parts) == 4:
                oid, author, timestamp_str, message = parts
                return CommitInfo(
                    oid=oid,
                    author=author,
                    date=datetime.fromtimestamp(int(timestamp_str)),
                    message=message
                )

        # =========================================================================
        # STEP 3: Fallback if the file has no commit history in the branch
        # =========================================================================
        return CommitInfo.unknown("No commit history found in this branch")

    def get_commit_by_file_oid(self, file_oid: str) -> CommitInfo:
        """
        Finds the commit associated with a specific file content hash (Blob OID)
        using the git log --find-object feature.
        """
        clean_oid = str(file_oid).strip()
        if not clean_oid:
            raise ValueError("Argument 'file_oid' must not be empty")

        # Format: SHA | AUTHOR | TIMESTAMP | COMMIT_SUBJECT
        # We use %ct (timestamp) to match the datetime object initialization in CommitInfo
        log_format = "--format=%H|%an|%ct|%s"
        
        # Execute git log searching for the exact object hash across the history
        log_output = self._run_git(["log", "-1", f"--find-object={clean_oid}", log_format])
        
        if not log_output:
            # Fallback if the file OID exists locally (e.g., in index) but has never been committed
            return CommitInfo.unknown("Content hash (OID) is completely untracked or modified locally")

        parts = log_output.strip().split("|", 3)
        if len(parts) != 4:
            raise CommandError(f"Unexpected git log output format for OID '{clean_oid}': {log_output}")
            
        oid, author, timestamp_str, message = parts
        
        return CommitInfo(
            oid=oid,
            author=author,
            date=datetime.fromtimestamp(int(timestamp_str)),
            message=message
        )


class ExternalTool:
    def __init__(
        self, 
        tool_name: str, 
        schema_name: str, 
        dbconn_config: Dict[str, Any], 
        tool_config: Dict[str, Any]
    ) -> None:
        """Initializes the external tool configuration and caches system encoding."""
        self.tool_name = tool_name
        self.schema_name = schema_name
        self.dbconn_config = dbconn_config
        self.tool_config = tool_config
        
        # Detect and cache system encoding once to save CPU cycles on multiple runs
        self.system_encoding = locale.getpreferredencoding(False)

        # Read tool config
        if TOOL_EXEC_ATTRIBUTE not in tool_config:
            raise CommandError(f"Missing required attribute '{TOOL_EXEC_ATTRIBUTE}' in tool configuration '{tool_name}'.")
        exec_attribute = tool_config[TOOL_EXEC_ATTRIBUTE]
        exec_path = Path(exec_attribute)
        if not exec_path.exists():
            raise CommandError(f"The path '{exec_path}' specified by attribute '{TOOL_EXEC_ATTRIBUTE}' in the tool configuration '{tool_name}' does not exists.")
        if not exec_path.is_file():
            raise CommandError(f"The path '{exec_path}' specified by attribute '{TOOL_EXEC_ATTRIBUTE}' in the tool configuration '{tool_name}' is not a file.")
        self.exec_path = exec_path

        if TOOL_ARGS_ATTRIBUTE not in tool_config:
            raise CommandError(f"There is no attribute '{TOOL_ARGS_ATTRIBUTE}' in the tool configuration '{tool_name}'.")
        self.args = tool_config[TOOL_ARGS_ATTRIBUTE]

        if TOOL_SUCCESS_RESULT_CODE_ATTRIBUTE not in tool_config:
            raise CommandError(f"There is no attribute '{TOOL_SUCCESS_RESULT_CODE_ATTRIBUTE}' in the tool configuration '{tool_name}'.")
        self.success_result_code = tool_config[TOOL_SUCCESS_RESULT_CODE_ATTRIBUTE]

    @classmethod
    def try_get(
        cls, 
        dir : Path, 
        schema_name : str, 
        dbconn_config : dict[str, Any], 
        toml_config : dict[str, Any]
    ) -> Self | None:
        tool_name = ExternalTool._try_get_tool_name(dir)
        if tool_name is None:
            return None

        if TOOLS_CONFIG_GROUP not in toml_config:
            raise CommandError(f"Missing configuration group '{TOOLS_CONFIG_GROUP}' in configuration file '{TOML_CONFIG_FILE}'.")
        tools_config = toml_config[TOOLS_CONFIG_GROUP]        
        
        if tool_name not in tools_config:
            raise CommandError(f"Unable find the specified external tool name '{tool_name}' in configuration group '{TOOLS_CONFIG_GROUP}'.")
        tool_config = tools_config[tool_name]
        
        result = cls(
            tool_name, schema_name, dbconn_config, tool_config
        )
        return result        

    @classmethod
    def _try_get_tool_name(cls, dir : Path) -> str|None:
        start_path = Path(dir) 
        if not start_path.exists():
            raise CommandError(f"The folder '{dir}' does not exists")
        if not start_path.is_dir():
            raise CommandError(f"The path '{dir}' is not a directory")        
        use_tool_file_name = start_path.joinpath(USE_TOOL_NAME_FILE_NAME)
        if not use_tool_file_name.exists():
            return None
        tool_name = read_as_trimmed_string(use_tool_file_name)
        return tool_name        

    def make_variables_dict_from_config_and_script_path(self, script_path: str) -> Dict[str, Any]:
        """Creates a token lookup dictionary for variable substitution."""
        result = {}
        for key, value in self.dbconn_config.items():
            variable_key = "${" + key.strip() + "}"  
            result[variable_key] = value
        result["${file}"] = script_path
        result["${schema_name}"] = self.schema_name
        return result

    def match_variables_to_args(self, variables: Dict[str, Any], args: List[str]) -> List[str]:
        """Maps argument placeholders to their actual python-internal unicode string values."""
        result = []
        for arg in args:
            variable_key = arg.strip() 
            if variable_key in variables:
                value_str = str(variables[variable_key])
                result.append(value_str)
            else:
                result.append(arg)
        return result 
    
    def run(self, script_path: str) -> int:
        """Executes the tool in a safe context, streaming its output using system-native encoding."""
        tool_absolute_path = self.exec_path.absolute()
        tool_args = self.args
        variables = self.make_variables_dict_from_config_and_script_path(script_path)
        tool_args_with_matched_variables = self.match_variables_to_args(variables, tool_args)
        command_line = [str(tool_absolute_path), *tool_args_with_matched_variables]
        
        with subprocess.Popen(
            args=command_line, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.STDOUT, 
            text=True,
            encoding=self.system_encoding,
            errors='replace'
        ) as process:
            
            if process.stdout:
                for line in iter(process.stdout.readline, ''):
                    print(line, end='') 

            result_code = process.wait() 

        if result_code != self.success_result_code:
            raise CommandError(f"The tool '{self.tool_name}' returned unsuccessful result code {result_code}!")
            
        return result_code 


class OwnMigration(ABC):    
    @abstractmethod
    def get_sql_to_check_if_need_migration(self):
        pass
    @abstractmethod
    def get_migration_ddl(self):
        pass
    @abstractmethod
    def get_migration_desc(self):
        pass

class MigrationCheckForOlderVersionControlTables (OwnMigration):
    def get_sql_to_check_if_need_migration(self) -> str:
        sql = """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = {schema_name_str} AND table_name = 'dbmigration_versions'
            ) AND NOT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = {schema_name_str} AND table_name = 'dbmigration_version_scripts'
            ) AS conditions_met;     
        """
        return sql
    def get_migration_ddl(self) -> str:
        raise CommandError(f"This version of dbmigration tools is incompatible with this schema.\n" 
                            "Please use the previous version available by tag 0.9.x or upgrade the current schema by deleting dbmigration_version and dbmigration_repeatable tables and running the update subcommand with --force-run-cleanup flag: \n" 
                            "i.e. dbmigration.py update <schema_name> <scripts_folder> --force-run-cleanup")
                           
    def get_migration_desc(self) -> str:
        desc = "Check for older version control tables"
        return desc

class BaseCommand(ABC):

    _all_own_migrations: list[OwnMigration] = [
       MigrationCheckForOlderVersionControlTables() 
    ]

    def apply_all_own_migrations(self) -> int:
        applied_count = 0
        for m in self._all_own_migrations:
            if not isinstance(m, OwnMigration):
                raise CommandError(f"Not a 'Migration' object found within the migrations collection")
            sql = m.get_sql_to_check_if_need_migration()
            formatted_sql = self.format_sql(sql, schema_name_identity=self.get_schema_name(), schema_name_str=self.args.schema_name)
            result = self.dbconn_get_single_value(formatted_sql, [])
            if result:
                ddl = m.get_migration_ddl()
                desc = m.get_migration_desc()
                formatted_ddl = self.format_sql(ddl, schema_name_identity=self.get_schema_name(), schema_name_str=self.args.schema_name)
                print(f"Run migration: {desc}...", flush=True, end="")
                self.dbconn_exec_with_no_result_in_tran(formatted_ddl, [])
                print(f"Done.")
                applied_count += 1
        return applied_count

    def check_if_all_own_migrations_are_applied(self) -> None:
        for m in self._all_own_migrations:
            if not isinstance(m, OwnMigration):
                raise CommandError(f"Not a 'Migration' object found within the migrations collection")
            sql = m.get_sql_to_check_if_need_migration()
            formatted_sql = self.format_sql(sql, schema_name_identity=self.get_schema_name(), schema_name_str=self.args.schema_name)
            result = self.dbconn_get_single_value(formatted_sql, [])
            if result:
                desc = m.get_migration_desc()
                raise CommandError(f"Run 'update' subcommand to update version control tables within the schema. The following migration need to be applied: {desc}")

    def get_default_dbenv(self, toml_config : dict[str, Any]) -> str:
        if DEFAULT_DBENV_CONFIG_ATTRIBUTE not in toml_config:
            raise CommandError(f"Missing required key '{DEFAULT_DBENV_CONFIG_ATTRIBUTE}' in configuration file '{TOML_CONFIG_FILE}'.")
        default_dbenv = toml_config[DEFAULT_DBENV_CONFIG_ATTRIBUTE]
        return str(default_dbenv)

    def get_dbenv_config(
            self, 
            toml_config : dict[str, Any], 
            dbenv_param : str
    ) -> tuple[dict[str, Any], str | None, bool]:
        if DBENVS_CONFIG_GROUP not in toml_config:
            raise CommandError(f"Missing required configuration group '{DBENVS_CONFIG_GROUP}' in configuration file '{TOML_CONFIG_FILE}'.")
        dbenvs_config = toml_config[DBENVS_CONFIG_GROUP]
        if dbenv_param not in dbenvs_config:
            raise CommandError(f"Missing configuration group '{DBENVS_CONFIG_GROUP}.{dbenv_param}' in configuration file '{TOML_CONFIG_FILE}'.")
        config_copy = copy.deepcopy(dbenvs_config[dbenv_param])
        run_tests_by = config_copy.pop(RUN_TESTS_BY_ATTRIBUTE, None)
        no_password = config_copy.pop(NO_PASSWORD_ATTRIBUTE, False)
        return config_copy, run_tests_by, no_password

    def get_script_dependencies(self, base_dir:Path, depth_within_base_dir:int, script_path:Path)->list[Path]:
        if not script_path.exists():
            raise CommandError(f"The path {script_path} does not exists.")
        if not script_path.is_file():
            raise CommandError(f"The path {script_path} is not a file.")
        start_path = pathlib.Path(base_dir)
        result_list = []
        with script_path.open("r", encoding="utf-8-sig", errors="replace") as script_file:
            lines = script_file.readlines()
            for line in lines:
                match = re.search(DEPENDS_ON_PATTERN, line)
                if match: 
                    found_match = match.group(1)
                    if found_match.startswith("@"):
                        dependency_path = resolve_relative_script_path(base_dir, depth_within_base_dir, found_match)
                        result_list.append(dependency_path)
                    else:
                        dependency_path = start_path.joinpath(found_match)
                        result_list.append(dependency_path)
        return result_list
    
    def resolve_scripts_dependencies_inner_recursive_loop(self, reversed_deps: dict[Path, list[Path]], script_to_add: Path, visited: list[Path] | None = None) -> list[Path]:
        # print(visited)
        if visited is None:
            visited = []
        if script_to_add in visited:
            cycle_path = " -> ".join([f"'{p.name}'" for p in visited]) + f" -> '{script_to_add.name}'"
            raise CommandError(f"Circular dependency detected! Path loop: {cycle_path}")
        result_list = [script_to_add]
        if script_to_add in reversed_deps:
            deps = reversed_deps[script_to_add]
            for dependency in deps:
                l = self.resolve_scripts_dependencies_inner_recursive_loop(
                    reversed_deps, 
                    dependency, 
                    [*visited, script_to_add]
                )
                result_list = [*result_list, *l]
        return result_list

    def resolve_scripts_dependencies(self, base_dir:Path, depth_within_base_dir:int, orig_script_list:list[Path], changed_scripts:list[Path]) -> list[Path]:
        assert depth_within_base_dir > 0

        resolved_changed_scripts = [p.resolve() for p in changed_scripts]     
        resolved_orig_script_list = [p.resolve() for p in orig_script_list]     
        reversed_deps = collections.defaultdict(list)
        for script_path in resolved_orig_script_list:
            script_deps = self.get_script_dependencies(base_dir, depth_within_base_dir, script_path)
            for dependency in script_deps:
                resolved_dependency = dependency.resolve()
                if not resolved_dependency.exists():
                    raise CommandError(f"The script '{dependency}' specified in '{script_path}' as a dependency does not exist.")
                if not resolved_dependency.is_file():
                    raise CommandError(f"The script '{dependency}' specified in '{script_path}' as a dependency is not a valid file.")
                if resolved_dependency not in resolved_orig_script_list:
                    raise CommandError(f"The script '{dependency}' (specified in '{script_path}') was not found in '{SCRIPT_LIST_FILE_NAME}' or in the origin scripts folder.")
                reversed_deps[resolved_dependency].append(script_path)
        # print(reversed_deps)     
        result_list = []
        for changed in resolved_changed_scripts:
            l = self.resolve_scripts_dependencies_inner_recursive_loop(reversed_deps, changed)
            result_list = [*result_list, *l]
        # print(result_list)
        # make the list unique
        result_list = list(dict.fromkeys(result_list)) 
        return result_list

    def get_sorted_scripts_from_dir(self, base_dir: Path, depth_within_base_dir: int, force_run_cleanup: bool = False, recursion_depth: int = 0) -> list[Path]:
        MAX_RECURSION_DEPTH = 25
        if recursion_depth > MAX_RECURSION_DEPTH:
            raise CommandError(f"Maximum recursion depth ({recursion_depth}) exceeded at '{base_dir}' due to circular path references.")
        start_path = Path(base_dir) 
        if not start_path.exists():
            raise CommandError(f"The folder '{base_dir}' does not exists")
        if not start_path.is_dir():
            raise CommandError(f"The path '{base_dir}' is not a directory")        
        script_list_file_path = start_path.joinpath(SCRIPT_LIST_FILE_NAME)
        sorted_files = []
        if script_list_file_path.exists():
            with script_list_file_path.open("r", encoding=self.file_read_encoding, errors=self.file_read_encoding_errors) as script_list_file:
                lines = script_list_file.readlines()
                for line in lines:
                    trimmed_str = line.strip()
                    if len(trimmed_str) == 0 or trimmed_str.startswith("#"):
                        continue              
                    if trimmed_str.startswith("!"):
                        print(f"Skip: {trimmed_str}")
                        continue
                    if trimmed_str.startswith("@"):
                        script_path = resolve_relative_script_path(start_path, depth_within_base_dir, trimmed_str)
                    else:
                        script_path = start_path.joinpath(trimmed_str)
                    script_name = script_path.name
                    if script_name == '*':
                        new_base_path = script_path.parent
                        scripts_to_add = self.get_sorted_scripts_from_dir(new_base_path, depth_within_base_dir, force_run_cleanup, recursion_depth + 1)
                        sorted_files = [*sorted_files, *scripts_to_add]
                    else:
                        if force_run_cleanup:
                            if len(sorted_files) == 0 and (not script_name == VERSION_CLEANUP_FILE_NAME):
                                raise CommandError(f"The list of scripts '{script_list_file_path}' must start with '{VERSION_CLEANUP_FILE_NAME}', but '{script_name}' was given.")
                        else:
                            if (script_name == VERSION_CLEANUP_FILE_NAME):
                                continue
                        if not script_path.exists():
                            raise CommandError(f"The file '{trimmed_str}' specified in script list file '{script_list_file_path}' does not exists") 
                        if not script_path.is_file():
                            raise CommandError(f"The file '{trimmed_str}' specified in script list file '{script_list_file_path}' is not a file")
                        sorted_files.append(script_path)
        else:
            all_files = []
            exclusions = [
                USE_TOOL_NAME_FILE_NAME, 
                TARGET_VERSION_FILE,
                VERSION_CLEANUP_FILE_NAME]
            exclusions_set = set(exclusions)
            for glob_filter in self.file_glob_filters:
                all_items = start_path.rglob(glob_filter)
                for item in all_items: 
                    if item.is_file() and not item.name in exclusions_set:
                        all_files.append(item)
            sorted_files = sorted(all_files)
            if force_run_cleanup:
                cleanup_file_path = start_path.joinpath(VERSION_CLEANUP_FILE_NAME)
                if not cleanup_file_path.exists():
                    raise CommandError(f"The file '{cleanup_file_path}' does not exists")
                if not cleanup_file_path.is_file():
                    raise CommandError(f"The path '{cleanup_file_path}' is not a file")
                sorted_files.insert(0, cleanup_file_path)
        return sorted_files

    def format_sql_comment(self, comment : str) -> str:
        """
        PostgreSQL's format() function ignores lines that start with a comment.
        This can lead to SQL injection vulnerability if a formatted placeholder 
        contains a newline character, for example:
        'BAD VERSION\n;TRUNCATE dbmigration_environment_id CASCADE; --'
        """
        result_str = comment.replace("\n", " ").replace("\r", "").strip() + "\n"
        if not result_str.startswith("--"):
            result_str = f"-- {result_str}"
        return result_str

    def format_sql_text(self, sql : str, **params) -> str:
        if self.dbconn is None:
            raise CommandError(f"DB connection is not initialized yet")
        composed_query = psycopg.sql.SQL(sql).format(**params)
        result_str = composed_query.as_string(self.dbconn)
        return result_str        

    def format_sql(self, sql: str, **params) -> psycopg.sql.Composed:
        result_query = psycopg.sql.SQL(sql).format(**params)
        return result_query

    def dbconn_get_single_value(
        self, 
        sql : str | psycopg.sql.Composed, 
        params : Sequence[Any] | Mapping[str, Any]
    ) -> Any | None:
        with self.dbconn.cursor() as cur:
            cur.execute(sql, params)
            try:
                row = cur.fetchone()
            except psycopg.ProgrammingError: # thrown in case of DDL or anonymous PL/pgSQL block
                return None
            return next(iter(row), None) if row is not None else None
        
    def dbconn_exec_with_no_result_in_tran(
        self, 
        sql: str | psycopg.sql.Composed, 
        params: Sequence[Any] | Mapping[str, Any]
    ) -> None:
        with self.dbconn:
            with self.dbconn.cursor() as cur:
                cur.execute(sql, params)

    def dbconn_get_connection_string(self, dbconn: psycopg.Connection[Any]) -> str:
        info = dbconn.info                
        host_val = getattr(info, "host", None)
        host = host_val if host_val else "[local_socket]"

        port_val = getattr(info, "port", None)        
        port = f":{port_val}" if port_val else ""
        
        return f"{info.user}@{host}{port}/{info.dbname}"
    
    def get_schema_name_arg(self) -> str:
        schema_name = self.args.schema_name
        if not schema_name:
            raise CommandError(f"The attribute self.args.schema_name must not be empty")
        return schema_name

    def get_schema_name(self) -> psycopg.sql.Identifier:
        schema_name = self.get_schema_name_arg()        
        return psycopg.sql.Identifier(schema_name)

    def check_if_schema_exists(self) -> bool:
        schema_name = self.get_schema_name_arg()
        sql = """
            SELECT EXISTS (
                SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = %s)"""
        value = self.dbconn_get_single_value(sql, (schema_name,))
        return bool(value)

    def get_scripts_path_arg(self) -> Path:
        if not self.args.scripts_path:
            raise CommandError("The path specified by 'scripts_path' must not be an empty string")
            
        scripts_path = Path(self.args.scripts_path)
        if not scripts_path.exists():
            raise CommandError(f"The path specified by 'scripts_path' argument does not exist: {str(scripts_path)}")
        if not scripts_path.is_dir():
            raise CommandError(f"The path specified by 'scripts_path' argument is not a valid directory: {str(scripts_path)}")        
        return scripts_path

    def get_resolved_scripts_dir(self) -> Path:
        return self.get_scripts_path_arg().resolve()
    
    def get_scripts_environment_id(self) -> str:            
        scripts_path = self.get_scripts_path_arg()
            
        target_environment_id_file_name = scripts_path.joinpath(TARGET_ENVIRONMENT_ID_FILE_NAME)
        
        if target_environment_id_file_name.exists():
            environment_id = read_as_trimmed_string(target_environment_id_file_name)
            if not environment_id:
                raise CommandError("The environment ID must not be an empty string")
            if len(environment_id) > NAME_LENGTH_LIMIT:
                raise CommandError(
                    f"The length of the environment ID taken from '{target_environment_id_file_name}' "
                    f"exceeds the limit: {NAME_LENGTH_LIMIT}"
                )
        else: 
            # Considering directory name as environment ID
            environment_id = scripts_path.resolve().name
            if not environment_id:
                raise CommandError("The environment ID must not be an empty string")
            if len(environment_id) > NAME_LENGTH_LIMIT:
                raise CommandError(
                    f"The length of the directory name specified by 'scripts_path' argument "
                    f"exceeds the limit: {NAME_LENGTH_LIMIT}"
                )
                
        return environment_id

    def get_stored_environment_id(self) -> str:
        schema_id = self.get_schema_name()
        sql = """
                SELECT id FROM {schema_name_identity}.dbmigration_environment_id ORDER BY created_at ASC LIMIT 1"""        
        formatted_sql = self.format_sql(sql, schema_name_identity=schema_id) 
        value = self.dbconn_get_single_value(formatted_sql, [])
        if value is None:
            raise CommandError("Schema consistency check failed: environment ID not found in table 'dbmigration_environment_id'.")            
        return value
    
    def get_search_path_for_scripts(self) -> str:            
        scripts_path = self.get_scripts_path_arg()   
        set_search_path_file = scripts_path.joinpath(SEARCH_PATH_FILE_NAME)
        if not set_search_path_file.exists():
            return self.get_schema_name_arg()
        if not set_search_path_file.is_file():
            raise CommandError(
                f"The search path file '{SEARCH_PATH_FILE_NAME}' "
                f"within scripts directory '{scripts_path}' is not a valid file"
            )
        trimmed_str = read_as_trimmed_string(set_search_path_file)
        return trimmed_str
    
    def set_session_search_path(self, search_path : str) -> None:
        print(f"Set session search path to: '{search_path}'.")
        sql = f"""
            SELECT pg_catalog.set_config('search_path', %s, false)"""
        result = self.dbconn_get_single_value(sql, (search_path,))
        if result != search_path:
            raise CommandError(f"Unexpected value '{result}' returned on attempt to set the search path")

    def check_if_table_exists(self, table_name : str) -> bool:
        schema_name = self.get_schema_name_arg()
        sql = """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s
            );
        """
        value = self.dbconn_get_single_value(sql, (schema_name, table_name))
        return bool(value)
    
    def check_if_version_table_include_baseline_version(self) -> bool:
        schema_id = self.get_schema_name()
        sql = """
            SELECT EXISTS (
                SELECT 1
                FROM {schema_name}.dbmigration_versions
                WHERE is_baseline IS TRUE
            );
        """
        formatted_sql = self.format_sql(sql, schema_name=schema_id)
        value = self.dbconn_get_single_value(formatted_sql, [])
        return bool(value)
    
    def get_latest_version_installed(self) -> str|None:
        schema_id = self.get_schema_name()
        sql = """
            SELECT MAX(version_id) FROM {schema_identity}.dbmigration_versions"""
        formatted_sql = self.format_sql(sql, schema_identity=schema_id)
        value = self.dbconn_get_single_value(formatted_sql, [])
        return value

    def check_if_any_latest_version_installed(self) -> str:
        value = self.get_latest_version_installed()
        if value is None:
            raise CommandError(f"Unable to get latest installed version")
        return value

    def check_if_repeatable_script_installed(self, git_blob_sha1: str, version: str, relative_path: str) -> bool:
        sql = """
            SELECT EXISTS (
                SELECT 1 FROM (
                    SELECT git_blob_sha1 
                    FROM {schema_name}.dbmigration_repeatable_scripts
                    WHERE relative_path = %s 
                      AND version_id = %s
                    ORDER BY created_at DESC
                    LIMIT 1
                ) latest
                WHERE latest.git_blob_sha1 = %s
            );
        """                
        formatted_sql = self.format_sql(sql, schema_name=self.get_schema_name())        
        params = (relative_path, version, git_blob_sha1)        
        value = self.dbconn_get_single_value(formatted_sql, params)
        return bool(value)


    def check_if_max_version_of_versioned_scripts_matches_repeatable_target(self) -> None:
        scripts_dir = self.get_resolved_scripts_dir()
        print(f"Performing a cross-check for consistency between the target version's repeatable scripts and the versioned scripts...")
        latest_version_in_baseline = None
        baseline_dir = scripts_dir.joinpath(BASELINE_DIR_NAME)
        if baseline_dir.exists():
            baseline_subdirs = [item.name for item in baseline_dir.iterdir() if item.is_dir()]
            baseline_subdirs_len = len(baseline_subdirs)
            if baseline_subdirs_len != 1:
                raise CommandError(
                    f"The baseline directory must include exactly one subdirectory with version scripts, "
                    f"but {baseline_subdirs_len} present."
                )
            latest_version_in_baseline = baseline_subdirs[0]

        latest_version_in_versioned = None
        versioned_dir = scripts_dir.joinpath(VERSIONED_DIR_NAME)
        if versioned_dir.exists():
            latest_version_in_versioned = max((item.name for item in versioned_dir.iterdir() if item.is_dir()), default=None)

        if latest_version_in_versioned and latest_version_in_baseline and latest_version_in_versioned <= latest_version_in_baseline:
            raise CommandError(
                f"The latest version of the subdirectory with the versions '{latest_version_in_versioned}' "
                f"must be greater than the version of the baseline scripts '{latest_version_in_baseline}'."
            )
    
        latest_version_in_scripts = max(
            filter(None, [latest_version_in_versioned, latest_version_in_baseline]), default=None)

        if latest_version_in_scripts is None:
            print(f"No baseline or versioned scripts found in scripts directory: '{scripts_dir}'")
            return
                
        target_version_in_repeatable = None
        repeatable_dir = scripts_dir.joinpath(REPEATABLE_DIR_NAME)
        if repeatable_dir.exists():
            target_version_file_path = repeatable_dir.joinpath(TARGET_VERSION_FILE)
            if target_version_file_path.exists():
                target_version_in_repeatable = read_as_trimmed_string(target_version_file_path)

        if target_version_in_repeatable is None:
            print(f"No repeatable scripts found in scripts directory: '{scripts_dir}'")
            return 

        if latest_version_in_scripts != target_version_in_repeatable:
            raise CommandError(f"The target version for repeatable scripts '{target_version_in_repeatable}' does not match the latest version in versioned scripts '{latest_version_in_scripts}'")

        print(f"Completed.")

    def do_initial_cross_checks(self) -> None:
        if not self.check_if_schema_exists():
            raise CommandError(f"The target schema '{self.args.schema_name}' is not accessible")
        search_path = self.get_search_path_for_scripts()
        if search_path != DEFAULT_SEARCH_PATH:
            self.set_session_search_path(search_path)
        else:
            print(f"Use the default users search path")     

    def check_if_stored_environment_id_matches_to_scripts_dir(self) -> None:
        stored_environment_id = self.get_stored_environment_id()
        scripts_environment_id = self.get_scripts_environment_id()
        if stored_environment_id != scripts_environment_id:
            scripts_path = self.get_scripts_path_arg()
            raise CommandError(f"The stored environment ID '{stored_environment_id}' in the target schema does not match the scripts directory '{scripts_path}'")
        print(f"Target schema environment ID matches the scripts directory ID: {stored_environment_id}")


    _required_version_control_tables = [
            "dbmigration_environment_id",
            "dbmigration_versions",
            "dbmigration_version_scripts",
            "dbmigration_repeatable_scripts",
    ]

    def check_if_all_version_control_tables_exist(self) -> None:
        schema_name = self.get_schema_name_arg()    
        for table_name in self._required_version_control_tables:
            if not self.check_if_table_exists(table_name):
                raise CommandError(
                    f"The schema '{schema_name}' is missing the version control table '{table_name}'"
                )
            
    def check_if_all_version_control_tables_do_not_exist(self) -> None:
        schema_name = self.get_schema_name_arg()    
        for table_name in self._required_version_control_tables:
            if self.check_if_table_exists(table_name):
                raise CommandError(
                    f"The schema '{schema_name}' already contains the version control table '{table_name}'"
                )

    def __init__(
        self, 
        config: dict[str, Any], 
        subparsers: Any, 
        command_name: str, 
        command_help: str
    ) -> None:
        self.config = config
        self.default_dbenv = self.get_default_dbenv(config)
        self.dbconn_settings, self.run_tests_by, self.no_password = self.get_dbenv_config(config, self.default_dbenv)
        self.use_run_tests_by_user = False

        if OPTIONS_CONFIG_GROUP not in self.config:
            raise CommandError(f"Missing configuration group '{OPTIONS_CONFIG_GROUP}' in configuration file '{TOML_CONFIG_FILE}'.")  
        self.options = config[OPTIONS_CONFIG_GROUP]
    
        self.file_read_encoding =  self.options.get("file_read_encoding", OPTIONS_DEFAULT_FILE_READ_ENCODING)
        self.file_read_encoding_errors =  self.options.get("file_read_encoding_errors", OPTIONS_DEFAULT_FILE_READ_ENCODING_ERRORS)
        self.file_glob_filters =  self.options.get("file_glob_filters", OPTIONS_DEFAULT_FILE_GLOB_FILTERS)
        
        self.parser = subparsers.add_parser(command_name, help=command_help)
        self.parser.add_argument("schema_name", type=str, help="the name of target database schema")
        self.parser.add_argument("--dbenv", type=str, default=self.default_dbenv, help="db environment name within TOML config")
        self.parser.add_argument("--host", type=str, default=None, help="db server host name")
        self.parser.add_argument("--port", type=int, default=None, help="db server port")
        self.parser.add_argument("--dbname", type=str, default=None, help="database name")
        self.parser.add_argument("--user", type=str, default=None, help="user name")
        self.parser.add_argument("-n","--no-password",  action="store_true", default=False, help="don't ask user password")
        self.parser.set_defaults(call=self) 

    def __enter__(self) -> Self:
        if self.args.dbenv is not None:
            self.dbconn_settings, self.run_tests_by, self.no_password = self.get_dbenv_config(self.config, self.args.dbenv)  
        if self.args.host is not None:
            self.dbconn_settings["host"]=self.args.host
        if self.args.port is not None:
            self.dbconn_settings["port"]=self.args.port
        
        if self.args.user is not None:
            self.dbconn_settings["user"]=self.args.user
        elif self.run_tests_by is not None and self.use_run_tests_by_user:
            self.dbconn_settings["user"]=self.run_tests_by
        
        if self.args.dbname is not None:
            self.dbconn_settings["dbname"]=self.args.dbname
            
        if not self.args.no_password and not self.no_password:
            password = None
            if self.use_run_tests_by_user:
                password = os.getenv(
                    DBCONN_TESTER_PASSWORD_ENVVAR_NAME,
                    os.getenv(DBCONN_USER_PASSWORD_ENVVAR_NAME))
            else:
                password = os.getenv(DBCONN_USER_PASSWORD_ENVVAR_NAME)
            if password is None:
                raise CommandError(f"The database user password must be specified via the environment variable '{DBCONN_USER_PASSWORD_ENVVAR_NAME}'.")
            self.dbconn_settings["password"]=password
        else:
            self.dbconn_settings["password"]=None

        try:
            self.dbconn = psycopg.connect(**self.dbconn_settings)
        except psycopg.Error as pg_error:
            error_message = str(pg_error)
            raise CommandError(f"Unable to establish connection to database server. Inner error: {error_message}")
        print(f"Opened db connection: '{self.dbconn_get_connection_string(self.dbconn)}'")
        self.dbconn.add_notice_handler(log_server_notices)
        self.dbconn.autocommit = True 
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool | None:
        if exc_type is not None:
            self.dbconn.rollback()
            print(f"Rolled back transaction.")
        if self.dbconn is not None:
            self.dbconn.close()
            print(f"Closed db connection.")
        return False # propagate the exception

    @abstractmethod
    def run(self) -> None:
        pass
    
    def __call__(self, args):
        self.args = args
        with self:
            self.run()

class UpdateCommand (BaseCommand):
    """Applies base, versioned, and repeatable scripts to the target database schema."""

    def run_baseline_scripts_with_external_tool(
        self, 
        version: str, 
        scripts_dir: Path, 
        scripts: list[Path], 
        tool: ExternalTool
    ) -> None:
        print(f"Running baseline scripts with external tool '{tool.exec_path}'")                
        script_infos = [ScriptFsInfo.get_info(scripts_dir, s) for s in scripts]
        for i in script_infos:
            print(f"Running script: {i!r}...")
            tool.run(i.script_path)            
        print(f"Setting the baseline version '{version}'...")        
        schema_id = self.get_schema_name()        
        version_sql = self.format_sql(
            "INSERT INTO {schema_name}.dbmigration_versions (version_id, is_baseline) VALUES (%s, TRUE);", 
            schema_name=schema_id
        )                                  
        script_sql = self.format_sql(
            "INSERT INTO {schema_name}.dbmigration_version_scripts (version_id, relative_path, git_blob_sha1) VALUES (%s, %s, %s);", 
            schema_name=schema_id
        )
        with self.dbconn.transaction():
            with self.dbconn.cursor() as cur:
                cur.execute(version_sql, (version,))                
                for i in script_infos:
                    cur.execute(script_sql, (version, i.relative_path, i.oid))                    
        print("Committed.")

    def run_baseline_scripts_each_in_own_tran(
        self, 
        version: str, 
        scripts_dir: Path, 
        scripts: list[Path]
    ) -> None:        
        script_infos = [ScriptFsInfo.get_info_with_text(scripts_dir, s) for s in scripts]
        for i in script_infos:
            print(f"Running script: {i!r}...")
            with self.dbconn.transaction():
                with self.dbconn.cursor() as cur:
                    cur.execute(i.text)                                  
            print(f"Committed.")
        print(f"Setting the baseline version to: '{version}'.")
        schema_id = self.get_schema_name()
        version_sql = self.format_sql(
            "INSERT INTO {schema_name}.dbmigration_versions (version_id, is_baseline) VALUES (%s, TRUE)",  
            schema_name=schema_id)                                  
        script_sql = self.format_sql(
            "INSERT INTO {schema_name}.dbmigration_version_scripts (version_id, relative_path, git_blob_sha1) VALUES (%s, %s, %s);\n", 
            schema_name=schema_id)
        with self.dbconn.transaction():
            with self.dbconn.cursor() as cur:
                cur.execute(version_sql, (version,))
                for i in script_infos:
                    cur.execute(script_sql, (version, i.relative_path, i.oid))
        print(f"Committed.")

    def rerun_versioned_scripts(
        self, 
        version: str, 
        scripts_dir: Path, 
        scripts: list[Path]
    ) -> None: 
        print(f"Reapply version {version}...")
        script_infos = [
            ScriptFsInfo.get_info_with_text(
                scripts_dir, s, self.file_read_encoding, encoding_errors=self.file_read_encoding_errors) for s in scripts]
        schema_id=self.get_schema_name()        
        with self.dbconn.transaction():
            with self.dbconn.cursor() as cur:
                formatted_sql = self.format_sql(
                    "DELETE FROM {schema_name}.dbmigration_version_scripts WHERE version_id=%s", schema_name=schema_id)
                cur.execute(formatted_sql, (version,))    
                formatted_sql = self.format_sql(
                    "DELETE FROM {schema_name}.dbmigration_versions WHERE version_id=%s", schema_name=schema_id)
                cur.execute(formatted_sql, (version,))    
                for i in script_infos:
                    print(f"Running script: {i!r}...")
                    cur.execute(i.text)                              
                formatted_sql = self.format_sql(
                    "INSERT INTO {schema_name}.dbmigration_versions (version_id, is_baseline) VALUES (%s, FALSE)", schema_name=schema_id)                                  
                cur.execute(formatted_sql, (version,))
                for i in script_infos:
                    formatted_sql = self.format_sql(
                        "INSERT INTO {schema_name}.dbmigration_version_scripts (version_id, relative_path, git_blob_sha1) VALUES (%s, %s, %s);\n", schema_name=schema_id)
                    cur.execute(formatted_sql, (version, i.relative_path, i.oid))        
        print(f"Committed.")

    def run_versioned_scripts_in_tran(
        self, 
        version: str, 
        scripts_dir: Path, 
        scripts: list[Path]
    ) -> None:             
        script_infos = [
            ScriptFsInfo.get_info_with_text(
                scripts_dir, s, self.file_read_encoding, encoding_errors=self.file_read_encoding_errors) for s in scripts]        
        print(f"Apply version {version}...")
        schema_id=self.get_schema_name() 
        with self.dbconn.transaction():
            with self.dbconn.cursor() as cur:
                for i in script_infos:
                    print(f"Running script: {i!r}...")
                    cur.execute(i.text)
                formatted_sql = self.format_sql(
                    "INSERT INTO {schema_name}.dbmigration_versions (version_id, is_baseline) VALUES (%s, FALSE)", schema_name=schema_id)                                  
                cur.execute(formatted_sql, (version,))
                for i in script_infos:
                    formatted_sql = self.format_sql(
                        "INSERT INTO {schema_name}.dbmigration_version_scripts (version_id, relative_path, git_blob_sha1) VALUES (%s, %s, %s);\n", schema_name=schema_id)
                    cur.execute(formatted_sql, (version, i.relative_path, i.oid))
        print(f"Committed.")


    def __init__(self, config: dict[str, Any], subparsers: Any) -> None: 
        super().__init__(config, subparsers, "update", UpdateCommand.__doc__)
        
        self.parser.add_argument(
            "--force-reapply-latest-version",  
            action="store_true", 
            help="clean up the latest version within the database and reapply the included *.sql scripts."
        )
        self.parser.add_argument(
            "--force-reapply-all-repeatable",  
            action="store_true", 
            help="reapply all repeatable scripts, regardless of changes."
        )
        self.parser.add_argument(
            "--force-run-cleanup",  
            action="store_true", 
            help="run the cleanup script before executing version-specific scripts."
        )
        self.parser.add_argument(
            "--skip-confirmation",  
            action="store_true", 
            help="skip confirmation before executing updates."
        )
        self.parser.add_argument(
            "scripts_path", 
            type=str, 
            help="source scripts repository path"
        )

    def apply_baseline_scripts(self) -> None:
        scripts_dir = self.get_resolved_scripts_dir()
        baseline_dir = scripts_dir.joinpath(BASELINE_DIR_NAME)
        if not baseline_dir.exists():
            print(f"The scripts directory '{scripts_dir}' is missing '{BASELINE_DIR_NAME}' subdirectory. Baseline scripts will be skipped.")
            return
        if self.check_if_version_table_include_baseline_version():
            print("The target schema already has the baseline version installed. Baseline scripts will be skipped.")
            return
        baseline_subdirs = [item for item in baseline_dir.iterdir() if item.is_dir()]
        baseline_subdirs_len = len(baseline_subdirs)
        if baseline_subdirs_len != 1:
            raise CommandError(f"The baseline path {baseline_dir} must have single subdirectory with the baseline scripts but {baseline_subdirs_len} was found")
        baseline_version_subdir = baseline_subdirs[0]
        baseline_version = baseline_version_subdir.name
        print(f"The baseline version to install {baseline_version}.")       
        print("Apply baseline scripts...")
        scripts_sorted = self.get_sorted_scripts_from_dir(
            baseline_version_subdir, BASELINE_FILES_DEPTH, force_run_cleanup = self.args.force_run_cleanup)
        
        external_tool = ExternalTool.try_get(
            baseline_version_subdir, self.get_schema_name_arg(), self.dbconn_settings, self.config)
        if external_tool:
            self.run_baseline_scripts_with_external_tool(
                baseline_version, scripts_dir, scripts_sorted, external_tool)
        else:
            self.run_baseline_scripts_each_in_own_tran(
                baseline_version, scripts_dir, scripts_sorted)

        print("Baseline scripts applied.")       

    def reapply_the_latest_version(self) -> None:
        scripts_dir = self.get_resolved_scripts_dir()
        versioned_dir = scripts_dir.joinpath(VERSIONED_DIR_NAME)
        if not versioned_dir.exists():
            print(f"The scripts directory '{scripts_dir}' is missing the required '{VERSIONED_DIR_NAME}' subdirectory.")
            return
        
        latest_installed = self.check_if_any_latest_version_installed()
        print(f"The latest installed version is {latest_installed}.")

        latest_version_dir = versioned_dir.joinpath(latest_installed)
        if not latest_version_dir.is_dir():
            raise CommandError(
                f"There is no subdirectory with scripts that matched to the "
                f"latest installed version '{latest_installed}'"
            )
        
        scripts_sorted = self.get_sorted_scripts_from_dir(
            latest_version_dir, VERSIONED_FILES_DEPTH, force_run_cleanup=True)
        if not scripts_sorted:
            filters_str = ",".join(self.file_glob_filters)
            raise CommandError(f"The scripts subdirectory '{latest_version_dir}' does not contain any '{filters_str}' scripts")
        
        self.rerun_versioned_scripts(latest_installed, scripts_dir, scripts_sorted)

    def apply_versioned_scripts(self) -> None:
        scripts_dir = self.get_resolved_scripts_dir()
        force_run_cleanup = self.args.force_run_cleanup
        versioned_dir = scripts_dir.joinpath(VERSIONED_DIR_NAME)
        if not versioned_dir.exists():
            print(f"The scripts directory '{scripts_dir}' is missing '{VERSIONED_DIR_NAME}' subdirectory. Version scripts will be skipped.")
            
            return

        if not self.check_if_version_table_include_baseline_version():
            raise CommandError("The baseline version must be installed before running versioned scripts")

        versioned_subdirs = [item for item in versioned_dir.iterdir() if item.is_dir()]
        if not versioned_subdirs:
            raise CommandError(f"The versioned scripts path {versioned_dir} must have at least one subdirectory but nothing was found")

        latest_installed = self.check_if_any_latest_version_installed()
        print(f"The latest installed version is {latest_installed}.")       

        newer_version_subdirs = [item for item in versioned_subdirs if item.name > latest_installed]
        if not newer_version_subdirs:
            print("No newer versions found for installation.")       
            return

        print(f"Found {len(newer_version_subdirs)} new versions for installation.")       
        print("Apply versioned scripts...")

        sorted_subdirs = sorted(newer_version_subdirs)

        for version_dir in sorted_subdirs:        
            version_id = version_dir.name
            scripts_sorted = self.get_sorted_scripts_from_dir(
                version_dir, VERSIONED_FILES_DEPTH, force_run_cleanup=force_run_cleanup
            )
            
            if not scripts_sorted:
                filters_str = ",".join(self.file_glob_filters)
                raise CommandError(f"The scripts subdirectory '{version_dir}' does not contain any '{filters_str}' scripts")
                
            self.run_versioned_scripts_in_tran(version_id, scripts_dir, scripts_sorted)       

        print("Versioned scripts applied.")

    def apply_repeatable_scripts(self, force_reapply: bool = False) -> None:        
        scripts_dir = self.get_resolved_scripts_dir()
        repeatable_dir = scripts_dir.joinpath(REPEATABLE_DIR_NAME)
        if not repeatable_dir.exists():
            print(f"The scripts directory '{scripts_dir}' is missing the required '{REPEATABLE_DIR_NAME}' subdirectory.")
            return

        print("Check repeatable scripts...")
        target_version_file_path = repeatable_dir.joinpath(TARGET_VERSION_FILE)
        if not target_version_file_path.exists():
            raise CommandError(f"The file with target version '{TARGET_VERSION_FILE}' does not exist in repeatable scripts subdirectory '{repeatable_dir}'.")

        target_version = read_as_trimmed_string(target_version_file_path)

        latest_installed_version = self.check_if_any_latest_version_installed() 
        if latest_installed_version != target_version:
            raise CommandError(f"The target version {target_version} for repeatable scripts does not match the latest installed version '{latest_installed_version}'.")                  

        print(f"Target version matches the latest installed version: '{target_version}'.")

        repeatable_scripts_sorted = self.get_sorted_scripts_from_dir(repeatable_dir, REPEATABLE_FILES_DEPTH)

        scripts_to_repeat = []
        if force_reapply:
            scripts_to_repeat = [*repeatable_scripts_sorted]
        else:
            script_infos = [
                ScriptFsInfo.get_info(scripts_dir, s) for s in repeatable_scripts_sorted
            ]
            scripts_to_repeat = [
                i.script_path
                for i in script_infos
                if not self.check_if_repeatable_script_installed(i.oid, target_version, i.relative_path)
            ]

        if not scripts_to_repeat:
            print("No modified repeatable scripts found for (re)installation.")       
            return

        scripts_to_repeat = self.resolve_scripts_dependencies(
            repeatable_dir, REPEATABLE_FILES_DEPTH, repeatable_scripts_sorted, scripts_to_repeat
        )
        
        script_infos = [
            ScriptFsInfo.get_info_with_text(
                scripts_dir, s, encoding=self.file_read_encoding, encoding_errors=self.file_read_encoding_errors
            ) 
            for s in scripts_to_repeat
        ]
        
        print(f"Found {len(script_infos)} scripts to re-run")
        print("Apply repeatable scripts...")

        schema_id = self.get_schema_name()
        repeatable_sql = self.format_sql(
            "INSERT INTO {schema_name}.dbmigration_repeatable_scripts (git_blob_sha1, version_id, relative_path) VALUES (%s, %s, %s)", 
            schema_name=schema_id
        )                                  

        for i in script_infos:
            print(f"Running script: {i!r}...")
            with self.dbconn.transaction():
                with self.dbconn.cursor() as cur:
                    cur.execute(i.text)
                    cur.execute(repeatable_sql, (i.oid, target_version, i.relative_path))
            print("Committed.")

        print("Repeatable scripts applied.")

    def run(self) -> None:
        if not self.args.skip_confirmation:
            print("You are going to run updates. Would you like to continue? [y/N]: ", end="", flush=True)
            answer = get_char().lower()
            if answer != 'y':
                raise CommandError("Cancelled by user")
        
        self.do_initial_cross_checks()        
        
        applied_count = self.apply_all_own_migrations()
        if applied_count > 0:
            print(f"Version control tables updated. Please rerun the tool to update the schema using your scripts.")
            return

        self.check_if_all_version_control_tables_exist()
        self.check_if_stored_environment_id_matches_to_scripts_dir() 

        scripts_dir = self.get_scripts_path_arg()        
        if self.args.force_reapply_latest_version:
            print(f"Performing reapply latest version from scripts repository: '{scripts_dir}'")
            self.reapply_the_latest_version()
            self.apply_repeatable_scripts(force_reapply=True)
            print("Reapplied.")
        else:
            print(f"Performing updates from scripts repository: '{scripts_dir}'")
            self.check_if_max_version_of_versioned_scripts_matches_repeatable_target()
            self.apply_baseline_scripts()
            self.apply_versioned_scripts()
            self.apply_repeatable_scripts(force_reapply=self.args.force_reapply_all_repeatable)
            print("Updated.")

class UpdateScriptBuilder:
    target_script_path: Path
    temp_script_path: Path
    written_body_bytes: int
    temp_file: TextIO | None

    def __init__(self, script_path: Path | str) -> None:
        self.target_script_path = Path(script_path)
        self.temp_script_path = Path(script_path).with_suffix(".temp")
        self.written_body_bytes = 0
        self.temp_file = None
    
    def check(self) -> None:
        assert self.target_script_path is not None, "self.target_script_path must be initialized"
        assert isinstance(self.target_script_path, Path), "self.target_script_path must be a pathlib.Path"

        if not self.target_script_path.parent.exists():
            raise CommandError(
                f"The parent directory '{self.target_script_path.parent}' does not exist"
            )
        try:
            self.target_script_path.touch(exist_ok=False)
        except FileExistsError:
            raise CommandError(
                f"The specified script file '{self.target_script_path}' already exists"
            )
        except PermissionError:
            raise CommandError(
                f"The specified script file '{self.target_script_path}' is not accessible for write"
            )
        except OSError as e:
            raise CommandError(
                f"System error while verifying path '{self.target_script_path}': {e}"
            )        
        try:
            self.temp_script_path.open("w").close()
        except Exception as e:
            raise CommandError(
                f"Unable to write to temporary target script file '{self.temp_script_path}'"
            )

    def get_written_body_bytes(self) -> int:
        return self.written_body_bytes

    def __enter__(self) -> Self:
        assert self.temp_script_path is not None, "self.temp_script_path must be initialized"
        assert isinstance(self.temp_script_path, Path), "self.temp_script_path must be a pathlib.Path"
        self.temp_file = self.temp_script_path.open("a", encoding="utf-8")
        return self

    def __exit__(
        self, 
        exc_type: Type[BaseException] | None, 
        exc_val: BaseException | None, 
        exc_tb: TracebackType | None
    ) -> bool:
        if self.temp_file and not self.temp_file.closed:
            self.temp_file.close()        
        if exc_type is not None:
            try:
                self.temp_script_path.unlink()
            except Exception:
                pass 
            try:
                self.target_script_path.unlink()
            except Exception:
                pass 
        return False 

    def write_header(self, s: str) -> None:
        assert self.temp_file is not None, "The temporary file is not initialized yet. Ensure you are inside the 'with' context."
        assert not self.temp_file.closed, "The temporary file is not opened. Ensure you are inside the 'with' context."
        self.temp_file.write(s)

    def write_body(self, s: str) -> None:
        assert self.temp_file is not None, "The temporary file is not initialized yet. Ensure you are inside the 'with' context."
        assert not self.temp_file.closed, "The temporary file is not opened. Ensure you are inside the 'with' context."

        written = self.temp_file.write(s)
        if written > 0:
            self.written_body_bytes += written

    def write_body_lines(self, lines: Iterable[str]) -> None:
        assert self.temp_file is not None, "The temporary file is not initialized yet. Ensure you are inside the 'with' context."
        assert not self.temp_file.closed, "The temporary file is not opened. Ensure you are inside the 'with' context."
        for s in lines:
            written = self.temp_file.write(s)
            if written > 0:
                self.written_body_bytes += written
    
    def cleanup(self) -> None:
        if self.temp_file and not self.temp_file.closed:
            self.temp_file.close()

        if self.temp_script_path is not None:
            try:
                self.temp_script_path.unlink()
            except Exception as e:
                message = str(e)
                print(f"Warning: Unable cleanup temporary file '{self.temp_script_path}'. Inner error: {message}")
        else:
            print(f"Warning: The temporary script path is not initialized")

        if self.target_script_path is not None:
            try:
                self.target_script_path.unlink()
            except Exception as e:
                message = str(e)
                print(f"Warning: Unable cleanup target file '{self.target_script_path}'. Inner error: {message}")
        else:
            print(f"Warning: The target script path is not initialized")

    def finalize(self) -> None:
        assert isinstance(self.temp_script_path, Path)
        assert isinstance(self.target_script_path, Path)

        if self.temp_file and not self.temp_file.closed:
            self.temp_file.close()

        try:
            if self.target_script_path.exists():
                self.target_script_path.unlink()
            self.temp_script_path.rename(self.target_script_path)
        except Exception as e:
            message = str(e)
            raise CommandError(
                f"Unable to rename temporary file '{self.temp_script_path}' to the target script file {self.target_script_path}. Inner error: {message}"
            )

class VerifyCommand (BaseCommand):
    """Validates the target schema and lists versioned and reproducible scripts to apply if the 'update' command is executed."""
    
    def make_dbconn_session_readonly(self) -> None:
        sql = """
            SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY"""
        with self.dbconn.cursor() as cur:
            cur.execute(sql)

    def get_baseline_version_installed(self) -> str|None:
        sql = """
                SELECT version_id FROM {schema_name}.dbmigration_versions WHERE is_baseline IS TRUE ORDER BY version_id DESC LIMIT 1"""
        schema_id = self.get_schema_name()
        formatted_sql = self.format_sql(sql, schema_name=schema_id)
        value = self.dbconn_get_single_value(formatted_sql, [])
        return value

    def cross_check_of_the_target_version_for_repeatable_scripts(self, target_version, latest_version_in_scripts, latest_installed_version):
        if latest_version_in_scripts is None and latest_installed_version is None:
            raise CommandError(f"Failed to check target version '{target_version}' because no version is installed and no versioned scripts were provided in the scripts directory.")
        elif latest_version_in_scripts is None:
            if target_version != latest_installed_version:
                raise CommandError(f"The target version '{target_version}' does not match the latest installed version '{latest_installed_version}'.")
        elif latest_installed_version is None:
            if target_version != latest_version_in_scripts:
                raise CommandError(f"The target version '{target_version}' does not match the latest scripts version '{latest_version_in_scripts}'.")
        elif latest_version_in_scripts > latest_installed_version:
            if target_version != latest_version_in_scripts:
                raise CommandError(f"The target version '{target_version}' does not match the latest scripts version '{latest_version_in_scripts}'.")
        elif latest_version_in_scripts <= latest_installed_version:
            if target_version != latest_installed_version:
                raise CommandError(f"The target version '{target_version}' does not match the latest installed version '{latest_installed_version}'.")
    
    def display_required_changes_by_commits(self, script_infos: list[ScriptFsInfo]) -> None:
        assert self.git is not None

        commits_group = collections.defaultdict(list)        
        for i in script_infos:
            commit_info = self.git.get_latest_commit(i.script_path)
            commits_group[commit_info].append(i)

        sorted_commits = sorted(
            commits_group.items(),
            key=lambda i: i[0].sort_key(), 
            reverse=True
        )

        for commit, scripts in sorted_commits:
            print(f"{commit!r}")
            for s in scripts:
                print(f"    {s!r}")                

    def display_required_changes(self, script_infos: list[ScriptFsInfo]) -> None:
        if self.git is None:
            for i in script_infos:
                print(f"  {i!r}")
        else:
            self.display_required_changes_by_commits(script_infos)

    def display_required_changes_by_path(self, scripts_dir: Path, scripts_sorted: list[Path]) -> None:
        script_infos = [ScriptFsInfo.get_info(scripts_dir, s) for s in scripts_sorted] 
        if self.git is None:
            for i in script_infos:
                print(f"  {i!r}")
        else:
            self.display_required_changes_by_commits(script_infos)

    def get_recent_changes_from_db(self, limit:int, window_minutes:int) -> list[TupleRow]:
        sql = """
            WITH latest_time AS (
                SELECT COALESCE(MAX(applied_at), NOW()) AS max_at
                FROM (
                    SELECT MAX(v.created_at) AS applied_at FROM {schema_name}.dbmigration_versions v
                    UNION ALL
                    SELECT MAX(r.created_at) AS applied_at FROM {schema_name}.dbmigration_repeatable_scripts r
                ) t
            )
            SELECT 
                v.created_at AS applied_at,
                'versioned' AS script_type,
                s.version_id,
                s.relative_path,
                s.git_blob_sha1
            FROM {schema_name}.dbmigration_version_scripts s
            JOIN {schema_name}.dbmigration_versions v ON s.version_id = v.version_id
            CROSS JOIN latest_time
            WHERE v.created_at >= latest_time.max_at - ({window_minutes} || ' minutes')::interval

            UNION ALL

            SELECT 
                r.created_at AS applied_at,
                'repeatable' AS script_type,
                r.version_id,
                r.relative_path,
                r.git_blob_sha1
            FROM {schema_name}.dbmigration_repeatable_scripts r
            CROSS JOIN latest_time
            WHERE r.created_at >= latest_time.max_at - ({window_minutes} || ' minutes')::interval

            ORDER BY applied_at DESC
            LIMIT {limit};

        """
        schema_id = self.get_schema_name()
        formatted_sql = self.format_sql(sql, schema_name=schema_id, limit=limit, window_minutes=window_minutes)        
        with self.dbconn.cursor() as cursor:
            cursor.execute(formatted_sql, [])
            rows = cursor.fetchall()
        return rows

    def display_recent_changes_grouped_by_git_commits(self, rows: list[TupleRow]) -> None:
        assert self.git is not None

        commits_group = collections.defaultdict(list)

        for applied_at, script_type, version_id, relative_path, git_blob_sha1 in rows:
            script_info = ScriptDbInfo(
                applied_at=applied_at, 
                script_type=script_type, 
                version_id=version_id, 
                relative_path=relative_path, 
                git_blob_sha1=git_blob_sha1)
            clean_oid = git_blob_sha1.strip()
            commit_info = self.git.get_commit_by_file_oid(clean_oid)
            commits_group[commit_info].append(script_info)
        
        sorted_commits = sorted(
            commits_group.items(), 
            key=lambda i: i[0].sort_key(),
            reverse=True
        )
        for commit, scripts in sorted_commits:
            print(f"{commit!r}")
            for s in scripts:
                print(f"  {s!r}")

    def display_recent_changes(self, limit:int = 10, window_minutes:int = 30) -> None:
        
        rows = self.get_recent_changes_from_db(limit, window_minutes)
        if not rows:
            return
        print(f"The list of recent changes were applied to the target schema:")

        if self.git is None:
            for applied_at, script_type, version_id, relative_path, git_blob_sha1 in rows:
                script_info = ScriptDbInfo(
                    applied_at=applied_at, 
                    script_type=script_type, 
                    version_id=version_id, 
                    relative_path=relative_path, 
                    git_blob_sha1=git_blob_sha1)
                print(f"  {script_info!r}")
        else:
            self.display_recent_changes_grouped_by_git_commits(rows)


    def __init__(self, config: dict[str, Any], subparsers: Any) -> None: 
        super().__init__(config, subparsers, "verify", VerifyCommand.__doc__)
        
        # for action="store_true" the value False is by default  
        self.parser.add_argument(
            "--skip-git-checks",  
            action="store_true", 
            help="skip grouping changes by git commits"
        )
        self.parser.add_argument(
            "--skip-display-recent-changes",  
            action="store_true", 
            help="skip display recent changes stored within target db schema"
        )
        self.parser.add_argument(
            "--build-update-script", 
            type=str, 
            help="the update script path if you want one as an additional result of the verify command"
        )
        self.parser.add_argument(
            "scripts_path", 
            type=str, 
            help="source scripts repository path"
        )        
        self.latest_version_in_scripts: str | None = None


    def write_search_path(self, search_path: str, builder: UpdateScriptBuilder) -> None:
        with builder:
            sql_comment = self.format_sql_comment(f"Setting session search path to: {search_path}")
            builder.write_header(sql_comment)
            sql_text = self.format_sql_text(
                "SELECT pg_catalog.set_config('search_path', {search_path}, false);\n\n", search_path=search_path)
            builder.write_header(sql_text)

    def write_baseline_scripts(self, version: str, scripts_dir: Path, scripts: list[Path], script_builder: UpdateScriptBuilder) -> None:
        encoding = self.file_read_encoding 
        errors = self.file_read_encoding_errors 
        script_infos = [ 
            ScriptFsInfo.get_info_with_text(
                scripts_dir, s, encoding=encoding, encoding_errors=errors) for s in scripts
        ]
        with script_builder:
            sql_comment = self.format_sql_comment(f"-- --------- BASELINE VERSION: {version} ---------")
            script_builder.write_body(sql_comment)
            for i in script_infos:
                script_builder.write_body(f"BEGIN;\n")
                sql_comment = self.format_sql_comment(f"-- Apply script: [{i.relative_path} (OID:{i.oid:.8})]")                    
                script_builder.write_body(sql_comment)
                script_builder.write_body_lines(i.text)
                script_builder.write_body(f"\n-- End of script.\n")
                script_builder.write_body(f"COMMIT;\n")
            schema_id = self.get_schema_name()
            script_builder.write_body(f"BEGIN;\n")
            sql_text = self.format_sql_text(
                "INSERT INTO {schema_name}.dbmigration_versions (version_id, is_baseline) VALUES ({version_id}, TRUE);\n", 
                schema_name=schema_id, version_id=version)
            script_builder.write_body(sql_text)
            for i in script_infos:
                sql_text = self.format_sql_text(
                    "INSERT INTO {schema_name}.dbmigration_version_scripts (version_id, relative_path, git_blob_sha1) VALUES ({version_id}, {relative_path},{git_blob_sha1});\n", 
                    schema_name=schema_id, version_id=version,relative_path=i.relative_path,git_blob_sha1=i.oid)
                script_builder.write_body(sql_text)
            script_builder.write_body(f"COMMIT;\n")

    def verify_baseline_scripts(self, script_builder: UpdateScriptBuilder | None) -> None:
        scripts_dir = self.get_resolved_scripts_dir()
        baseline_dir = scripts_dir.joinpath(BASELINE_DIR_NAME)
        if not baseline_dir.exists():
            print(f"The scripts directory '{scripts_dir}' is missing '{BASELINE_DIR_NAME}' subdirectory. Baseline scripts will be skipped.")
            return
        
        if self.check_if_version_table_include_baseline_version():
            installed_baseline_version = self.get_baseline_version_installed()
            print(f"The target schema has the baseline version installed: {installed_baseline_version}")
            return
        baseline_subdirs = [item for item in baseline_dir.iterdir() if item.is_dir()]
        if (baseline_subdirs_len := len(baseline_subdirs)) != 1:
            raise CommandError(f"The baseline path {baseline_dir} must have single subdirectory "
                               f"with the baseline scripts but {baseline_subdirs_len} was found")
        baseline_version_subdir = baseline_subdirs[0]
        baseline_version = baseline_version_subdir.name

        scripts_sorted = self.get_sorted_scripts_from_dir(baseline_version_subdir, BASELINE_FILES_DEPTH)
        print(f"Baseline scripts to install: ")
        self.display_required_changes_by_path(scripts_dir, scripts_sorted)

        if script_builder:
            self.write_baseline_scripts(baseline_version, scripts_dir, scripts_sorted, script_builder)

        # remember latest version in scripts for the further use in verify_repeatable()
        self.latest_version_in_scripts = baseline_version

    def write_versioned_scripts(self, version : str, scripts_dir: Path, scripts: list[Path], script_builder: UpdateScriptBuilder) -> None:
        encoding = self.file_read_encoding 
        errors = self.file_read_encoding_errors 
        script_infos = [ 
            ScriptFsInfo.get_info_with_text(
                scripts_dir, s, encoding=encoding, encoding_errors=errors) for s in scripts
        ]
        with script_builder:            
            sql_comment = self.format_sql_comment(f"-- --------- VERSION: {version} ---------")
            script_builder.write_body(sql_comment)
            script_builder.write_body(f"\nBEGIN;\n")
            for i in script_infos:
                sql_comment = self.format_sql_comment(f"-- Apply script: [{i.relative_path} (OID:{i.oid:.8})]")
                script_builder.write_body(sql_comment)
                script_builder.write_body_lines(i.text)
                script_builder.write_body(f"\n-- End of script.\n")
            schema_id = self.get_schema_name()
            sql_text = self.format_sql_text(
                "INSERT INTO {schema_name}.dbmigration_versions (version_id, is_baseline) VALUES ({version_id}, FALSE);\n", 
                schema_name=schema_id, version_id=version)
            script_builder.write_body(sql_text)
            for i in script_infos:
                sql_text = self.format_sql_text(
                    "INSERT INTO {schema_name}.dbmigration_version_scripts (version_id, relative_path, git_blob_sha1) VALUES ({version_id}, {relative_path},{git_blob_sha1});\n", 
                    schema_name=schema_id, version_id=version,relative_path=i.relative_path,git_blob_sha1=i.oid)
                script_builder.write_body(sql_text)
            script_builder.write_body(f"COMMIT;\n")

    def verify_versioned_scripts(self, script_builder: UpdateScriptBuilder | None) -> None:
        scripts_dir = self.get_resolved_scripts_dir()
        versioned_dir = scripts_dir.joinpath(VERSIONED_DIR_NAME)

        if not versioned_dir.exists():
            print(f"The scripts directory '{scripts_dir}' is missing '{VERSIONED_DIR_NAME}' subdirectory. Version scripts will be skipped.")
            return
        
        versioned_subdirs = [item for item in versioned_dir.iterdir() if item.is_dir()]
        if not versioned_subdirs:
            raise CommandError(
                f"Versioned scripts path {versioned_dir} must contain at least one subdirectory, but none were found")

        latest_installed_version = self.get_latest_version_installed()
        if latest_installed_version is not None:
            newer_version_subdirs = [item for item in versioned_subdirs if item.name > latest_installed_version]
        else:
            newer_version_subdirs = versioned_subdirs

        if not newer_version_subdirs:
            print(f"The latest installed version is {latest_installed_version}. No newer scripts found for installation.")       
            return
        
        newer_version_subdirs_sorted = sorted(newer_version_subdirs)
        
        # remember latest version in scripts for the further use in verify_repeatable()
        latest_version = newer_version_subdirs_sorted[-1].name

        if self.latest_version_in_scripts is not None and latest_version <= self.latest_version_in_scripts:
            raise CommandError(
                f"The latest script version '{latest_version}' must be greater than the baseline script version '{self.latest_version_in_scripts}'.")

        self.latest_version_in_scripts = latest_version
    
        print(f"Versioned scripts to install: ")    
        for version_dir in newer_version_subdirs_sorted:    
            scripts_sorted = self.get_sorted_scripts_from_dir(version_dir, VERSIONED_FILES_DEPTH)
            if not scripts_sorted:
                filters_str = ",".join(self.file_glob_filters)
                raise CommandError(f"The scripts subdirectory '{version_dir}' does not contain any '{filters_str}' scripts.")
            self.display_required_changes_by_path(scripts_dir, scripts_sorted)
            if script_builder:
                version_id = version_dir.name
                self.write_versioned_scripts(version_id, scripts_dir, scripts_sorted, script_builder)

    def write_repeatable_scripts(self, target_version: str, script_info_with_text_list: list[ScriptFsInfo], script_builder: UpdateScriptBuilder) -> None:
        with script_builder:
            sql_comment = self.format_sql_comment(f"-- --------- REPEATABLE SCRIPTS FOR VERSION: {target_version} ---------")
            script_builder.write_body(sql_comment)
            schema_id = self.get_schema_name()
            for i in script_info_with_text_list:
                script_builder.write_body(f"\nBEGIN;\n")
                sql_comment = self.format_sql_comment(f"-- Apply script: [{i.relative_path} (OID:{i.oid:.8})]")
                script_builder.write_body(sql_comment) 
                if not i.text:
                    ValueError(f"The text property of script info must not be empty string")
                script_builder.write_body_lines(i.text)
                script_builder.write_body(f"\n")
                script_builder.write_body("-- End of script.\n")  
                sql_text = self.format_sql_text(
                    "INSERT INTO {schema_name}.dbmigration_repeatable_scripts (git_blob_sha1, version_id, relative_path) VALUES ({git_blob_sha1}, {version_id}, {relative_path});\n", 
                    schema_name=schema_id, git_blob_sha1=i.oid, version_id=target_version, relative_path=str(i.relative_path))
                script_builder.write_body(sql_text)
                script_builder.write_body(f"COMMIT;\n")

    def verify_repeatable_scripts(self, script_builder: UpdateScriptBuilder | None) -> None:
        scripts_dir = self.get_resolved_scripts_dir()
        repeatable_dir = scripts_dir.joinpath(REPEATABLE_DIR_NAME)
        if not repeatable_dir.exists():
            print(f"The scripts directory '{scripts_dir}' is missing '{REPEATABLE_DIR_NAME}' subdirectory. Repeatable scrips will be skipped.")
            return

        target_version_file_path = repeatable_dir.joinpath(TARGET_VERSION_FILE)
        if not target_version_file_path.exists():
            raise CommandError(f"The target version file '{TARGET_VERSION_FILE}' does not exist in the repeatable scripts subdirectory '{repeatable_dir}'.")

        target_version = read_as_trimmed_string(target_version_file_path)

        latest_installed_version = self.get_latest_version_installed()
        if latest_installed_version is None:
           print("No versions are installed in the database schema.") 

        self.cross_check_of_the_target_version_for_repeatable_scripts(target_version, self.latest_version_in_scripts, latest_installed_version)

        print(f"Target version for repeatable scripts: '{target_version}'.")

        repeatable_scripts_sorted = self.get_sorted_scripts_from_dir(repeatable_dir, REPEATABLE_FILES_DEPTH)

        script_infos = [
            ScriptFsInfo.get_info(scripts_dir, s) for s in repeatable_scripts_sorted
        ]
        scripts_to_repeat = [
            i.script_path
            for i in script_infos
            if not self.check_if_repeatable_script_installed(i.oid, target_version, i.relative_path)
        ]

        if not scripts_to_repeat:
            print("No modified repeatable scripts found for (re)installation.")
            return

        scripts_to_repeat = self.resolve_scripts_dependencies(
            repeatable_dir, REPEATABLE_FILES_DEPTH, repeatable_scripts_sorted, scripts_to_repeat
        )
        
        script_infos = [
            ScriptFsInfo.get_info_with_text(
                scripts_dir, s, encoding=self.file_read_encoding, encoding_errors=self.file_read_encoding_errors
            ) 
            for s in scripts_to_repeat
        ]
        print("Repeatable scripts to (re)install: ")
        self.display_required_changes(script_infos)

        if script_builder:
            self.write_repeatable_scripts(target_version, script_infos, script_builder)

    def run(self) -> None:
        self.make_dbconn_session_readonly()
        self.do_initial_cross_checks()        
        self.check_if_all_own_migrations_are_applied()
        self.check_if_all_version_control_tables_exist()
        self.check_if_stored_environment_id_matches_to_scripts_dir() 
        self.check_if_max_version_of_versioned_scripts_matches_repeatable_target()

        self.git = None
        if not self.args.skip_git_checks:
            scripts_dir = self.get_resolved_scripts_dir()
            self.git = GitChecker.try_get(self.config, scripts_dir)

        script_builder = None
        script_path = self.args.build_update_script
        if self.args.build_update_script is not None:
            script_builder = UpdateScriptBuilder(script_path)
            script_builder.check() 
        try:            
            if script_builder is not None:
                search_path = self.get_search_path_for_scripts()
                self.write_search_path(search_path, script_builder)
            self.verify_baseline_scripts(script_builder)
            self.verify_versioned_scripts(script_builder)
            self.verify_repeatable_scripts(script_builder)
            # finalize writing update script
            if script_builder is not None:
                written = script_builder.get_written_body_bytes()
                if written > 0:
                    script_builder.finalize()
                    print(f"Update script is written to '{script_path}'.")
                else:
                    script_builder.cleanup()
                    print(f"No updates to write for script '{script_path}'. Temp file cleaned up")                
        except Exception:
            if script_builder is not None:
                script_builder.cleanup()
            raise
        if not self.args.skip_display_recent_changes:
            self.display_recent_changes(RECENT_CHANGES_LIMIT, RECENT_CHANGES_WINDOW_MINUTES)

class InitCommand (BaseCommand):
    """Creates version control tables in an empty database schema."""

    def check_if_schema_is_empty(self) -> bool:
        sql = """
            SELECT NOT EXISTS (
                SELECT 1
                FROM pg_class c
                JOIN pg_namespace s ON s.oid = c.relnamespace
                WHERE s.nspname = %s
            )
        """
        schema_name = self.get_schema_name_arg()        
        value = self.dbconn_get_single_value(sql, (schema_name,))
        return bool(value)
    
    def create_version_tracking_tables(self, environment_id: str) -> None:
        ddl = """
            CREATE TABLE {schema_name}.dbmigration_environment_id (
                id VARCHAR(64) NOT NULL,
                is_singleton BOOL NOT NULL DEFAULT TRUE, 
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                created_by VARCHAR(64) NOT NULL DEFAULT SESSION_USER,
                created_from INET DEFAULT INET_CLIENT_ADDR(),
                CONSTRAINT dbmigration_environment_primary_key PRIMARY KEY(id),
                -- restricts insertion of any secondary records
                CONSTRAINT dbmigration_is_singleton_must_be_true CHECK (is_singleton = TRUE),
                CONSTRAINT dbmigration_table_must_contain_only_one_environment UNIQUE (is_singleton)
            );
            GRANT SELECT ON TABLE {schema_name}.dbmigration_environment_id TO PUBLIC;
                    
            CREATE TABLE {schema_name}.dbmigration_versions (
                version_id VARCHAR(64) NOT NULL,
                is_baseline BOOL NOT NULL DEFAULT FALSE,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                created_by VARCHAR(64) NOT NULL DEFAULT SESSION_USER,
                created_from INET DEFAULT INET_CLIENT_ADDR(),
                CONSTRAINT dbmigration_versions_primary_key PRIMARY KEY(version_id) 
            );
            GRANT SELECT ON TABLE {schema_name}.dbmigration_versions TO PUBLIC;

            CREATE TABLE {schema_name}.dbmigration_version_scripts (
                version_id VARCHAR(64) NOT NULL,
                relative_path VARCHAR(2048) NOT NULL,
                git_blob_sha1 VARCHAR(64) NOT NULL,
                CONSTRAINT dbmigration_version_scripts_primary_key PRIMARY KEY(version_id, relative_path),
                CONSTRAINT dbmigration_version_scripts_version_foreign_key FOREIGN KEY (version_id)
                    REFERENCES {schema_name}.dbmigration_versions (version_id)
                    ON DELETE CASCADE
            );
            GRANT SELECT ON TABLE {schema_name}.dbmigration_version_scripts TO PUBLIC;

            CREATE TABLE {schema_name}.dbmigration_repeatable_scripts (
                version_id VARCHAR(64) NOT NULL,
                relative_path VARCHAR(2048) NOT NULL,
                created_at TIMESTAMP(6) WITH TIME ZONE NOT NULL DEFAULT CLOCK_TIMESTAMP(),
                git_blob_sha1 VARCHAR(64) NOT NULL,
                created_by VARCHAR(64) NOT NULL DEFAULT SESSION_USER,
                created_from INET DEFAULT INET_CLIENT_ADDR(),
                CONSTRAINT dbmigration_repeatable_scripts_primary_key PRIMARY KEY(version_id, relative_path, created_at),
                CONSTRAINT dbmigration_repeatable_scripts_version_foreign_key FOREIGN KEY (version_id)
                    REFERENCES {schema_name}.dbmigration_versions (version_id)
                    ON DELETE CASCADE
            );
            GRANT SELECT ON TABLE {schema_name}.dbmigration_repeatable_scripts TO PUBLIC;
        """
        dml = """
            INSERT INTO {schema_name}.dbmigration_environment_id (id, is_singleton) VALUES (%s, TRUE);
        """
        schema_id = self.get_schema_name()
        with self.dbconn.transaction():        
            with self.dbconn.cursor() as cur:
                formatted_ddl = self.format_sql(ddl, schema_name=schema_id)
                cur.execute(formatted_ddl, [])
                formatted_dml = self.format_sql(dml, schema_name=schema_id)
                cur.execute(formatted_dml, (environment_id,))

    def __init__(self, config: dict[str,Any], subparsers: Any) -> None: 
        super().__init__(config, subparsers, "init", InitCommand.__doc__)
        self.parser.add_argument("scripts_path", type=str, help="source scripts repository path")
        self.parser.add_argument("--force-init",  action="store_true", default=False, help="Force create version control tables even on non empty schema")

    def run(self) -> None:
        schema_name = self.get_schema_name_arg()
        if not self.check_if_schema_exists():
            raise CommandError(f"The target schema '{schema_name}' is not accessible")
        self.set_session_search_path(schema_name)

        force_init = self.args.force_init
        if not self.check_if_schema_is_empty():
            if not force_init:
                raise CommandError(f"The target schema '{schema_name}' must be empty")
            self.check_if_all_version_control_tables_do_not_exist()
            print(f"WARNING: Schema is not empty!")

        environment_id = self.get_scripts_environment_id()

        print(f"Creating the version control tables with environment ID: '{environment_id}'")
        self.create_version_tracking_tables(environment_id)
        print(f"Created.")

class TestFailed(Exception):
    """A unit test error."""

class RunTestsCommand (BaseCommand):
    """Runs db unit test scripts to the target database schema."""

    def run_conditional(self, cursor: Cursor[TupleRow], scripts_dir: Path, script_path:Path, script_text: str) -> None:
        path = Path(script_path)
        file_name = path.name
        relative_script_path = get_script_path_for_log(scripts_dir, script_path)
        print(f"Running test: '{relative_script_path}'...", end="", flush=True)
        if file_name.startswith(IS_TRUE_THAT_TEST_PREFIX):
            cursor.execute(script_text)
            result_number = 0
            for results in cursor.results():
                result_number += 1
                if cursor.rowcount > 0:
                    row = cursor.fetchone()
                    value = row[0] if row is not None else False
                    if not value:
                        raise TestFailed(f"({result_number}) Expected true, got {value}!") 
        elif file_name.startswith(DETECT_MISSING_TEST_PREFIX):
            has_failed = False
            cursor.execute(script_text)
            result_number = 0
            for results in cursor.results():
                result_number += 1
                if cursor.rowcount > 0:
                    columns = [desc[0] for desc in cursor.description]
                    print(f"FAIL. ({result_number}) Missing records:")
                    print("=================================")
                    for row in cursor:
                        items = [f"{k}: {v}" for k, v in zip(columns, row)]
                        line = ", ".join(items)
                        print(line)
                    has_failed = True
            if has_failed:
                raise TestFailed(f"Expected no results!")
        elif file_name.startswith(ASSURE_THAT_TEST_PREFIX):
            cursor.execute(script_text)
        else:
            raise TestFailed(f"Unable to detect test type from script name '{file_name}'. It should start with one of the following prefixes: '{IS_TRUE_THAT_TEST_PREFIX}','{DETECT_MISSING_TEST_PREFIX}','{ASSURE_THAT_TEST_PREFIX}'")
        print(f"PASS")

    def is_subpath_of(self, child: Path, parent: Path) -> bool:
        child_parts = Path(child).absolute().parts
        parent_parts = Path(parent).absolute().parts        
        return child_parts[:len(parent_parts)] == parent_parts
    
    def make_savepoint_id(self, folder: Path) -> psycopg.sql.Identifier:
        hash_str = str(hash(folder))
        return psycopg.sql.Identifier("savepoint_" + hash_str)

    def run_test_scripts_each_in_own_tran(self, scripts_dir:Path, scripts: list[Path]) -> None:
        self.fail_count = 0
        self.pass_count = 0
        with self.dbconn.cursor() as cur:
            cur.execute("BEGIN") # start global tran for tests
            setup_folder_stack = []            
            for script_path in scripts:
                script_name = script_path.name
                with open(script_path, 'rt', encoding=self.file_read_encoding, errors=self.file_read_encoding_errors) as f:
                    script_text = f.read()

                if len(setup_folder_stack) > 0:
                    script_folder = str(script_path.absolute().parent)
                    latest_item = setup_folder_stack[-1]
                    if not self.is_subpath_of(script_folder, latest_item):
                        setup_folder_stack.pop()
                        savepoint_id = self.make_savepoint_id(setup_folder)
                        formatted_sql = self.format_sql("ROLLBACK TO SAVEPOINT {savepoint_id}", savepoint_id=savepoint_id)
                        cur.execute(formatted_sql)
                        print(f"Rolled back to savepoint.")
                
                if script_name == SETUP_TESTS_FILE_NAME:
                    setup_folder = str(script_path.absolute().parent)
                    setup_folder_stack.append(setup_folder)
                    savepoint_id = self.make_savepoint_id(setup_folder)
                    formatted_sql = self.format_sql("SAVEPOINT {savepoint_id}", savepoint_id=savepoint_id)
                    print(f"Make savepoint...")
                    cur.execute(formatted_sql)
                    relative_script_path = get_script_path_for_log(scripts_dir, script_path)
                    print(f"Running setup: '{relative_script_path}'...", end="", flush=True)
                    cur.execute(script_text)
                    print(f"DONE")
                    continue
                else:
                    cur.execute("SAVEPOINT savepoint_test_boundary")
                    try:
                        self.run_conditional(cur, scripts_dir, script_path, script_text)
                        self.pass_count += 1
                    except TestFailed as e:
                        self.fail_count += 1
                        print(f"FAIL.", e)
                    except Exception as e:
                        self.fail_count += 1
                        error_type_name = type(e).__name__ 
                        print(f"FAIL. {error_type_name}:", e)
                    cur.execute("ROLLBACK TO SAVEPOINT savepoint_test_boundary")

            cur.execute("ROLLBACK") # rollback global tran for tests

    def __init__(self, config: dict[str, Any], subparsers: Any) -> None:       
        super().__init__(config, subparsers, "run-tests", RunTestsCommand.__doc__)
        self.parser.add_argument("scripts_path", type=str, help="source scripts repository path")
        self.parser.add_argument("--skip-env-checks",  action="store_true", default=False, help="Skip version and environment ID checks to run tests in any plain environment not made by the tool itself")
    
    def __enter__(self) -> Self:
        self.use_run_tests_by_user = True
        return super().__enter__()

    def run_unit_test_scripts(self, scripts_dir: Path) -> None:
        unit_tests_dir = scripts_dir.joinpath(TESTS_DIR_NAME)
        if not unit_tests_dir.exists():
            raise CommandError(f"The scripts directory '{scripts_dir}' is missing the required '{TESTS_DIR_NAME}' subdirectory.")

        if not self.args.skip_env_checks:
            target_version_file_path = unit_tests_dir.joinpath(TARGET_VERSION_FILE)
            if not target_version_file_path.exists():
                raise CommandError(f"The file with target version '{TARGET_VERSION_FILE}' does not exists in unit tests scripts subdirectory '{unit_tests_dir}'.")
            target_version = read_as_trimmed_string(target_version_file_path)
            latest_installed_version = self.check_if_any_latest_version_installed() 
            if latest_installed_version != target_version:
                raise CommandError(f"The target version {target_version} for unit test scripts does not match the latest installed version {latest_installed_version}.")                  
            print(f"Target version matches the latest installed version: '{target_version}'")

        scripts_sorted = self.get_sorted_scripts_from_dir(unit_tests_dir, TESTS_FILES_DEPTH)        
        self.run_test_scripts_each_in_own_tran(scripts_dir, scripts_sorted)
        if self.fail_count > 0:
            raise CommandError(f"Tests failed: {self.fail_count}, passed: {self.pass_count}.")
        else:
            print(f"All {self.pass_count} tests passed.")
            

    def run(self) -> None:
        self.do_initial_cross_checks()
        if not self.args.skip_env_checks:
            self.check_if_all_own_migrations_are_applied()
            self.check_if_all_version_control_tables_exist() 
            self.check_if_stored_environment_id_matches_to_scripts_dir()
        scripts_dir = self.get_resolved_scripts_dir()    
        print(f"Running unit tests for scripts repository: '{scripts_dir}'")
        self.run_unit_test_scripts(scripts_dir)

def read_toml_config() -> dict[str, Any]:
    script_dir = pathlib.Path(__file__).absolute().parent
    target_path = script_dir.joinpath(TOML_CONFIG_FILE)
    with open(target_path, 'rb') as f:
        config = tomllib.load(f)
        return config

def main():
    try:
        config = read_toml_config()

        parser = argparse.ArgumentParser(description=__doc__)    
        subparsers = parser.add_subparsers(dest="cmd", help="Available subcommands")

        UpdateCommand(config, subparsers)
        VerifyCommand(config, subparsers)
        InitCommand(config, subparsers)
        RunTestsCommand(config, subparsers)

        # Parse arguments
        args = parser.parse_args()

        # Call the function associated with the subcommand
        if hasattr(args, 'call'):
            args.call(args)
        else:
            # If no subcommand is given, print help (or handle as needed)
            parser.print_help()
        return 0
    except CommandError as e:    
        error_type_name = type(e).__name__ 
        print(f"Command error:", e)
        return 1
    except psycopg.Error as e:    
        error_type_name = type(e).__name__ 
        print(f"Server error:", e)
        return 1
    except Exception as e:
        error_type_name = type(e).__name__ 
        print(f"Error: {error_type_name}:", e)
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())

