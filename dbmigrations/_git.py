from __future__ import annotations

import getpass
import shutil
import subprocess
import sys
import os
from datetime import datetime
from pathlib import Path
from typing import Any, NamedTuple, Self

from _constants import (
    GIT_CMD_CONFIG_ATTRIBUTE,
    UNCOMMITTED_DATE_LABEL,
    UNCOMMITTED_SHA_LABEL,
)
from _errors import CommandError
from _i18n import _
from _output import Output

_git_output = Output()



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
            message=message or _("Uncommitted changes")
        )

    @classmethod
    def unknown(cls, message: str | None = None) -> Self:
        return cls(
            oid=None,
            author=None,
            date=None,
            message=message or _("No commit history found")
        )

    @property
    def is_uncommitted(self):
        return self.oid is None 

    def __repr__(self) -> str:
        date_label = self.date.strftime("%Y-%m-%d") if self.date is not None else UNCOMMITTED_DATE_LABEL
        oid_label = self.oid[:8] if self.oid else UNCOMMITTED_SHA_LABEL
        message_label = self.message if self.message else _("Uncommitted changes")
        author_label = self.author if self.author else _("Unknown author")
        return _(
            "[{oid_label}] {date_label} - {message_label}\n"
            "  Author: {author_label}"
        ).format(
            oid_label=oid_label,
            date_label=date_label,
            message_label=message_label,
            author_label=author_label
        )
    
    def sort_key(self) -> tuple[datetime, str, str]:
        if self.date is not None:
            sort_date = self.date.replace(tzinfo=None)
        else:
            sort_date = datetime.min
            
        sort_author = self.author if self.author else ""
        sort_oid = self.oid if self.oid else ""        
        return (sort_date, sort_author, sort_oid)

class GitChecker:

    def __init__(self, git_cmd: Path, repo_root: Path, out: Output | None = None):
        self.git_cmd = git_cmd
        self.repo_root = repo_root
        self.out = out if out is not None else Output()

    @classmethod
    def try_get(cls, toml_config: dict[str, Any], scripts_dir: Path, out: Output | None = None) -> Self | None:
        # 1. Look up the git executable path
        git_cmd = cls._try_get_git_cmd_path(toml_config)
        if git_cmd is None:
            return None

        # 2. Locate the root directory of the Git repository
        repo_root = cls._try_get_git_repo_root(git_cmd, scripts_dir)
        if repo_root is None:
            return None
                    
        return cls(git_cmd, repo_root, out)
    
    @classmethod
    def _try_get_git_cmd_path(cls, toml_config: dict[str, Any]) -> Path | None:
        if GIT_CMD_CONFIG_ATTRIBUTE in toml_config:
            cmd_path_str = toml_config[GIT_CMD_CONFIG_ATTRIBUTE]
            cmd_path = Path(cmd_path_str)
            if not cmd_path.exists():
                raise CommandError(
                    _(
                        "The git cmd specified in {git_cmd_config_attribute} of TOML config does not exist!"
                    ).format(git_cmd_config_attribute=GIT_CMD_CONFIG_ATTRIBUTE)
                )
            return cmd_path        
        
        cmd_path_str = shutil.which("git")
        if cmd_path_str is None:
            _git_output.print(_("Warning: Git executable was not found in system PATH. Git features are disabled."))
            return None
            
        return Path(cmd_path_str)
        
    @classmethod
    def _try_get_git_repo_root(cls, git_cmd: Path, scripts_dir: Path) -> Path | None:
        resolved_dir = Path(scripts_dir).resolve()
        if not resolved_dir.is_dir():
            raise CommandError(
                _("The specified path '{scripts_dir}' is invalid or not a directory!")
                .format(scripts_dir=scripts_dir)
            )
        
        try:
            # Find the .git root directory using the rev-parse command
            res = subprocess.run(
                [str(git_cmd), "-C", str(resolved_dir), "rev-parse", "--show-toplevel"],
                capture_output=True, text=True, check=True
            )
            return Path(res.stdout.strip())
        except (subprocess.CalledProcessError, FileNotFoundError):
            _git_output.print(
                _(
                    "Warning: A valid Git repository root was not found for '{scripts_dir}'. "
                    "Git features are disabled."
                ).format(scripts_dir=scripts_dir)
            )
            return None

    def _run_git(self, args: list[str]) -> str:
        """Helper method to safely execute Git commands."""
        cmd = [str(self.git_cmd), "-C", str(self.repo_root)] + args
        res = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
        if res.returncode != 0:
            return ""
        return res.stdout

    def get_blob_content_by_oid(self, file_oid: str) -> str | None:
        """
        Fetches the raw blob content stored in the repository for a given blob OID.
        Returns None if the OID is empty or the blob is not found in the local repository.
        """
        clean_oid = str(file_oid).strip()
        if not clean_oid:
            return None

        res = subprocess.run(
            [str(self.git_cmd), "-C", str(self.repo_root), "cat-file", "blob", clean_oid],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        if res.returncode != 0:
            return None
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
                    return CommitInfo.uncommitted(_("File is untracked by Git"))
                else:
                    return CommitInfo.uncommitted(
                        _("File is modified ({status_code})").format(status_code=status_code.strip())
                    )

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
        return CommitInfo.unknown(_("No commit history found in this branch"))

    def get_commit_by_file_oid(self, file_oid: str) -> CommitInfo:
        """
        Finds the commit associated with a specific file content hash (Blob OID)
        using the git log --find-object feature.
        """
        clean_oid = str(file_oid).strip()
        if not clean_oid:
            raise ValueError(_("Argument 'file_oid' must not be empty"))

        # Format: SHA | AUTHOR | TIMESTAMP | COMMIT_SUBJECT
        # We use %ct (timestamp) to match the datetime object initialization in CommitInfo
        log_format = "--format=%H|%an|%ct|%s"
        
        # Execute git log searching for the exact object hash across the history
        log_output = self._run_git(["log", "-1", f"--find-object={clean_oid}", log_format])
        
        if not log_output:
            # Fallback if the file OID exists locally (e.g., in index) but has never been committed
            return CommitInfo.unknown(_("Content hash (OID) is completely untracked or modified locally"))

        parts = log_output.strip().split("|", 3)
        if len(parts) != 4:
            raise CommandError(
                _("Unexpected git log output format for OID '{clean_oid}': {log_output}")
                .format(clean_oid=clean_oid, log_output=log_output)
            )
            
        oid, author, timestamp_str, message = parts
        
        return CommitInfo(
            oid=oid,
            author=author,
            date=datetime.fromtimestamp(int(timestamp_str)),
            message=message
        )
