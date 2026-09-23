from dataclasses import dataclass
from typing import Any



@dataclass(frozen=True)
class CommonCliOptions:
    """Options shared by every subcommand (target schema, scripts path, DB connection overrides)."""
    schema_name: str
    dbenv: str
    scripts_path: str
    host: str | None = None
    port: int | None = None
    dbname: str | None = None
    user: str | None = None
    no_password: bool = False

@dataclass(frozen=True)
class UpdateOptions(CommonCliOptions):
    force_reapply_latest_version: bool = False
    force_reapply_all_repeatable: bool = False
    force_run_cleanup: bool = False
    skip_confirmation: bool = False

@dataclass(frozen=True)
class VerifyOptions(CommonCliOptions):
    skip_git_checks: bool = False
    skip_diffs: bool = False
    skip_display_recent_changes: bool = False
    build_update_script: str | None = None

@dataclass(frozen=True)
class InitOptions(CommonCliOptions):
    force_init: bool = False

@dataclass(frozen=True)
class RunTestsOptions(CommonCliOptions):
    skip_env_checks: bool = False

@dataclass(frozen=True)
class Deps:
    """External dependencies shared by command classes (dependency injection)."""
    config: dict[str, Any]
    db_settings: dict[str, Any]
