#!/usr/bin/env python3
"""Unified command launcher shared by the CLI and the TUI."""
from __future__ import annotations

from typing import Any

from _config import build_connection_settings
from _confirmation import Confirmation
from _db import DbConnection
from _options import CommonCliOptions, Deps
from _output import Output


def launch_command(
    cmd_cls: type,
    opts: CommonCliOptions,
    config: dict[str, Any],
    out: Output | None = None,
    confirm: Confirmation | None = None,
    use_run_tests_by_user: bool = False,
) -> int:
    """Builds dependencies and runs a command inside a managed DB connection.

    ``out`` and ``confirm`` default to CLI behavior (stdout output, terminal
    y/N prompt); the TUI injects queue/splash-based implementations.
    """
    deps = Deps(
        config=config,
        db_settings=build_connection_settings(config, opts, use_run_tests_by_user),
    )
    command = cmd_cls(opts, deps, out, confirm)
    with DbConnection(deps.db_settings, out) as db:
        return command.run(db)


def run_command(
    cmd_cls: type,
    opts: CommonCliOptions,
    config: dict[str, Any],
    use_run_tests_by_user: bool = False,
) -> int:
    """CLI entry kept for compatibility, backed by launch_command()."""
    return launch_command(cmd_cls, opts, config, use_run_tests_by_user=use_run_tests_by_user)