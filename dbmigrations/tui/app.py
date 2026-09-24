"""Main TUI application: central log, right command panel, bottom status bar."""

from __future__ import annotations

import asyncio
from dataclasses import fields
from pathlib import Path
from typing import Any

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.widgets import Footer, Header, ProgressBar

from _config import build_connection_settings
from _confirmation import TuiConfirm
from _constants import (
    LOG_MAX_LINES_ATTR_NAME,
    OPTIONS_CONFIG_GROUP,
    OPTIONS_DEFAULT_LOG_MAX_LINES,
)
from _errors import CommandError
from _git import GitChecker
from _i18n import _
from _options import CommonCliOptions
from _scripts import get_git_blob_sha1_for_file_path, render_script_diff_text
from _state import State, StateProbe
from tui.screens.file_viewer import FileViewerScreen
from tui.widgets.command_panel import CommandPanel, RunCommandRequested
from tui.widgets.log_panel import LogPanel
from tui.widgets.status_bar import StatusBar
from tui.worker import CommandRunner


class MainApp(App[bool]):
    """dbmigration TUI: starts with a full verify and offers init/update/verify/run-tests."""

    TITLE = "dbmigration"
    CSS = """
    TerminalScreen {
        layout: horizontal;
    }
    #log_area {
        width: 1fr;
        height: 100%;
    }
    #log_progress {
        display: none;
        height: 1;
        padding: 0 1;
    }
    #log_progress ProgressBar {
        width: 100%;
        height: 1;
    }
    #log_panel {
        width: 1fr;
        height: 1fr;
    }
    #command_panel {
        width: 46;
        height: 100%;
    }
    """

    BINDINGS = [
        Binding("ctrl+r", "run_current", _("Verify")),
        Binding("ctrl+c", "cancel", _("Cancel"), show=False),
        Binding("ctrl+shift+c", "copy_all", _("Copy log")),
        Binding("alt+c", "copy_all", _("Copy log"), show=False),
        Binding("ctrl+shift+x", "copy_view", _("Copy view")),
        Binding("r", "refresh_state", _("Refresh")),
        Binding("q", "quit", _("Quit")),
    ]

    def __init__(
        self,
        config: dict[str, Any],
        opts: CommonCliOptions,
        auto_verify: bool = True,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.config = config
        self.opts = opts
        self.auto_verify = auto_verify
        self.db_settings = build_connection_settings(
            config, opts, use_run_tests_by_user=False
        )
        self.state: State | None = None
        self.runner: CommandRunner | None = None
        self.git_checker: GitChecker | None = None
        self.exit_code: int | None = None
        self.log_max_lines: int = self._read_log_max_lines()
        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._confirm = TuiConfirm(self)
        self._auto_verify_started = False

    def _read_log_max_lines(self) -> int:
        options = (
            self.config.get(OPTIONS_CONFIG_GROUP, {})
            if isinstance(self.config, dict)
            else {}
        )
        try:
            value = int(options.get(LOG_MAX_LINES_ATTR_NAME, OPTIONS_DEFAULT_LOG_MAX_LINES))
        except (TypeError, ValueError):
            return OPTIONS_DEFAULT_LOG_MAX_LINES
        return value if value >= 1 else OPTIONS_DEFAULT_LOG_MAX_LINES

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal():
            with Vertical(id="log_area"):
                yield ProgressBar(
                    total=None,
                    show_eta=False,
                    show_percentage=False,
                    id="log_progress",
                )
                yield LogPanel(id="log_panel", max_lines=self.log_max_lines)
            yield CommandPanel(state=self.state, id="command_panel")
        yield StatusBar(id="status_bar")
        yield Footer()

    def on_mount(self) -> None:
        self.sub_title = (
            f"{self.opts.dbenv} / {self.opts.schema_name} / {self.get_scripts_path()}"
        )
        self.runner = CommandRunner(self, self._queue, self._get_loop())
        try:
            self.git_checker = GitChecker.try_get(
                self.config, Path(self.get_scripts_path())
            )
        except Exception:
            self.git_checker = None
        self.call_after_refresh(self.refresh_state)

    def on_log_panel_oid_activated(self, message: LogPanel.OidActivated) -> None:
        if self.git_checker is None:
            self.notify(
                _("Git repository is not available"),
                title=message.path or f"OID:{message.oid}",
                severity="error",
            )
            return
        title = f"{message.path} (OID: {message.oid})" if message.path \
            else f"OID: {message.oid}"
        checker = self.git_checker
        scripts_path = self.opts.scripts_path
        relative_path = message.path or ""

        def script_loader() -> str | None:
            return checker.get_blob_content_by_oid(message.oid)

        def diff_loader() -> str:
            blob = checker.get_blob_content_by_oid(message.oid)
            if blob is None:
                raise CommandError(
                    _(
                        "Blob content for OID {oid} was not found in the "
                        "local repository."
                    ).format(oid=message.oid)
                )
            file_path = Path(scripts_path).parent.resolve() / relative_path
            if not file_path.is_file():
                raise CommandError(
                    _("The file '{file_path}' does not exists").format(
                        file_path=file_path
                    )
                )
            new_text = file_path.read_text(
                encoding="utf-8", errors="replace"
            )
            new_oid = get_git_blob_sha1_for_file_path(file_path)
            return render_script_diff_text(
                blob, new_text, relative_path, message.oid, new_oid
            )

        self.push_screen(
            FileViewerScreen(
                title,
                script_loader,
                oid=message.oid,
                diff_loader=diff_loader,
            )
        )

    def _get_loop(self) -> asyncio.AbstractEventLoop:
        return asyncio.get_running_loop()

    def get_scripts_path(self) -> str:
        return self.opts.scripts_path

    def start_verify(self) -> None:
        from commands import VerifyCommand
        from _options import VerifyOptions

        opts = VerifyOptions(
            **{
                field.name: getattr(self.opts, field.name)
                for field in fields(self.opts)
            }
        )
        self.start_command(VerifyCommand, opts)

    def on_run_command_requested(self, message: RunCommandRequested) -> None:
        if self.runner is None or self.runner.running:
            return
        entry = CommandPanel.lookup(message.entry_name)
        opts_type = entry.opts_type
        option_values = {
            field.name: (field.name in message.option_attributes)
            for field in fields(opts_type)
            if field.name in entry_option_attributes(entry)
        }
        base = {
            field.name: getattr(self.opts, field.name)
            for field in fields(self.opts)
        }
        opts = opts_type(**base, **option_values)
        self.start_command(entry.cmd_cls, opts)

    def start_command(self, cmd_cls: type, opts: CommonCliOptions) -> None:
        if self.runner is None or self.runner.running:
            return
        assert self.runner is not None
        self.runner.start(cmd_cls, opts, self._confirm)
        self.log_progress.display = True
        self.status_bar.set_running()

    def on_log_line(self, line: str) -> None:
        self.log_panel.append_line(line)
        script_name = LogPanel.parse_script_name(line)
        if script_name:
            self.status_bar.current_script = script_name
        self.status_bar.tick_spinner()

    def on_command_error(self, message: str) -> None:
        self.log_panel.append_error(message)

    def on_cancel_requested(self) -> None:
        self.status_bar.cancel_requested = True
        self.refresh_status()

    def on_command_finished(self, exit_code: int) -> None:
        self.exit_code = exit_code
        self.status_bar.set_exit_code(exit_code)
        self.log_progress.display = False
        self.log_panel.append_line(
            _("Command finished with exit code: {exit_code}").format(exit_code=exit_code)
        )
        self.call_after_refresh(self.refresh_state)

    def action_refresh_state(self) -> None:
        self.log_panel.append_line(_("Refreshing state..."))
        self.refresh_state()

    def refresh_state(self) -> None:
        async def _run() -> None:
            probe = StateProbe(
                dbconn_settings=self.db_settings,
                scripts_path=self.opts.scripts_path,
                schema_name=self.opts.schema_name,
            )
            self.state = await asyncio.to_thread(probe.probe)
            self.command_panel.update_state(self.state)
            self.refresh_status()
            if self.state.connection_error:
                self.log_panel.append_line(
                    _("Unable to probe the database: {error}").format(
                        error=self.state.connection_error
                    )
                )
            else:
                self.log_panel.append_line(self.describe_state())
            if (
                self.auto_verify
                and not self._auto_verify_started
                and self.state.control_tables_exist
            ):
                self._auto_verify_started = True
                self.start_verify()

        self.run_worker(_run())

    def describe_state(self) -> str:
        git_state = _("yes") if self.state.is_git_repo else _("no")
        return _(
            "State refreshed: {n} commands available, git repository: {repo}"
        ).format(n=self.command_panel.available_count(), repo=git_state)

    def action_cancel(self) -> None:
        if self.runner is not None and self.runner.running:
            self.status_bar.cancel_requested = True
            self.runner.cancel()

    def action_copy_all(self) -> None:
        text = self.log_panel.as_plain_text().strip("\n")
        count = len(self.log_panel.lines)
        if not text:
            self.notify(_("Nothing to copy"))
            return
        self.copy_to_clipboard(text)
        self.notify(_("Copied {n} lines to clipboard").format(n=count))

    def action_copy_view(self) -> None:
        text = self.log_panel.copy_visible().strip("\n")
        if not text:
            self.notify(_("Nothing to copy"))
            return
        count = text.count("\n") + 1
        self.copy_to_clipboard(text)
        self.notify(_("Copied {n} lines to clipboard").format(n=count))

    def action_quit(self) -> None:
        self.exit()

    def action_run_current(self) -> None:
        self.start_verify()

    def refresh_status(self) -> None:
        bar = self.query_one(StatusBar)
        bar.refresh()

    @property
    def log_panel(self) -> LogPanel:
        return self.query_one("#log_panel")

    @property
    def log_progress(self) -> ProgressBar:
        return self.query_one("#log_progress")

    @property
    def command_panel(self) -> CommandPanel:
        return self.query_one("#command_panel")

    @property
    def status_bar(self) -> StatusBar:
        return self.query_one("#status_bar")


def entry_option_attributes(entry: Any) -> list[str]:
    return [name for name, _ in entry.options]


def run_tui(config: dict[str, Any], opts: CommonCliOptions) -> int:
    """Entry point called from the CLI `tui` subcommand (lazy textual import)."""
    app = MainApp(config=config, opts=opts)
    result = app.run()
    return int(app.exit_code) if app.exit_code is not None else 0