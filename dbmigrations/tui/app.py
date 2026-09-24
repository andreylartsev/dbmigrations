"""Main TUI application: central log, right command panel, bottom status bar."""

from __future__ import annotations

import asyncio
from dataclasses import fields
from pathlib import Path
from typing import Any

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal
from textual.widgets import Footer, Header

from _config import build_connection_settings
from _confirmation import TuiConfirm
from _git import GitChecker
from _i18n import _
from _options import CommonCliOptions
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
    #log_panel {
        width: 1fr;
        height: 100%;
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
        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._confirm = TuiConfirm(self)
        self._auto_verify_started = False

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal():
            yield LogPanel(id="log_panel")
            yield CommandPanel(state=self.state, id="command_panel")
        yield StatusBar(id="status_bar")
        yield Footer()

    def on_mount(self) -> None:
        self.sub_title = (
            f"{self.opts.schema_name} / {self.get_scripts_path()}"
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
        self.run_worker(self._show_oid_file(message.oid, message.path))

    async def _show_oid_file(self, oid: str, path: str) -> None:
        content = await asyncio.to_thread(
            self.git_checker.get_blob_content_by_oid, oid
        )
        if not content:
            self.notify(
                _(
                    "Blob content for OID {oid} was not found in the "
                    "local repository."
                ).format(oid=oid),
                severity="error",
            )
            return
        title = f"{path} ({oid})" if path else f"OID: {oid}"
        self.push_screen(FileViewerScreen(title, content))

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
        self.log_panel.clear()
        assert self.runner is not None
        self.runner.start(cmd_cls, opts, self._confirm)
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