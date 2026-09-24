"""Unit tests for the TUI layer (stage 6): queue output, worker, confirm, app.

These tests do not need a database: the worker's ``launch_command`` and the
app's ``StateProbe``/``build_connection_settings`` are stubbed so no real
connection is attempted.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from _confirmation import TuiConfirm
from _options import CommonCliOptions
from _output import QueueOutput


class FakeLaunch:
    """Replaces _launch.launch_command: emits lines, returns a fake exit code."""

    def __init__(self, exit_code: int = 0, error: Exception | None = None) -> None:
        self.exit_code = exit_code
        self.error = error
        self.called_with: dict[str, Any] | None = None

    def __call__(self, cmd_cls, opts, config, out=None, confirm=None, use_run_tests_by_user=False):
        self.called_with = {"cmd_cls": cmd_cls, "opts": opts, "config": config,
                            "out": out, "confirm": confirm,
                            "use_run_tests_by_user": use_run_tests_by_user}
        if self.error is not None:
            raise self.error
        out.print("First line")
        out.print("Running script: [checked]")
        out.print("done")
        out.print("")
        return self.exit_code


class FakeProbeState:
    def __init__(self, state: Any) -> None:
        self._state = state

    def probe(self) -> Any:
        return self._state


def make_state(**kwargs: Any) -> Any:
    from _state import State

    defaults = dict(
        schema_name="s",
        scripts_path="/tmp/scripts",
        connection_error=None,
        schema_exists=True,
        schema_is_empty=True,
        control_tables_exist=True,
        latest_version_installed=None,
        baseline_version_installed=None,
        scripts_path_exists=True,
        is_git_repo=False,
        baseline_dir_exists=False,
        versions_dir_exists=False,
        repeatable_dir_exists=False,
        tests_dir_exists=False,
        target_version_file_exists=False,
        target_version=None,
        target_environment_id_file_exists=False,
        set_search_path_file_exists=False,
    )
    defaults.update(kwargs)
    return State(**defaults)


class FakeApp:
    """Minimal host app used by CommandRunner tests."""

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        self.config = config or {}
        self.log_lines: list[str] = []
        self.finished: list[int] = []
        self.errors: list[str] = []
        self.cancels: int = 0

    def call_from_thread(self, fn, *args):
        return fn(*args)

    def on_log_line(self, line: str) -> None:
        self.log_lines.append(line)

    def on_command_finished(self, exit_code: int) -> None:
        self.finished.append(exit_code)

    def on_command_error(self, message: str) -> None:
        self.errors.append(message)

    def on_cancel_requested(self) -> None:
        self.cancels += 1


@pytest.fixture
def fake_launch(monkeypatch):
    fake = FakeLaunch()
    monkeypatch.setattr("_launch.launch_command", fake)
    return fake


# ---------------------------------------------------------------------------
# QueueOutput
# ---------------------------------------------------------------------------

def test_queue_output_buffers_partial_lines() -> None:
    lines: list[str] = []
    q = QueueOutput(None)

    class Loop:
        def is_running(self) -> bool:
            return False

    q._loop = Loop()
    q._queue = _QueueFake(lines)
    q.print("a", end="")        # no newline -> kept in buffer
    assert q._partial == "a"
    q.print("b")
    assert lines == ["ab"]
    q.print("c\nd", end="")
    assert lines == ["ab", "c"]
    assert q._partial == "d"


class _QueueFake:
    def __init__(self, lines: list[str]) -> None:
        self._lines = lines

    def put_nowait(self, line: str) -> None:
        self._lines.append(line)


# ---------------------------------------------------------------------------
# CommandRunner
# ---------------------------------------------------------------------------

def test_runner_drains_output_and_reports(fake_launch) -> None:
    app = FakeApp()
    loop = asyncio.new_event_loop()
    runner = make_runner(app, loop)

    run_until_done(loop, runner)

    assert app.finished == [0]
    assert not app.errors
    assert app.log_lines == ["First line", "Running script: [checked]", "done", ""]

    out = fake_launch.called_with["out"]
    assert hasattr(out, "print")


def test_runner_reports_error(fake_launch) -> None:
    fake_launch.error = RuntimeError("boom")
    app = FakeApp()
    loop = asyncio.new_event_loop()
    runner = make_runner(app, loop)

    run_until_done(loop, runner)

    assert app.finished == [1]
    assert "boom" in app.errors[0]


def make_runner(app: FakeApp, loop: asyncio.AbstractEventLoop):
    from tui.worker import CommandRunner

    return CommandRunner(app, asyncio.Queue(), loop)


def run_until_done(loop: asyncio.AbstractEventLoop, runner: Any) -> None:
    async def wait() -> None:
        runner.start(
            object,
            CommonCliOptions(schema_name="s", dbenv="d", scripts_path="/p"),
            confirm=None,
        )
        while getattr(runner, "_task", None) is not None:
            await asyncio.sleep(0.01)

    loop.run_until_complete(wait())


def test_confirm_delegates_to_modal_screen(monkeypatch) -> None:
    from tui.screens.confirm_screen import ConfirmScreen

    class MockApp:
        def __init__(self) -> None:
            self.pushed: list[tuple[Any, Any]] = []
            self.answer: bool = True

        def call_from_thread(self, fn, *args):
            return fn(*args)

        def push_screen(self, screen, callback=None):
            self.pushed.append((screen, callback))
            callback(self.answer)

    app = MockApp()
    confirm = TuiConfirm(app)

    result = confirm.confirm("Continue?")

    assert result is True
    assert len(app.pushed) == 1
    screen, callback = app.pushed[0]
    assert isinstance(screen, ConfirmScreen)
    assert callable(callback)


def test_confirm_screen_full_modal_shows_message_and_answers() -> None:
    import asyncio

    from textual.app import App as TextualApp

    from tui.screens.confirm_screen import ConfirmScreen

    answers: list[bool | None] = []

    async def scenario() -> None:
        app = TextualApp()
        async with app.run_test() as pilot:
            screen = ConfirmScreen(
                "You are going to run updates. Would you like to continue? [y/N]: "
            )
            app.push_screen(screen, answers.append)
            await pilot.pause()
            text = screen.query_one("#confirm_message").content
            assert "You are going to run updates" in text
            assert "[y/N]" not in text
            await pilot.press("y")
            await pilot.pause()
            assert answers == [True]

    asyncio.run(scenario())


# ---------------------------------------------------------------------------
# TUI app (headless)
# ---------------------------------------------------------------------------

def _make_app(
    state: Any, fake_launch: FakeLaunch, monkeypatch, *, auto_verify: bool = False
) -> Any:
    import tui.app as ta

    monkeypatch.setattr(ta, "StateProbe", lambda **kw: FakeProbeState(state))
    monkeypatch.setattr(ta, "build_connection_settings", lambda *a, **kw: {})
    opts = CommonCliOptions(
        schema_name="s",
        dbenv="d",
        scripts_path="/tmp/scripts",
    )
    return ta.MainApp(config={}, opts=opts, auto_verify=auto_verify)


def test_main_app_headless_boot_and_quit(fake_launch, monkeypatch) -> None:
    import asyncio

    import textual
    from textual.pilot import Pilot

    state = make_state()
    app = _make_app(state, fake_launch, monkeypatch)

    async def scenario() -> None:
        async with app.run_test(size=(140, 60)) as pilot:
            await pilot.pause(0.2)
            assert app.state is not None
            assert app.state.schema_exists
            await pilot.press("q")
            await pilot.pause(0.05)

    asyncio.run(scenario())


def test_main_app_run_verify_via_button(fake_launch, monkeypatch) -> None:
    import asyncio

    from commands import VerifyCommand

    state = make_state(control_tables_exist=True)
    app = _make_app(state, fake_launch, monkeypatch)

    async def scenario() -> None:
        async with app.run_test(size=(140, 60)) as pilot:
            await pilot.pause(0.2)
            # status is idle, buttons enabled because control tables exist
            await pilot.click("#run_verify")
            await pilot.pause(0.2)
            # give the worker a moment, then wait until the runner is done
            while getattr(app.runner, "_task", None) is not None:
                await pilot.pause(0.02)
            await pilot.pause(0.05)
            assert app.exit_code == 0
            rendered = "".join(str(strip) for strip in app.log_panel.lines)
            assert "First line" in rendered
            assert "done" in rendered

    asyncio.run(scenario())
    assert fake_launch.called_with is not None
    assert fake_launch.called_with["cmd_cls"] is VerifyCommand
    assert fake_launch.called_with["use_run_tests_by_user"] is False


def test_main_app_run_update_via_button_wrong_cmd_cls_regression(
    fake_launch, monkeypatch
) -> None:
    import asyncio

    from commands import UpdateCommand

    state = make_state(control_tables_exist=True)
    app = _make_app(state, fake_launch, monkeypatch)

    async def scenario() -> None:
        async with app.run_test(size=(140, 60)) as pilot:
            await pilot.pause(0.2)
            await pilot.click("#run_update")
            await pilot.pause(0.2)
            while getattr(app.runner, "_task", None) is not None:
                await pilot.pause(0.02)
            await pilot.pause(0.05)
            assert app.exit_code == 0

    asyncio.run(scenario())
    assert fake_launch.called_with is not None
    assert fake_launch.called_with["cmd_cls"] is UpdateCommand


def test_main_app_run_tests_via_button_uses_tester_user(fake_launch, monkeypatch) -> None:
    import asyncio

    state = make_state(control_tables_exist=True, tests_dir_exists=True)
    app = _make_app(state, fake_launch, monkeypatch)

    async def scenario() -> None:
        async with app.run_test(size=(140, 60)) as pilot:
            await pilot.pause(0.2)
            await pilot.click("#run_run-tests")
            await pilot.pause(0.2)
            while getattr(app.runner, "_task", None) is not None:
                await pilot.pause(0.02)
            await pilot.pause(0.05)
            assert app.exit_code == 0

    asyncio.run(scenario())
    assert fake_launch.called_with is not None
    assert fake_launch.called_with["use_run_tests_by_user"] is True


def test_main_app_copy_log_shortcut(fake_launch, monkeypatch) -> None:
    import asyncio

    state = make_state(control_tables_exist=True)
    app = _make_app(state, fake_launch, monkeypatch)

    async def scenario() -> None:
        async with app.run_test(size=(140, 60)) as pilot:
            await pilot.pause(0.2)
            await pilot.click("#run_verify")
            await pilot.pause(0.2)
            while getattr(app.runner, "_task", None) is not None:
                await pilot.pause(0.02)
            await pilot.pause(0.05)
            await pilot.press("ctrl+shift+c")
            await pilot.pause(0.05)

    asyncio.run(scenario())
    clipboard = app._clipboard or ""
    assert "First line" in clipboard
    assert "done" in clipboard


def test_log_panel_as_plain_text(fake_launch, monkeypatch) -> None:
    import asyncio

    state = make_state(control_tables_exist=True)
    app = _make_app(state, fake_launch, monkeypatch)
    text = ""

    async def scenario() -> None:
        nonlocal text
        async with app.run_test(size=(140, 60)) as pilot:
            await pilot.pause(0.2)
            await pilot.click("#run_verify")
            await pilot.pause(0.2)
            while getattr(app.runner, "_task", None) is not None:
                await pilot.pause(0.02)
            await pilot.pause(0.05)
            text = app.log_panel.as_plain_text()

    asyncio.run(scenario())
    assert "First line" in text
    assert "done" in text
    assert "Strip(" not in text
    assert "Segment(" not in text


def test_main_app_auto_verify_on_start(fake_launch, monkeypatch) -> None:
    import asyncio

    state = make_state(control_tables_exist=True)
    app = _make_app(state, fake_launch, monkeypatch, auto_verify=True)

    async def scenario() -> None:
        async with app.run_test(size=(140, 60)) as pilot:
            await pilot.pause(0.2)
            while getattr(app.runner, "_task", None) is not None:
                await pilot.pause(0.02)
            await pilot.pause(0.05)
            assert app.exit_code == 0

    asyncio.run(scenario())


def test_main_app_disables_unavailable_commands(fake_launch, monkeypatch) -> None:
    import asyncio

    from textual.widgets import Button

    state = make_state(control_tables_exist=False, schema_exists=False)
    app = _make_app(state, fake_launch, monkeypatch)

    async def scenario() -> None:
        async with app.run_test(size=(140, 60)) as pilot:
            await pilot.pause(0.2)
            for widget_id in ("run_init", "run_update", "run_verify"):
                button = app.command_panel.query_one(f"#{widget_id}", Button)
                assert button.disabled

    asyncio.run(scenario())


def test_log_panel_parse_script_name() -> None:
    from tui.widgets.log_panel import LogPanel

    assert LogPanel.parse_script_name("Running script: [hello.sql]") == "hello.sql"
    assert LogPanel.parse_script_name("Run migration: v1.sql") == "own migrations"
    assert LogPanel.parse_script_name("unrelated line") == ""


def test_log_panel_error_renders_red_not_markup() -> None:
    import asyncio

    from textual.app import App as TextualApp

    from tui.widgets.log_panel import LogPanel

    async def scenario() -> None:
        app = TextualApp()
        async with app.run_test(size=(80, 20)) as pilot:
            panel = LogPanel(id="panel")
            await app.mount(panel)
            panel.append_error("Отменено пользователем")
            await pilot.pause()
            strip = panel.lines[0]
            assert strip.text == "Отменено пользователем"
            assert "[red]" not in strip.text
            assert any(
                segment.style is not None
                and segment.style.color is not None
                and segment.style.color.name == "red"
                for segment in strip
            )

    asyncio.run(scenario())


def test_log_panel_mouse_drag_copies_range(fake_launch, monkeypatch) -> None:
    import asyncio

    from textual import events

    state = make_state(control_tables_exist=True)
    app = _make_app(state, fake_launch, monkeypatch)

    async def scenario() -> None:
        async with app.run_test(size=(140, 60)) as pilot:
            await pilot.pause(0.2)
            await pilot.click("#run_verify")
            await pilot.pause(0.2)
            while getattr(app.runner, "_task", None) is not None:
                await pilot.pause(0.02)
            await pilot.pause(0.05)
            panel = app.log_panel
            first_row = panel.content_region.y
            await pilot.mouse_down("#log_panel", offset=(10, first_row + 1))
            await pilot.pause(0.02)
            panel.on_mouse_move(
                events.MouseMove(
                    x=10, y=first_row + 3, delta_x=0, delta_y=0, button=1,
                    widget=panel, shift=False, meta=False, ctrl=False,
                )
            )
            await pilot.mouse_up("#log_panel", offset=(10, first_row + 3))
            await pilot.pause(0.05)

    asyncio.run(scenario())
    clipboard = app._clipboard or ""
    assert "Running script" in clipboard
    assert "done" in clipboard
    assert "First line" not in clipboard


def test_log_panel_click_without_drag_does_not_copy(fake_launch, monkeypatch) -> None:
    import asyncio

    state = make_state(control_tables_exist=True)
    app = _make_app(state, fake_launch, monkeypatch)

    async def scenario() -> None:
        async with app.run_test(size=(140, 60)) as pilot:
            await pilot.pause(0.2)
            await pilot.click("#run_verify")
            await pilot.pause(0.2)
            while getattr(app.runner, "_task", None) is not None:
                await pilot.pause(0.02)
            await pilot.pause(0.05)
            panel = app.log_panel
            first_row = panel.content_region.y
            await pilot.mouse_down("#log_panel", offset=(10, first_row + 1))
            await pilot.mouse_up("#log_panel", offset=(10, first_row + 1))
            await pilot.pause(0.05)

    asyncio.run(scenario())
    assert not (app._clipboard or "")


def test_log_panel_shift_click_copies_range(fake_launch, monkeypatch) -> None:
    import asyncio

    state = make_state(control_tables_exist=True)
    app = _make_app(state, fake_launch, monkeypatch)

    async def scenario() -> None:
        async with app.run_test(size=(140, 60)) as pilot:
            await pilot.pause(0.2)
            await pilot.click("#run_verify")
            await pilot.pause(0.2)
            while getattr(app.runner, "_task", None) is not None:
                await pilot.pause(0.02)
            await pilot.pause(0.05)
            panel = app.log_panel
            first_row = panel.content_region.y
            await pilot.mouse_down("#log_panel", offset=(10, first_row + 1))
            await pilot.pause(0.02)
            await pilot.mouse_down(
                "#log_panel", offset=(10, first_row + 3), shift=True
            )
            await pilot.pause(0.05)

    asyncio.run(scenario())
    clipboard = app._clipboard or ""
    assert "Running script" in clipboard
    assert "done" in clipboard
    assert "First line" not in clipboard


OID_LINE = (
    "  [2026-09-23 09:57:36 | repeatable | V000 | "
    "common/repeatable/fn_get_environment_name.sql (OID: ced95c6d)]"
)


def test_log_panel_extract_oid_and_path_from_recent_line() -> None:
    from tui.widgets.log_panel import LogPanel

    assert LogPanel.extract_oid(OID_LINE) == "ced95c6d"
    assert (
        LogPanel.extract_path(OID_LINE)
        == "common/repeatable/fn_get_environment_name.sql"
    )
    assert LogPanel.extract_oid("no oid here") == ""
    assert LogPanel.extract_path("no oid here") == ""


def test_log_panel_click_on_oid_line_activates_message(fake_launch, monkeypatch) -> None:
    import asyncio

    from tui import app as ta

    state = make_state(control_tables_exist=True)
    app = _make_app(state, fake_launch, monkeypatch)
    seen: list[Any] = []
    monkeypatch.setattr(
        ta.MainApp,
        "on_log_panel_oid_activated",
        lambda self, message: seen.append(message),
    )

    async def scenario() -> None:
        async with app.run_test(size=(140, 60)) as pilot:
            await pilot.pause(0.2)
            panel = app.log_panel
            panel.append_line("First line")
            panel.append_line(OID_LINE)
            await pilot.pause(0.05)
            first_row = panel.content_region.y
            await pilot.click(
                "#log_panel", offset=(10, first_row + len(panel.lines) - 1)
            )
            await pilot.pause(0.05)

    asyncio.run(scenario())
    assert len(seen) == 1
    assert seen[0].oid == "ced95c6d"
    assert seen[0].path == "common/repeatable/fn_get_environment_name.sql"


def test_log_panel_click_on_plain_line_no_oid_message(
    fake_launch, monkeypatch,
) -> None:
    import asyncio

    from tui import app as ta

    state = make_state(control_tables_exist=True)
    app = _make_app(state, fake_launch, monkeypatch)
    seen: list[Any] = []
    monkeypatch.setattr(
        ta.MainApp,
        "on_log_panel_oid_activated",
        lambda self, message: seen.append(message),
    )

    async def scenario() -> None:
        async with app.run_test(size=(140, 60)) as pilot:
            await pilot.pause(0.2)
            panel = app.log_panel
            panel.append_line("First line")
            panel.append_line("plain text without an oid")
            await pilot.pause(0.05)
            first_row = panel.content_region.y
            await pilot.click(
                "#log_panel", offset=(10, first_row + len(panel.lines) - 1)
            )
            await pilot.pause(0.05)

    asyncio.run(scenario())
    assert seen == []


def test_main_app_oid_activated_opens_file_viewer(fake_launch, monkeypatch) -> None:
    import asyncio

    from tui.screens.file_viewer import FileViewerScreen
    from tui.widgets.log_panel import LogPanel

    state = make_state(control_tables_exist=True)
    app = _make_app(state, fake_launch, monkeypatch)

    class FakeGit:
        def get_blob_content_by_oid(self, oid: str) -> str | None:
            return "SELECT 1;  -- resolved from oid\n"

    content = "SELECT 1;  -- resolved from oid\n"

    async def scenario() -> None:
        async with app.run_test(size=(140, 60)) as pilot:
            await pilot.pause(0.2)
            monkeypatch.setattr(app, "git_checker", FakeGit())
            app.log_panel.post_message(
                LogPanel.OidActivated(oid="ced95c6d", path="p.sql")
            )
            await pilot.pause(0.3)
            assert isinstance(app.screen, FileViewerScreen)
            viewer = app.screen
            assert viewer.query_one("#file_title", object).content == (
                "p.sql (ced95c6d)"
            )
            assert content in "\n".join(
                strip.text
                for strip in viewer.query_one("#file_content", object).lines
            )
            await pilot.press("escape")
            await pilot.pause(0.05)
            assert not isinstance(app.screen, FileViewerScreen)

    asyncio.run(scenario())


def test_main_app_oid_activated_no_git_notifies(fake_launch, monkeypatch) -> None:
    import asyncio

    from tui.widgets.log_panel import LogPanel

    state = make_state(control_tables_exist=True)
    app = _make_app(state, fake_launch, monkeypatch)
    notices: list[str] = []
    monkeypatch.setattr(
        app, "notify", lambda message, **kw: notices.append(message)
    )

    async def scenario() -> None:
        async with app.run_test(size=(140, 60)) as pilot:
            await pilot.pause(0.2)
            monkeypatch.setattr(app, "git_checker", None)
            app.log_panel.post_message(
                LogPanel.OidActivated(oid="ced95c6d", path="p.sql")
            )
            await pilot.pause(0.3)

    asyncio.run(scenario())
    assert notices and "Git repository is not available" in notices[0]


def test_tui_subcommand_registered() -> None:
    from dbmigration import build_parser

    parser = build_parser({"default_dbenv": "test"})
    names: set[str] = set()
    for action in parser._actions:
        if getattr(action, "choices", None):
            names |= set(action.choices)
    assert "tui" in names