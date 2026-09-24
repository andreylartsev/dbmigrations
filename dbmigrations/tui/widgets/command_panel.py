"""Right command panel: available commands, their option checkboxes, and Run buttons."""

from __future__ import annotations

from collections.abc import Callable

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.message import Message
from textual.widgets import Button, Checkbox, Static

from commands import InitCommand, RunTestsCommand, UpdateCommand, VerifyCommand
from _i18n import _
from _options import InitOptions, RunTestsOptions, UpdateOptions, VerifyOptions
from _state import State


class CommandEntry:
    """Declarative description of one runnable command for the panel."""

    def __init__(
        self,
        name: str,
        title: str,
        options: list[tuple[str, str]],
        available: Callable[[State], bool],
        opts_type: type,
        cmd_cls: type,
    ) -> None:
        self.name = name
        self.title = title
        self.options = options
        self.available = available
        self.opts_type = opts_type
        self.cmd_cls = cmd_cls


class RunCommandRequested(Message):
    """Emitted when the user presses a Run button in the command panel."""

    def __init__(self, entry_name: str, option_attributes: set[str]) -> None:
        super().__init__()
        self.entry_name = entry_name
        self.option_attributes = option_attributes


class CommandPanel(Vertical):
    """List of commands with fixed option checkboxes."""

    DEFAULT_CSS = """
    CommandPanel {
        width: 46;
        height: 100%;
        border: round $accent;
        padding: 1 2;
        background: $panel;
    }
    .panel-title {
        text-style: bold;
        color: $text;
        margin-bottom: 1;
    }
    .command-title {
        text-style: bold;
        color: $accent;
        margin-top: 1;
    }
    #panel_body {
        height: 1fr;
        overflow-y: auto;
    }
    Checkbox {
        margin-top: 1;
    }
    Button {
        margin-top: 1;
        width: 100%;
    }
    """

    INIT_OPTIONS = [
        ("force_init", _("force init on non-empty schema")),
    ]
    UPDATE_OPTIONS = [
        ("force_reapply_latest_version", _("re-apply latest version")),
        ("force_reapply_all_repeatable", _("re-apply all repeatable")),
        ("force_run_cleanup", _("run cleanup")),
    ]
    VERIFY_OPTIONS = [
        ("skip_diffs", _("skip diffs")),
        ("skip_display_recent_changes", _("skip recent changes")),
    ]

    ENTRIES = [
        CommandEntry(
            "init",
            _("Init"),
            INIT_OPTIONS,
            available=lambda state: state.can_init,
            opts_type=InitOptions,
            cmd_cls=InitCommand,
        ),
        CommandEntry(
            "update",
            _("Update"),
            UPDATE_OPTIONS,
            available=lambda state: state.can_update,
            opts_type=UpdateOptions,
            cmd_cls=UpdateCommand,
        ),
        CommandEntry(
            "verify",
            _("Verify"),
            VERIFY_OPTIONS,
            available=lambda state: state.can_verify,
            opts_type=VerifyOptions,
            cmd_cls=VerifyCommand,
        ),
        CommandEntry(
            "run-tests",
            _("Run Tests"),
            [],
            available=lambda state: state.can_run_tests,
            opts_type=RunTestsOptions,
            cmd_cls=RunTestsCommand,
        ),
    ]

    @classmethod
    def lookup(cls, name: str) -> CommandEntry:
        for entry in cls.ENTRIES:
            if entry.name == name:
                return entry
        raise KeyError(name)

    def __init__(self, state: State | None = None, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._state = state

    def compose(self) -> ComposeResult:
        yield Static(_("Commands"), classes="panel-title")
        with Vertical(id="panel_body"):
            yield from self._render_entries()

    def update_state(self, state: State) -> None:
        self._state = state
        for entry in self.ENTRIES:
            available = self._is_entry_available(entry.name)
            title = (
                entry.title
                if available
                else f"{entry.title} ({_('unavailable')})"
            )
            self.query_one(f"#cmd_title_{entry.name}", Static).update(title)
            for attr_name, _label in entry.options:
                self.query_one(
                    f"#opt_{entry.name}_{attr_name}", Checkbox
                ).disabled = not available
            self.query_one(f"#run_{entry.name}", Button).disabled = not available

    def _render_entries(self) -> list:
        widgets = []
        for entry in self.ENTRIES:
            available = self._is_entry_available(entry.name)
            title = (
                entry.title
                if available
                else f"{entry.title} ({_('unavailable')})"
            )
            widgets.append(Static(title, id=f"cmd_title_{entry.name}", classes="command-title"))
            for attr_name, label in entry.options:
                widgets.append(
                    Checkbox(
                        label,
                        id=f"opt_{entry.name}_{attr_name}",
                        value=False,
                        disabled=not available,
                    )
                )
            widgets.append(
                Button(
                    _("Run {title}").format(title=entry.title),
                    id=f"run_{entry.name}",
                    variant="primary",
                    disabled=not available,
                )
            )
        return widgets

    def _is_entry_available(self, entry_name: str) -> bool:
        entry = self.lookup(entry_name)
        if self._state is None:
            return False
        return entry.available(self._state)

    def available_count(self) -> int:
        return sum(
            1 for entry in self.ENTRIES if self._is_entry_available(entry.name)
        )

    def on_button_pressed(self, event: Button.Pressed) -> None:
        button_id = event.button.id or ""
        if not button_id.startswith("run_"):
            return
        entry_name = button_id[len("run_"):]
        checked: set[str] = set()
        for checkbox in self.query(Checkbox):
            if checkbox.id and checkbox.id.startswith(f"opt_{entry_name}_") and checkbox.value:
                checked.add(checkbox.id.split("_", 2)[2])
        self.post_message(RunCommandRequested(entry_name, checked))