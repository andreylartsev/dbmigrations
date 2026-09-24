"""Compact modal confirmation screen: a dialog sized to the question text
with Yes/No keys and buttons (styled like the file viewer window)."""

from __future__ import annotations

import re

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Static

from _i18n import _

_PROMPT_SUFFIX = re.compile(r"\s*\[y/N\]:\s*$")


class ConfirmScreen(ModalScreen[bool]):
    """Asks the user to confirm an operation; dismisses with True/False."""

    BINDINGS = [
        ("y", "answer(True)", _("Yes")),
        ("n", "answer(False)", _("No")),
        ("escape", "answer(False)", _("No")),
        ("enter", "answer(True)", _("Yes")),
    ]

    CSS = """
    ConfirmScreen {
        align: center middle;
    }
    #confirm_dialog {
        width: auto;
        height: auto;
        max-width: 80%;
        border: round $accent;
        background: $surface;
        padding: 1 2;
    }
    #confirm_message {
        width: auto;
        height: auto;
    }
    #confirm_buttons {
        width: auto;
        height: auto;
        margin-top: 1;
        align-horizontal: center;
    }
    #confirm_buttons Button {
        margin: 0 1;
    }
    """

    def __init__(self, message: str, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._message = _PROMPT_SUFFIX.sub("", message)

    def compose(self) -> ComposeResult:
        with Vertical(id="confirm_dialog"):
            yield Static(self._message, id="confirm_message")
            with Horizontal(id="confirm_buttons"):
                yield Button(_("Yes"), id="confirm_yes", variant="primary")
                yield Button(_("No"), id="confirm_no", variant="error")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        self._submit(event.button.id == "confirm_yes")

    def action_answer(self, answer: bool) -> None:
        self._submit(answer)

    def _submit(self, answer: bool) -> None:
        self.dismiss(answer)

    def check_action(
        self, action: str, parameters: tuple[object, ...]
    ) -> bool | None:
        if action == "answer":
            return True
        return super().check_action(action, parameters)