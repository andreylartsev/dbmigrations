"""Modal y/N confirmation screen used instead of the raw terminal get_char()."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Static

from _i18n import _


class ConfirmScreen(ModalScreen[bool]):
    """Asks the user to confirm an operation; dismisses with True/False."""

    BINDINGS = [
        ("y", "answer(True)", _("Yes")),
        ("n", "answer(False)", _("No")),
        ("escape", "answer(False)", _("No")),
        ("enter", "answer(True)", _("Yes")),
    ]

    def __init__(self, message: str, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._message = message

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