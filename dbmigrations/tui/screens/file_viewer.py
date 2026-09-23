"""Modal full-screen viewer for a file blob opened from an (OID: ...) reference."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.screen import ModalScreen
from textual.widgets import RichLog, Static

from _i18n import _


class FileViewerScreen(ModalScreen[None]):
    """Shows the contents of a git blob; any close key dismisses the screen."""

    BINDINGS = [
        ("escape", "close", _("Close")),
        ("q", "close", _("Close")),
        ("enter", "close", _("Close")),
    ]

    CSS = """
    FileViewerScreen {
        align: center middle;
    }
    #file_viewer_dialog {
        width: 92%;
        height: 92%;
        border: round $accent;
        background: $surface;
    }
    #file_title {
        height: 1;
        padding: 0 1;
        text-style: bold;
        color: $text;
    }
    #file_content {
        height: 1fr;
        padding: 0 1;
    }
    """

    def __init__(self, title: str, content: str, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._title = title
        self._content = content

    def compose(self) -> ComposeResult:
        with Vertical(id="file_viewer_dialog"):
            yield Static(self._title, id="file_title")
            yield RichLog(
                id="file_content",
                auto_scroll=False,
                wrap=True,
                markup=False,
                highlight=True,
            )

    def on_mount(self) -> None:
        self.query_one("#file_content", RichLog).write(self._content)

    def action_close(self) -> None:
        self.dismiss(None)

    def check_action(
        self, action: str, parameters: tuple[object, ...]
    ) -> bool | None:
        if action == "close":
            return True
        return super().check_action(action, parameters)