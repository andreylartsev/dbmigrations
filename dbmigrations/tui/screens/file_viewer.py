"""Modal full-screen viewer for a file blob opened from an (OID: ...) reference.

The dialog opens immediately showing a progress indicator, while the blob
content is fetched asynchronously (in a worker thread). Once ready, the
script text replaces the loading area in the center of the dialog.
"""

from __future__ import annotations

import asyncio
from typing import Callable

from rich.text import Text as RichText
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Footer, ProgressBar, RichLog, Static

from _i18n import _


class FileViewerScreen(ModalScreen[None]):
    """Shows the contents of a git blob; any close key dismisses the screen."""

    BINDINGS = [
        Binding("escape", "close", _("Close")),
        Binding("q", "close", _("Close"), show=False),
        Binding("enter", "close", _("Close"), show=False),
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
    #file_progress {
        height: 1;
        padding: 0 1;
    }
    #file_progress ProgressBar {
        width: 24;
        height: 1;
    }
    #file_progress_label {
        width: auto;
        height: 1;
        padding-left: 1;
        color: $text;
    }
    #file_content {
        height: 1fr;
        padding: 0 1;
    }
    """

    def __init__(
        self,
        title: str,
        loader: Callable[[], str | None],
        *,
        oid: str = "",
    ) -> None:
        super().__init__()
        self._title = title
        self._loader = loader
        self._oid = oid

    def compose(self) -> ComposeResult:
        with Vertical(id="file_viewer_dialog"):
            yield Static(self._title, id="file_title")
            with Horizontal(id="file_progress"):
                yield ProgressBar(
                    total=None,
                    show_eta=False,
                    show_percentage=False,
                    id="file_progress_bar",
                )
                yield Static(
                    _("Loading script content..."), id="file_progress_label"
                )
            yield RichLog(
                id="file_content",
                auto_scroll=False,
                wrap=True,
                markup=False,
                highlight=True,
            )
            yield Footer()

    def on_mount(self) -> None:
        self.run_worker(self._load())

    async def _load(self) -> None:
        content: str | None = None
        error = ""
        try:
            content = await asyncio.to_thread(self._loader)
        except Exception as exc:  # noqa: BLE001 - surface any load failure in the dialog
            error = str(exc)
        self.query_one("#file_progress").remove()
        page = self.query_one("#file_content", RichLog)
        if content:
            page.write(content)
        elif error:
            page.write(
                RichText(
                    _("Failed to load blob content: {error}").format(error=error),
                    style="red",
                )
            )
        else:
            page.write(
                RichText(
                    _(
                        "Blob content for OID {oid} was not found in the "
                        "local repository."
                    ).format(oid=self._oid),
                    style="red",
                )
            )

    def action_close(self) -> None:
        self.dismiss(None)

    def check_action(
        self, action: str, parameters: tuple[object, ...]
    ) -> bool | None:
        if action == "close":
            return True
        return super().check_action(action, parameters)