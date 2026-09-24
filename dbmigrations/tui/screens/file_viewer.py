"""Modal viewer for a file blob opened from an (OID: ...) reference.

The dialog opens immediately showing a progress indicator, while the content
is fetched asynchronously (in a worker thread). Once ready, the loaded text
(a script body or a diff) replaces the loading row in the center. A panel on
the right (like the main window) offers two radio buttons: show the script
text of the clicked OID, or show a diff between that version and the current
file in the scripts repository. A click anywhere outside the dialog dismisses
the screen.
"""

from __future__ import annotations

import asyncio
from typing import Callable

from rich.text import Text as RichText
from textual import events
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import (
    Footer,
    Header,
    ProgressBar,
    RadioButton,
    RadioSet,
    RichLog,
    Static,
)

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
        height: 1fr;
        border: round $accent;
        background: $surface;
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
    #file_main {
        width: 100%;
        height: 1fr;
    }
    #file_body {
        width: 1fr;
        height: 100%;
    }
    #file_content {
        height: 1fr;
        padding: 0 1;
    }
    #file_viewer_panel {
        width: 32;
        height: 100%;
        border-left: round $accent;
        background: $panel;
        padding: 1 2;
    }
    .panel-title {
        text-style: bold;
        color: $text;
        margin-bottom: 1;
    }
    RadioSet {
        margin-top: 1;
    }
    """

    def __init__(
        self,
        title: str,
        loader: Callable[[], str | None],
        *,
        oid: str = "",
        diff_loader: Callable[[], str] | None = None,
    ) -> None:
        super().__init__()
        self.sub_title = title
        self._script_loader = loader
        self._diff_loader = diff_loader
        self._oid = oid
        self._mode = "script"
        self._gen = 0

    def compose(self) -> ComposeResult:
        yield Header(show_clock=False)
        with Vertical(id="file_viewer_dialog"):
            with Horizontal(id="file_main"):
                with Vertical(id="file_body"):
                    with Horizontal(id="file_progress"):
                        yield ProgressBar(
                            total=None,
                            show_eta=False,
                            show_percentage=False,
                            id="file_progress_bar",
                        )
                        yield Static(
                            _("Loading script content..."),
                            id="file_progress_label",
                        )
                    yield RichLog(
                        id="file_content",
                        auto_scroll=False,
                        wrap=True,
                        markup=False,
                        highlight=True,
                    )
                with Vertical(id="file_viewer_panel"):
                    yield Static(_("View mode"), classes="panel-title")
                    with RadioSet(id="view_mode_set"):
                        yield RadioButton(
                            _("Show script text"),
                            id="radio_script",
                            value=True,
                        )
                        yield RadioButton(
                            _("Show as diff"),
                            id="radio_diff",
                            value=False,
                        )
        yield Footer()

    def on_mount(self) -> None:
        self._start_load()

    def _start_load(self) -> None:
        self._gen += 1
        self.run_worker(self._load(self._gen))

    def _mode_loader(self) -> Callable[[], str | None]:
        if self._mode == "diff" and self._diff_loader is not None:
            return self._diff_loader
        return self._script_loader

    async def _load(self, gen: int) -> None:
        progress = self.query_one("#file_progress")
        progress.display = True
        content: str | None = None
        error = ""
        try:
            content = await asyncio.to_thread(self._mode_loader())
        except Exception as exc:  # noqa: BLE001 - surface any load failure in the dialog
            error = str(exc)
        if gen != self._gen:
            return
        progress.display = False
        page = self.query_one("#file_content", RichLog)
        page.clear()
        if content:
            page.write(content)
        elif error:
            page.write(
                RichText(
                    _("Failed to load content: {error}").format(error=error),
                    style="red",
                )
            )
        elif self._mode == "diff":
            page.write(
                RichText(
                    _("The applied and current versions are identical."),
                    style="dim",
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

    def on_radio_set_changed(self, event: RadioSet.Changed) -> None:
        button_id = event.pressed.id or ""
        self._mode = "script" if button_id == "radio_script" else "diff"
        self._start_load()

    def on_click(self, event: events.Click) -> None:
        dialog = self.query_one("#file_viewer_dialog")
        if dialog.region.contains(event.screen_x, event.screen_y):
            return
        self.action_close()

    def action_close(self) -> None:
        self.dismiss(None)

    def check_action(
        self, action: str, parameters: tuple[object, ...]
    ) -> bool | None:
        if action == "close":
            return True
        return super().check_action(action, parameters)