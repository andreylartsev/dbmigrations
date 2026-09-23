"""Bottom status bar: worker state, spinner, exit code, stop hint."""

from __future__ import annotations

from textual.reactive import reactive
from textual.widget import Widget

from _i18n import _

_SPINNER = ("⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏")


class StatusBar(Widget):
    """Shows idle / running <script> / cancel requested / exit code."""

    DEFAULT_CSS = """
    StatusBar {
        height: 1;
        padding: 0 2;
        color: $text;
        background: $boost;
        dock: bottom;
    }
    """

    mode: reactive[str | None] = reactive(None)
    current_script: reactive[str] = reactive("")
    cancel_requested: reactive[bool] = reactive(False)
    exit_code: reactive[int | None] = reactive(None)
    _spinner_index: int = 0

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def set_running(self, current_script: str = "") -> None:
        self.mode = "running"
        self.current_script = current_script
        self.cancel_requested = False
        self.exit_code = None

    def set_idle(self) -> None:
        self.mode = "idle"
        self.current_script = ""
        self.cancel_requested = False

    def set_exit_code(self, code: int) -> None:
        self.exit_code = code
        self.mode = None

    def tick_spinner(self) -> None:
        if self.mode != "running":
            return
        self._spinner_index = (self._spinner_index + 1) % len(_SPINNER)
        self.refresh()

    def get_script_of(self) -> str:
        return self.current_script

    def render(self) -> str:
        if self.mode == "running":
            spinner = _SPINNER[self._spinner_index % len(_SPINNER)]
            if self.cancel_requested:
                label = _("cancel requested")
                return f"{spinner} {label} ({_('finishing current script')}: {self.current_script})"
            script = self.current_script or _("...")
            return f"{spinner} {_('running')}: {script}"
        if self.exit_code is not None:
            return _("exit code: {code}").format(code=self.exit_code)
        return _("idle")