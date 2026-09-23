"""RichLog-based central log panel with script start lines highlighted."""

from __future__ import annotations

import re

from rich.style import Style
from rich.text import Text as RichText
from textual import events
from textual.message import Message
from textual.widgets import RichLog

from _i18n import _

_SCRIPT_START_PREFIXES = ("Running script:", "Run migration:")
_SCRIPT_NAME_PATTERN = re.compile(r"^Running script:\s*\[([^\]]+)\]")
_SELECTION_BACKGROUND = "#005f87"
_OID_PATTERN = re.compile(r"\(OID:\s*([0-9a-f]+)\)")
_OID_LINK_STYLE = Style(color="bright_blue", underline=True)


class LogPanel(RichLog):
    """Shows the command output stream; highlights currently running scripts."""

    class OidActivated(Message):
        """A click landed on a log line that carries a git (OID: ...) reference."""

        def __init__(
            self,
            oid: str,
            path: str = "",
            index: int | None = None,
        ) -> None:
            super().__init__()
            self.oid = oid
            self.path = path
            self.index = index

    DEFAULT_CSS = """
    LogPanel {
        width: 1fr;
        height: 100%;
        padding: 1 2;
        border: round $accent;
        background: $surface;
    }
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(
            *args,
            highlight=True,
            markup=False,
            wrap=True,
            auto_scroll=True,
            **kwargs,
        )
        self._anchor: int | None = None
        self._end: int | None = None
        self._dragging: bool = False
        self._saved_strips: dict[int, object] = {}

    def append_line(self, text: str) -> None:
        if not text:
            return
        if text.startswith(_SCRIPT_START_PREFIXES):
            self.write(RichText(text, style="bold cyan"))
            return
        match = _OID_PATTERN.search(text)
        if match:
            rich = RichText(text)
            rich.stylize(_OID_LINK_STYLE, match.start(), match.end())
            self.write(rich)
        else:
            self.write(text)

    @staticmethod
    def extract_oid(text: str) -> str:
        match = _OID_PATTERN.search(text)
        return match.group(1) if match else ""

    @staticmethod
    def extract_path(text: str) -> str:
        match = _OID_PATTERN.search(text)
        if not match:
            return ""
        prefix = text[: match.start()]
        return prefix.rsplit("|", 1)[-1].strip()

    def as_plain_text(self) -> str:
        return "\n".join(strip.text for strip in self.lines)

    def copy_visible(self) -> str:
        lines = self.lines
        if not lines:
            return ""
        region = self.content_region
        start = max(0, int(self.scroll_offset.y))
        end = min(len(lines), start + region.height)
        return "\n".join(line.text for line in lines[start:end])

    def copy_selection(self) -> str:
        lines = self.lines
        start, end = self.selection_range()
        if start is None or end is None:
            return ""
        return "\n".join(lines[i].text for i in range(start, end + 1))

    def selection_range(self) -> tuple[int | None, int | None]:
        if self._anchor is None:
            return None, None
        return self._anchor, self._end if self._end is not None else self._anchor

    def line_index_at_y(self, event_y: int) -> int | None:
        region = self.content_region
        index = int(self.scroll_offset.y) + (event_y - region.y)
        if not (0 <= index < len(self.lines)):
            return None
        return index

    def highlight_selection(self, start: int, end: int) -> None:
        if start > end:
            start, end = end, start
        for index in range(start, end + 1):
            if index in self._saved_strips:
                continue
            self._saved_strips[index] = self.lines[index]
            self.lines[index] = self.lines[index].apply_style(
                Style(bgcolor=_SELECTION_BACKGROUND)
            )
        self._end = end
        self.refresh()

    def clear_selection(self) -> None:
        for index, strip in self._saved_strips.items():
            self.lines[index] = strip
        self._saved_strips = {}
        self._anchor = None
        self._end = None
        self.refresh()

    def on_mouse_down(self, event: events.MouseDown) -> None:
        if event.button != 1:
            return
        index = self.line_index_at_y(event.y)
        if index is None:
            return
        if event.shift and self._anchor is not None:
            self.highlight_selection(self._anchor, index)
            text = self.copy_selection()
            event.stop()
            self._copy_and_clear(text)
            return
        self.clear_selection()
        self._anchor = index
        self._end = index
        self._dragging = True
        self.highlight_selection(index, index)
        event.stop()

    def on_mouse_move(self, event: events.MouseMove) -> None:
        if self._anchor is None or not self._dragging:
            return
        index = self.line_index_at_y(event.y)
        if index is None:
            return
        self.highlight_selection(self._anchor, index)
        event.stop()

    def on_mouse_up(self, event: events.MouseUp) -> None:
        if self._anchor is None:
            return
        if event.button != 1:
            return
        index = self.line_index_at_y(event.y)
        self._dragging = False
        if index is None or index == self._anchor:
            if index is not None:
                line_text = self.lines[index].text
                oid = self.extract_oid(line_text)
                if oid:
                    self.post_message(
                        self.OidActivated(
                            oid=oid,
                            path=self.extract_path(line_text),
                            index=index,
                        )
                    )
            self.clear_selection()
            event.stop()
            return
        text = self.copy_selection()
        self.clear_selection()
        event.stop()
        self._copy_and_clear(text)

    def _copy_and_clear(self, text: str) -> None:
        if not text:
            return
        self.app.copy_to_clipboard(text.strip("\n"))
        self.app.notify(
            _("Copied {n} lines to clipboard").format(n=text.count("\n") + 1)
        )

    @staticmethod
    def parse_script_name(text: str) -> str:
        match = _SCRIPT_NAME_PATTERN.search(text)
        if match:
            return match.group(1).strip()
        if text.startswith("Run migration:"):
            return "own migrations"
        return ""