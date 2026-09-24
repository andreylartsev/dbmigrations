"""RichLog-based central log panel with script start lines highlighted."""

from __future__ import annotations

import re

from rich.style import Style
from rich.text import Text as RichText
from textual import events
from textual.message import Message
from textual.widgets import RichLog

_SCRIPT_START_PREFIXES = ("Running script:", "Run migration:")
_SCRIPT_NAME_PATTERN = re.compile(r"^Running script:\s*\[([^\]]+)\]")
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

    def __init__(self, *args, max_lines: int | None = None, **kwargs) -> None:
        super().__init__(
            *args,
            highlight=True,
            markup=False,
            wrap=True,
            auto_scroll=True,
            max_lines=max_lines,
            **kwargs,
        )

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

    def append_error(self, message: object) -> None:
        self.write(RichText(str(message), style="red"))

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
        if "|" in prefix:
            return prefix.rsplit("|", 1)[-1].strip()
        head = prefix.rsplit("[", 1)[-1] if "[" in prefix else prefix
        return head.strip()

    def oid_info_at(self, index: int) -> tuple[str, str]:
        """Return (oid, path) around the clicked row.

        Long log entries wrap across several rows, so the ``(OID: …)`` part may
        live on a row below the click. Walks down from the clicked row until it
        finds an OID reference (also combining a row with its successor in case
        the hexadecimal id is split across the wrap).
        """
        total = len(self.lines)
        if 0 <= index < total:
            text = self.lines[index].text
            oid = self.extract_oid(text)
            if oid:
                return oid, self.extract_path(text)
        for i in range(index, min(total, index + 8)):
            current = self.lines[i].text
            match = _OID_PATTERN.search(current)
            if match:
                return match.group(1), self.extract_path(current)
            if i + 1 < total:
                combined = current + "\n" + self.lines[i + 1].text
                match = _OID_PATTERN.search(combined)
                if match:
                    return match.group(1), self.extract_path(combined)
        return "", ""

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

    def line_index_at_y(self, event_y: int) -> int | None:
        region = self.content_region
        index = int(self.scroll_offset.y) + (event_y - region.y)
        if not (0 <= index < len(self.lines)):
            return None
        return index

    def on_mouse_up(self, event: events.MouseUp) -> None:
        if event.button != 1:
            return
        index = self.line_index_at_y(event.y)
        if index is None:
            return
        oid, path = self.oid_info_at(index)
        if oid:
            self.post_message(
                self.OidActivated(oid=oid, path=path, index=index)
            )
        event.stop()

    @staticmethod
    def parse_script_name(text: str) -> str:
        match = _SCRIPT_NAME_PATTERN.search(text)
        if match:
            return match.group(1).strip()
        if text.startswith("Run migration:"):
            return "own migrations"
        return ""