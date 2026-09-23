#!/usr/bin/env python3
"""Confirmation prompts: terminal y/N (get_char) and injectable hooks for the TUI."""
from __future__ import annotations

import sys

from _output import Output


class Confirmation:
    """Protocol for yes/no prompts used by commands."""

    def confirm(self, message: str) -> bool:
        raise NotImplementedError

    def __call__(self, message: str) -> bool:
        return self.confirm(message)


def get_char() -> str:
    result = ""    
    if sys.platform == "win32":
        import msvcrt        
        char_bytes = msvcrt.getch()        
        # Handle special/function keys (arrows, F1-F12) which emit a prefix byte
        if char_bytes in (b"\x00", b"\xe0"):
            msvcrt.getch()  # Consume the second trailing byte of the special key
            result = ""
        else:
            result = char_bytes.decode("utf-8", "ignore")
            print(result, flush=True)            
    else:
        import termios
        import tty        
        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        try:
            tty.setcbreak(fd)
            result = sys.stdin.read(1)
            print(result, flush=True)
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)            
    return result 


class ConsoleConfirm(Confirmation):
    """Terminal y/N prompt that echoes the typed character via get_char()."""

    def __init__(self, out: Output | None = None) -> None:
        self.out = out if out is not None else Output()

    def confirm(self, message: str) -> bool:
        self.out.print(message, end="", flush=True)
        answer = get_char().lower()
        return answer == "y"


class TuiConfirm(Confirmation):
    """Confirmation resolved through a modal screen of the running TUI app.

    ``confirm()`` is meant to be called from the worker thread: it asks the
    UI thread (via ``App.call_from_thread``) to present the modal and blocks
    the worker until the user answers.
    """

    def __init__(self, app: object) -> None:
        self._app = app

    def confirm(self, message: str) -> bool:
        from concurrent.futures import Future

        future: "Future[bool]" = Future()
        self._app.call_from_thread(self._present, message, future)
        return future.result()

    def _present(self, message: str, future: "Future[bool]") -> None:
        from tui.screens.confirm_screen import ConfirmScreen

        self._app.push_screen(ConfirmScreen(message), callback=future.set_result)