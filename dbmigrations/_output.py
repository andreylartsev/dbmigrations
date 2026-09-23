#!/usr/bin/env python3
"""Output sink: routes user-facing text to a callable sink (TUI queue) or sys.stdout."""
from __future__ import annotations

import asyncio
import sys
from typing import Any, Callable


class Output:
    """Sink for user-facing text output.

    With no sink it prints to ``sys.stdout`` (regular CLI behavior). With a sink
    it delegates every rendered line to ``sink(line)`` (e.g. an ``asyncio.Queue``
    in the TUI worker). The sink receives the fully-rendered text including the
    trailing ``end``, so writers do not need to re-check ``flush``.
    """

    def __init__(self, sink: Callable[[str], None] | None = None) -> None:
        self._sink = sink

    def print(
            self,
            *args: Any,
            sep: str = " ",
            end: str = "\n",
            flush: bool = False,
    ) -> None:
        text = sep.join(str(a) for a in args) + end
        if self._sink is None:
            print(text, end="", flush=flush)
        else:
            self._sink(text)


class QueueOutput(Output):
    """An Output that accumulates partial writes and posts completed lines to a queue.

    Thread-safe: when a running asyncio loop is available (the UI thread) each
    completed line is scheduled onto it with ``call_soon_threadsafe``, so this
    output may be written from a worker thread while the loop runs elsewhere.
    Partial lines (``end=""``, ``flush=True``) are buffered until a newline
    arrives, keeping the TUI log line-grained.
    """

    def __init__(self, queue: Any, loop: asyncio.AbstractEventLoop | None = None) -> None:
        super().__init__()
        self._queue = queue
        self._loop = loop
        self._partial = ""

    def print(
            self,
            *args: Any,
            sep: str = " ",
            end: str = "\n",
            flush: bool = False,
    ) -> None:
        self._partial += sep.join(str(a) for a in args) + end
        while "\n" in self._partial:
            line, self._partial = self._partial.split("\n", 1)
            self._post(line)

    def _post(self, line: str) -> None:
        if self._loop is not None and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._queue.put_nowait, line)
        else:
            self._queue.put_nowait(line)