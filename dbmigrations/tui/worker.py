"""Command worker: runs launch_command in a thread and drains the log queue."""

from __future__ import annotations

import asyncio
from typing import Any

from _confirmation import Confirmation
from _output import QueueOutput

_END_MARKER = object()


class CommandRunner:
    """Runs a command in a background thread while the UI drains its output queue.

    The command writes through a thread-safe ``QueueOutput``; the UI thread
    copies completed lines into the log panel until the worker signals done.
    A finished (or failed) run is reported back via ``on_command_finished`` /
    ``on_command_error`` on the hosting app.
    """

    def __init__(
        self,
        app: Any,
        queue: asyncio.Queue[str],
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        self._app = app
        self._queue = queue
        self._loop = loop
        self._task: asyncio.Task[None] | None = None

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    def start(self, cmd_cls: type, opts: Any, confirm: Confirmation) -> None:
        from commands import RunTestsCommand

        use_run_tests_by_user = cmd_cls is RunTestsCommand
        out = QueueOutput(self._queue, loop=self._loop)
        self._task = self._loop.create_task(
            self._run_and_drain(cmd_cls, opts, confirm, out, use_run_tests_by_user)
        )

    def cancel(self) -> None:
        if self.running:
            self._app.on_cancel_requested()

    async def _run_and_drain(
        self,
        cmd_cls: type,
        opts: Any,
        confirm: Confirmation,
        out: QueueOutput,
        use_run_tests_by_user: bool = False,
    ) -> None:
        drain_task = self._loop.create_task(self._drain())
        try:
            exit_code = await self._loop.run_in_executor(
                None, self._execute_sync, cmd_cls, opts, confirm, out,
                use_run_tests_by_user,
            )
        except Exception as error:  # noqa: BLE001 - surface any worker failure in the UI
            self._app.on_command_error(str(error))
            exit_code = 1
        self._loop.call_soon_threadsafe(self._queue.put_nowait, _END_MARKER)
        await drain_task
        self._task = None
        self._app.on_command_finished(exit_code)

    def _execute_sync(
        self,
        cmd_cls: type,
        opts: Any,
        confirm: Confirmation,
        out: QueueOutput,
        use_run_tests_by_user: bool = False,
    ) -> int:
        from _launch import launch_command

        return launch_command(
            cmd_cls, opts, self._app.config, out=out, confirm=confirm,
            use_run_tests_by_user=use_run_tests_by_user,
        )

    async def _drain(self) -> None:
        while True:
            try:
                line = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                await asyncio.sleep(0.03)
                continue
            if line is _END_MARKER:
                return
            self._app.on_log_line(line)