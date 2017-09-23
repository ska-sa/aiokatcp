import logging
import asyncio
import re
from typing import Any, Optional, SupportsBytes, cast

from . import core


logger = logging.getLogger(__name__)
DEFAULT_LIMIT = 16 * 1024**2
_BLANK_RE = re.compile(br'^[ \t]*\n?$')


async def _discard_to_eol(stream: asyncio.StreamReader) -> None:
    """Discard all data up to and including the next newline, or end of file."""
    while True:
        try:
            await stream.readuntil()
        except asyncio.IncompleteReadError:
            break     # EOF reached
        except asyncio.LimitOverrunError as error:
            # Extract the data that's already in the buffer
            # The cast is to work around
            # https://github.com/python/typeshed/issues/1622
            consumed = cast(Any, error).consumed  # type: int
            await stream.readexactly(consumed)
        else:
            break


async def read_message(stream: asyncio.StreamReader) -> Optional[core.Message]:
    """Read a single message from an asynchronous stream.

    If EOF is reached before reading the newline, returns ``None`` if
    there was no data, otherwise raises :exc:`core.KatcpSyntaxError`.

    Raises
    ------
    core.KatcpSyntaxError
        if the line was too long or malformed.
    """
    while True:
        try:
            raw = await stream.readuntil()
        except asyncio.IncompleteReadError as error:
            # Casts are to work around
            # https://github.com/python/typeshed/issues/1622
            raw = cast(Any, error).partial
            if not raw:
                return None    # End of stream reached
        except asyncio.LimitOverrunError:
            await _discard_to_eol(stream)
            raise core.KatcpSyntaxError('Message exceeded stream buffer size')
        if not _BLANK_RE.match(raw):
            return core.Message.parse(raw)


class FailReply(Exception):
    """Indicate to the remote end that a request failed, without backtrace"""


class Connection(object):
    def __init__(self, owner: Any,
                 reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
                 is_server: bool) -> None:
        self.owner = owner
        self.reader = reader
        self.writer = writer  # type: Optional[asyncio.StreamWriter]
        self._writer_lock = asyncio.Lock(loop=owner.loop)
        self.is_server = is_server
        self._task = None     # type: Optional[asyncio.Task]

    def start(self) -> asyncio.Task:
        self._task = self.owner.loop.create_task(self._run())
        return self._task

    async def write_message(self, msg: core.Message) -> None:
        async with self._writer_lock:
            if self.writer is None:
                return     # We previously detected that it was closed
            try:
                # cast to work around https://github.com/python/mypy/issues/3989
                raw = bytes(cast(SupportsBytes, msg))
                self.writer.write(raw)
                await self.writer.drain()
            except ConnectionError as error:
                logger.warn('Connection closed before message could be sent: %s', error)
                self.writer.close()
                self.writer = None

    async def _run(self) -> None:
        try:
            try:
                while True:
                    try:
                        msg = await read_message(self.reader)
                    except core.KatcpSyntaxError as error:
                        logger.warn('Malformed message received', exc_info=True)
                        if self.is_server:
                            # TODO: #log informs are supposed to go to all clients
                            await self.write_message(
                                core.Message.inform('log', 'error', str(error)))
                    else:
                        if msg is None:   # EOF received
                            break
                        self.owner.handle_message(self, msg)
            finally:
                async with self._writer_lock:
                    if self.writer is not None:
                        self.writer.close()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            logger.exception('Exception in connection handler', exc_info=True)

    async def stop(self) -> None:
        task = self._task
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            finally:
                if self._task is task:
                    self._task = None
