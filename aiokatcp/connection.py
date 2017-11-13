# Copyright 2017 National Research Foundation (Square Kilometre Array)
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# 3. Neither the name of the copyright holder nor the names of its contributors
# may be used to endorse or promote products derived from this software without
# specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import logging
import asyncio
import re
import ipaddress
import socket
import time
from typing import Any, Optional, SupportsBytes, Iterable, cast

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
    there was no data, otherwise raises
    :exc:`aiokatcp.core.KatcpSyntaxError`.

    Parameters
    ----------
    stream
        Input stream

    Raises
    ------
    aiokatcp.core.KatcpSyntaxError
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
        # Set TCP_NODELAY to avoid unnecessary transmission delays. This is
        # on by default in asyncio from Python 3.6, but not in 3.5.
        writer.get_extra_info('socket').setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.owner = owner
        self.reader = reader
        self.writer = writer  # type: Optional[asyncio.StreamWriter]
        host, port = writer.get_extra_info('peername')
        self.address = core.Address(ipaddress.ip_address(host), port)
        self._drain_lock = asyncio.Lock(loop=owner.loop)
        self.is_server = is_server
        self._task = None     # type: Optional[asyncio.Task]
        self.logger = logging.LoggerAdapter(logger, dict(address=self.address))

    def start(self) -> asyncio.Task:
        self._task = self.owner.loop.create_task(self._run())
        self._task.add_done_callback(self._done_callback)
        return self._task

    def _close_writer(self):
        if self.writer is not None:
            self.writer.close()
            self.writer = None

    def write_messages(self, msgs: Iterable[core.Message]) -> None:
        """Write a stream of messages to the connection.

        Connection errors are logged and swallowed.
        """
        if self.writer is None:
            return     # We previously detected that it was closed
        try:
            # cast to work around https://github.com/python/mypy/issues/3989
            raw = b''.join(bytes(cast(SupportsBytes, msg)) for msg in msgs)
            self.writer.write(raw)
            self.logger.debug('Sent message %r', raw)
        except ConnectionError as error:
            self.logger.warning('Connection closed before message could be sent: %s', error)
            self._close_writer()

    def write_message(self, msg: core.Message) -> None:
        """Write a message to the connection.

        Connection errors are logged and swallowed.
        """
        self.write_messages([msg])

    async def drain(self) -> None:
        """Block until the outgoing write buffer is small enough."""
        # The Python 3.5 implementation of StreamWriter.drain is not reentrant,
        # so we use a lock.
        async with self._drain_lock:
            if self.writer is not None:
                try:
                    await self.writer.drain()
                except ConnectionError as error:
                    self.logger.warning('Connection closed while draining: %s', error)
                    self._close_writer()

    async def _run(self) -> None:
        while True:
            # If the output buffer gets too full, pause processing requests
            await self.drain()
            try:
                msg = await read_message(self.reader)
            except core.KatcpSyntaxError as error:
                self.logger.warning('Malformed message received', exc_info=True)
                if self.is_server:
                    # TODO: #log informs are supposed to go to all clients
                    self.write_message(
                        core.Message.inform('log', 'error', time.time(), __name__, str(error)))
            else:
                if msg is None:   # EOF received
                    break

                self.owner.handle_message(self, msg)

    def _done_callback(self, task: asyncio.Future) -> None:
        if not task.cancelled():
            try:
                task.result()
            except Exception:
                self.logger.exception('Exception in connection handler')

    async def stop(self) -> None:
        task = self._task
        if task is not None and not task.done():
            task.cancel()
            await asyncio.wait([task], loop=self.owner.loop)
        self._task = None
        self._close_writer()
