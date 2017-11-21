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

import asyncio
import logging
import re
import warnings
from typing import Any, List, Callable, Tuple
# Only used in type comments, so flake8 complains
from typing import Dict   # noqa: F401

from . import core, connection
from .connection import FailReply, InvalidReply


logger = logging.getLogger(__name__)


class _PendingRequest:
    def __init__(self, mid: int, loop: asyncio.AbstractEventLoop) -> None:
        self.mid = mid
        self.informs = []      # type: List[core.Message]
        self.reply = loop.create_future()


class Client:
    def __init__(self, host: str, port: int, *,
                 limit: int = connection.DEFAULT_LIMIT,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        if loop is None:
            loop = asyncio.get_event_loop()
        self._connected = asyncio.Event(loop=loop)
        self._connection = None         # type: connection.Connection
        self.host = host
        self.port = port
        self.loop = loop
        self._limit = limit
        self._pending = {}              # type: Dict[int, _PendingRequest]
        self._next_mid = 1
        self._run_task = loop.create_task(self._run())
        self._connected_callbacks = []  # type: List[Callable[[], None]]
        self._disconnected_callbacks = []   # type: List[Callable[[], None]]

    def __del__(self) -> None:
        if self._run_task is not None:
            warnings.warn('unclosed Client {!r}'.format(self), ResourceWarning)
            self._run_task.cancel()

    async def handle_message(self, conn: connection.Connection, msg: core.Message) -> None:
        if msg.mtype == core.Message.Type.REQUEST:
            logger.info('Received unexpected request %s from server', msg.name)
            return
        if msg.mid is not None:
            try:
                req = self._pending[msg.mid]
            except KeyError:
                logger.debug('Received %r with unknown message ID %s (possibly cancelled request)',
                             bytes(msg))    # type: ignore
            else:
                if msg.mtype == core.Message.Type.REPLY:
                    req.reply.set_result(msg)
                elif msg.mtype == core.Message.Type.INFORM:
                    req.informs.append(msg)
                else:
                    logger.warning('Unknown message type %s', msg.mtype)
        else:
            self.handle_inform(msg)

    def handle_inform(self, msg):
        logger.debug('Received %s', bytes(msg))
        # TODO: provide dispatch mechanism for informs
        if msg.name == 'version-connect':
            if len(msg.arguments) >= 2 and msg.arguments[0] == b'katcp-protocol':
                good = False
                match = re.match(b'^(\d+)\.(\d+)(?:-(.+))?$', msg.arguments[1])
                if not match:
                    logger.warning('Unparsable katcp-protocol %s', msg.arguments[1])
                else:
                    major = int(match.group(1))
                    minor = int(match.group(2))
                    logger.debug('Protocol version %d.%d', major, minor)
                    flags = match.group(3)
                    if major != 5:
                        logger.warning('Unknown protocol version %d.%d', major, minor)
                    elif flags is None or b'I' not in flags:
                        logger.warning('Message IDs not supported, but required by aiokatcp')
                    else:
                        good = True
                if good:
                    # Safety in case a race condition causes the connection to
                    # die before this function was called.
                    if self._connection is not None:
                        self._on_connected()
                elif self._connection is not None:
                    self.loop.create_task(self._connection.stop())

    def add_connected_callback(self, callback: Callable[[], None]) -> None:
        self._connected_callbacks.append(callback)

    def add_disconnected_callback(self, callback: Callable[[], None]) -> None:
        self._disconnected_callbacks.append(callback)

    @property
    def is_connected(self):
        return self._connected.is_set()

    def _on_connected(self):
        if not self.is_connected:
            self._connected.set()
            for callback in self._connected_callbacks:
                callback()

    def _on_disconnected(self):
        if self.is_connected:
            self._connected.clear()
            for req in self._pending.values():
                if not req.reply.done():
                    req.reply.set_exception(ConnectionResetError('Connection to server lost'))
            self._pending.clear()
            for callback in self._disconnected_callbacks:
                callback()

    async def _run(self) -> None:
        while True:
            # Open the connection. Based on asyncio.open_connection.
            reader = asyncio.StreamReader(limit=self._limit, loop=self.loop)
            protocol = connection.ConvertCRProtocol(reader, loop=self.loop)
            try:
                transport, _ = await self.loop.create_connection(
                    lambda: protocol, self.host, self.port)
            except ConnectionError as error:
                logger.warning('Failed to connect to %s:%d: %s',
                               self.host, self.port, error)
                await asyncio.sleep(1, loop=self.loop)
                continue
            writer = asyncio.StreamWriter(transport, protocol, reader, self.loop)
            self._connection = connection.Connection(self, reader, writer, False)
            # Process replies until connection closes. _on_connected is
            # called by the version-info inform handler.
            try:
                connection_task = self._connection.start()
                await asyncio.wait([connection_task], loop=self.loop)
            finally:
                if self._connection:
                    await self._connection.stop()
                    self._connection = None
                if self.is_connected:
                    self._on_disconnected()
            # TODO: exponential randomised backoff
            await asyncio.sleep(1, loop=self.loop)

    async def close(self) -> None:
        self._run_task.cancel()
        try:
            await self._run_task
            self._run_task = None
        except asyncio.CancelledError:
            pass

    async def wait_connected(self) -> None:
        await self._connected.wait()

    @classmethod
    async def connect(cls, host: str, port: int, *,
                      limit: int = connection.DEFAULT_LIMIT,
                      loop: asyncio.AbstractEventLoop = None) -> 'Client':
        client = cls(host, port, limit=limit, loop=loop)
        await client.wait_connected()
        return client

    async def request_raw(self, name: str, *args: Any) -> Tuple[core.Message, List[core.Message]]:
        if not self.is_connected:
            raise BrokenPipeError('Not connected')
        mid = self._next_mid
        self._next_mid += 1
        req = _PendingRequest(mid, self.loop)
        self._pending[mid] = req
        try:
            msg = core.Message(core.Message.Type.REQUEST, name, *args, mid=mid)
            self._connection.write_message(msg)
            reply_msg = await req.reply
            return reply_msg, req.informs
        finally:
            self._pending.pop(mid, None)

    async def request(self, name: str, *args: Any) -> Tuple[List[bytes], List[core.Message]]:
        reply_msg, informs = await self.request_raw(name, *args)
        type_ = core.Message.INVALID if not reply_msg.arguments else reply_msg.arguments[0]
        error = '' if len(reply_msg.arguments) <= 1 else reply_msg.arguments[1]
        if type_ == core.Message.OK:
            return reply_msg.arguments[1:], informs
        elif type_ == core.Message.FAIL:
            raise FailReply(error)
        else:
            raise InvalidReply(error)
