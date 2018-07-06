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
from typing import Optional     # noqa: F401

import asynctest

from aiokatcp.core import Message, KatcpSyntaxError
from aiokatcp.connection import read_message, Connection
from .test_utils import timelimit


class TestReadMessage(asynctest.TestCase):
    forbid_get_event_loop = True

    async def test_read(self) -> None:
        data = b'?help[123] foo\n#log info msg\n \t\n\n!help[123] bar\n\n'
        reader = asyncio.StreamReader(loop=self.loop)
        reader.feed_data(data)
        reader.feed_eof()
        msg = await read_message(reader)
        self.assertEqual(msg, Message.request('help', 'foo', mid=123))
        msg = await read_message(reader)
        self.assertEqual(msg, Message.inform('log', 'info', 'msg'))
        msg = await read_message(reader)
        self.assertEqual(msg, Message.reply('help', 'bar', mid=123))
        msg = await read_message(reader)
        self.assertIsNone(msg)

    async def test_read_overrun(self) -> None:
        data = b'!foo a_string_that_doesnt_fit_in_the_buffer\n!foo short_string\n'
        reader = asyncio.StreamReader(limit=25, loop=self.loop)
        reader.feed_data(data)
        reader.feed_eof()
        with self.assertRaises(KatcpSyntaxError):
            await read_message(reader)
        msg = await read_message(reader)
        self.assertEqual(msg, Message.reply('foo', 'short_string'))

    async def test_read_overrun_eof(self) -> None:
        data = b'!foo a_string_that_doesnt_fit_in_the_buffer'
        reader = asyncio.StreamReader(limit=25, loop=self.loop)
        reader.feed_data(data)
        reader.feed_eof()
        with self.assertRaises(KatcpSyntaxError):
            await read_message(reader)
        msg = await read_message(reader)
        self.assertIsNone(msg)

    async def test_read_partial(self) -> None:
        data = b'!foo nonewline'
        reader = asyncio.StreamReader(loop=self.loop)
        reader.feed_data(data)
        reader.feed_eof()
        with self.assertRaises(KatcpSyntaxError):
            await read_message(reader)


@timelimit
class TestConnection(asynctest.TestCase):
    async def setUp(self) -> None:
        self.writer = None       # type: Optional[asyncio.StreamWriter]
        self.reader = None       # type: Optional[asyncio.StreamReader]
        #: If non-None, _ok_reply waits on it before sending the reply
        self.ok_wait = None      # type: Optional[asyncio.Semaphore]
        #: Released by _ok_reply once the reply is sent
        self.ok_done = asyncio.Semaphore(0, loop=self.loop)
        #: Set ready once the connection has been established
        self._ready = asyncio.Event(loop=self.loop)
        self.server = await asyncio.start_server(
            self._client_connected_cb, '127.0.0.1', 0, loop=self.loop)
        host, port = self.server.sockets[0].getsockname()    # type: ignore
        self.owner = asynctest.MagicMock()
        self.owner.loop = self.loop
        self.owner.handle_message = asynctest.CoroutineMock(side_effect=self._ok_handler)
        self.remote_reader, self.remote_writer = await asyncio.open_connection(
            host, port, loop=self.loop)
        # Ensure the server side of the connection is ready
        await self._ready.wait()

    async def tearDown(self) -> None:
        self.server.close()
        await self.server.wait_closed()
        self.remote_writer.close()
        if self.writer:
            self.writer.close()

    def make_connection(self, is_server: bool) -> Connection:
        assert self.reader    # keeps mypy happy
        assert self.writer
        conn = Connection(self.owner, self.reader, self.writer, is_server)
        self.addCleanup(conn.wait_closed)
        self.addCleanup(conn.close)
        return conn

    def _client_connected_cb(self, reader: asyncio.StreamReader,
                             writer: asyncio.StreamWriter) -> None:
        self.reader = reader
        self.writer = writer
        self._ready.set()

    async def _ok_reply(self, conn, msg):
        if self.ok_wait:
            await self.ok_wait.acquire()
        conn.write_message(Message.reply_to_request(msg, 'ok'))
        await conn.drain()
        self.ok_done.release()

    async def _ok_handler(self, conn, msg):
        self.loop.create_task(self._ok_reply(conn, msg))

    async def test_write_message(self) -> None:
        conn = self.make_connection(True)
        conn.write_message(Message.reply('ok', mid=1))
        await conn.drain()
        line = await self.remote_reader.readline()
        self.assertEqual(line, b'!ok[1]\n')

    async def test_run(self) -> None:
        conn = self.make_connection(True)
        self.remote_writer.write(b'?watchdog[2]\n')
        await self.ok_done.acquire()
        self.owner.handle_message.assert_called_with(conn, Message.request('watchdog', mid=2))
        reply = await self.remote_reader.readline()
        self.assertEqual(reply, b'!watchdog[2] ok\n')
        # Check that it exits when the client disconnects its write end
        self.remote_writer.write_eof()
        # We need to give the packets time to go through the system
        await conn.wait_closed()

    async def test_disconnected(self) -> None:
        conn = self.make_connection(True)
        # Don't send the reply until the socket is closed
        self.ok_wait = asyncio.Semaphore(0, loop=self.loop)
        self.remote_writer.write(b'?watchdog[2]\n?watchdog[3]\n?watchdog[4]\n')
        self.remote_writer.close()
        await asynctest.exhaust_callbacks(self.loop)
        self.ok_wait.release()
        self.ok_wait.release()
        with self.assertLogs('aiokatcp.connection', logging.WARNING) as cm:
            # Wait for first two watchdogs and the close to go through
            await self.ok_done.acquire()
            await self.ok_done.acquire()
            await conn.wait_closed()
            self.owner.handle_message.assert_called_with(conn, Message.request('watchdog', mid=4))
        # Note: should only be one warning, not two
        self.assertEqual(1, len(cm.output))
        self.assertRegex(
            cm.output[0],
            r'^WARNING:aiokatcp\.connection:Connection closed .*: Connection lost \[.*\]$')
        # Allow the final watchdog to go through. This just provides test coverage
        # that Connection.write_message handles the writer having already gone away.
        self.ok_wait.release()
        await asynctest.exhaust_callbacks(self.loop)

    async def test_malformed(self) -> None:
        conn = self.make_connection(True)
        self.remote_writer.write(b'malformed\n')
        self.remote_writer.write_eof()
        with self.assertLogs('aiokatcp.connection', logging.WARN) as cm:
            # Wait for the close to go through
            await conn.wait_closed()
            self.owner.handle_message.assert_not_called()
        self.assertEqual(len(cm.output), 1)
        self.assertRegex(cm.output[0], 'Malformed message received.*')

    async def test_close_early(self) -> None:
        conn = self.make_connection(True)
        conn.close()
        await conn.wait_closed()

    async def test_close(self) -> None:
        conn = self.make_connection(True)
        await asynctest.exhaust_callbacks(self.loop)
        conn.close()
        await conn.wait_closed()

    async def test_exception(self) -> None:
        self.owner.handle_message.side_effect = RuntimeError('test error')
        conn = self.make_connection(True)
        await asynctest.exhaust_callbacks(self.loop)
        self.remote_writer.write(b'?watchdog[2]\n')
        self.remote_writer.close()
        with self.assertLogs('aiokatcp.connection', logging.ERROR) as cm:
            await conn.wait_closed()
        self.assertEqual(len(cm.output), 1)
        self.assertRegex(cm.output[0], '(?s)Exception in connection handler.*test error')
