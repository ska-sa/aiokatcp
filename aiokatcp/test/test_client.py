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
import re
from typing import Tuple, Type, Pattern, Match

import asynctest

from aiokatcp import Client, FailReply, InvalidReply
from .test_utils import timelimit


@timelimit
class TestClient(asynctest.TestCase):
    async def make_server(self) -> Tuple[asyncio.AbstractServer, asyncio.Queue]:
        """Start a server listening on localhost.

        Returns
        -------
        server
            Asyncio server
        client_queue
            Queue which is populated with `(reader, writer)` tuples as they connect
        """
        def callback(reader, writer):
            client_queue.put_nowait((reader, writer))

        client_queue = asyncio.Queue(loop=self.loop)   # type: asyncio.Queue
        server = await asyncio.start_server(callback, '127.0.0.1', 0, loop=self.loop)
        self.addCleanup(server.close)
        return server, client_queue

    async def make_client(
            self,
            server: asyncio.AbstractServer,
            client_queue: asyncio.Queue,
            client_cls: Type[Client] = Client) \
            -> Tuple[Client, asyncio.StreamReader, asyncio.StreamWriter]:
        host, port = server.sockets[0].getsockname()    # type: ignore
        client = client_cls(host, port, loop=self.loop)
        self.addCleanup(client.close)
        (reader, writer) = await client_queue.get()
        return client, reader, writer

    @timelimit(1)
    async def setUp(self) -> None:
        self.server, self.client_queue = await self.make_server()
        self.client, self.remote_reader, self.remote_writer = \
            await self.make_client(self.server, self.client_queue)
        self.addCleanup(self.remote_writer.close)

    async def _check_received(self, pattern: Pattern[bytes]) -> Match:
        line = await self.remote_reader.readline()
        self.assertRegex(line, pattern)
        return pattern.match(line)

    async def _write(self, data: bytes) -> None:
        self.remote_writer.write(data)
        await self.remote_writer.drain()

    async def test_connect(self) -> None:
        self.remote_writer.write(b'#version-connect katcp-protocol 5.0-IM\n')
        await self.client.wait_connected()

    async def test_request_ok(self) -> None:
        await self.test_connect()
        future = self.loop.create_task(self.client.request('echo'))
        await self._check_received(re.compile(br'^\?echo\[1\]\n\Z'))
        await self._write(b'!echo[1] ok\n')
        result = await future
        self.assertEqual(result, ([], []))
        # Again, with arguments. This also tests MID incrementing, non-ASCII
        # characters, and null escaping.
        arg = b'h\xaf\xce\0'
        arg_esc = b'h\xaf\xce\\0'  # katcp escaping
        arg_esc_re = re.escape(arg_esc)
        future = self.loop.create_task(self.client.request('echo', b'123', arg))
        await self._check_received(re.compile(br'^\?echo\[2\] 123 ' + arg_esc_re + br'\n\Z'))
        await self._write(b'!echo[2] ok 123 ' + arg_esc + b'\n')
        result = await future
        self.assertEqual(result, ([b'123', arg], []))

    async def test_request_fail(self) -> None:
        await self.test_connect()
        future = self.loop.create_task(self.client.request('failme'))
        await self._check_received(re.compile(br'^\?failme\[1\]\n\Z'))
        await self._write(b'!failme[1] fail Error\\_message\n')
        with self.assertRaisesRegex(FailReply, '^Error message$'):
            await future

    async def test_request_fail_no_msg(self) -> None:
        await self.test_connect()
        future = self.loop.create_task(self.client.request('failme'))
        await self._check_received(re.compile(br'^\?failme\[1\]\n\Z'))
        await self._write(b'!failme[1] fail\n')
        with self.assertRaisesRegex(FailReply, '^$'):
            await future

    async def test_request_fail_msg_bad_encoding(self) -> None:
        await self.test_connect()
        future = self.loop.create_task(self.client.request('failme'))
        await self._check_received(re.compile(br'^\?failme\[1\]\n\Z'))
        await self._write(b'!failme[1] fail \xaf\n')
        with self.assertRaisesRegex(FailReply, '^\uFFFD$'):
            await future

    async def test_request_invalid(self) -> None:
        await self.test_connect()
        future = self.loop.create_task(self.client.request('invalid-request'))
        await self._check_received(re.compile(br'^\?invalid-request\[1\]\n\Z'))
        await self._write(b'!invalid-request[1] invalid Unknown\_request\n')
        with self.assertRaisesRegex(InvalidReply, '^Unknown request$'):
            await future

    async def test_request_no_code(self) -> None:
        await self.test_connect()
        future = self.loop.create_task(self.client.request('invalid-request'))
        await self._check_received(re.compile(br'^\?invalid-request\[1\]\n\Z'))
        await self._write(b'!invalid-request[1]\n')
        with self.assertRaisesRegex(InvalidReply, '^$'):
            await future

    async def test_no_connection(self) -> None:
        # Open a second client, which will not get the #version-connect
        client, reader, writer = \
            await self.make_client(self.server, self.client_queue)
        with self.assertRaises(BrokenPipeError):
            await client.request('help')

    async def test_connection_reset(self) -> None:
        await self.test_connect()
        self.remote_writer.close()
        with self.assertRaises(ConnectionResetError):
            await self.client.request('help')
