# Copyright 2017, 2019 National Research Foundation (Square Kilometre Array)
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
import logging
import gc
import unittest
import unittest.mock
from typing import Tuple, Type, Pattern, Match, cast

from nose.tools import nottest, istest
import asynctest

from aiokatcp import Client, FailReply, InvalidReply, ProtocolError, Message
from .test_utils import timelimit


class DummyClient(Client):
    """Client with some informs for testing"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.foos = asyncio.Queue(loop=self.loop)
        self.unhandled = asyncio.Queue(loop=self.loop)

    def inform_foo(self, string: str, integer: int) -> None:
        self.foos.put_nowait((string, integer))

    def inform_exception(self) -> None:
        raise RuntimeError('I crashed')

    def unhandled_inform(self, msg: Message) -> None:
        self.unhandled.put_nowait(msg)


@nottest
class BaseTestClient(unittest.TestCase):
    async def make_server(self, loop: asyncio.AbstractEventLoop) \
            -> Tuple[asyncio.AbstractServer, asyncio.Queue]:
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

        client_queue = asyncio.Queue(loop=loop)   # type: asyncio.Queue
        server = await asyncio.start_server(callback, '127.0.0.1', 0, loop=loop)
        self.addCleanup(server.wait_closed)
        self.addCleanup(server.close)
        return server, client_queue


@timelimit
@nottest
class BaseTestClientAsync(BaseTestClient, asynctest.TestCase):
    async def make_client(
            self,
            server: asyncio.AbstractServer,
            client_queue: asyncio.Queue,
            client_cls: Type[Client] = DummyClient,
            auto_reconnect=True) \
            -> Tuple[Client, asyncio.StreamReader, asyncio.StreamWriter]:
        host, port = server.sockets[0].getsockname()    # type: ignore
        client = client_cls(host, port, auto_reconnect=auto_reconnect, loop=self.loop)
        self.addCleanup(client.wait_closed)
        self.addCleanup(client.close)
        (reader, writer) = await client_queue.get()
        return client, reader, writer

    async def check_received(self, pattern: Pattern[bytes]) -> Match:
        line = await self.remote_reader.readline()
        # mypy thinks assertRegex requires a Pattern[str]
        self.assertRegex(line, pattern)    # type: ignore
        # cast keeps mypy happy (it can't tell that it will always match after the assert)
        return cast(Match, pattern.match(line))

    async def write(self, data: bytes) -> None:
        self.remote_writer.write(data)
        await self.remote_writer.drain()

    async def wait_connected(self) -> None:
        self.remote_writer.write(b'#version-connect katcp-protocol 5.0-IM\n')
        await self.client.wait_connected()
        # Make sure that wait_connected works when already connected
        await self.client.wait_connected()


@timelimit
@istest
class TestClient(BaseTestClientAsync):
    @timelimit(1)
    async def setUp(self) -> None:
        self.server, self.client_queue = await self.make_server(self.loop)
        self.client, self.remote_reader, self.remote_writer = \
            await self.make_client(self.server, self.client_queue)
        self.addCleanup(self.remote_writer.close)

    async def test_request_ok(self) -> None:
        await self.wait_connected()
        future = self.loop.create_task(self.client.request('echo'))
        await self.check_received(re.compile(br'^\?echo\[1\]\n\Z'))
        await self.write(b'!echo[1] ok\n')
        result = await future
        self.assertEqual(result, ([], []))
        # Again, with arguments. This also tests MID incrementing, non-ASCII
        # characters, and null escaping.
        arg = b'h\xaf\xce\0'
        arg_esc = b'h\xaf\xce\\0'  # katcp escaping
        arg_esc_re = re.escape(arg_esc)
        future = self.loop.create_task(self.client.request('echo', b'123', arg))
        await self.check_received(re.compile(br'^\?echo\[2\] 123 ' + arg_esc_re + br'\n\Z'))
        await self.write(b'!echo[2] ok 123 ' + arg_esc + b'\n')
        result = await future
        self.assertEqual(result, ([b'123', arg], []))

    async def test_request_fail(self) -> None:
        await self.wait_connected()
        future = self.loop.create_task(self.client.request('failme'))
        await self.check_received(re.compile(br'^\?failme\[1\]\n\Z'))
        await self.write(b'!failme[1] fail Error\\_message\n')
        with self.assertRaisesRegex(FailReply, '^Error message$'):
            await future

    async def test_request_fail_no_msg(self) -> None:
        await self.wait_connected()
        future = self.loop.create_task(self.client.request('failme'))
        await self.check_received(re.compile(br'^\?failme\[1\]\n\Z'))
        await self.write(b'!failme[1] fail\n')
        with self.assertRaisesRegex(FailReply, '^$'):
            await future

    async def test_request_fail_msg_bad_encoding(self) -> None:
        await self.wait_connected()
        future = self.loop.create_task(self.client.request('failme'))
        await self.check_received(re.compile(br'^\?failme\[1\]\n\Z'))
        await self.write(b'!failme[1] fail \xaf\n')
        with self.assertRaisesRegex(FailReply, '^\uFFFD$'):
            await future

    async def test_request_invalid(self) -> None:
        await self.wait_connected()
        future = self.loop.create_task(self.client.request('invalid-request'))
        await self.check_received(re.compile(br'^\?invalid-request\[1\]\n\Z'))
        await self.write(b'!invalid-request[1] invalid Unknown\\_request\n')
        with self.assertRaisesRegex(InvalidReply, '^Unknown request$'):
            await future

    async def test_request_no_code(self) -> None:
        await self.wait_connected()
        future = self.loop.create_task(self.client.request('invalid-request'))
        await self.check_received(re.compile(br'^\?invalid-request\[1\]\n\Z'))
        await self.write(b'!invalid-request[1]\n')
        with self.assertRaisesRegex(InvalidReply, '^$'):
            await future

    async def test_request_with_informs(self) -> None:
        await self.wait_connected()
        future = self.loop.create_task(self.client.request('help'))
        await self.check_received(re.compile(br'^\?help\[1\]\n\Z'))
        await self.write(b'#help[1] help Show\\_help\n')
        await self.write(b'#help[1] halt Halt\n')
        await self.write(b'!help[1] ok 2\n')
        result = await future
        self.assertEqual(result, ([b'2'], [
            Message.inform('help', b'help', b'Show help', mid=1),
            Message.inform('help', b'halt', b'Halt', mid=1)
        ]))

    async def test_inform(self) -> None:
        client = cast(DummyClient, self.client)
        await self.wait_connected()
        with self.assertLogs(logging.getLogger('aiokatcp.client')) as cm:
            # Put in bad ones before the good one, so that as soon as we've
            # received the good one from the queue we can finish the test.
            await self.write(b'#exception\n#foo bad notinteger\n#foo \xc3\xa9 123\n')
            inform = await client.foos.get()
        self.assertRegex(cm.output[0], 'I crashed')
        self.assertRegex(cm.output[1], 'error in inform')
        self.assertEqual(inform, ('é', 123))

    async def test_unhandled_inform(self) -> None:
        client = cast(DummyClient, self.client)
        await self.wait_connected()
        await self.write(b'#unhandled arg\n')
        msg = await client.unhandled.get()
        self.assertEqual(msg, Message.inform('unhandled', b'arg'))

    async def test_unsolicited_reply(self) -> None:
        await self.wait_connected()
        future = self.loop.create_task(self.client.request('echo'))
        with self.assertLogs(logging.getLogger('aiokatcp.client'), logging.DEBUG):
            await self.write(b'!surprise[3]\n!echo[1] ok\n')
            await future

    async def test_receive_request(self) -> None:
        await self.wait_connected()
        future = self.loop.create_task(self.client.request('echo'))
        with self.assertLogs(logging.getLogger('aiokatcp.client')):
            await self.write(b'?surprise\n!echo[1] ok\n')
            await future

    async def test_reply_no_mid(self) -> None:
        await self.wait_connected()
        future = self.loop.create_task(self.client.request('echo'))
        with self.assertLogs(logging.getLogger('aiokatcp.client')):
            await self.write(b'!surprise ok\n!echo[1] ok\n')
            await future

    async def test_context_manager(self) -> None:
        async with self.client:
            pass
        await self.client.wait_closed()

    async def test_connect(self) -> None:
        host, port = self.server.sockets[0].getsockname()    # type: ignore
        client_task = self.loop.create_task(DummyClient.connect(host, port, loop=self.loop))
        (reader, writer) = await self.client_queue.get()
        await asynctest.exhaust_callbacks(self.loop)
        self.assertFalse(client_task.done())
        writer.write(b'#version-connect katcp-protocol 5.0-IM\n')
        client = await client_task
        assert client.is_connected
        client.close()
        writer.close()
        await client.wait_closed()

    async def test_unparsable_protocol(self) -> None:
        with self.assertLogs(logging.getLogger('aiokatcp.client')) as cm:
            self.remote_writer.write(b'#version-connect katcp-protocol notvalid\n')
            line = await self.remote_reader.read()
        self.assertEqual(line, b'')
        self.assertRegex(cm.output[0], 'Unparsable katcp-protocol')

    async def test_bad_protocol(self) -> None:
        with self.assertLogs(logging.getLogger('aiokatcp.client')) as cm:
            self.remote_writer.write(b'#version-connect katcp-protocol 4.0-I\n')
            line = await self.remote_reader.read()
        self.assertEqual(line, b'')
        self.assertRegex(cm.output[0], r'Unknown protocol version 4\.0')

    async def test_no_connection(self) -> None:
        # Open a second client, which will not get the #version-connect
        client, reader, writer = \
            await self.make_client(self.server, self.client_queue)
        self.addCleanup(writer.close)
        with self.assertRaises(BrokenPipeError):
            await client.request('help')

    async def test_connection_reset(self) -> None:
        await self.wait_connected()
        self.remote_writer.close()
        with self.assertRaises(ConnectionResetError):
            await self.client.request('help')

    async def test_disconnected(self) -> None:
        await self.wait_connected()
        await self.write(b'#disconnect Server\\_exiting\n')
        await self.client.wait_disconnected()
        with self.assertRaises(BrokenPipeError):
            await self.client.request('help')

    async def test_bad_address(self) -> None:
        client = DummyClient('invalid.invalid', 1)
        self.addCleanup(client.close)
        # While invalid.invalid will fail to resolve, it may take some time.
        # By mocking it, we ensure that the test runs without the need for
        # an estimated sleep.
        with unittest.mock.patch.object(self.loop, 'getaddrinfo', side_effect=OSError):
            with self.assertLogs(logging.getLogger('aiokatcp.client')) as cm:
                task = self.loop.create_task(client.wait_connected())
                await asynctest.exhaust_callbacks(self.loop)
        self.assertRegex(cm.output[0], 'Failed to connect to invalid.invalid:1: ')
        task.cancel()


@timelimit
@istest
class TestClientNoReconnect(TestClient):
    @timelimit(1)
    async def setUp(self) -> None:
        self.server, self.client_queue = await self.make_server(self.loop)
        self.client, self.remote_reader, self.remote_writer = \
            await self.make_client(self.server, self.client_queue, auto_reconnect=False)
        self.addCleanup(self.remote_writer.close)

    async def test_unparsable_protocol(self) -> None:
        self.remote_writer.write(b'#version-connect katcp-protocol notvalid\n')
        line = await self.remote_reader.read()
        self.assertEqual(line, b'')
        with self.assertRaises(ProtocolError):
            await self.client.wait_connected()

    async def test_bad_protocol(self) -> None:
        # Different approach to test_unparsable_protocol, to get more coverage
        wait_task = self.loop.create_task(self.client.wait_connected())
        self.remote_writer.write(b'#version-connect katcp-protocol 4.0-I\n')
        line = await self.remote_reader.read()
        self.assertEqual(line, b'')
        with self.assertRaises(ProtocolError):
            await wait_task

    async def test_disconnected(self) -> None:
        await self.wait_connected()
        await self.write(b'#disconnect Server\\_exiting\n')
        await self.client.wait_disconnected()
        with self.assertRaises(BrokenPipeError):
            await self.client.request('help')
        with self.assertRaises(ConnectionResetError):
            await self.client.wait_connected()

    async def test_connect_failed(self) -> None:
        host, port = self.server.sockets[0].getsockname()    # type: ignore
        client_task = self.loop.create_task(
            DummyClient.connect(host, port, auto_reconnect=False, loop=self.loop))
        (reader, writer) = await self.client_queue.get()
        await asynctest.exhaust_callbacks(self.loop)
        self.assertFalse(client_task.done())
        writer.close()
        with self.assertRaises(ConnectionAbortedError):
            await client_task


@timelimit
@istest
class TestClientNoMidSupport(BaseTestClientAsync):
    @timelimit(1)
    async def setUp(self) -> None:
        self.server, self.client_queue = await self.make_server(self.loop)
        self.client, self.remote_reader, self.remote_writer = \
            await self.make_client(self.server, self.client_queue)
        self.addCleanup(self.remote_writer.close)

    async def test_single(self):
        self.remote_writer.write(b'#version-connect katcp-protocol 5.0-M\n')
        await self.client.wait_connected()
        future = self.loop.create_task(self.client.request('echo'))
        await self.check_received(re.compile(br'^\?echo\n\Z'))
        await self.write(b'#echo an\\_inform\n')
        await self.write(b'!echo ok\n')
        result = await future
        self.assertEqual(result, ([], [Message.inform('echo', b'an inform')]))

    async def test_concurrent(self):
        self.remote_writer.write(b'#version-connect katcp-protocol 5.0-M\n')
        await self.client.wait_connected()
        future1 = self.loop.create_task(self.client.request('echo', 1))
        future2 = self.loop.create_task(self.client.request('echo', 2))
        for i in range(2):
            match = await self.check_received(re.compile(br'^\?echo (1|2)\n\Z'))
            await self.write(b'#echo value ' + match.group(1) + b'\n')
            await self.write(b'!echo ok ' + match.group(1) + b'\n')
        result1 = await future1
        self.assertEqual(result1, ([b'1'], [Message.inform('echo', b'value', b'1')]))
        result2 = await future2
        self.assertEqual(result2, ([b'2'], [Message.inform('echo', b'value', b'2')]))


@istest
class TestUnclosedClient(BaseTestClient, unittest.TestCase):
    async def body(self) -> None:
        server, client_queue = await self.make_server(self.loop)
        host, port = server.sockets[0].getsockname()    # type: ignore
        client = DummyClient(host, port, loop=self.loop)  # noqa: F841
        (reader, writer) = await client_queue.get()
        writer.close()
        server.close()
        await server.wait_closed()

    def test(self) -> None:
        with self.assertWarnsRegex(ResourceWarning, 'unclosed Client'):
            self.loop = asyncio.new_event_loop()
            self.loop.run_until_complete(self.body())
            self.loop.close()
            # Run a few times for PyPy's benefit
            gc.collect()
            gc.collect()
