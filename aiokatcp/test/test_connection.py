import asyncio
import logging

import asynctest

from aiokatcp.core import Message, KatcpSyntaxError
from aiokatcp.connection import read_message, Connection


class TestReadMessage(asynctest.TestCase):
    forbid_get_event_loop = True

    async def test_read(self):
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

    async def test_read_overrun(self):
        data = b'!foo a_string_that_doesnt_fit_in_the_buffer\n!foo short_string\n'
        reader = asyncio.StreamReader(limit=25, loop=self.loop)
        reader.feed_data(data)
        reader.feed_eof()
        with self.assertRaises(KatcpSyntaxError):
            await read_message(reader)
        msg = await read_message(reader)
        self.assertEqual(msg, Message.reply('foo', 'short_string'))

    async def test_read_overrun_eof(self):
        data = b'!foo a_string_that_doesnt_fit_in_the_buffer'
        reader = asyncio.StreamReader(limit=25, loop=self.loop)
        reader.feed_data(data)
        reader.feed_eof()
        with self.assertRaises(KatcpSyntaxError):
            await read_message(reader)
        msg = await read_message(reader)
        self.assertIsNone(msg)

    async def test_read_partial(self):
        data = b'!foo nonewline'
        reader = asyncio.StreamReader(loop=self.loop)
        reader.feed_data(data)
        reader.feed_eof()
        with self.assertRaises(KatcpSyntaxError):
            await read_message(reader)


class TestConnection(asynctest.TestCase):
    def _client_connected_cb(self, reader, writer):
        self.reader = reader
        self.writer = writer
        self._ready.set()

    async def _ok_reply(self, conn, msg):
        if self.ok_delay:
            await asyncio.sleep(self.ok_delay, loop=self.loop)
        await conn.write_message(Message.reply_to_request(msg, 'ok'))
        self.ok_done.release()

    def _ok_handler(self, conn, msg):
        self.loop.create_task(self._ok_reply(conn, msg))

    async def setUp(self):
        self.writer = None
        self.reader = None
        self.ok_delay = 0.0
        self.ok_done = asyncio.Semaphore(0, loop=self.loop)
        self._ready = asyncio.Event(loop=self.loop)
        self.server = await asyncio.start_server(
            self._client_connected_cb, 'localhost', 0, loop=self.loop)
        host, port = self.server.sockets[0].getsockname()
        self.owner = asynctest.MagicMock()
        self.owner.loop = self.loop
        self.owner.handle_message = asynctest.Mock(side_effect=self._ok_handler)
        self.remote_reader, self.remote_writer = await asyncio.open_connection(
                host, port, loop=self.loop)
        # Ensure the server side of the connection is ready
        await self._ready.wait()

    async def tearDown(self):
        self.server.close()
        await self.server.wait_closed()
        self.remote_writer.close()
        if self.writer:
            self.writer.close()

    async def test_write_message(self):
        conn = Connection(self.owner, self.reader, self.writer, True)
        await conn.write_message(Message.reply('ok', mid=1))
        line = await self.remote_reader.readline()
        self.assertEqual(line, b'!ok[1]\n')

    async def test_run(self):
        conn = Connection(self.owner, self.reader, self.writer, True)
        task = conn.start()
        self.addCleanup(conn.stop)
        self.remote_writer.write(b'?watchdog[2]\n')
        await asyncio.wait_for(self.ok_done.acquire(), timeout=2, loop=self.loop)
        self.owner.handle_message.assert_called_with(conn, Message.request('watchdog', mid=2))
        reply = await self.remote_reader.readline()
        self.assertEqual(reply, b'!watchdog[2] ok\n')
        # Check that it exits when the client disconnects its write end
        self.remote_writer.write_eof()
        # We need to give the packets time to go through the system
        await asyncio.wait_for(task, timeout=2, loop=self.loop)

    async def test_disconnected(self):
        conn = Connection(self.owner, self.reader, self.writer, True)
        task = conn.start()
        self.ok_delay = 0.1     # Need time for the socket close to propagate
        self.addCleanup(conn.stop)
        self.remote_writer.write(b'?watchdog[2]\n?watchdog[3]\n')
        self.remote_writer.close()
        with self.assertLogs('aiokatcp.connection', logging.WARN) as cm:
            # Wait for both watchdogs and the close to go through
            await asyncio.wait_for(self.ok_done.acquire(), timeout=2, loop=self.loop)
            await asyncio.wait_for(self.ok_done.acquire(), timeout=2, loop=self.loop)
            await asyncio.wait_for(task, timeout=2, loop=self.loop)
            self.owner.handle_message.assert_called_with(conn, Message.request('watchdog', mid=3))
        # Note: should only be one warning, not two
        self.assertEqual(cm.output, [
            'WARNING:aiokatcp.connection:Connection closed before message could be sent: '
            'Connection lost'])

    async def test_malformed(self):
        conn = Connection(self.owner, self.reader, self.writer, True)
        task = conn.start()
        self.addCleanup(conn.stop)
        self.remote_writer.write(b'malformed\n')
        self.remote_writer.write_eof()
        with self.assertLogs('aiokatcp.connection', logging.WARN) as cm:
            # Wait for the close to go through
            await asyncio.wait_for(task, timeout=2, loop=self.loop)
            self.owner.handle_message.assert_not_called()
        self.assertEqual(len(cm.output), 1)
        self.assertRegex(cm.output[0], 'Malformed message received.*')

    async def test_cancelled_early(self):
        conn = Connection(self.owner, self.reader, self.writer, True)
        task = conn.start()
        self.addCleanup(conn.stop)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

    async def test_cancelled(self):
        conn = Connection(self.owner, self.reader, self.writer, True)
        task = conn.start()
        self.addCleanup(conn.stop)
        await asynctest.exhaust_callbacks(self.loop)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

    async def test_exception(self):
        self.owner.handle_message.side_effect = RuntimeError('test error')
        conn = Connection(self.owner, self.reader, self.writer, True)
        task = conn.start()
        self.addCleanup(conn.stop)
        await asynctest.exhaust_callbacks(self.loop)
        self.remote_writer.write(b'?watchdog[2]\n')
        self.remote_writer.close()
        with self.assertLogs('aiokatcp.connection', logging.ERROR) as cm:
            await asyncio.wait_for(task, timeout=2, loop=self.loop)
        self.assertEqual(len(cm.output), 1)
        self.assertRegex(cm.output[0], '(?s)Exception in connection handler.*test error')
