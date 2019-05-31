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
import enum
import unittest
import unittest.mock
from unittest.mock import call
from typing import Tuple, Type, Pattern, Match, cast

from nose.tools import nottest, istest
import asynctest

from aiokatcp import (Client, FailReply, InvalidReply, ProtocolError, Message,
                      Sensor, SensorWatcher, AbstractSensorWatcher, SyncState, encode)
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

    async def check_received(self, data: bytes) -> None:
        line = await self.remote_reader.readline()
        self.assertEqual(line, data)

    async def check_received_regex(self, pattern: Pattern[bytes]) -> Match:
        line = await self.remote_reader.readline()
        self.assertRegex(line, pattern)
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
        await self.check_received(b'?echo[1]\n')
        await self.write(b'!echo[1] ok\n')
        result = await future
        self.assertEqual(result, ([], []))
        # Again, with arguments. This also tests MID incrementing, non-ASCII
        # characters, and null escaping.
        arg = b'h\xaf\xce\0'
        arg_esc = b'h\xaf\xce\\0'  # katcp escaping
        future = self.loop.create_task(self.client.request('echo', b'123', arg))
        await self.check_received(b'?echo[2] 123 ' + arg_esc + b'\n')
        await self.write(b'!echo[2] ok 123 ' + arg_esc + b'\n')
        result = await future
        self.assertEqual(result, ([b'123', arg], []))

    async def test_request_fail(self) -> None:
        await self.wait_connected()
        future = self.loop.create_task(self.client.request('failme'))
        await self.check_received(b'?failme[1]\n')
        await self.write(b'!failme[1] fail Error\\_message\n')
        with self.assertRaisesRegex(FailReply, '^Error message$'):
            await future

    async def test_request_fail_no_msg(self) -> None:
        await self.wait_connected()
        future = self.loop.create_task(self.client.request('failme'))
        await self.check_received(b'?failme[1]\n')
        await self.write(b'!failme[1] fail\n')
        with self.assertRaisesRegex(FailReply, '^$'):
            await future

    async def test_request_fail_msg_bad_encoding(self) -> None:
        await self.wait_connected()
        future = self.loop.create_task(self.client.request('failme'))
        await self.check_received(b'?failme[1]\n')
        await self.write(b'!failme[1] fail \xaf\n')
        with self.assertRaisesRegex(FailReply, '^\uFFFD$'):
            await future

    async def test_request_invalid(self) -> None:
        await self.wait_connected()
        future = self.loop.create_task(self.client.request('invalid-request'))
        await self.check_received(b'?invalid-request[1]\n')
        await self.write(b'!invalid-request[1] invalid Unknown\\_request\n')
        with self.assertRaisesRegex(InvalidReply, '^Unknown request$'):
            await future

    async def test_request_no_code(self) -> None:
        await self.wait_connected()
        future = self.loop.create_task(self.client.request('invalid-request'))
        await self.check_received(b'?invalid-request[1]\n')
        await self.write(b'!invalid-request[1]\n')
        with self.assertRaisesRegex(InvalidReply, '^$'):
            await future

    async def test_request_with_informs(self) -> None:
        await self.wait_connected()
        future = self.loop.create_task(self.client.request('help'))
        await self.check_received(b'?help[1]\n')
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

    async def test_inform_callback(self) -> None:
        def callback(string: str, integer: int) -> None:
            values.put_nowait((string, integer))

        values = asyncio.Queue(loop=self.loop)    # type: asyncio.Queue[Tuple[str, int]]
        client = cast(DummyClient, self.client)
        client.add_inform_callback('bar', callback)
        await self.wait_connected()
        await self.write(b'#bar hello 42\n')
        value = await values.get()
        self.assertEqual(value, ('hello', 42))
        client.remove_inform_callback('bar', callback)
        self.assertEqual(client._inform_callbacks, {})

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
class TestSensorMonitor(BaseTestClientAsync):
    """Test the sensor monitoring interface.

    This mocks out the :class:`~.AbstractSensorWatcher`.
    """

    @timelimit(1)
    async def setUp(self) -> None:
        self.server, self.client_queue = await self.make_server(self.loop)
        self.client, self.remote_reader, self.remote_writer = \
            await self.make_client(self.server, self.client_queue, auto_reconnect=True)
        self.addCleanup(self.remote_writer.close)
        self.watcher = unittest.mock.Mock(autospec=AbstractSensorWatcher)
        self.client.add_sensor_watcher(self.watcher)
        # Make it possible to wait for particular informs to reach the client
        # and get processed.
        self.wakeups = asyncio.Queue(loop=self.loop)  # type: asyncio.Queue[None]
        self.client.add_inform_callback('wakeup',
                                        lambda *args: self.wakeups.put_nowait(None))

    async def wakeup(self) -> None:
        """Wait for #wakeup sent from server to be received"""
        await self.wakeups.get()
        await asynctest.exhaust_callbacks(self.loop)

    async def connect(self) -> None:
        """Get as far as the monitor issuing ``?sensor-list``"""
        await self.wait_connected()
        self.watcher.state_updated.assert_called_with(SyncState.SYNCING)
        self.watcher.reset_mock()
        await self.check_received(b'?sensor-list[1]\n')

    async def sensor_list(self) -> None:
        """Send the sensor list and wait for ``?sensor-sampling``"""
        await self.write(
            b'#sensor-list[1] device-status Device\\_status \\@ discrete ok degraded fail\n'
            b'!sensor-list[1] ok 1\n')
        await self.check_received(b'?sensor-sampling[2] device-status auto\n')
        self.assertEqual(self.watcher.mock_calls, [
            call.batch_start(),
            call.sensor_added('device-status', 'Device status', '', 'discrete',
                              b'ok', b'degraded', b'fail'),
            call.batch_stop()
        ])
        self.watcher.reset_mock()

    async def sensor_sampling(self) -> None:
        """Reply to ``?sensor-sampling``"""
        await self.write(
            b'#sensor-status 123456789.0 1 device-status nominal ok\n'
            b'!sensor-sampling[2] ok device-status auto\n'
            b'#wakeup\n')
        await self.wakeup()
        self.assertEqual(self.watcher.mock_calls, [
            call.batch_start(),
            call.sensor_updated('device-status', b'ok', Sensor.Status.NOMINAL,
                                123456789.0),
            call.batch_stop(),
            call.state_updated(SyncState.SYNCED)
        ])
        self.watcher.reset_mock()

    async def test_init(self) -> None:
        await self.connect()
        await self.sensor_list()
        await self.sensor_sampling()

    async def interface_changed(self):
        """Send a ``#interface-changed`` inform and wait for ``?sensor-list``"""
        await self.write(b'#interface-changed sensor-list\n#wakeup\n')
        await self.wakeup()
        self.watcher.state_updated.assert_called_with(SyncState.SYNCING)
        self.watcher.reset_mock()
        await self.check_received(b'?sensor-list[3]\n')

    async def test_add_remove_sensors(self):
        await self.test_init()
        await self.interface_changed()
        await self.write(
            b'#sensor-list[3] temp Temperature F float\n'
            b'!sensor-list[3] ok 1\n')
        await self.check_received(b'?sensor-sampling[4] temp auto\n')
        self.assertEqual(self.watcher.mock_calls, [
            call.batch_start(),
            call.sensor_added('temp', 'Temperature', 'F', 'float'),
            call.sensor_removed('device-status'),
            call.batch_stop()
        ])
        self.watcher.reset_mock()

        await self.write(
            b'#sensor-status 123456790.0 1 temp warn 451.0\n'
            b'!sensor-sampling[4] ok temp auto\n'
            b'#wakeup\n')
        await self.wakeup()
        self.assertEqual(self.watcher.mock_calls, [
            call.batch_start(),
            call.sensor_updated('temp', b'451.0', Sensor.Status.WARN, 123456790.0),
            call.batch_stop(),
            call.state_updated(SyncState.SYNCED)
        ])
        self.watcher.reset_mock()

    async def test_replace_sensor(self):
        """Sensor has the same name but different parameters"""
        await self.test_init()
        await self.interface_changed()
        await self.write(
            b'#sensor-list[3] device-status A\\_different\\_status \\@ int\n'
            b'!sensor-list[3] ok 1\n')
        await self.check_received(b'?sensor-sampling[4] device-status auto\n')
        self.assertEqual(self.watcher.mock_calls, [
            call.batch_start(),
            call.sensor_added('device-status', 'A different status', '', 'int'),
            call.batch_stop()
        ])
        self.watcher.reset_mock()

        await self.write(
            b'#sensor-status 123456791.0 1 device-status nominal 123\n'
            b'!sensor-sampling[4] ok device-status auto\n'
            b'#wakeup\n')
        await self.wakeup()
        self.assertEqual(self.watcher.mock_calls, [
            call.batch_start(),
            call.sensor_updated('device-status', b'123', Sensor.Status.NOMINAL, 123456791.0),
            call.batch_stop(),
            call.state_updated(SyncState.SYNCED)
        ])
        self.watcher.reset_mock()

    async def test_sensor_vanished(self):
        """Sensor vanishes immediately after sensor-list reply."""
        await self.connect()
        await self.sensor_list()
        await self.write(
            b'#interface-changed sensor-list\n'
            b"!sensor-sampling[2] fail Unknown\\_sensor\\_'device-status'\n")
        await self.check_received(b'?sensor-list[3]\n')
        await self.write(b'!sensor-list[3] ok 0\n#wakeup\n')
        await self.wakeup()
        self.assertEqual(self.watcher.mock_calls, [
            call.state_updated(SyncState.SYNCING),
            call.batch_start(),
            call.sensor_removed('device-status'),
            call.batch_stop(),
            call.state_updated(SyncState.SYNCED)
        ])
        self.watcher.reset_mock()

    async def test_sensor_vanished2(self):
        """Sensor vanishes immediately after sensor-list reply (second case).

        This is similar to :meth:`test_sensor_vanished`, but the inform arrives
        only after the failure in ``?sensor-sampling``.
        """
        await self.connect()
        await self.sensor_list()
        await self.write(
            b"!sensor-sampling[2] fail Unknown\\_sensor\\_'device-status'\n"
            b'#wakeup\n')
        # Wait until the update task finishes before sending interface-changed
        await self.wakeup()
        await self.write(b'#interface-changed sensor-list\n')
        await self.check_received(b'?sensor-list[3]\n')
        await self.write(b'!sensor-list[3] ok 0\n#wakeup\n')
        await self.wakeup()
        self.assertEqual(self.watcher.mock_calls, [
            call.state_updated(SyncState.SYNCED),
            call.state_updated(SyncState.SYNCING),
            call.batch_start(),
            call.sensor_removed('device-status'),
            call.batch_stop(),
            call.state_updated(SyncState.SYNCED)
        ])
        self.watcher.reset_mock()

    async def test_remove_sensor_watcher(self):
        """Removing the last watcher unsubscribes"""
        await self.test_init()
        self.client.remove_sensor_watcher(self.watcher)
        await self.check_received(b'?sensor-sampling[3] device-status none\n')
        await self.write(b'!sensor-sampling[3] ok device-status none\n')

    async def test_close(self):
        """Closing the client must update the state"""
        await self.test_init()
        self.client.close()
        await self.client.wait_closed()
        self.assertEqual(self.watcher.mock_calls, [
            call.state_updated(SyncState.CLOSED)
        ])

    async def test_disconnect(self):
        """When the connection drops, the state must change appropriately"""
        await self.test_init()
        await self.write(b'#disconnect Testing\n')
        (self.remote_reader, self.remote_writer) = await self.client_queue.get()

        await self.wait_connected()
        self.watcher.state_updated.assert_called_with(SyncState.SYNCING)
        self.watcher.reset_mock()
        await self.check_received(b'?sensor-list[3]\n')
        await self.write(
            b'#sensor-list[3] device-status Device\\_status \\@ discrete ok degraded fail\n'
            b'!sensor-list[3] ok 1\n')
        await self.check_received(b'?sensor-sampling[4] device-status auto\n')
        await self.write(
            b'#sensor-status 123456789.0 1 device-status nominal ok\n'
            b'!sensor-sampling[4] ok device-status auto\n'
            b'#wakeup\n')
        await self.wakeup()
        self.assertEqual(self.watcher.mock_calls, [
            call.batch_start(),
            # No sensor_added because the sensor was already known
            call.batch_stop(),
            call.batch_start(),
            call.sensor_updated('device-status', b'ok', Sensor.Status.NOMINAL, 123456789.0),
            call.batch_stop(),
            call.state_updated(SyncState.SYNCED)
        ])
        self.watcher.reset_mock()


class DummySensorWatcher(SensorWatcher):
    def rewrite_name(self, name: str) -> str:
        return 'test_' + name


class DummyEnum(enum.Enum):
    THING_ONE = 1
    THING_TWO = 2


class TestSensorWatcher(asynctest.TestCase):
    """Test :class:`~.SensorWatcher`."""
    def setUp(self):
        client = unittest.mock.MagicMock()
        client.loop = self.loop
        self.watcher = DummySensorWatcher(client, enum_types=[DummyEnum])

    def test_construct(self):
        self.assertEqual(len(self.watcher.sensors), 0)
        self.assertFalse(self.watcher.synced.is_set())

    def test_sensor_added(self):
        self.watcher.batch_start()
        self.watcher.sensor_added('foo', 'A sensor', 'F', 'float')
        self.watcher.batch_stop()
        self.assertEqual(len(self.watcher.sensors), 1)
        sensor = self.watcher.sensors['test_foo']
        self.assertEqual(sensor.name, 'test_foo')
        self.assertEqual(sensor.description, 'A sensor')
        self.assertEqual(sensor.units, 'F')
        self.assertEqual(sensor.stype, float)
        self.assertEqual(sensor.status, Sensor.Status.UNKNOWN)

    def test_sensor_added_discrete(self):
        self.watcher.batch_start()
        self.watcher.sensor_added('disc', 'Discrete sensor', '', 'discrete', b'abc', b'def-xyz')
        self.watcher.sensor_added('disc2', 'Discrete sensor 2', '', 'discrete', b'abc', b'def-xyz')
        self.watcher.batch_stop()
        self.assertEqual(len(self.watcher.sensors), 2)
        sensor = self.watcher.sensors['test_disc']
        self.assertEqual(sensor.name, 'test_disc')
        self.assertEqual(sensor.description, 'Discrete sensor')
        self.assertEqual(sensor.units, '')
        self.assertEqual(sensor.type_name, 'discrete')
        self.assertEqual(sensor.status, Sensor.Status.UNKNOWN)
        members = [encode(member) for member in sensor.stype.__members__.values()]
        self.assertEqual(members, [b'abc', b'def-xyz'])
        self.assertIs(self.watcher.sensors['test_disc'].stype,
                      self.watcher.sensors['test_disc2'].stype,
                      'Enum cache did not work')

    def test_sensor_added_known_discrete(self):
        self.watcher.batch_start()
        self.watcher.sensor_added('disc', 'Discrete sensor', '', 'discrete',
                                  b'thing-one', b'thing-two')
        self.watcher.batch_stop()
        self.assertEqual(len(self.watcher.sensors), 1)
        sensor = self.watcher.sensors['test_disc']
        self.assertEqual(sensor.name, 'test_disc')
        self.assertEqual(sensor.description, 'Discrete sensor')
        self.assertEqual(sensor.units, '')
        self.assertEqual(sensor.type_name, 'discrete')
        self.assertIs(sensor.stype, DummyEnum)
        self.assertEqual(sensor.status, Sensor.Status.UNKNOWN)

    def test_sensor_added_bad_type(self):
        self.watcher.batch_start()
        self.watcher.sensor_added('foo', 'A sensor', 'F', 'blah')
        self.watcher.batch_stop()
        self.assertEqual(len(self.watcher.sensors), 0)
        self.watcher.logger.warning.assert_called_once_with(
            'Type %s is not recognised, skipping sensor %s', 'blah', 'foo')

    def test_sensor_removed(self):
        self.test_sensor_added()

        self.watcher.batch_start()
        self.watcher.sensor_removed('foo')
        self.watcher.batch_stop()
        self.assertEqual(len(self.watcher.sensors), 0)

    def test_sensor_updated(self):
        self.test_sensor_added()

        self.watcher.batch_start()
        self.watcher.sensor_updated('foo', b'12.5', Sensor.Status.WARN, 1234567890.0)
        self.watcher.batch_stop()
        sensor = self.watcher.sensors['test_foo']
        self.assertEqual(sensor.value, 12.5)
        self.assertEqual(sensor.status, Sensor.Status.WARN)
        self.assertEqual(sensor.timestamp, 1234567890.0)

    def test_sensor_updated_bad_value(self):
        self.test_sensor_added()

        self.watcher.batch_start()
        self.watcher.sensor_updated('foo', b'not a float', Sensor.Status.WARN, 1234567890.0)
        self.watcher.batch_stop()
        self.watcher.logger.warning.assert_called_once_with(
            'Sensor %s: value %r does not match type %s: %s',
            'foo', b'not a float', 'float', unittest.mock.ANY)

    def test_sensor_updated_unknown_sensor(self):
        self.test_sensor_added()

        self.watcher.batch_start()
        self.watcher.sensor_updated('bar', b'123.0', Sensor.Status.WARN, 1234567890.0)
        self.watcher.batch_stop()
        self.watcher.logger.warning.assert_called_once_with(
            'Received update for unknown sensor %s', 'bar')

    def test_state_updated(self):
        self.test_sensor_added()

        self.watcher.state_updated(SyncState.SYNCING)
        self.assertFalse(self.watcher.synced.is_set())
        self.assertEqual(len(self.watcher.sensors), 1)
        self.assertEqual(self.watcher.sensors['test_foo'].status, Sensor.Status.UNKNOWN)

        self.watcher.state_updated(SyncState.SYNCED)
        self.assertTrue(self.watcher.synced.is_set())
        self.assertEqual(len(self.watcher.sensors), 1)
        self.assertEqual(self.watcher.sensors['test_foo'].status, Sensor.Status.UNKNOWN)

        self.watcher.state_updated(SyncState.DISCONNECTED)
        self.assertFalse(self.watcher.synced.is_set())
        # Disconnecting should set all sensors to UNREACHABLE
        self.assertEqual(self.watcher.sensors['test_foo'].status, Sensor.Status.UNREACHABLE)


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
        await self.check_received(b'?echo\n')
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
            match = await self.check_received_regex(re.compile(br'^\?echo (1|2)\n\Z'))
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
