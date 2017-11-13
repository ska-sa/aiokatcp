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

import enum
import re
import asyncio
import ipaddress
import unittest
import unittest.mock
import logging
from typing import Tuple, Iterable, Union, Pattern, SupportsBytes, Type, cast
from typing import List   # noqa: F401

import asynctest

import aiokatcp
from aiokatcp.core import Message, Address
from aiokatcp.connection import FailReply
from aiokatcp.server import DeviceServer, RequestContext, SensorSet
from aiokatcp.sensor import Sensor
from aiokatcp.test.test_connection import timelimit


class Foo(enum.Enum):
    FIRST_VALUE = 1
    SECOND_VALUE = 2


class TestSensorSet(unittest.TestCase):
    def setUp(self):
        self.conn = unittest.mock.MagicMock()
        self.connections = set([self.conn])
        self.ss = SensorSet(self.connections)
        self.sensors = [Sensor(int, 'name{}'.format(i)) for i in range(5)]
        # A different set of sensors with the same names
        self.alt_sensors = [Sensor(float, 'name{}'.format(i)) for i in range(5)]
        self.ss.add(self.sensors[0])

    def _assert_sensors(self, ss, sensors):
        """Assert that `ss` has the same sensors as `sensors`"""
        ordered = sorted(ss.values(), key=lambda x: x.name)
        self.assertEqual(ordered, sensors)

    def test_construct(self):
        """Test that setUp put things into the right state."""
        self._assert_sensors(self.ss, [self.sensors[0]])

    def test_add(self):
        # Add a new one
        self.ss.add(self.sensors[1])
        self._assert_sensors(self.ss, [self.sensors[0], self.sensors[1]])
        # Add the same one
        self.ss.add(self.sensors[0])
        self._assert_sensors(self.ss, [self.sensors[0], self.sensors[1]])
        # Replace one
        self.conn.set_sampler.assert_not_called()
        self.ss.add(self.alt_sensors[1])
        self._assert_sensors(self.ss, [self.sensors[0], self.alt_sensors[1]])
        self.conn.set_sampler.assert_called_once_with(self.sensors[1], None)

    def test_remove(self):
        # Try to remove non-existent name
        with self.assertRaises(KeyError):
            self.ss.remove(self.sensors[4])
        # Try to remove one with the same name as an existing one
        with self.assertRaises(KeyError):
            self.ss.remove(self.alt_sensors[0])
        self._assert_sensors(self.ss, [self.sensors[0]])
        # Remove one
        self.conn.set_sampler.assert_not_called()
        self.ss.remove(self.sensors[0])
        self._assert_sensors(self.ss, [])
        self.conn.set_sampler.assert_called_once_with(self.sensors[0], None)

    def test_discard(self):
        # Try to remove non-existent name
        self.ss.discard(self.sensors[4])
        # Try to remove one with the same name as an existing one
        self.ss.discard(self.alt_sensors[0])
        self._assert_sensors(self.ss, [self.sensors[0]])
        # Remove one
        self.conn.set_sampler.assert_not_called()
        self.ss.discard(self.sensors[0])
        self._assert_sensors(self.ss, [])
        self.conn.set_sampler.assert_called_once_with(self.sensors[0], None)

    def test_clear(self):
        self.ss.add(self.sensors[1])
        self.ss.clear()
        self._assert_sensors(self.ss, [])
        self.conn.set_sampler.assert_any_call(self.sensors[0], None)
        self.conn.set_sampler.assert_any_call(self.sensors[1], None)

    def test_popitem(self):
        self.ss.add(self.sensors[1])
        items = []
        try:
            for _ in range(100):   # To prevent infinite loop if it's broken
                items.append(self.ss.popitem())
        except KeyError:
            pass
        items.sort(key=lambda x: x[0])
        self.assertEqual(items, [('name0', self.sensors[0]), ('name1', self.sensors[1])])
        self.conn.set_sampler.assert_any_call(self.sensors[0], None)
        self.conn.set_sampler.assert_any_call(self.sensors[1], None)

    def test_pop_absent(self):
        # Non-existent name
        with self.assertRaises(KeyError):
            self.ss.pop('name4')
        # Non-existent with defaults
        self.assertIsNone(self.ss.pop('name4', None))
        self.assertEqual(self.ss.pop('name4', 'foo'), 'foo')
        # Remove one
        self.conn.set_sampler.assert_not_called()
        self.assertIs(self.ss.pop('name0'), self.sensors[0])
        self._assert_sensors(self.ss, [])
        self.conn.set_sampler.assert_called_once_with(self.sensors[0], None)

    def test_delitem(self):
        # Try to remove non-existent name
        with self.assertRaises(KeyError):
            del self.ss['name4']
        self._assert_sensors(self.ss, [self.sensors[0]])
        # Remove one
        self.conn.set_sampler.assert_not_called()
        del self.ss['name0']
        self._assert_sensors(self.ss, [])
        self.conn.set_sampler.assert_called_once_with(self.sensors[0], None)

    def test_getitem(self):
        # Non-existing name
        with self.assertRaises(KeyError):
            self.ss['name4']
        # Existing name
        self.assertIs(self.ss['name0'], self.sensors[0])

    def test_get(self):
        # Non-existing name
        self.assertIsNone(self.ss.get('name4'))
        self.assertIsNone(self.ss.get('name4', None))
        self.assertEqual(self.ss.get('name4', 'foo'), 'foo')
        # Existing name
        self.assertIs(self.ss.get('name0'), self.sensors[0])

    def test_len(self):
        self.assertEqual(len(self.ss), 1)
        self.ss.add(self.sensors[1])
        self.assertEqual(len(self.ss), 2)

    def test_contains(self):
        self.assertIn(self.sensors[0], self.ss)
        self.assertNotIn(self.alt_sensors[0], self.ss)
        self.assertNotIn(self.sensors[1], self.ss)

    def test_bool(self):
        self.assertTrue(self.ss)
        self.ss.clear()
        self.assertFalse(self.ss)

    def test_keys(self):
        self.ss.add(self.sensors[1])
        self.assertEqual(sorted(self.ss.keys()), ['name0', 'name1'])

    def test_values(self):
        self.ss.add(self.sensors[1])
        self.assertEqual(sorted(self.ss.values(), key=lambda x: x.name),
                         [self.sensors[0], self.sensors[1]])

    def test_items(self):
        self.ss.add(self.sensors[1])
        self.assertEqual(sorted(self.ss.items()), [
            ('name0', self.sensors[0]),
            ('name1', self.sensors[1])
        ])

    def test_iter(self):
        self.assertEqual(sorted(iter(self.ss)), ['name0'])

    def test_copy(self):
        self.assertEqual(self.ss.copy(), {'name0': self.sensors[0]})


class DummyServer(DeviceServer):
    VERSION = 'dummy-1.0'
    BUILD_STATE = 'dummy-build-1.0.0'

    def __init__(self, *, loop=None):
        super().__init__('127.0.0.1', 0, loop=loop)
        self.event = asyncio.Event(loop=self.loop)
        self.wait_reached = asyncio.Event(loop=self.loop)
        sensor = Sensor(int, 'counter-queries', 'number of ?counter queries',
                        default=0,
                        initial_status=Sensor.Status.NOMINAL)
        self.sensors.add(sensor)
        self._counter = sensor
        sensor = Sensor(Foo, 'foo', 'nonsense')
        self.sensors.add(sensor)
        sensor = Sensor(float, 'float-sensor', 'generic float sensor')
        self.sensors.add(sensor)

    async def request_increment_counter(self, ctx: RequestContext) -> None:
        """Increment a counter"""
        self._counter.value += 1

    async def request_echo(self, ctx: RequestContext, *args: str) -> Tuple:
        """Return the arguments to the caller"""
        return tuple(args)

    async def request_bytes_arg(self, ctx: RequestContext, some_bytes) -> bytes:
        """An argument with no annotation"""
        if not isinstance(some_bytes, bytes):
            raise FailReply('expected bytes, got {}'.format(type(some_bytes)))
        return some_bytes

    async def request_double(self, ctx: RequestContext, number: float, scale: float = 2) -> float:
        """Take a float and double it"""
        return scale * number

    async def request_wait(self, ctx: RequestContext) -> None:
        """Wait for an internal event to fire"""
        self.wait_reached.set()
        await self.event.wait()

    async def request_crash(self, ctx: RequestContext) -> None:
        """Request that always raises an exception"""
        raise RuntimeError("help I fell over")


class BadServer(DummyServer):
    """Server with some badly-behaved requests."""
    async def request_double_reply(self, ctx: RequestContext) -> None:
        """Tries to send a reply twice"""
        ctx.reply('ok', 'reply1')
        ctx.reply('ok', 'reply2')

    async def request_reply_return(self, ctx: RequestContext) -> int:
        """Sends an explicit reply and returns a value"""
        ctx.reply('ok', 'reply1')
        return 3

    async def request_inform_after_reply(self, ctx: RequestContext) -> None:
        """Call inform after reply"""
        ctx.reply('ok', 'reply1')
        ctx.inform('inform1')

    async def request_informs_after_reply(self, ctx: RequestContext) -> None:
        """Call informs after reply"""
        ctx.reply('ok', 'reply1')
        ctx.informs(['inform1'])


class DeviceServerTestMixin(asynctest.TestCase):
    """Mixin for device server tests.

    It creates a server and a reader and writer that form a client.
    """
    async def make_server(self, server_cls: Type[DummyServer]) -> DummyServer:
        server = server_cls(loop=self.loop)
        await server.start()
        self.addCleanup(server.stop)
        return server

    async def make_client(self, server) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        host, port = server.server.sockets[0].getsockname()    # type: ignore
        remote_reader, remote_writer = await asyncio.open_connection(
            host, port, loop=self.loop)
        self.addCleanup(remote_writer.close)
        return remote_reader, remote_writer

    async def setUp(self) -> None:
        self.server = await self.make_server(DummyServer)
        self.remote_reader, self.remote_writer = await self.make_client(self.server)

    async def _readline(self) -> bytes:
        return await self.remote_reader.readline()

    async def _write(self, data: bytes) -> None:
        self.remote_writer.write(data)
        await self.remote_writer.drain()

    async def _check_reply(self, lines: Iterable[Union[bytes, Pattern[bytes]]]) -> None:
        for expected in lines:
            actual = await self._readline()
            if isinstance(expected, bytes):
                self.assertEqual(ascii(actual), ascii(expected))
            else:
                self.assertRegex(actual, expected)

    async def get_version_info(self, prefix: bytes = b'#version-connect') -> None:
        await self._check_reply([
            prefix + b' katcp-protocol 5.0-MI\n',
            prefix + ' katcp-library aiokatcp-{} aiokatcp-{}\n'.format(
                aiokatcp.minor_version(), aiokatcp.__version__).encode('ascii'),
            prefix + b' katcp-device dummy-1.0 dummy-build-1.0.0\n'])


@timelimit
class TestDeviceServer(DeviceServerTestMixin, asynctest.TestCase):
    async def setUp(self) -> None:
        patcher = unittest.mock.patch('time.time', return_value=123456789.0)
        patcher.start()
        self.addCleanup(patcher.stop)
        await super().setUp()

    async def test_start_twice(self) -> None:
        """Calling start twice raises :exc:`RuntimeError`"""
        with self.assertRaises(RuntimeError):
            await self.server.start()

    async def test_help(self) -> None:
        await self.get_version_info()
        await self._write(b'?help[1]\n')
        commands = sorted([
            'increment-counter', 'echo', 'bytes-arg', 'double', 'wait', 'crash',
            'client-list', 'log-level', 'sensor-list', 'sensor-sampling',
            'sensor-value', 'halt', 'help', 'watchdog', 'version-list'
        ])
        expected = [re.compile(br'^#help\[1\] ' + cmd.encode('ascii') + b' [^ ]+$\n')
                    for cmd in commands]   # type: List[Union[bytes, Pattern[bytes]]]
        expected.append(b'!help[1] ok %d\n' % (len(commands),))
        await self._check_reply(expected)

    async def test_help_command(self) -> None:
        await self.get_version_info()
        await self._write(b'?help[1] increment-counter\n')
        await self._check_reply([
            br'#help[1] increment-counter Increment\_a\_counter' + b'\n',
            b'!help[1] ok 1\n'])

    async def test_help_bad_command(self) -> None:
        await self.get_version_info()
        await self._write(b'?help[1] increment_counter\n')  # note: _ instead of -
        await self._check_reply([
            br'!help[1] fail request\_increment_counter\_is\_not\_known' + b'\n'])

    async def test_watchdog(self) -> None:
        await self.get_version_info()
        await self._write(b'?watchdog[2]\n')
        await self._check_reply([b'!watchdog[2] ok\n'])

    async def test_client_list(self) -> None:
        await self.get_version_info()
        await self._write(b'?client-list[3]\n')
        host, port = self.remote_writer.get_extra_info('sockname')
        client_addr = Address(ipaddress.ip_address(host), port)
        await self._check_reply([
            b'#client-list[3] ' + bytes(cast(SupportsBytes, client_addr)) + b'\n',
            b'!client-list[3] ok 1\n'])

    async def test_sensor_list_no_filter(self) -> None:
        await self.get_version_info()
        await self._write(b'?sensor-list[4]\n')
        await self._check_reply([
            br'#sensor-list[4] counter-queries number\_of\_?counter\_queries \@ integer' + b'\n',
            br'#sensor-list[4] float-sensor generic\_float\_sensor \@ float' + b'\n',
            br'#sensor-list[4] foo nonsense \@ discrete first-value second-value' + b'\n',
            b'!sensor-list[4] ok 3\n'])

    async def test_sensor_list_simple_filter(self) -> None:
        await self.get_version_info()
        await self._write(b'?sensor-list[5] foo\n')
        await self._check_reply([
            br'#sensor-list[5] foo nonsense \@ discrete first-value second-value' + b'\n',
            b'!sensor-list[5] ok 1\n'])

    async def test_sensor_list_simple_no_match(self) -> None:
        await self.get_version_info()
        await self._write(b'?sensor-list[5] bar\n')
        await self._check_reply([
            br"!sensor-list[5] fail unknown\_sensor\_'bar'" + b'\n'])

    async def test_sensor_list_regex_filter(self) -> None:
        await self.get_version_info()
        await self._write(b'?sensor-list[6] /[a-z-]+/\n')
        await self._check_reply([
            br'#sensor-list[6] counter-queries number\_of\_?counter\_queries \@ integer' + b'\n',
            br'#sensor-list[6] float-sensor generic\_float\_sensor \@ float' + b'\n',
            br'#sensor-list[6] foo nonsense \@ discrete first-value second-value' + b'\n',
            b'!sensor-list[6] ok 3\n'])

    async def test_sensor_list_regex_filter_empty(self) -> None:
        await self.get_version_info()
        await self._write(b'?sensor-list[7] /unmatched/\n')
        await self._check_reply([
            b'!sensor-list[7] ok 0\n'])

    async def test_sensor_list_regex_bad_regex(self) -> None:
        await self.get_version_info()
        await self._write(b'?sensor-list[8] /(/\n')
        await self._check_reply([
            re.compile(br'^!sensor-list\[8\] fail .+\n$')])

    async def test_sensor_value(self) -> None:
        await self.get_version_info()
        await self._write(b'?sensor-value[9]\n')
        await self._check_reply([
            b'#sensor-value[9] 123456789.0 1 counter-queries nominal 0\n',
            b'#sensor-value[9] 123456789.0 1 float-sensor unknown 0.0\n',
            b'#sensor-value[9] 123456789.0 1 foo unknown first-value\n',
            b'!sensor-value[9] ok 3\n'])

    async def test_sensor_value_filter(self) -> None:
        # The other filter tests are omitted since they're covered by the
        # sensor-list tests.
        await self.get_version_info()
        await self._write(b'?sensor-value[9] counter-queries\n')
        await self._check_reply([
            b'#sensor-value[9] 123456789.0 1 counter-queries nominal 0\n',
            b'!sensor-value[9] ok 1\n'])

    async def test_version_list(self) -> None:
        await self.get_version_info()
        await self._write(b'?version-list[10]\n')
        await self.get_version_info(prefix=b'#version-list[10]')
        await self._check_reply([b'!version-list[10] ok 3\n'])

    async def test_halt(self) -> None:
        await self.get_version_info()
        await self._write(b'?halt[11]\n')
        await self._check_reply([
            b'!halt[11] ok\n',
            br'#disconnect server\_shutting\_down' + b'\n',
            b''])   # Empty string indicates EOF
        await self.server.join()

    async def test_halt_while_waiting(self) -> None:
        await self.get_version_info()
        await self._write(b'?wait[1]\n?halt[2]\n')
        await self._check_reply([
            b'!halt[2] ok\n',
            b'!wait[1] fail request\\_cancelled\n',
            b'#disconnect server\\_shutting\\_down\n',
            b''])    # Empty line indicates EOF

    async def test_too_few_params(self) -> None:
        await self.get_version_info()
        await self._write(b'?double\n')
        await self._check_reply([re.compile(br'^!double fail .*\n$')])

    async def test_too_many_params(self) -> None:
        await self.get_version_info()
        await self._write(b'?double 1 2 3\n')
        await self._check_reply([re.compile(br'^!double fail .*\n$')])

    async def test_unknown_request(self) -> None:
        await self.get_version_info()
        await self._write(b'?cheese[1]\n')
        await self._check_reply([
            re.compile(br'^!cheese\[1\] invalid unknown\\_request\\_cheese\n$')])

    async def test_crash_request(self) -> None:
        await self.get_version_info()
        await self._write(b'?crash\n')
        await self._check_reply([
            re.compile(br'^!crash fail .*help\\_I\\_fell\\_over.*$')])

    async def test_variadic(self) -> None:
        """Test a request that takes a ``*args``."""
        await self.get_version_info()
        await self._write(b'?echo hello world\n')
        await self._check_reply([b'!echo ok hello world\n'])

    async def test_no_annotation(self) -> None:
        """Argument without annotation is passed through raw"""
        await self.get_version_info()
        await self._write(b'?bytes-arg raw\n')
        await self._check_reply([b'!bytes-arg ok raw\n'])

    async def test_bad_arg_type(self) -> None:
        await self.get_version_info()
        await self._write(b'?double bad\n')
        await self._check_reply([
            br"!double fail could\_not\_convert\_string\_to\_float:\_'bad'" + b'\n'])

    async def test_concurrent(self) -> None:
        await self.get_version_info()
        await self._write(b'?wait[1]\n?echo[2] test\n')
        await self._check_reply([b'!echo[2] ok test\n'])
        self.server.event.set()
        await self._check_reply([b'!wait[1] ok\n'])

    async def test_client_connected_inform(self) -> None:
        """A second client connecting sends ``#client-connected`` to the first"""
        await self.get_version_info()
        host, port = self.server.server.sockets[0].getsockname()    # type: ignore
        reader2, writer2 = await asyncio.open_connection(host, port, loop=self.loop)
        self.addCleanup(writer2.close)
        client_host, client_port = writer2.get_extra_info('sockname')
        client_addr = Address(ipaddress.ip_address(client_host), client_port)
        client_addr_bytes = bytes(cast(SupportsBytes, client_addr))
        await self._check_reply([b'#client-connected ' + client_addr_bytes + b'\n'])

    async def test_message_while_stopping(self) -> None:
        await self.get_version_info()
        await self._write(b'?wait\n')
        # Wait for the request_wait to be launched
        await self.server.wait_reached.wait()
        # Start stopping the server, but wait for outstanding tasks
        stop_task = self.loop.create_task(self.server.stop(cancel=False))
        await self._write(b'?watchdog\n')   # Should be ignored, because we're stopping
        # Give the ?watchdog time to make it through to the message handler
        await asyncio.sleep(0.1, loop=self.loop)
        self.server.event.set()   # Releases the ?wait
        await self._check_reply([
            b'!wait ok\n',
            b'#disconnect server\\_shutting\\_down\n',
            b''])
        await stop_task

    async def test_mass_inform(self) -> None:
        await self.get_version_info()
        self.server.mass_inform('test-inform', 123)
        await self._check_reply([b'#test-inform 123\n'])

    async def test_log_level(self) -> None:
        # We have to use a name that doesn't start with aiokatcp., because
        # those get filtered out.
        logger = logging.getLogger('_aiokatcp.test.test_logger')
        logger.propagate = False
        logger.level = logging.DEBUG
        handler = DummyServer.LogHandler(self.server)
        logger.addHandler(handler)
        self.addCleanup(logger.removeHandler, handler)
        await self.get_version_info()
        await self._write(b'?log-level\n')
        await self._check_reply([b'!log-level ok warn\n'])
        logger.info('foo')  # Should not be reported
        await self._write(b'?log-level info\n')
        await self._check_reply([b'!log-level ok info\n'])
        logger.info('bar')  # Should be reported
        await self._check_reply([
            re.compile(br'^#log info 123456789\.0 _aiokatcp.test.test_logger '
                       br'test_server\.py:\d+:\\_bar\n')])


class TestDeviceServerClocked(DeviceServerTestMixin, asynctest.ClockedTestCase):
    """Tests for :class:`.DeviceServer` that use a fake clock.

    These are largely to do with sensor sampling.
    """
    def _mock_time(self) -> float:
        return self.loop.time() - self._start_time + 123456789.0

    async def setUp(self) -> None:
        self._start_time = self.loop.time()
        patcher = unittest.mock.patch('time.time', new=self._mock_time)
        patcher.start()
        self.addCleanup(patcher.stop)
        await super().setUp()

    async def test_sensor_sampling_invalid(self) -> None:
        """Invalid strategy for ``?sensor-strategy``"""
        await self.get_version_info()
        await self._write(b'?sensor-sampling float-sensor carrot\n')
        await self._check_reply([
            br"!sensor-sampling fail b'carrot'\_is\_not\_a\_valid\_value\_for\_Strategy" + b'\n'])

    async def test_sensor_sampling_none_params(self) -> None:
        """None strategy must not accept parameters"""
        await self.get_version_info()
        await self._write(b'?sensor-sampling float-sensor none 4\n')
        await self._check_reply([
            br'!sensor-sampling fail expected\_0\_strategy\_arguments,\_found\_1' + b'\n'])

    async def test_sensor_sampling_too_few_params(self) -> None:
        await self.get_version_info()
        await self._write(b'?sensor-sampling float-sensor event-rate 4\n')
        await self._check_reply([
            br'!sensor-sampling fail expected\_2\_strategy\_arguments,\_found\_1' + b'\n'])

    async def test_sensor_sampling_too_many_params(self) -> None:
        await self.get_version_info()
        await self._write(b'?sensor-sampling float-sensor event-rate 4 5 6\n')
        await self._check_reply([
            br'!sensor-sampling fail expected\_2\_strategy\_arguments,\_found\_3' + b'\n'])

    async def test_sensor_sampling_bad_parameter(self) -> None:
        await self.get_version_info()
        await self._write(b'?sensor-sampling float-sensor period foo\n')
        await self._check_reply([
            br"!sensor-sampling fail could\_not\_convert\_string\_to\_float:\_'foo'" + b'\n'])

    async def test_sensor_sampling_auto(self) -> None:
        await self.get_version_info()
        await self._write(b'?sensor-sampling float-sensor auto\n')
        await self._check_reply([
            b'#sensor-status 123456789.0 1 float-sensor unknown 0.0\n',
            b'!sensor-sampling ok float-sensor auto\n'])
        # Set to a new value
        await self.advance(1.0)
        self.server.sensors['float-sensor'].value = 1.25
        # Set to the same value again - must still update
        await self.advance(1.0)
        self.server.sensors['float-sensor'].value = 1.25
        await self._check_reply([
            b'#sensor-status 123456790.0 1 float-sensor nominal 1.25\n',
            b'#sensor-status 123456791.0 1 float-sensor nominal 1.25\n'])

    async def test_sensor_sampling_period(self) -> None:
        await self.get_version_info()
        await self._write(b'?sensor-sampling float-sensor period 2.5\n')
        await self._check_reply([
            b'#sensor-status 123456789.0 1 float-sensor unknown 0.0\n',
            b'!sensor-sampling ok float-sensor period 2.5\n'])
        await self.advance(3.0)
        self.server.sensors['float-sensor'].value = 1.25
        await self.advance(5.0)
        await self._write(b'?watchdog\n')
        await self._check_reply([
            b'#sensor-status 123456789.0 1 float-sensor unknown 0.0\n',
            b'#sensor-status 123456792.0 1 float-sensor nominal 1.25\n',
            b'#sensor-status 123456792.0 1 float-sensor nominal 1.25\n',
            b'!watchdog ok\n'])

    async def test_sensor_sampling_period_zero(self) -> None:
        await self.get_version_info()
        await self._write(b'?sensor-sampling float-sensor period 0.0\n')
        await self._check_reply([
            br'!sensor-sampling fail period\_must\_be\_positive' + b'\n'])

    async def test_sensor_sampling_event(self) -> None:
        await self.get_version_info()
        await self._write(b'?sensor-sampling float-sensor event\n')
        await self._check_reply([
            b'#sensor-status 123456789.0 1 float-sensor unknown 0.0\n',
            b'!sensor-sampling ok float-sensor event\n'])
        # Set to a new value
        await self.advance(1.0)
        self.server.sensors['float-sensor'].value = 1.25
        # Set to the same value again - must not update
        await self.advance(1.0)
        self.server.sensors['float-sensor'].value = 1.25
        # Set to the same value, but change the status
        await self.advance(1.0)
        self.server.sensors['float-sensor'].set_value(1.25, status=Sensor.Status.WARN)
        await self._check_reply([
            b'#sensor-status 123456790.0 1 float-sensor nominal 1.25\n',
            b'#sensor-status 123456792.0 1 float-sensor warn 1.25\n'])

    async def test_sensor_sampling_differential_bad_type(self) -> None:
        await self.get_version_info()
        await self._write(b'?sensor-sampling foo differential 1\n')
        await self._check_reply([
            br'!sensor-sampling fail differential\_strategies\_only\_valid\_for\_integer'
            br'\_and\_float\_sensors' + b'\n'])

    async def test_sensor_sampling_differential(self) -> None:
        await self.get_version_info()
        await self._write(b'?sensor-sampling float-sensor differential 1.5\n')
        await self._check_reply([
            b'#sensor-status 123456789.0 1 float-sensor unknown 0.0\n',
            b'!sensor-sampling ok float-sensor differential 1.5\n'])
        # Set to a valid value, less than the difference away (but a status change)
        await self.advance(1.0)
        self.server.sensors['float-sensor'].value = 1.25
        # Set to the same value
        await self.advance(1.0)
        self.server.sensors['float-sensor'].value = 1.25
        # Set to a new value, within the delta
        await self.advance(1.0)
        self.server.sensors['float-sensor'].value = 2.5
        # Set to a new value, within the delta of the last update
        await self.advance(1.0)
        self.server.sensors['float-sensor'].value = 0.2
        # Set to a new value, outside the delta
        await self.advance(1.0)
        self.server.sensors['float-sensor'].value = -1.0
        await self._check_reply([
            b'#sensor-status 123456790.0 1 float-sensor nominal 1.25\n',
            b'#sensor-status 123456794.0 1 float-sensor nominal -1.0\n'])

    async def test_sensor_sampling_event_rate(self) -> None:
        await self.get_version_info()
        await self._write(b'?sensor-sampling float-sensor event-rate 1.0 10.0\n')
        await self._check_reply([
            b'#sensor-status 123456789.0 1 float-sensor unknown 0.0\n',
            b'!sensor-sampling ok float-sensor event-rate 1.0 10.0\n'])
        # Set to a valid value, after the short period has passed
        await self.advance(1.0)
        self.server.sensors['float-sensor'].value = 1.25
        # Change the twice value, within the short period
        await self.advance(0.25)
        self.server.sensors['float-sensor'].value = 1.0
        await self.advance(0.25)
        self.server.sensors['float-sensor'].value = 0.5
        # Leave it alone until just before the long period triggers
        await self.advance(10.25)
        # Inject a message directly into the server, so that its reply can
        # be reliably sequenced relative to the other informs.
        conn = next(iter(self.server._connections))
        self.server.handle_message(conn, Message.request('watchdog'))
        # Wait for the long period to trigger
        await self.advance(0.5)

        await self._check_reply([
            b'#sensor-status 123456790.0 1 float-sensor nominal 1.25\n',
            b'#sensor-status 123456790.5 1 float-sensor nominal 0.5\n',
            b'!watchdog ok\n',
            b'#sensor-status 123456790.5 1 float-sensor nominal 0.5\n'])

    async def test_sensor_sampling_differential_rate(self) -> None:
        await self.get_version_info()
        await self._write(b'?sensor-sampling float-sensor differential-rate 1.5 1.0 10.0\n')
        await self._check_reply([
            b'#sensor-status 123456789.0 1 float-sensor unknown 0.0\n',
            b'!sensor-sampling ok float-sensor differential-rate 1.5 1.0 10.0\n'])
        # Set to a valid value, after the short period has passed
        await self.advance(1.0)
        self.server.sensors['float-sensor'].value = 1.25
        # Change the twice value, within the short period
        await self.advance(0.25)
        self.server.sensors['float-sensor'].value = 5.0
        await self.advance(0.25)
        self.server.sensors['float-sensor'].value = 0.5
        # Let it trigger, then change it, but not enough to trigger
        await self.advance(4.0)
        self.server.sensors['float-sensor'].value = 1.5
        # Leave it alone until just before the long period triggers
        await self.advance(6.25)
        # Inject a message directly into the server, so that its reply can
        # be reliably sequenced relative to the other informs.
        conn = next(iter(self.server._connections))
        self.server.handle_message(conn, Message.request('watchdog'))
        # Wait for the long period to trigger
        await self.advance(0.5)

        await self._check_reply([
            b'#sensor-status 123456790.0 1 float-sensor nominal 1.25\n',
            b'#sensor-status 123456790.5 1 float-sensor nominal 0.5\n',
            b'!watchdog ok\n',
            b'#sensor-status 123456794.5 1 float-sensor nominal 1.5\n'])

    async def test_sensor_sampling_none(self) -> None:
        await self.get_version_info()
        await self._write(b'?sensor-sampling float-sensor event\n')
        await self._check_reply([
            b'#sensor-status 123456789.0 1 float-sensor unknown 0.0\n',
            b'!sensor-sampling ok float-sensor event\n'])
        await self._write(b'?sensor-sampling float-sensor none\n')
        await self._check_reply([
            b'!sensor-sampling ok float-sensor none\n'])
        self.server.sensors['float-sensor'].value = 1.0
        # Must not report the update
        await self._write(b'?watchdog\n')
        await self._check_reply([b'!watchdog ok\n'])

    async def test_sensor_sampling_query(self) -> None:
        await self.get_version_info()
        await self._write(b'?sensor-sampling float-sensor event\n'
                          b'?sensor-sampling float-sensor\n')
        await self._check_reply([
            b'#sensor-status 123456789.0 1 float-sensor unknown 0.0\n',
            b'!sensor-sampling ok float-sensor event\n',
            b'!sensor-sampling ok float-sensor event\n'])

    async def test_sensor_sampling_unknown_sensor(self) -> None:
        await self.get_version_info()
        await self._write(b'?sensor-sampling bad-sensor event\n')
        await self._check_reply([
            br"!sensor-sampling fail unknown\_sensor\_'bad-sensor'" + b'\n'])


@timelimit
class TestBadDeviceServer(DeviceServerTestMixin, asynctest.TestCase):
    async def setUp(self) -> None:
        patcher = unittest.mock.patch('time.time', return_value=123456789.0)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.server = await self.make_server(BadServer)
        self.remote_reader, self.remote_writer = await self.make_client(self.server)

    async def test_double_reply(self) -> None:
        await self.get_version_info()
        await self._write(b'?double-reply\n')
        await self._check_reply([
            b'!double-reply ok reply1\n',
            re.compile(br'^#log error [0-9.]+ aiokatcp\.server Traceback')])

    async def test_reply_return(self) -> None:
        await self.get_version_info()
        await self._write(b'?reply-return\n')
        await self._check_reply([
            b'!reply-return ok reply1\n',
            re.compile(br'^#log error [0-9.]+ aiokatcp\.server Traceback')])

    async def test_inform_after_reply(self) -> None:
        await self.get_version_info()
        await self._write(b'?inform-after-reply\n')
        await self._check_reply([
            b'!inform-after-reply ok reply1\n',
            re.compile(br'^#log error [0-9.]+ aiokatcp\.server Traceback')])

    async def test_informs_after_reply(self) -> None:
        await self.get_version_info()
        await self._write(b'?informs-after-reply\n')
        await self._check_reply([
            b'!informs-after-reply ok reply1\n',
            re.compile(br'^#log error [0-9.]+ aiokatcp\.server Traceback')])


class TestDeviceServerMeta(unittest.TestCase):
    """Test that the metaclass picks up invalid constructions"""
    def test_missing_help(self) -> None:
        with self.assertRaises(TypeError):
            class MyBadServer(DummyServer):
                def request_no_help(self):
                    pass

    def test_too_few_parameters(self) -> None:
        with self.assertRaises(TypeError):
            class MyBadServer(DummyServer):
                def request_too_few(self):
                    """Not enough parameters"""

    def test_missing_version(self) -> None:
        with self.assertRaises(TypeError) as cm:
            class MyBadServer(DeviceServer):
                BUILD_INFO = 'build-info'
            MyBadServer('127.0.0.1', 0)
        self.assertEqual(str(cm.exception), 'MyBadServer does not define VERSION')

    def test_missing_build_state(self) -> None:
        with self.assertRaises(TypeError) as cm:
            class MyBadServer(DeviceServer):
                VERSION = 'version'
            MyBadServer('127.0.0.1', 0)
        self.assertEqual(str(cm.exception), 'MyBadServer does not define BUILD_STATE')
