import enum
import re
import asyncio
import ipaddress
import unittest
import unittest.mock
from typing import Tuple, Iterable, Union, Pattern, SupportsBytes, cast
from typing import List   # noqa: F401

import asynctest

from aiokatcp.core import Message, Address
from aiokatcp.server import DeviceServer, RequestContext
from aiokatcp.sensor import Sensor
from aiokatcp.test.test_connection import timelimit


class Foo(enum.Enum):
    FIRST_VALUE = 1
    SECOND_VALUE = 2


class DummyServer(DeviceServer):
    VERSION = 'dummy-1.0'
    BUILD_STATE = 'dummy-build-1.0.0'

    def __init__(self, *, loop=None):
        super().__init__('127.0.0.1', 0, loop=loop)
        self.event = asyncio.Event(loop=self.loop)
        sensor = Sensor(int, 'counter-queries', 'number of ?counter queries',
                        default=0,
                        initial_status=Sensor.Status.NOMINAL)
        self.add_sensor(sensor)
        self._counter = sensor
        sensor = Sensor(Foo, 'foo', 'nonsense')
        self.add_sensor(sensor)

    async def request_increment_counter(self, ctx: RequestContext) -> None:
        """Increment a counter"""
        self._counter.set_value(self._counter.value + 1)

    async def request_echo(self, ctx: RequestContext, *args: str) -> Tuple:
        """Return the arguments to the caller"""
        return tuple(args)

    async def request_double(self, ctx: RequestContext, number: float) -> float:
        """Take a float and double it"""
        return 2 * number

    async def request_wait(self, ctx: RequestContext) -> None:
        """Wait for an internal event to fire"""
        await self.event.wait()

    async def request_crash(self, ctx: RequestContext) -> None:
        """Request that always raises an exception"""
        raise RuntimeError("help I fell over")

    async def request_show_msg(self, ctx: RequestContext, *, msg: Message) -> int:
        """Request that reports its message ID"""
        return msg.mid or 0


@timelimit
class TestDeviceServer(asynctest.TestCase):
    async def _stop_server(self) -> None:
        await self.server.stop()

    async def setUp(self) -> None:
        patcher = unittest.mock.patch('time.time', return_value=123456789.0)
        patcher.start()
        self.addCleanup(patcher.stop)

        self.server = DummyServer(loop=self.loop)
        await self.server.start()
        self.addCleanup(self._stop_server)
        host, port = self.server.server.sockets[0].getsockname()    # type: ignore
        self.remote_reader, self.remote_writer = await asyncio.open_connection(
            host, port, loop=self.loop)
        self.addCleanup(self.remote_writer.close)

    async def _readline(self) -> bytes:
        return await self.remote_reader.readline()

    async def _write(self, data: bytes) -> None:
        self.remote_writer.write(data)
        await self.remote_writer.drain()

    async def _check_reply(self, lines: Iterable[Union[bytes, Pattern[bytes]]]) -> None:
        for expected in lines:
            actual = await self._readline()
            if isinstance(expected, bytes):
                self.assertEqual(actual, expected)
            else:
                self.assertRegex(actual, expected)

    async def _get_version_info(self, prefix: bytes = b'#version-connect') -> None:
        await self._check_reply([
            prefix + b' katcp-protocol 5.0-MI\n',
            prefix + b' katcp-library aiokatcp-0.1 aiokatcp-0.1\n',
            prefix + b' katcp-device dummy-1.0 dummy-build-1.0.0\n'])

    async def test_help(self) -> None:
        await self._get_version_info()
        await self._write(b'?help[1]\n')
        commands = sorted([
            'increment-counter', 'echo', 'double', 'wait', 'crash', 'show-msg',
            'client-list', 'sensor-list', 'sensor-value',
            'halt', 'help', 'watchdog', 'version-list'
        ])
        expected = [re.compile(br'^#help\[1\] ' + cmd.encode('ascii') + b' [^ ]+$\n')
                    for cmd in commands]   # type: List[Union[bytes, Pattern[bytes]]]
        expected.append(b'!help[1] ok %d\n' % (len(commands),))
        await self._check_reply(expected)

    async def test_help_command(self) -> None:
        await self._get_version_info()
        await self._write(b'?help[1] increment-counter\n')
        await self._check_reply([
            br'#help[1] increment-counter Increment\_a\_counter' + b'\n',
            b'!help[1] ok 1\n'])

    async def test_help_bad_command(self) -> None:
        await self._get_version_info()
        await self._write(b'?help[1] increment_counter\n')  # note: _ instead of -
        await self._check_reply([
            br'!help[1] fail request\_increment_counter\_is\_not\_known' + b'\n'])

    async def test_watchdog(self) -> None:
        await self._get_version_info()
        await self._write(b'?watchdog[2]\n')
        await self._check_reply([b'!watchdog[2] ok\n'])

    async def test_client_list(self) -> None:
        await self._get_version_info()
        await self._write(b'?client-list[3]\n')
        host, port = self.remote_writer.get_extra_info('sockname')
        client_addr = Address(ipaddress.ip_address(host), port)
        await self._check_reply([
            b'#client-list[3] ' + bytes(cast(SupportsBytes, client_addr)) + b'\n',
            b'!client-list[3] ok 1\n'])

    async def test_sensor_list_no_filter(self) -> None:
        await self._get_version_info()
        await self._write(b'?sensor-list[4]\n')
        await self._check_reply([
            br'#sensor-list[4] counter-queries number\_of\_?counter\_queries \@ integer' + b'\n',
            br'#sensor-list[4] foo nonsense \@ discrete first-value second-value' + b'\n',
            b'!sensor-list[4] ok 2\n'])

    async def test_sensor_list_simple_filter(self) -> None:
        await self._get_version_info()
        await self._write(b'?sensor-list[5] foo\n')
        await self._check_reply([
            br'#sensor-list[5] foo nonsense \@ discrete first-value second-value' + b'\n',
            b'!sensor-list[5] ok 1\n'])

    async def test_sensor_list_simple_no_match(self) -> None:
        await self._get_version_info()
        await self._write(b'?sensor-list[5] bar\n')
        await self._check_reply([
            br"!sensor-list[5] fail unknown\_sensor\_'bar'" + b'\n'])

    async def test_sensor_list_regex_filter(self) -> None:
        await self._get_version_info()
        await self._write(b'?sensor-list[6] /[a-z-]+/\n')
        await self._check_reply([
            br'#sensor-list[6] counter-queries number\_of\_?counter\_queries \@ integer' + b'\n',
            br'#sensor-list[6] foo nonsense \@ discrete first-value second-value' + b'\n',
            b'!sensor-list[6] ok 2\n'])

    async def test_sensor_list_regex_filter_empty(self) -> None:
        await self._get_version_info()
        await self._write(b'?sensor-list[7] /unmatched/\n')
        await self._check_reply([
            b'!sensor-list[7] ok 0\n'])

    async def test_sensor_list_regex_bad_regex(self) -> None:
        await self._get_version_info()
        await self._write(b'?sensor-list[8] /(/\n')
        await self._check_reply([
            re.compile(br'^!sensor-list\[8\] fail .+\n$')])

    async def test_sensor_value(self) -> None:
        await self._get_version_info()
        await self._write(b'?sensor-value[9]\n')
        await self._check_reply([
            b'#sensor-value[9] 123456789.0 1 counter-queries nominal 0\n',
            b'#sensor-value[9] 123456789.0 1 foo unknown first-value\n',
            b'!sensor-value[9] ok 2\n'])

    async def test_sensor_value_filter(self) -> None:
        # The other filter tests are omitted since they're covered by the
        # sensor-list tests.
        await self._get_version_info()
        await self._write(b'?sensor-value[9] counter-queries\n')
        await self._check_reply([
            b'#sensor-value[9] 123456789.0 1 counter-queries nominal 0\n',
            b'!sensor-value[9] ok 1\n'])

    async def test_version_list(self) -> None:
        await self._get_version_info()
        await self._write(b'?version-list[10]\n')
        await self._get_version_info(prefix=b'#version-list[10]')
        await self._check_reply([b'!version-list[10] ok 3\n'])

    async def test_halt(self) -> None:
        await self._get_version_info()
        await self._write(b'?halt[11]\n')
        await self._check_reply([
            b'!halt[11] ok\n',
            br'#disconnect server\_shutting\_down' + b'\n',
            b''])   # Empty string indicates EOF
        await self.server.join()

    async def test_too_few_params(self) -> None:
        await self._get_version_info()
        await self._write(b'?double\n')
        await self._check_reply([re.compile(br'^!double fail .*\n$')])

    async def test_too_many_params(self) -> None:
        await self._get_version_info()
        await self._write(b'?double 1 2\n')
        await self._check_reply([re.compile(br'^!double fail .*\n$')])

    async def test_unknown_request(self) -> None:
        await self._get_version_info()
        await self._write(b'?cheese[1]\n')
        await self._check_reply([
            re.compile(br'^!cheese\[1\] invalid unknown\\_request\\_cheese\n$')])

    async def test_crash_request(self) -> None:
        await self._get_version_info()
        await self._write(b'?crash\n')
        await self._check_reply([
            re.compile(br'^!crash fail .*help\\_I\\_fell\\_over.*$')])

    async def test_halt_while_waiting(self) -> None:
        await self._get_version_info()
        await self._write(b'?wait[1]\n?halt[2]\n')
        await self._check_reply([
            b'!halt[2] ok\n',
            b'!wait[1] fail request\\_cancelled\n',
            b'#disconnect server\\_shutting\\_down\n',
            b''])    # Empty line indicates EOF

    async def test_variadic(self) -> None:
        """Test a request that takes a *args."""
        await self._get_version_info()
        await self._write(b'?echo hello world\n')
        await self._check_reply([b'!echo ok hello world\n'])

    async def test_msg_keyword(self) -> None:
        """Test a request that takes a `msg` keyword-only argument."""
        await self._get_version_info()
        await self._write(b'?show-msg[123]\n')
        await self._check_reply([b'!show-msg[123] ok 123\n'])

    async def test_bad_arg_type(self) -> None:
        await self._get_version_info()
        await self._write(b'?double bad\n')
        await self._check_reply([
            br"!double fail could\_not\_convert\_string\_to\_float:\_'bad'" + b'\n'])

    async def test_concurrent(self) -> None:
        await self._get_version_info()
        await self._write(b'?wait[1]\n?echo[2] test\n')
        await self._check_reply([b'!echo[2] ok test\n'])
        self.server.event.set()
        await self._check_reply([b'!wait[1] ok\n'])


class TestDeviceServerMeta(unittest.TestCase):
    """Test that the metaclass picks up invalid constructions"""
    def test_missing_help(self) -> None:
        with self.assertRaises(TypeError):
            class BadServer(DummyServer):
                def request_no_help(self):
                    pass

    def test_too_few_parameters(self) -> None:
        with self.assertRaises(TypeError):
            class BadServer(DummyServer):
                def request_too_few(self):
                    """Not enough parameters"""

    def test_missing_version(self) -> None:
        with self.assertRaises(TypeError) as cm:
            class BadServer(DeviceServer):
                BUILD_INFO = 'build-info'
            BadServer('127.0.0.1', 0)
        self.assertEqual(str(cm.exception), 'BadServer does not define VERSION')

    def test_missing_build_state(self) -> None:
        with self.assertRaises(TypeError) as cm:
            class BadServer(DeviceServer):
                VERSION = 'version'
            BadServer('127.0.0.1', 0)
        self.assertEqual(str(cm.exception), 'BadServer does not define BUILD_STATE')
