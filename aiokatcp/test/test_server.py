import enum
import re
import asyncio
import ipaddress
import unittest.mock
from typing import Tuple, Iterable, List, Union, Pattern, SupportsBytes, cast

import asynctest

from aiokatcp.core import Message, Address
from aiokatcp.connection import FailReply
from aiokatcp.server import DeviceServer, RequestContext
from aiokatcp.sensor import Sensor
from aiokatcp.test.test_connection import timelimit


class Foo(enum.Enum):
    FIRST_VALUE = 1
    SECOND_VALUE = 2


class DummyServer(DeviceServer):
    VERSION = 'dummy-1.0'
    BUILD_STATE = 'dummy-build-1.0.0'

    def __init__(self):
        super().__init__('127.0.0.1', 0)
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

    async def request_sleep(self, ctx: RequestContext, time: float) -> None:
        """Sleep for some amount of time"""
        await asyncio.sleep(time, loop=self.loop)

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

        self.server = DummyServer()
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
            'increment-counter', 'echo', 'sleep', 'crash', 'show-msg',
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
        # TODO: should also get a #disconnect from the server
        await self._get_version_info()
        await self._write(b'?halt[11]\n')
        await self._check_reply([b'!halt[11] ok\n'])
        await self.server.join()

    async def test_too_few_params(self) -> None:
        await self._get_version_info()
        await self._write(b'?sleep\n')
        await self._check_reply([re.compile(br'^!sleep fail .*\n$')])

    async def test_too_many_params(self) -> None:
        await self._get_version_info()
        await self._write(b'?sleep 1 2\n')
        await self._check_reply([re.compile(br'^!sleep fail .*\n$')])

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
        await self._write(b'?sleep[1] 1000\n?halt[2]\n')
        await self._check_reply([
            b'!halt[2] ok\n',
            b'!sleep[1] fail request\\_cancelled\n'])
