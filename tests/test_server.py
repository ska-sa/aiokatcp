# Copyright 2017, 2019-2020, 2022 National Research Foundation (SARAO)
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
import enum
import ipaddress
import logging
import re
from typing import AsyncGenerator, Awaitable, Callable, Iterable, List, Pattern, Tuple, Union

import async_solipsism
import pytest

import aiokatcp
from aiokatcp.connection import FailReply
from aiokatcp.core import Address
from aiokatcp.sensor import Sensor, SensorSampler
from aiokatcp.server import DeviceServer, RequestContext

_StreamPair = Tuple[asyncio.StreamReader, asyncio.StreamWriter]


@pytest.fixture
def event_loop():
    loop = async_solipsism.EventLoop()
    yield loop
    loop.close()


class Foo(enum.Enum):
    FIRST_VALUE = 1
    SECOND_VALUE = 2


class DummyServer(DeviceServer):
    VERSION = "dummy-1.0"
    BUILD_STATE = "dummy-build-1.0.0"

    def __init__(self):
        super().__init__("::1", 7777)
        self.event = asyncio.Event()
        self.wait_reached = asyncio.Event()
        sensor = Sensor(
            int,
            "counter-queries",
            "number of ?counter queries",
            default=0,
            initial_status=Sensor.Status.NOMINAL,
        )
        self.sensors.add(sensor)
        self._counter = sensor
        sensor = Sensor(Foo, "foo", "nonsense")
        self.sensors.add(sensor)
        sensor = Sensor(float, "float-sensor", "generic float sensor")
        self.sensors.add(sensor)
        sensor = Sensor(
            int,
            "auto-override",
            "overrides the auto strategy",
            auto_strategy=SensorSampler.Strategy.PERIOD,
            auto_strategy_parameters=(2.5,),
        )
        self.sensors.add(sensor)
        self.on_stop_called = 0
        self.crash_on_stop = False  # Set to true to raise in on_stop

    async def request_increment_counter(self, ctx: RequestContext) -> None:
        """Increment a counter"""
        self._counter.value += 1

    async def request_echo(self, ctx: RequestContext, *args: str) -> Tuple:
        """Return the arguments to the caller"""
        return tuple(args)

    async def request_bytes_arg(self, ctx: RequestContext, some_bytes) -> bytes:
        """An argument with no annotation"""
        if not isinstance(some_bytes, bytes):
            raise FailReply(f"expected bytes, got {type(some_bytes)}")
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

    async def request_add_bulk_sensors(self, ctx: RequestContext) -> None:
        """Add some sensors for a bulk sensor sampling test."""
        for i in range(100):
            self.sensors.add(
                Sensor(
                    int,
                    f"bulk{i}",
                    "bulk test sensor",
                    default=123,
                    initial_status=Sensor.Status.NOMINAL,
                )
            )

    async def request_remove_bulk_sensors(self, ctx: RequestContext) -> None:
        """Remove the sensors added by ?add-bulk-sensors."""
        # Adjust number of sleeps as necessary for the
        # test_sensor_sampling_bulk_remove_midway test.
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        for i in range(100):
            name = f"bulk{i}"
            self.sensors.pop(name, None)

    async def on_stop(self) -> None:
        self.on_stop_called += 1
        if self.crash_on_stop:
            raise RuntimeError("crash_on_stop is true")


class BadServer(DummyServer):
    """Server with some badly-behaved requests."""

    async def request_double_reply(self, ctx: RequestContext) -> None:
        """Tries to send a reply twice"""
        ctx.reply("ok", "reply1")
        ctx.reply("ok", "reply2")

    async def request_reply_return(self, ctx: RequestContext) -> int:
        """Sends an explicit reply and returns a value"""
        ctx.reply("ok", "reply1")
        return 3

    async def request_inform_after_reply(self, ctx: RequestContext) -> None:
        """Call inform after reply"""
        ctx.reply("ok", "reply1")
        ctx.inform("inform1")

    async def request_informs_after_reply(self, ctx: RequestContext) -> None:
        """Call informs after reply"""
        ctx.reply("ok", "reply1")
        ctx.informs(["inform1"])


@pytest.fixture
async def server(request) -> AsyncGenerator[DummyServer, None]:
    marker = request.node.get_closest_marker("server_cls")
    server_cls = marker.args[0] if marker is not None else DummyServer
    server = server_cls()
    await server.start()
    yield server
    await server.stop()


@pytest.fixture
async def reader_writer_factory(
    server: DummyServer,
) -> AsyncGenerator[Callable[[], Awaitable[_StreamPair]], None]:
    host, port = server.sockets[0].getsockname()[:2]
    writers = []

    async def factory() -> _StreamPair:
        reader, writer = await asyncio.open_connection(host, port)
        writers.append(writer)
        await get_version_info(reader)
        return reader, writer

    yield factory
    for writer in writers:
        writer.close()
    for writer in writers:
        try:
            await writer.wait_closed()
        except ConnectionError:
            pass


@pytest.fixture
async def reader_writer(reader_writer_factory: Callable[[], Awaitable[_StreamPair]]) -> _StreamPair:
    return await reader_writer_factory()


@pytest.fixture
def reader(reader_writer: _StreamPair) -> asyncio.StreamReader:
    return reader_writer[0]


@pytest.fixture
def writer(reader_writer: _StreamPair) -> asyncio.StreamWriter:
    return reader_writer[1]


async def check_reply(reader, lines: Iterable[Union[bytes, Pattern[bytes]]]) -> None:
    for expected in lines:
        actual = await reader.readline()
        if isinstance(expected, bytes):
            assert ascii(actual) == ascii(expected)
        else:
            assert expected.search(actual)


async def get_version_info(
    reader: asyncio.StreamReader, prefix: bytes = b"#version-connect"
) -> None:
    await check_reply(
        reader,
        [
            prefix + b" katcp-protocol 5.1-MIB\n",
            prefix
            + " katcp-library aiokatcp-{} aiokatcp-{}\n".format(
                aiokatcp.minor_version(), aiokatcp.__version__
            ).encode("ascii"),
            prefix + b" katcp-device dummy-1.0 dummy-build-1.0.0\n",
        ],
    )


@pytest.fixture
def mock_time(mocker, event_loop) -> None:
    mocker.patch("time.time", lambda: event_loop.time() + 123456789.0)


async def test_start_twice(server: DummyServer) -> None:
    """Calling start twice raises :exc:`RuntimeError`"""
    with pytest.raises(RuntimeError):
        await server.start()


async def test_sockets_not_started() -> None:
    """An unstarted server must return an empty socket tuple."""
    assert DummyServer().sockets == ()


async def test_carriage_return(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    writer.write(b"?watchdog[2]\r")
    assert await reader.readline() == b"!watchdog[2] ok\n"


async def test_help(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    writer.write(b"?help[1]\n")
    commands = sorted(
        [
            "increment-counter",
            "echo",
            "bytes-arg",
            "double",
            "wait",
            "crash",
            "add-bulk-sensors",
            "remove-bulk-sensors",
            "client-list",
            "log-level",
            "sensor-list",
            "sensor-sampling",
            "sensor-value",
            "halt",
            "help",
            "watchdog",
            "version-list",
        ]
    )
    expected: List[Union[bytes, Pattern[bytes]]] = [
        re.compile(rb"^#help\[1\] " + cmd.encode("ascii") + b" [^ ]+$\n") for cmd in commands
    ]
    expected.append(b"!help[1] ok %d\n" % (len(commands),))
    await check_reply(reader, expected)


async def test_help_command(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    writer.write(b"?help[1] increment-counter\n")
    await check_reply(
        reader,
        [
            rb"#help[1] increment-counter Increment\_a\_counter" + b"\n",
            b"!help[1] ok 1\n",
        ],
    )


async def test_help_bad_command(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    writer.write(b"?help[1] increment_counter\n")  # note: _ instead of -
    line = await reader.readline()
    assert line == rb"!help[1] fail request\_increment_counter\_is\_not\_known" + b"\n"


async def test_watchdog(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    writer.write(b"?watchdog[2]\n")
    assert await reader.readline() == b"!watchdog[2] ok\n"


async def test_client_list(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    writer.write(b"?client-list[3]\n")
    host, port = writer.get_extra_info("sockname")[:2]
    client_addr = Address(ipaddress.ip_address(host), port)
    await check_reply(
        reader,
        [b"#client-list[3] " + bytes(client_addr) + b"\n", b"!client-list[3] ok 1\n"],
    )


async def test_sensor_list_no_filter(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    writer.write(b"?sensor-list[4]\n")
    await check_reply(
        reader,
        [
            rb"#sensor-list[4] auto-override overrides\_the\_auto\_strategy \@ integer" + b"\n",
            rb"#sensor-list[4] counter-queries number\_of\_?counter\_queries \@ integer" + b"\n",
            rb"#sensor-list[4] float-sensor generic\_float\_sensor \@ float" + b"\n",
            rb"#sensor-list[4] foo nonsense \@ discrete first-value second-value" + b"\n",
            b"!sensor-list[4] ok 4\n",
        ],
    )


async def test_sensor_list_simple_filter(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    writer.write(b"?sensor-list[5] foo\n")
    await check_reply(
        reader,
        [
            rb"#sensor-list[5] foo nonsense \@ discrete first-value second-value" + b"\n",
            b"!sensor-list[5] ok 1\n",
        ],
    )


async def test_sensor_list_simple_no_match(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    writer.write(b"?sensor-list[5] bar\n")
    assert await reader.readline() == rb"!sensor-list[5] fail Unknown\_sensor\_'bar'" + b"\n"


async def test_sensor_list_regex_filter(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    writer.write(b"?sensor-list[6] /^[b-z][a-z-]+/\n")
    await check_reply(
        reader,
        [
            rb"#sensor-list[6] counter-queries number\_of\_?counter\_queries \@ integer" + b"\n",
            rb"#sensor-list[6] float-sensor generic\_float\_sensor \@ float" + b"\n",
            rb"#sensor-list[6] foo nonsense \@ discrete first-value second-value" + b"\n",
            b"!sensor-list[6] ok 3\n",
        ],
    )


async def test_sensor_list_regex_filter_empty(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    writer.write(b"?sensor-list[7] /unmatched/\n")
    assert await reader.readline() == b"!sensor-list[7] ok 0\n"


async def test_sensor_list_regex_bad_regex(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    writer.write(b"?sensor-list[8] /(/\n")
    await check_reply(reader, [re.compile(rb"^!sensor-list\[8\] fail .+\n$")])


async def test_sensor_value(
    mock_time, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    writer.write(b"?sensor-value[9]\n")
    await check_reply(
        reader,
        [
            b"#sensor-value[9] 123456789.0 1 auto-override unknown 0\n",
            b"#sensor-value[9] 123456789.0 1 counter-queries nominal 0\n",
            b"#sensor-value[9] 123456789.0 1 float-sensor unknown 0.0\n",
            b"#sensor-value[9] 123456789.0 1 foo unknown first-value\n",
            b"!sensor-value[9] ok 4\n",
        ],
    )


async def test_sensor_value_filter(
    mock_time, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    # The other filter tests are omitted since they're covered by the
    # sensor-list tests.
    writer.write(b"?sensor-value[9] counter-queries\n")
    await check_reply(
        reader,
        [
            b"#sensor-value[9] 123456789.0 1 counter-queries nominal 0\n",
            b"!sensor-value[9] ok 1\n",
        ],
    )


async def test_version_list(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    writer.write(b"?version-list[10]\n")
    await get_version_info(reader, prefix=b"#version-list[10]")
    assert await reader.readline() == b"!version-list[10] ok 3\n"


async def test_halt(
    server: DummyServer, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    writer.write(b"?halt[11]\n")
    await check_reply(
        reader,
        [
            b"!halt[11] ok\n",
            rb"#disconnect server\_shutting\_down" + b"\n",
            b"",  # Empty string indicates EOF
        ],
    )
    await server.join()


async def test_halt_while_waiting(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    writer.write(b"?wait[1]\n?halt[2]\n")
    await check_reply(
        reader,
        [
            b"!halt[2] ok\n",
            b"!wait[1] fail request\\_cancelled\n",
            b"#disconnect server\\_shutting\\_down\n",
            b"",  # Empty line indicates EOF
        ],
    )


async def test_too_few_params(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    writer.write(b"?double\n")
    await check_reply(reader, [re.compile(rb"^!double fail .*\n$")])


async def test_too_many_params(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    writer.write(b"?double 1 2 3\n")
    await check_reply(reader, [re.compile(rb"^!double fail .*\n$")])


async def test_unknown_request(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    writer.write(b"?cheese[1]\n")
    await check_reply(reader, [re.compile(rb"^!cheese\[1\] invalid unknown\\_request\\_cheese\n$")])


async def test_crash_request(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    writer.write(b"?crash\n")
    await check_reply(reader, [re.compile(rb"^!crash fail .*help\\_I\\_fell\\_over.*$")])


async def test_variadic(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    """Test a request that takes a ``*args``."""
    writer.write(b"?echo hello world\n")
    assert await reader.readline() == b"!echo ok hello world\n"


async def test_no_annotation(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    """Argument without annotation is passed through raw"""
    writer.write(b"?bytes-arg raw\n")
    assert await reader.readline() == b"!bytes-arg ok raw\n"


async def test_bad_arg_type(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    writer.write(b"?double bad\n")
    line = await reader.readline()
    assert line == rb"!double fail could\_not\_convert\_string\_to\_float:\_'bad'" + b"\n"


async def test_concurrent(
    server: DummyServer, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    writer.write(b"?wait[1]\n?echo[2] test\n")
    assert await reader.readline() == b"!echo[2] ok test\n"
    server.event.set()
    assert await reader.readline() == b"!wait[1] ok\n"


async def test_client_connected_inform(
    reader_writer_factory: Callable[[], Awaitable[_StreamPair]],
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    """A second client connecting sends ``#client-connected`` to the first"""
    reader2, writer2 = await reader_writer_factory()
    client_host, client_port = writer2.get_extra_info("sockname")[:2]
    client_addr = Address(ipaddress.ip_address(client_host), client_port)
    client_addr_bytes = bytes(client_addr)
    assert await reader.readline() == b"#client-connected " + client_addr_bytes + b"\n"


async def test_message_while_stopping(
    event_loop: asyncio.AbstractEventLoop,
    server: DummyServer,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    writer.write(b"?wait\n")
    # Wait for the request_wait to be launched
    await server.wait_reached.wait()
    # Start stopping the server, but wait for outstanding tasks
    stop_task = event_loop.create_task(server.stop(cancel=False))
    writer.write(b"?watchdog\n")  # Should be ignored, because we're stopping
    # Ensure the ?watchdog makes it through to the message handler
    await asyncio.sleep(1)
    server.event.set()  # Releases the ?wait
    await check_reply(reader, [b"!wait ok\n", b"#disconnect server\\_shutting\\_down\n", b""])
    await stop_task


async def test_mass_inform(server: DummyServer, reader: asyncio.StreamReader) -> None:
    server.mass_inform("test-inform", 123)
    assert await reader.readline() == b"#test-inform 123\n"


async def test_log_level(
    server: DummyServer,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    mock_time,
) -> None:
    # We have to use a name that doesn't start with aiokatcp., because
    # those get filtered out.
    logger = logging.getLogger("_aiokatcp.test.test_logger")
    logger.propagate = False
    logger.level = logging.DEBUG
    handler = DummyServer.LogHandler(server)
    logger.addHandler(handler)
    try:
        writer.write(b"?log-level\n")
        assert await reader.readline() == b"!log-level ok warn\n"
        logger.info("foo")  # Should not be reported
        writer.write(b"?log-level info\n")
        assert await reader.readline() == b"!log-level ok info\n"
        logger.info("bar")  # Should be reported
        await check_reply(
            reader,
            [
                re.compile(
                    rb"^#log info 123456789\.0 _aiokatcp.test.test_logger "
                    rb"test_server\.py:\d+:\\_bar\n"
                )
            ],
        )
    finally:
        logger.removeHandler(handler)


async def test_on_stop(server: DummyServer) -> None:
    assert server.on_stop_called == 0
    server.halt()
    await server.join()
    assert server.on_stop_called == 1
    # on_stop should not be called when already stopped
    await server.stop()
    assert server.on_stop_called == 1


async def test_on_stop_raises() -> None:
    server = DummyServer()
    server.crash_on_stop = True
    await server.start()
    with pytest.raises(RuntimeError, match="crash_on_stop"):
        await server.stop()


async def test_service_task(server: DummyServer) -> None:
    intervals = 0

    async def service_task():
        nonlocal intervals
        while True:
            await asyncio.sleep(1)
            intervals += 1

    task = asyncio.create_task(service_task())
    server.add_service_task(task)
    await asyncio.sleep(3.5)
    await server.stop()
    assert intervals == 3
    assert task.done()


async def test_service_task_exception() -> None:
    async def service_task():
        raise RuntimeError("boom")

    server = DummyServer()
    task = asyncio.create_task(service_task())
    server.add_service_task(task)
    await server.start()
    with pytest.raises(RuntimeError, match="boom"):
        await server.join()


async def test_service_task_early_exit(server: DummyServer) -> None:
    async def service_task():
        await asyncio.sleep(1)

    task = asyncio.create_task(service_task())
    server.add_service_task(task)
    assert task in server.service_tasks
    await asyncio.sleep(2)
    # Should now have finished and been removed
    assert task not in server.service_tasks


async def test_slow_client(server: DummyServer, reader: asyncio.StreamReader) -> None:
    server.max_backlog = 32768
    big_str = b"x" * 200000
    assert len(server._connections) == 1
    server.mass_inform("big", big_str)
    assert len(server._connections) == 0


async def test_sensor_sampling_invalid(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    """Invalid strategy for ``?sensor-strategy``"""
    writer.write(b"?sensor-sampling float-sensor carrot\n")
    assert (
        await reader.readline()
        == rb"!sensor-sampling fail b'carrot'\_is\_not\_a\_valid\_value\_for\_Strategy" + b"\n"
    )


async def test_sensor_sampling_none_params(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    """None strategy must not accept parameters"""
    writer.write(b"?sensor-sampling float-sensor none 4\n")
    line = await reader.readline()
    assert line == rb"!sensor-sampling fail expected\_0\_strategy\_arguments,\_found\_1" + b"\n"


async def test_sensor_sampling_too_few_params(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    writer.write(b"?sensor-sampling float-sensor event-rate 4\n")
    line = await reader.readline()
    assert line == rb"!sensor-sampling fail expected\_2\_strategy\_arguments,\_found\_1" + b"\n"


async def test_sensor_sampling_too_many_params(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    writer.write(b"?sensor-sampling float-sensor event-rate 4 5 6\n")
    line = await reader.readline()
    assert line == rb"!sensor-sampling fail expected\_2\_strategy\_arguments,\_found\_3" + b"\n"


async def test_sensor_sampling_bad_parameter(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    writer.write(b"?sensor-sampling float-sensor period foo\n")
    line = await reader.readline()
    assert line == rb"!sensor-sampling fail could\_not\_convert\_string\_to\_float:\_'foo'" + b"\n"


async def test_sensor_sampling_auto(
    mock_time,
    server: DummyServer,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    writer.write(b"?sensor-sampling float-sensor auto\n")
    await check_reply(
        reader,
        [
            b"#sensor-status 123456789.0 1 float-sensor unknown 0.0\n",
            b"!sensor-sampling ok float-sensor auto\n",
        ],
    )
    # Set to a new value
    await asyncio.sleep(1.0)
    server.sensors["float-sensor"].value = 1.25
    # Set to the same value again - must still update
    await asyncio.sleep(1.0)
    server.sensors["float-sensor"].value = 1.25
    await check_reply(
        reader,
        [
            b"#sensor-status 123456790.0 1 float-sensor nominal 1.25\n",
            b"#sensor-status 123456791.0 1 float-sensor nominal 1.25\n",
        ],
    )


async def test_sensor_sampling_period(
    mock_time,
    server: DummyServer,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    writer.write(b"?sensor-sampling float-sensor period 2.5\n")
    await check_reply(
        reader,
        [
            b"#sensor-status 123456789.0 1 float-sensor unknown 0.0\n",
            b"!sensor-sampling ok float-sensor period 2.5\n",
        ],
    )
    await asyncio.sleep(3.0)
    server.sensors["float-sensor"].value = 1.25
    await asyncio.sleep(5.0)
    writer.write(b"?watchdog\n")
    await check_reply(
        reader,
        [
            b"#sensor-status 123456789.0 1 float-sensor unknown 0.0\n",
            b"#sensor-status 123456792.0 1 float-sensor nominal 1.25\n",
            b"#sensor-status 123456792.0 1 float-sensor nominal 1.25\n",
            b"!watchdog ok\n",
        ],
    )


async def test_sensor_sampling_period_zero(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    writer.write(b"?sensor-sampling float-sensor period 0.0\n")
    assert await reader.readline() == rb"!sensor-sampling fail period\_must\_be\_positive" + b"\n"


async def test_sensor_sampling_event(
    mock_time,
    server: DummyServer,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    writer.write(b"?sensor-sampling float-sensor event\n")
    await check_reply(
        reader,
        [
            b"#sensor-status 123456789.0 1 float-sensor unknown 0.0\n",
            b"!sensor-sampling ok float-sensor event\n",
        ],
    )
    # Set to a new value
    await asyncio.sleep(1.0)
    server.sensors["float-sensor"].value = 1.25
    # Set to the same value again - must not update
    await asyncio.sleep(1.0)
    server.sensors["float-sensor"].value = 1.25
    # Set to the same value, but change the status
    await asyncio.sleep(1.0)
    server.sensors["float-sensor"].set_value(1.25, status=Sensor.Status.WARN)
    await check_reply(
        reader,
        [
            b"#sensor-status 123456790.0 1 float-sensor nominal 1.25\n",
            b"#sensor-status 123456792.0 1 float-sensor warn 1.25\n",
        ],
    )


async def test_sensor_sampling_differential_bad_type(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    writer.write(b"?sensor-sampling foo differential 1\n")
    await check_reply(
        reader,
        [
            rb"!sensor-sampling fail differential\_strategies\_only\_valid\_for\_integer"
            rb"\_and\_float\_sensors" + b"\n"
        ],
    )


async def test_sensor_sampling_differential(
    mock_time,
    server: DummyServer,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    writer.write(b"?sensor-sampling float-sensor differential 1.5\n")
    await check_reply(
        reader,
        [
            b"#sensor-status 123456789.0 1 float-sensor unknown 0.0\n",
            b"!sensor-sampling ok float-sensor differential 1.5\n",
        ],
    )
    # Set to a valid value, less than the difference away (but a status change)
    await asyncio.sleep(1.0)
    server.sensors["float-sensor"].value = 1.25
    # Set to the same value
    await asyncio.sleep(1.0)
    server.sensors["float-sensor"].value = 1.25
    # Set to a new value, within the delta
    await asyncio.sleep(1.0)
    server.sensors["float-sensor"].value = 2.5
    # Set to a new value, within the delta of the last update
    await asyncio.sleep(1.0)
    server.sensors["float-sensor"].value = 0.2
    # Set to a new value, outside the delta
    await asyncio.sleep(1.0)
    server.sensors["float-sensor"].value = -1.0
    await check_reply(
        reader,
        [
            b"#sensor-status 123456790.0 1 float-sensor nominal 1.25\n",
            b"#sensor-status 123456794.0 1 float-sensor nominal -1.0\n",
        ],
    )


async def test_sensor_sampling_event_rate(
    mock_time,
    server: DummyServer,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    writer.write(b"?sensor-sampling float-sensor event-rate 1.0 10.0\n")
    await check_reply(
        reader,
        [
            b"#sensor-status 123456789.0 1 float-sensor unknown 0.0\n",
            b"!sensor-sampling ok float-sensor event-rate 1.0 10.0\n",
        ],
    )
    # Set to a valid value, after the short period has passed
    await asyncio.sleep(1.0)
    server.sensors["float-sensor"].value = 1.25
    # Change the twice value, within the short period
    await asyncio.sleep(0.25)
    server.sensors["float-sensor"].value = 1.0
    await asyncio.sleep(0.25)
    server.sensors["float-sensor"].value = 0.5
    # Leave it alone until just before the long period triggers
    await asyncio.sleep(10.25)
    writer.write(b"?watchdog\n")
    # Wait for the long period to trigger
    await asyncio.sleep(0.5)

    await check_reply(
        reader,
        [
            b"#sensor-status 123456790.0 1 float-sensor nominal 1.25\n",
            b"#sensor-status 123456790.5 1 float-sensor nominal 0.5\n",
            b"!watchdog ok\n",
            b"#sensor-status 123456790.5 1 float-sensor nominal 0.5\n",
        ],
    )


async def test_sensor_sampling_differential_rate(
    mock_time,
    server: DummyServer,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    writer.write(b"?sensor-sampling float-sensor differential-rate 1.5 1.0 10.0\n")
    await check_reply(
        reader,
        [
            b"#sensor-status 123456789.0 1 float-sensor unknown 0.0\n",
            b"!sensor-sampling ok float-sensor differential-rate 1.5 1.0 10.0\n",
        ],
    )
    # Set to a valid value, after the short period has passed
    await asyncio.sleep(1.0)
    server.sensors["float-sensor"].value = 1.25
    # Change the twice value, within the short period
    await asyncio.sleep(0.25)
    server.sensors["float-sensor"].value = 5.0
    await asyncio.sleep(0.25)
    server.sensors["float-sensor"].value = 0.5
    # Let it trigger, then change it, but not enough to trigger
    await asyncio.sleep(4.0)
    server.sensors["float-sensor"].value = 1.5
    # Leave it alone until just before the long period triggers
    await asyncio.sleep(6.25)
    writer.write(b"?watchdog\n")
    # Wait for the long period to trigger
    await asyncio.sleep(0.5)

    await check_reply(
        reader,
        [
            b"#sensor-status 123456790.0 1 float-sensor nominal 1.25\n",
            b"#sensor-status 123456790.5 1 float-sensor nominal 0.5\n",
            b"!watchdog ok\n",
            b"#sensor-status 123456794.5 1 float-sensor nominal 1.5\n",
        ],
    )


async def test_sensor_sampling_none(
    mock_time,
    server: DummyServer,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    writer.write(b"?sensor-sampling float-sensor event\n")
    await check_reply(
        reader,
        [
            b"#sensor-status 123456789.0 1 float-sensor unknown 0.0\n",
            b"!sensor-sampling ok float-sensor event\n",
        ],
    )
    writer.write(b"?sensor-sampling float-sensor none\n")
    assert await reader.readline() == b"!sensor-sampling ok float-sensor none\n"
    server.sensors["float-sensor"].value = 1.0
    # Must not report the update
    writer.write(b"?watchdog\n")
    assert await reader.readline() == b"!watchdog ok\n"


async def test_sensor_sampling_query(
    mock_time, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    writer.write(b"?sensor-sampling float-sensor event\n" b"?sensor-sampling float-sensor\n")
    await check_reply(
        reader,
        [
            b"#sensor-status 123456789.0 1 float-sensor unknown 0.0\n",
            b"!sensor-sampling ok float-sensor event\n",
            b"!sensor-sampling ok float-sensor event\n",
        ],
    )


async def test_sensor_sampling_unknown_sensor(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    writer.write(b"?sensor-sampling bad-sensor event\n")
    line = await reader.readline()
    assert line == rb"!sensor-sampling fail Unknown\_sensor\_'bad-sensor'" + b"\n"


async def test_sensor_sampling_auto_override(
    mock_time,
    server: DummyServer,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    writer.write(b"?sensor-sampling auto-override auto\n")
    await check_reply(
        reader,
        [
            b"#sensor-status 123456789.0 1 auto-override unknown 0\n",
            b"!sensor-sampling ok auto-override auto\n",
        ],
    )
    await asyncio.sleep(3.0)
    server.sensors["auto-override"].value = 1
    await asyncio.sleep(5.0)
    writer.write(b"?watchdog\n")
    await check_reply(
        reader,
        [
            b"#sensor-status 123456789.0 1 auto-override unknown 0\n",
            b"#sensor-status 123456792.0 1 auto-override nominal 1\n",
            b"#sensor-status 123456792.0 1 auto-override nominal 1\n",
            b"!watchdog ok\n",
        ],
    )


async def test_sensor_sampling_bulk(
    mock_time,
    server: DummyServer,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    writer.write(b"?sensor-sampling float-sensor,counter-queries event\n")
    await check_reply(
        reader,
        [
            b"#sensor-status 123456789.0 1 float-sensor unknown 0.0\n",
            b"#sensor-status 123456789.0 1 counter-queries nominal 0\n",
            b"!sensor-sampling ok float-sensor,counter-queries event\n",
        ],
    )
    # Check that the strategy has been set
    writer.write(b"?sensor-sampling float-sensor\n")
    writer.write(b"?sensor-sampling counter-queries\n")
    await check_reply(
        reader,
        [
            b"!sensor-sampling ok float-sensor event\n",
            b"!sensor-sampling ok counter-queries event\n",
        ],
    )


async def test_sensor_sampling_bulk_missing_sensor(
    server: DummyServer, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    writer.write(b"?sensor-sampling float-sensor,missing event\n")
    await check_reply(reader, [rb"!sensor-sampling fail Unknown\_sensor\_'missing'" + b"\n"])
    # Check that the strategy has not been set on the first sensor
    writer.write(b"?sensor-sampling float-sensor\n")
    await check_reply(reader, [b"!sensor-sampling ok float-sensor none\n"])


async def test_sensor_sampling_bulk_invalid_strategy(
    server: DummyServer, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    writer.write(b"?sensor-sampling float-sensor,foo differential 1\n")
    await check_reply(
        reader,
        [
            rb"!sensor-sampling fail differential\_strategies\_only\_valid\_for\_integer"
            rb"\_and\_float\_sensors" + b"\n"
        ],
    )
    # Check that the strategy has not been set on the first sensor
    writer.write(b"?sensor-sampling float-sensor\n")
    await check_reply(reader, [b"!sensor-sampling ok float-sensor none\n"])


async def test_sensor_sampling_bulk_duplicate_names(
    server: DummyServer, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    writer.write(b"?sensor-sampling float-sensor,float-sensor auto\n")
    await check_reply(reader, [rb"!sensor-sampling fail Duplicate\_sensor\_name" + b"\n"])


async def test_sensor_sampling_bulk_remove_midway(
    mock_time,
    monkeypatch,
    server: DummyServer,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    """Remove a sensor while the subscription process is in action."""
    # This test is slightly fragile as it depends on the order of tasks
    # being scheduled by asyncio. If it starts failing, the number of
    # sleeps in remove-bulk-sensors might need to be adjusted.
    monkeypatch.setattr("aiokatcp.server._BULK_SENSOR_BATCH", 2)
    writer.write(b"?add-bulk-sensors\n")
    writer.write(b"?sensor-sampling bulk1,bulk2,bulk3,bulk4,bulk5 auto\n")
    writer.write(b"?remove-bulk-sensors\n")
    await check_reply(
        reader,
        [
            b"!add-bulk-sensors ok\n",
            b"#sensor-status 123456789.0 1 bulk1 nominal 123\n",
            b"#sensor-status 123456789.0 1 bulk2 nominal 123\n",
            b"!remove-bulk-sensors ok\n",
            b"#sensor-status 123456789.0 1 bulk3 nominal 123\n",
            b"#sensor-status 123456789.0 1 bulk4 nominal 123\n",
            b"#sensor-status 123456789.0 1 bulk5 nominal 123\n",
            b"!sensor-sampling ok bulk1,bulk2,bulk3,bulk4,bulk5 auto\n",
        ],
    )
    # ensure that bulk3-bulk5 didn't end up subscribed despite removal
    # of the sensors.
    assert next(iter(server._connections))._samplers == {}


@pytest.mark.server_cls.with_args(BadServer)
async def test_double_reply(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    writer.write(b"?double-reply\n")
    await check_reply(
        reader,
        [
            b"!double-reply ok reply1\n",
            re.compile(rb"^#log error [0-9.]+ aiokatcp\.server Traceback"),
        ],
    )


@pytest.mark.server_cls.with_args(BadServer)
async def test_reply_return(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    writer.write(b"?reply-return\n")
    await check_reply(
        reader,
        [
            b"!reply-return ok reply1\n",
            re.compile(rb"^#log error [0-9.]+ aiokatcp\.server Traceback"),
        ],
    )


@pytest.mark.server_cls.with_args(BadServer)
async def test_inform_after_reply(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    writer.write(b"?inform-after-reply\n")
    await check_reply(
        reader,
        [
            b"!inform-after-reply ok reply1\n",
            re.compile(rb"^#log error [0-9.]+ aiokatcp\.server Traceback"),
        ],
    )


@pytest.mark.server_cls.with_args(BadServer)
async def test_informs_after_reply(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    writer.write(b"?informs-after-reply\n")
    await check_reply(
        reader,
        [
            b"!informs-after-reply ok reply1\n",
            re.compile(rb"^#log error [0-9.]+ aiokatcp\.server Traceback"),
        ],
    )


# Metaclass tests for invalid constructions


def test_metaclass_missing_help() -> None:
    with pytest.raises(TypeError):

        class MyBadServer(DummyServer):
            def request_no_help(self):
                pass


def test_metaclass_too_few_parameters() -> None:
    with pytest.raises(TypeError):

        class MyBadServer(DummyServer):
            def request_too_few(self):
                """Not enough parameters"""


def test_metaclass_missing_version() -> None:
    with pytest.raises(TypeError, match="MyBadServer does not define VERSION"):

        class MyBadServer(DeviceServer):
            BUILD_INFO = "build-info"

        MyBadServer("127.0.0.1", 0)


def test_metaclass_missing_build_state() -> None:
    with pytest.raises(TypeError, match="MyBadServer does not define BUILD_STATE"):

        class MyBadServer(DeviceServer):
            VERSION = "version"

        MyBadServer("127.0.0.1", 0)
