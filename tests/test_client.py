# Copyright 2017, 2019-2020, 2022, 2024-2025 National Research Foundation (SARAO)
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

import asyncio.base_events
import enum
import functools
import gc
import hashlib
import itertools
import logging
import re
import unittest
import unittest.mock
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    List,
    Sequence,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)
from unittest.mock import call

import async_solipsism
import hypothesis.strategies as st
import pytest
from hypothesis.stateful import Bundle, RuleBasedStateMachine, invariant, precondition, rule
from typing_extensions import Concatenate, ParamSpec

from aiokatcp import (
    AbstractSensorWatcher,
    Client,
    DeviceServer,
    DeviceStatus,
    FailReply,
    InvalidReply,
    Message,
    ProtocolError,
    Reading,
    Sensor,
    SensorWatcher,
    SyncState,
    encode,
)
from aiokatcp.client import _MonitoredSensor

_ClientQueue = Union["asyncio.Queue[Tuple[asyncio.StreamReader, asyncio.StreamWriter]]"]
_P = ParamSpec("_P")
_T = TypeVar("_T")


@pytest.fixture
def event_loop_policy():
    return async_solipsism.EventLoopPolicy()


class DummyClient(Client):
    """Client with some informs for testing."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.foos = asyncio.Queue()
        self.unhandled = asyncio.Queue()

    def inform_foo(self, string: str, integer: int) -> None:
        self.foos.put_nowait((string, integer))

    def inform_exception(self) -> None:
        raise RuntimeError("I crashed")

    def unhandled_inform(self, msg: Message) -> None:
        self.unhandled.put_nowait(msg)


@pytest.fixture
async def client_queue() -> _ClientQueue:
    """Queue to which client connections are added as they connect to :meth:`server`."""
    return asyncio.Queue()


@pytest.fixture
async def server(client_queue) -> AsyncGenerator[asyncio.base_events.Server, None]:
    """Start a server listening on [::1]:7777."""

    def callback(reader, writer):
        client_queue.put_nowait((reader, writer))

    server = await asyncio.start_server(callback, "::1", 7777)
    yield server
    server.close()
    await server.wait_closed()


class Channel:
    """A single client-server connection.

    On the client end it uses a :class:`.Client`, and on the server end it uses
    a (reader, writer) pair. It contains utility methods for simple
    interactions between the two.
    """

    def __init__(
        self, client: Client, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        self.client = client
        self.reader = reader
        self.writer = writer

    async def wait_connected(self) -> None:
        self.writer.write(b"#version-connect katcp-protocol 5.1-IMB\n")
        await self.client.wait_connected()
        # Make sure that wait_connected works when already connected
        await self.client.wait_connected()

    async def close(self) -> None:
        self.client.close()
        self.writer.close()
        await self.client.wait_closed()
        try:
            await self.writer.wait_closed()
        except ConnectionError:
            pass

    @classmethod
    async def create(
        cls,
        server: asyncio.base_events.Server,
        client_queue: _ClientQueue,
        client_cls: Type[Client] = DummyClient,
        auto_reconnect=True,
    ) -> "Channel":
        host, port = server.sockets[0].getsockname()[:2]
        client = client_cls(host, port, auto_reconnect=auto_reconnect)
        (reader, writer) = await client_queue.get()
        return cls(client, reader, writer)


@pytest.fixture
async def channel(request, server, client_queue):
    marker = request.node.get_closest_marker("channel_cls")
    channel_cls = marker.args[0] if marker is not None else Channel
    marker = request.node.get_closest_marker("channel_args")
    args = marker.args if marker is not None else ()
    kwargs = marker.kwargs if marker is not None else {}
    channel = await channel_cls.create(server, client_queue, *args, **kwargs)
    yield channel
    await channel.close()


async def test_request_ok(channel) -> None:
    await channel.wait_connected()
    future = asyncio.create_task(channel.client.request("echo"))
    assert await channel.reader.readline() == b"?echo[1]\n"
    channel.writer.write(b"!echo[1] ok\n")
    result = await future
    assert result == ([], [])
    # Again, with arguments. This also tests MID incrementing, non-ASCII
    # characters, and null escaping.
    arg = b"h\xaf\xce\0"
    arg_esc = b"h\xaf\xce\\0"  # katcp escaping
    future = asyncio.create_task(channel.client.request("echo", b"123", arg))
    assert await channel.reader.readline() == b"?echo[2] 123 " + arg_esc + b"\n"
    channel.writer.write(b"!echo[2] ok 123 " + arg_esc + b"\n")
    result = await future
    assert result == ([b"123", arg], [])


async def test_request_fail(channel) -> None:
    await channel.wait_connected()
    future = asyncio.create_task(channel.client.request("failme"))
    assert await channel.reader.readline() == b"?failme[1]\n"
    channel.writer.write(b"!failme[1] fail Error\\_message\n")
    with pytest.raises(FailReply, match="^Error message$"):
        await future


async def test_request_fail_no_msg(channel) -> None:
    await channel.wait_connected()
    future = asyncio.create_task(channel.client.request("failme"))
    assert await channel.reader.readline() == b"?failme[1]\n"
    channel.writer.write(b"!failme[1] fail\n")
    with pytest.raises(FailReply, match="^$"):
        await future


async def test_request_fail_msg_bad_encoding(channel) -> None:
    await channel.wait_connected()
    future = asyncio.create_task(channel.client.request("failme"))
    assert await channel.reader.readline() == b"?failme[1]\n"
    channel.writer.write(b"!failme[1] fail \xaf\n")
    with pytest.raises(FailReply, match="^\uFFFD$"):
        await future


async def test_request_invalid(channel) -> None:
    await channel.wait_connected()
    future = asyncio.create_task(channel.client.request("invalid-request"))
    assert await channel.reader.readline() == b"?invalid-request[1]\n"
    channel.writer.write(b"!invalid-request[1] invalid Unknown\\_request\n")
    with pytest.raises(InvalidReply, match="^Unknown request$"):
        await future


async def test_request_no_code(channel) -> None:
    await channel.wait_connected()
    future = asyncio.create_task(channel.client.request("invalid-request"))
    assert await channel.reader.readline() == b"?invalid-request[1]\n"
    channel.writer.write(b"!invalid-request[1]\n")
    with pytest.raises(InvalidReply, match="^$"):
        await future


async def test_request_with_informs(channel) -> None:
    await channel.wait_connected()
    future = asyncio.create_task(channel.client.request("help"))
    assert await channel.reader.readline() == b"?help[1]\n"
    channel.writer.write(b"#help[1] help Show\\_help\n")
    channel.writer.write(b"#help[1] halt Halt\n")
    channel.writer.write(b"!help[1] ok 2\n")
    result = await future
    assert result == (
        [b"2"],
        [
            Message.inform("help", b"help", b"Show help", mid=1),
            Message.inform("help", b"halt", b"Halt", mid=1),
        ],
    )


async def test_sensor_reading_explicit_type(channel) -> None:
    await channel.wait_connected()
    future = asyncio.create_task(channel.client.sensor_reading("device-status", DeviceStatus))
    assert await channel.reader.readline() == b"?sensor-value[1] device-status\n"
    channel.writer.write(b"#sensor-value[1] 1234567890.1 1 device-status nominal ok\n")
    channel.writer.write(b"!sensor-value[1] ok 1\n")
    result = await future
    assert result == Reading(1234567890.1, Sensor.Status.NOMINAL, DeviceStatus.OK)


async def test_sensor_reading_int(channel) -> None:
    await channel.wait_connected()
    future = asyncio.create_task(channel.client.sensor_reading("foo"))
    assert await channel.reader.readline() == b"?sensor-list[1] foo\n"
    channel.writer.write(b"#sensor-list[1] foo description\\_stuff unit integer\n")
    channel.writer.write(b"!sensor-list[1] ok 1\n")
    assert await channel.reader.readline() == b"?sensor-value[2] foo\n"
    channel.writer.write(b"#sensor-value[2] 1234567890.1 1 device-status warn 7\n")
    channel.writer.write(b"!sensor-value[2] ok 1\n")
    result = await future
    assert result == Reading(1234567890.1, Sensor.Status.WARN, 7)


async def test_sensor_reading_discrete(channel) -> None:
    await channel.wait_connected()
    future = asyncio.create_task(channel.client.sensor_reading("foo"))
    assert await channel.reader.readline() == b"?sensor-list[1] foo\n"
    channel.writer.write(b"#sensor-list[1] foo description\\_stuff unit discrete hello world\n")
    channel.writer.write(b"!sensor-list[1] ok 1\n")
    assert await channel.reader.readline() == b"?sensor-value[2] foo\n"
    channel.writer.write(b"#sensor-value[2] 1234567890.1 1 device-status warn hello\n")
    channel.writer.write(b"!sensor-value[2] ok 1\n")
    result = await future
    assert result == Reading(1234567890.1, Sensor.Status.WARN, b"hello")


async def test_sensor_reading_missing(channel) -> None:
    await channel.wait_connected()
    future = asyncio.create_task(channel.client.sensor_reading("foo", str))
    assert await channel.reader.readline() == b"?sensor-value[1] foo\n"
    channel.writer.write(b"!sensor-value[1] fail Unknown\\_sensor\\_'foo'\n")
    with pytest.raises(FailReply):
        await future


async def test_sensor_reading_wrong_count(channel) -> None:
    await channel.wait_connected()
    future = asyncio.create_task(channel.client.sensor_reading("/foo/", str))
    assert await channel.reader.readline() == b"?sensor-value[1] /foo/\n"
    channel.writer.write(b"#sensor-value[1] 1234567890.1 1 foo1 nominal ok\n")
    channel.writer.write(b"#sensor-value[1] 1234567890.2 1 foo2 nominal ok\n")
    channel.writer.write(b"!sensor-value[1] ok 2\n")
    with pytest.raises(FailReply, match="Server returned 2 sensors, but only 1 expected"):
        await future


async def test_sensor_value_ok(channel) -> None:
    await channel.wait_connected()
    future = asyncio.create_task(channel.client.sensor_value("foo", int))
    assert await channel.reader.readline() == b"?sensor-value[1] foo\n"
    channel.writer.write(b"#sensor-value[1] 1234567890.1 1 device-status warn 7\n")
    channel.writer.write(b"!sensor-value[1] ok 1\n")
    result = await future
    assert result == 7


async def test_sensor_value_invalid_status(channel) -> None:
    await channel.wait_connected()
    future = asyncio.create_task(channel.client.sensor_value("foo", int))
    assert await channel.reader.readline() == b"?sensor-value[1] foo\n"
    channel.writer.write(b"#sensor-value[1] 1234567890.1 1 device-status unknown 7\n")
    channel.writer.write(b"!sensor-value[1] ok 1\n")
    with pytest.raises(ValueError):
        await future


async def test_inform(channel, caplog) -> None:
    client = cast(DummyClient, channel.client)
    await channel.wait_connected()
    with caplog.at_level(logging.INFO, "aiokatcp.client"):
        # Put in bad ones before the good one, so that as soon as we've
        # received the good one from the queue we can finish the test.
        channel.writer.write(b"#exception\n#foo bad notinteger\n#foo \xc3\xa9 123\n")
        inform = await client.foos.get()
    assert caplog.records[0].exc_info[1].args[0] == "I crashed"
    assert re.match("error in inform", caplog.records[1].message)
    assert inform == ("é", 123)


async def test_unhandled_inform(channel) -> None:
    await channel.wait_connected()
    channel.writer.write(b"#unhandled arg\n")
    msg = await channel.client.unhandled.get()
    assert msg == Message.inform("unhandled", b"arg")


async def test_inform_callback(channel) -> None:
    def callback(string: str, integer: int) -> None:
        values.put_nowait((string, integer))

    values = asyncio.Queue()  # type: asyncio.Queue[Tuple[str, int]]
    client = cast(DummyClient, channel.client)
    client.add_inform_callback("bar", callback)
    await channel.wait_connected()
    channel.writer.write(b"#bar hello 42\n")
    value = await values.get()
    assert value == ("hello", 42)
    client.remove_inform_callback("bar", callback)
    assert client._inform_callbacks == {}


async def test_unsolicited_reply(channel, caplog) -> None:
    await channel.wait_connected()
    future = asyncio.create_task(channel.client.request("echo"))
    with caplog.at_level(logging.DEBUG, "aiokatcp.client"):
        channel.writer.write(b"!surprise[3]\n!echo[1] ok\n")
        await future
    assert re.search("Received .* with unknown message ID", caplog.text)


async def test_receive_request(channel, caplog) -> None:
    await channel.wait_connected()
    future = asyncio.create_task(channel.client.request("echo"))
    with caplog.at_level(logging.INFO, "aiokatcp.client"):
        channel.writer.write(b"?surprise\n!echo[1] ok\n")
        await future
    assert re.search("Received unexpected request", caplog.text)


async def test_reply_no_mid(channel, caplog) -> None:
    await channel.wait_connected()
    future = asyncio.create_task(channel.client.request("echo"))
    with caplog.at_level(logging.INFO, "aiokatcp.client"):
        channel.writer.write(b"!surprise ok\n!echo[1] ok\n")
        await future
    assert re.search("Received unexpected REPLY .* without message ID", caplog.text)


async def test_context_manager(channel) -> None:
    async with channel.client:
        pass
    await channel.client.wait_closed()


async def test_connect(server, client_queue) -> None:
    host, port = server.sockets[0].getsockname()[:2]
    client_task = asyncio.create_task(DummyClient.connect(host, port))
    (reader, writer) = await client_queue.get()
    await asyncio.sleep(1)
    assert not client_task.done()
    writer.write(b"#version-connect katcp-protocol 5.1-IMB\n")
    client = await client_task
    assert client.is_connected
    client.close()
    writer.close()
    await client.wait_closed()


async def test_unparsable_protocol(channel, caplog) -> None:
    with caplog.at_level(logging.INFO, "aiokatcp.client"):
        channel.writer.write(b"#version-connect katcp-protocol notvalid\n")
        line = await channel.reader.read()
    assert line == b""
    assert re.search("Unparsable katcp-protocol", caplog.text)


async def test_bad_protocol(channel, caplog) -> None:
    with caplog.at_level(logging.INFO, "aiokatcp.client"):
        channel.writer.write(b"#version-connect katcp-protocol 4.0-I\n")
        line = await channel.reader.read()
    assert line == b""
    assert re.search(r"Unknown protocol version 4\.0", caplog.text)


async def test_no_connection(channel) -> None:
    # Do not send #version-connect
    with pytest.raises(BrokenPipeError):
        await channel.client.request("help")


async def test_connection_reset(channel) -> None:
    await channel.wait_connected()
    channel.writer.close()
    with pytest.raises(ConnectionResetError):
        await channel.client.request("help")


async def test_disconnected(channel) -> None:
    await channel.wait_connected()
    channel.writer.write(b"#disconnect Server\\_exiting\n")
    await channel.client.wait_disconnected()
    with pytest.raises(BrokenPipeError):
        await channel.client.request("help")


async def test_bad_address(caplog) -> None:
    client = DummyClient("invalid.invalid", 1)
    try:
        with caplog.at_level(logging.WARNING, "aiokatcp.client"):
            task = asyncio.create_task(client.wait_connected())
            await asyncio.sleep(1)
        assert re.search("Failed to connect to invalid.invalid:1: ", caplog.text)
        task.cancel()
    finally:
        client.close()
        await client.wait_closed()


class SensorWatcherChannel(Channel):
    """Mock out :class:`.AbstractSensorWatcher` and add to the client."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.watcher = unittest.mock.Mock(autospec=AbstractSensorWatcher)
        self.client.add_sensor_watcher(self.watcher)

    async def connect(self) -> None:
        """Get as far as the monitor issuing ``?sensor-list``"""
        await self.wait_connected()
        self.watcher.state_updated.assert_called_with(SyncState.SYNCING)
        self.watcher.reset_mock()
        assert await self.reader.readline() == b"?sensor-list[1]\n"

    async def sensor_list(self) -> None:
        """Send the sensor list and wait for ``?sensor-sampling``"""
        self.writer.write(
            b"#sensor-list[1] device-status Device\\_status \\@ discrete ok degraded fail\n"
            b"!sensor-list[1] ok 1\n"
        )
        assert await self.reader.readline() == b"?sensor-sampling[2] device-status auto\n"
        assert self.watcher.mock_calls == [
            call.batch_start(),
            call.filter(
                "device-status",
                "Device status",
                "",
                "discrete",
                b"ok",
                b"degraded",
                b"fail",
            ),
            call.sensor_added(
                "device-status",
                "Device status",
                "",
                "discrete",
                b"ok",
                b"degraded",
                b"fail",
            ),
            call.batch_stop(),
        ]
        self.watcher.reset_mock()

    async def sensor_sampling(self) -> None:
        """Reply to ``?sensor-sampling``"""
        self.writer.write(
            b"#sensor-status 123456789.0 1 device-status nominal ok\n"
            b"!sensor-sampling[2] ok device-status auto\n"
            b"#wakeup\n"
        )
        await asyncio.sleep(1)
        assert self.watcher.mock_calls == [
            call.batch_start(),
            call.sensor_updated("device-status", b"ok", Sensor.Status.NOMINAL, 123456789.0),
            call.batch_stop(),
            call.state_updated(SyncState.SYNCED),
        ]
        self.watcher.reset_mock()

    async def init(self) -> None:
        await self.connect()
        await self.sensor_list()
        await self.sensor_sampling()

    async def interface_changed(self) -> None:
        """Send a ``#interface-changed`` inform and wait for ``?sensor-list``"""
        self.writer.write(b"#interface-changed sensor-list\n#wakeup\n")
        await asyncio.sleep(1)
        self.watcher.state_updated.assert_called_with(SyncState.SYNCING)
        self.watcher.reset_mock()
        assert await self.reader.readline() == b"?sensor-list[3]\n"


@pytest.mark.channel_cls.with_args(SensorWatcherChannel)
class TestSensorMonitor:
    """Test the sensor monitoring interface.

    This mocks out the :class:`~.AbstractSensorWatcher`.

    See also :class:`SensorMonitorStateMachine`, which uses randomised tests.
    """

    async def test_init(self, channel) -> None:
        await channel.init()

    async def test_add_remove_sensors(self, channel):
        await channel.init()
        await channel.interface_changed()
        channel.writer.write(
            b"#sensor-list[3] temp Temperature F float\n"
            b"#sensor-list[3] pressure Pressure Pa float\n"
            b"!sensor-list[3] ok 1\n"
        )
        assert await channel.reader.readline() == b"?sensor-sampling[4] pressure,temp auto\n"
        assert channel.watcher.mock_calls == [
            call.batch_start(),
            call.filter("temp", "Temperature", "F", "float"),
            call.sensor_added("temp", "Temperature", "F", "float"),
            call.filter("pressure", "Pressure", "Pa", "float"),
            call.sensor_added("pressure", "Pressure", "Pa", "float"),
            call.sensor_removed("device-status"),
            call.batch_stop(),
        ]
        channel.watcher.reset_mock()

        channel.writer.write(
            b"#sensor-status 123456790.0 1 temp warn 451.0\n"
            b"#sensor-status 123456791.0 1 pressure nominal 101.0\n"
            b"!sensor-sampling[4] ok temp,pressure auto\n"
        )
        await asyncio.sleep(1)
        # Note: in future batches may be less fine-grained
        assert channel.watcher.mock_calls == [
            call.batch_start(),
            call.sensor_updated("temp", b"451.0", Sensor.Status.WARN, 123456790.0),
            call.batch_stop(),
            call.batch_start(),
            call.sensor_updated("pressure", b"101.0", Sensor.Status.NOMINAL, 123456791.0),
            call.batch_stop(),
            call.state_updated(SyncState.SYNCED),
        ]

    async def test_remove_sensor_while_subscribing(self, channel):
        """Sensor is removed between notification and subscription attempt."""
        await channel.init()
        await channel.interface_changed()
        channel.writer.write(
            b"#sensor-list[3] temp Temperature F float\n"
            b"#sensor-list[3] pressure Pressure Pa float\n"
            b"!sensor-list[3] ok 1\n"
        )
        assert await channel.reader.readline() == b"?sensor-sampling[4] pressure,temp auto\n"

        channel.writer.write(rb"!sensor-sampling[4] fail Unknown\_sensor\_'pressure'" + b"\n")
        assert await channel.reader.readline() == b"?sensor-sampling[5] pressure auto\n"
        channel.writer.write(rb"!sensor-sampling[5] fail Unknown\_sensor\_'pressure'" + b"\n")
        assert await channel.reader.readline() == b"?sensor-sampling[6] temp auto\n"
        channel.writer.write(
            b"#sensor-status 123456790.0 1 temp warn 451.0\n" b"!sensor-sampling[6] ok temp auto\n"
        )
        await asyncio.sleep(1)

        assert channel.watcher.mock_calls == [
            call.batch_start(),
            call.filter("temp", "Temperature", "F", "float"),
            call.sensor_added("temp", "Temperature", "F", "float"),
            call.filter("pressure", "Pressure", "Pa", "float"),
            call.sensor_added("pressure", "Pressure", "Pa", "float"),
            call.sensor_removed("device-status"),
            call.batch_stop(),
            call.batch_start(),
            call.sensor_updated("temp", b"451.0", Sensor.Status.WARN, 123456790.0),
            call.batch_stop(),
            call.state_updated(SyncState.SYNCED),
        ]

    async def test_replace_sensor(self, channel):
        """Sensor has the same name but different parameters"""
        await channel.init()
        await channel.interface_changed()
        channel.writer.write(
            b"#sensor-list[3] device-status A\\_different\\_status \\@ int\n"
            b"!sensor-list[3] ok 1\n"
        )
        assert await channel.reader.readline() == b"?sensor-sampling[4] device-status auto\n"
        assert channel.watcher.mock_calls == [
            call.batch_start(),
            call.filter("device-status", "A different status", "", "int"),
            call.sensor_added("device-status", "A different status", "", "int"),
            call.batch_stop(),
        ]
        channel.watcher.reset_mock()

        channel.writer.write(
            b"#sensor-status 123456791.0 1 device-status nominal 123\n"
            b"!sensor-sampling[4] ok device-status auto\n"
        )
        await asyncio.sleep(1)
        assert channel.watcher.mock_calls == [
            call.batch_start(),
            call.sensor_updated("device-status", b"123", Sensor.Status.NOMINAL, 123456791.0),
            call.batch_stop(),
            call.state_updated(SyncState.SYNCED),
        ]

    async def test_sensor_vanished(self, channel):
        """Sensor vanishes immediately after sensor-list reply."""
        await channel.connect()
        await channel.sensor_list()
        channel.writer.write(
            b"#interface-changed sensor-list\n"
            b"!sensor-sampling[2] fail Unknown\\_sensor\\_'device-status'\n"
        )
        assert await channel.reader.readline() == b"?sensor-list[3]\n"
        channel.writer.write(b"!sensor-list[3] ok 0\n")
        await asyncio.sleep(1)
        assert channel.watcher.mock_calls == [
            call.batch_start(),
            call.sensor_removed("device-status"),
            call.batch_stop(),
            call.state_updated(SyncState.SYNCED),
        ]

    async def test_sensor_vanished2(self, channel):
        """Sensor vanishes immediately after sensor-list reply (second case).

        This is similar to :meth:`test_sensor_vanished`, but the inform arrives
        only after the failure in ``?sensor-sampling``.
        """
        await channel.connect()
        await channel.sensor_list()
        channel.writer.write(b"!sensor-sampling[2] fail Unknown\\_sensor\\_'device-status'\n")
        # Wait until the update task finishes before sending interface-changed
        await asyncio.sleep(1)
        channel.writer.write(b"#interface-changed sensor-list\n")
        assert await channel.reader.readline() == b"?sensor-list[3]\n"
        channel.writer.write(b"!sensor-list[3] ok 0\n")
        await asyncio.sleep(1)
        assert channel.watcher.mock_calls == [
            call.state_updated(SyncState.SYNCED),
            call.state_updated(SyncState.SYNCING),
            call.batch_start(),
            call.sensor_removed("device-status"),
            call.batch_stop(),
            call.state_updated(SyncState.SYNCED),
        ]

    async def test_remove_sensor_watcher(self, channel):
        """Removing the last watcher unsubscribes"""
        await channel.init()
        channel.client.remove_sensor_watcher(channel.watcher)
        assert await channel.reader.readline() == b"?sensor-sampling[3] device-status none\n"
        channel.writer.write(b"!sensor-sampling[3] ok device-status none\n")

    async def test_close(self, channel):
        """Closing the client must update the state"""
        await channel.init()
        channel.client.close()
        await channel.client.wait_closed()
        assert channel.watcher.mock_calls == [call.state_updated(SyncState.CLOSED)]

    async def test_disconnect(self, channel, client_queue):
        """When the connection drops, the state must change appropriately"""
        await channel.init()
        channel.writer.write(b"#disconnect Testing\n")
        await channel.writer.drain()
        channel.writer.close()
        (channel.reader, channel.writer) = await client_queue.get()

        await channel.wait_connected()
        channel.watcher.state_updated.assert_called_with(SyncState.SYNCING)
        channel.watcher.reset_mock()
        assert await channel.reader.readline() == b"?sensor-list[3]\n"
        channel.writer.write(
            b"#sensor-list[3] device-status Device\\_status \\@ discrete ok degraded fail\n"
            b"!sensor-list[3] ok 1\n"
        )
        assert await channel.reader.readline() == b"?sensor-sampling[4] device-status auto\n"
        channel.writer.write(
            b"#sensor-status 123456789.0 1 device-status nominal ok\n"
            b"!sensor-sampling[4] ok device-status auto\n"
        )
        await asyncio.sleep(1)
        assert channel.watcher.mock_calls == [
            call.batch_start(),
            # No sensor_added because the sensor was already known
            call.batch_stop(),
            call.batch_start(),
            call.sensor_updated("device-status", b"ok", Sensor.Status.NOMINAL, 123456789.0),
            call.batch_stop(),
            call.state_updated(SyncState.SYNCED),
        ]

    async def test_unexpected_update(self, channel):
        """If a ``#sensor-status`` inform arrives we weren't expecting, we're subscribed.

        This can happen if we remove a watcher but receive the update before we
        can unsubscribe. It could also happen if unsubscribing failed for some
        server-side reason, or just because the server is buggy.
        """
        await channel.init()
        watcher2 = unittest.mock.Mock(autospec=AbstractSensorWatcher)
        watcher2.filter.return_value = False
        channel.client.add_sensor_watcher(watcher2)
        channel.client.remove_sensor_watcher(channel.watcher)
        # watcher2 filters out all sensors, so we now unsubscribe from device-status
        assert await channel.reader.readline() == b"?sensor-sampling[3] device-status none\n"
        channel.writer.write(
            b"#sensor-status 123456790.0 1 device-status nominal ok\n"
            b"!sensor-sampling[3] ok device-status\n"
        )
        await asyncio.sleep(1)
        # This one is unexpected - we will try to unsubscribe again
        channel.writer.write(b"#sensor-status 123456791.0 1 device-status warn degraded\n")
        assert await channel.reader.readline() == b"?sensor-sampling[4] device-status none\n"
        channel.writer.write(b"!sensor-sampling[4] ok device-status\n")
        await channel.writer.drain()

    @pytest.mark.parametrize(
        "update",
        [
            b"#sensor-status 123456789.0 hello\n",  # Count is not integer
            b"#sensor-status 123456789.0 -1\n",  # Count is negative
            b"#sensor-status 123456789.0 2 device-status nominal ok\n",  # Count is wrong
            b"#sensor-status 123456789.0 1 device-status\xFF nominal ok\n",  # Non-UTF-8 name
            b"#sensor-status 123456789.0 1 device-status spam ok\n",  # Invalid status
            b"#sensor-status 123456789.0 1 unknown nominal ok\n",  # Unknown sensor
        ],
    )
    async def test_invalid_update(self, channel, update):
        """An invalidly-formatted ``#sensor-status`` inform has no effect."""
        await channel.init()
        channel.writer.write(update)
        await channel.writer.drain()

    @pytest.mark.channel_args(auto_reconnect=False)
    async def test_no_reconnect(self, channel):
        """Test the SensorMonitor stops gracefully when one-shot connection terminates."""
        await channel.init()
        channel.writer.write(b"#interface-changed sensor-list\n")
        await channel.writer.drain()
        channel.writer.close()
        await channel.writer.wait_closed()
        await channel.client.wait_disconnected()
        # Give anything that might go wrong on disconnection time to go wrong.
        await asyncio.sleep(1)


class DummySensorWatcher(SensorWatcher):
    def rewrite_name(self, name: str) -> Union[str, Sequence[str]]:
        if name == "bar":
            return ["test_bar1", "test_bar2"]
        return "test_" + name


class DummyEnum(enum.Enum):
    THING_ONE = 1
    THING_TWO = 2


class TestSensorWatcher:
    """Test :class:`~.SensorWatcher`."""

    @pytest.fixture
    def client(self, event_loop) -> unittest.mock.MagicMock:
        client = unittest.mock.MagicMock()
        client.loop = event_loop
        return client

    @pytest.fixture
    def watcher(self, client: unittest.mock.MagicMock) -> DummySensorWatcher:
        return DummySensorWatcher(client, enum_types=[DummyEnum])

    def test_construct(self, watcher: DummySensorWatcher) -> None:
        assert len(watcher.sensors) == 0
        assert not watcher.synced.is_set()

    def test_sensor_added(self, watcher: DummySensorWatcher) -> None:
        watcher.batch_start()
        watcher.sensor_added("foo", "A sensor", "F", "float")
        watcher.sensor_added("bar", "A duplicated sensor", "s", "integer")
        watcher.batch_stop()
        assert len(watcher.sensors) == 3
        sensor = watcher.sensors["test_foo"]
        assert sensor.name == "test_foo"
        assert sensor.description == "A sensor"
        assert sensor.units == "F"
        assert sensor.stype is float
        assert sensor.status == Sensor.Status.UNKNOWN
        for name in ["test_bar1", "test_bar2"]:
            sensor = watcher.sensors[name]
            assert sensor.name == name
            assert sensor.description == "A duplicated sensor"
            assert sensor.units == "s"
            assert sensor.stype is int
            assert sensor.status == Sensor.Status.UNKNOWN

    def test_sensor_added_discrete(self, watcher: DummySensorWatcher) -> None:
        watcher.batch_start()
        watcher.sensor_added("disc", "Discrete sensor", "", "discrete", b"abc", b"def-xyz")
        watcher.sensor_added("disc2", "Discrete sensor 2", "", "discrete", b"abc", b"def-xyz")
        watcher.batch_stop()
        assert len(watcher.sensors) == 2
        sensor = watcher.sensors["test_disc"]
        assert sensor.name == "test_disc"
        assert sensor.description == "Discrete sensor"
        assert sensor.units == ""
        assert sensor.type_name == "discrete"
        assert sensor.status == Sensor.Status.UNKNOWN
        members = [encode(member) for member in sensor.stype.__members__.values()]
        assert members == [b"abc", b"def-xyz"]
        assert (
            watcher.sensors["test_disc"].stype is watcher.sensors["test_disc2"].stype
        ), "Enum cache did not work"

    def test_sensor_added_known_discrete(self, watcher: DummySensorWatcher) -> None:
        watcher.batch_start()
        watcher.sensor_added("disc", "Discrete sensor", "", "discrete", b"thing-one", b"thing-two")
        watcher.batch_stop()
        assert len(watcher.sensors) == 1
        sensor = watcher.sensors["test_disc"]
        assert sensor.name == "test_disc"
        assert sensor.description == "Discrete sensor"
        assert sensor.units == ""
        assert sensor.type_name == "discrete"
        assert sensor.stype is DummyEnum
        assert sensor.status == Sensor.Status.UNKNOWN

    def test_sensor_added_bad_type(self, watcher: DummySensorWatcher) -> None:
        watcher.batch_start()
        watcher.sensor_added("foo", "A sensor", "F", "blah")
        watcher.batch_stop()
        assert len(watcher.sensors) == 0
        watcher.logger.warning.assert_called_once_with(  # type: ignore
            "Type %s is not recognised, skipping sensor %s", "blah", "foo"
        )

    def test_sensor_removed(self, watcher: DummySensorWatcher) -> None:
        self.test_sensor_added(watcher)

        watcher.batch_start()
        watcher.sensor_removed("foo")
        watcher.sensor_removed("bar")
        watcher.batch_stop()
        assert len(watcher.sensors) == 0

    def test_sensor_updated(self, watcher: DummySensorWatcher) -> None:
        self.test_sensor_added(watcher)

        watcher.batch_start()
        watcher.sensor_updated("foo", b"12.5", Sensor.Status.WARN, 1234567890.0)
        watcher.sensor_updated("bar", b"42", Sensor.Status.ERROR, 1234567891.5)
        watcher.batch_stop()
        sensor = watcher.sensors["test_foo"]
        assert sensor.reading == Reading(1234567890.0, Sensor.Status.WARN, 12.5)
        for name in ["test_bar1", "test_bar2"]:
            sensor = watcher.sensors[name]
            assert sensor.reading == Reading(1234567891.5, Sensor.Status.ERROR, 42)

    def test_sensor_updated_bad_value(self, watcher: DummySensorWatcher) -> None:
        self.test_sensor_added(watcher)

        watcher.batch_start()
        watcher.sensor_updated("foo", b"not a float", Sensor.Status.WARN, 1234567890.0)
        watcher.batch_stop()
        watcher.logger.warning.assert_called_once_with(  # type: ignore
            "Sensor %s: value %r does not match type %s: %s",
            "foo",
            b"not a float",
            "float",
            unittest.mock.ANY,
        )

    def test_sensor_updated_unknown_sensor(self, watcher: DummySensorWatcher) -> None:
        self.test_sensor_added(watcher)

        watcher.batch_start()
        watcher.sensor_updated("spam", b"123.0", Sensor.Status.WARN, 1234567890.0)
        watcher.batch_stop()
        watcher.logger.warning.assert_called_once_with(  # type: ignore
            "Received update for unknown sensor %s", "spam"
        )

    def test_state_updated(self, watcher: DummySensorWatcher) -> None:
        self.test_sensor_added(watcher)

        watcher.state_updated(SyncState.SYNCING)
        assert not watcher.synced.is_set()
        assert len(watcher.sensors) == 3
        assert watcher.sensors["test_foo"].status == Sensor.Status.UNKNOWN

        watcher.state_updated(SyncState.SYNCED)
        assert watcher.synced.is_set()
        assert len(watcher.sensors) == 3
        assert watcher.sensors["test_foo"].status == Sensor.Status.UNKNOWN

        watcher.state_updated(SyncState.DISCONNECTED)
        assert not watcher.synced.is_set()
        # Disconnecting should set all sensors to UNREACHABLE
        for name in ["test_foo", "test_bar1", "test_bar2"]:
            assert watcher.sensors[name].status == Sensor.Status.UNREACHABLE


class BasicServer(DeviceServer):
    VERSION = "dummy-1.0"
    BUILD_STATE = "dummy-build-1.0.0"


def run_in_loop(
    func: Callable[Concatenate["SensorWatcherStateMachine", _P], Awaitable[_T]]
) -> Callable[Concatenate["SensorWatcherStateMachine", _P], _T]:
    """Decorator used by :class:`SensorWatcherStateMachine`.

    The wrapped coroutine function will be run until completion in the event
    loop.
    """

    def wrapper(self: "SensorWatcherStateMachine", *args: _P.args, **kwargs: _P.kwargs) -> _T:
        return self.loop.run_until_complete(func(self, *args, **kwargs))

    functools.update_wrapper(
        wrapper, func, assigned=["__module__", "__name__", "__qualname__", "__doc__"]
    )
    return wrapper


class HashFilterSensorWatcher(SensorWatcher):
    """Sensor watcher that filters based on the SHA256 hash of the sensor description.

    If the hash, interpreted as an integer, is equivalent to an element of
    `phase` modulo `mod` then the sensor is retained; otherwise it is
    discarded.
    """

    def __init__(
        self,
        client: Client,
        enum_types: Sequence[Type[enum.Enum]] = (),
        *,
        mod: int,
        phases: Set[int],
    ) -> None:
        super().__init__(client, enum_types)
        self._mod = mod
        self._phases = phases

    def filter(self, name: str, description: str, units: str, type_name: str, *args: bytes) -> bool:
        m = hashlib.sha256()
        m.update(description.encode())
        return int.from_bytes(m.digest(), "big") % self._mod in self._phases


def sensor_value_strategy(sensor: Sensor) -> st.SearchStrategy:
    """Determine a suitable hypothesis strategy for the value of a sensor."""
    if sensor.stype is float:
        # Disallow nans because it causes problems for comparison.
        return st.floats(allow_nan=False)
    elif sensor.stype is int:
        return st.integers()
    elif sensor.stype is bytes:
        return st.binary()
    elif sensor.stype is bool:
        return st.booleans()
    else:
        raise TypeError(f"Unhandled sensor type {sensor.type_name}")


@st.composite
def mod_phases_strategy(draw: st.DrawFn) -> Tuple[int, Set[int]]:
    """Strategy used to generate :class:`HashFilterSensorWatcher` parameters.

    It generates a modulus and a set of values modulo that modulus.
    """
    mod = draw(st.integers(min_value=1, max_value=10))
    phases = draw(st.sets(st.integers(min_value=0, max_value=mod - 1), max_size=mod))
    # Hypothesis shrinks sets by removing elements, but more interesting
    # examples tend to have filters that accept more sensors, so we invert
    # the set.
    phases = set(range(mod)) - phases
    return mod, phases


class SensorWatcherStateMachine(RuleBasedStateMachine):
    """State machine for testing sensor watchers.

    This state machine creates a number of watchers with different filters,
    adds, removes, replaces and updates sensors, and after each step, checks
    that the state in the watchers matches the state in the server.

    Unfortunately :class:`.RuleBasedStateMachine` doesn't support asyncio
    natively. To make things work, each rule that needs to do asynchronous
    work runs the event loop just until that work is complete.

    This exercises the simpler code-paths. It does not test error handling/race
    conditions (e.g. sensor is missing by the time you subscribe).
    """

    def __init__(self) -> None:
        super().__init__()
        self.loop = async_solipsism.EventLoop()
        self.watchers: List[HashFilterSensorWatcher] = []
        # Unique value that is appended to the units of each sensor created.
        # This ensures that any replacement of a sensor with one of the same
        # name (but different properties) can be detected by the client so
        # that it can resubscribe. In reality nothing prevents a server from
        # doing this, but it's a shortcoming of the protocol that this cannot
        # be detected.
        self.counter = itertools.count()
        self._start()

    def teardown(self):
        self._stop()
        self.loop.close()

    @run_in_loop
    async def _start(self) -> None:
        self.server = BasicServer(host="::1", port=1234)
        await self.server.start()
        self.client = await Client.connect("::1", 1234)

    @run_in_loop
    async def _stop(self) -> None:
        self.client.close()
        await self.client.wait_closed()
        await self.server.stop()

    sensor_names = Bundle("sensor_names")

    @rule(
        target=sensor_names,
        name=st.text(
            alphabet=st.characters(
                codec="us-ascii", categories=("L", "Nd"), include_characters=".-_"
            )
        ),
    )
    def add_sensor_name(self, name: str) -> str:
        return name

    @rule(
        name=sensor_names,
        description=st.text(),
        units=st.text(),
        stype=st.sampled_from([bytes, int, float, bool]),
    )
    @run_in_loop
    async def add_sensor(self, stype: Type, name: str, description: str, units: str) -> None:
        self.server.sensors.add(Sensor(stype, name, description, f"{units} [{next(self.counter)}]"))
        self.server.mass_inform("interface-changed")

    @precondition(lambda self: self.server.sensors)  # Must have a sensor to remove
    @rule(
        name=st.runner().flatmap(lambda self: st.sampled_from(sorted(self.server.sensors.keys())))
    )
    @run_in_loop
    async def remove_sensor(self, name: str) -> None:
        del self.server.sensors[name]
        self.server.mass_inform("interface-changed")

    def sensor_update_strategy(self) -> st.SearchStrategy[Tuple[str, Any]]:
        """Generate a strategy that will return a sensor name and a value for that sensor.

        The sensor value will be suitable to the type of the sensor.
        """
        return st.one_of(
            *(
                st.tuples(st.just(sensor.name), sensor_value_strategy(sensor))
                for sensor in self.server.sensors.values()
            )
        )

    @precondition(lambda self: self.server.sensors)  # Must have a sensor to update
    @rule(
        name_value=st.runner().flatmap(lambda self: self.sensor_update_strategy()),
        status=st.sampled_from(sorted(Sensor.Status)),
    )
    @run_in_loop
    async def update_value(self, name_value: Tuple[str, Any], status: Sensor.Status) -> None:
        name, value = name_value
        self.server.sensors[name].set_value(value, status=status)

    @rule(mod_phases=mod_phases_strategy())
    @run_in_loop
    async def add_watcher(self, mod_phases: Tuple[int, Set[int]]) -> None:
        mod, phases = mod_phases
        watcher = HashFilterSensorWatcher(self.client, mod=mod, phases=phases)
        self.watchers.append(watcher)
        self.client.add_sensor_watcher(watcher)

    @precondition(lambda self: self.watchers)  # Must have a watcher to remove
    @rule(
        idx=st.runner().flatmap(
            lambda self: st.integers(min_value=0, max_value=len(self.watchers) - 1)
        )
    )
    @run_in_loop
    async def remove_watcher(self, idx: int) -> None:
        self.client.remove_sensor_watcher(self.watchers[idx])
        del self.watchers[idx]

    @rule()
    @run_in_loop
    async def step(self) -> None:
        """Allow the event loop to progress for one iteration."""
        await asyncio.sleep(0)

    @precondition(lambda self: self.client.is_connected)
    @rule()
    @run_in_loop
    async def disconnect(self) -> None:
        """Disconnect the client.

        Note that the client will automatically reconnect.
        """
        # Server isn't really shutting down, but sending this inform will
        # cause the client to disconnect.
        self.server.mass_inform("disconnect", "Server shutting down")
        await self.client.wait_disconnected()

    @invariant()
    def check_reading_invariant(self) -> None:
        """Check that we don't have any readings stored for unsubscribed sensors."""
        if self.client._sensor_monitor is not None:
            for s in self.client._sensor_monitor._sensors.values():
                assert s.subscribed or s.reading is None

    @invariant()
    def check_need_subscribe_invariant(self) -> None:
        """Check that `_need_subscribe` is consistent with sensor state."""
        if self.client._sensor_monitor is not None:
            expected = set()
            for s in self.client._sensor_monitor._sensors.values():
                if (
                    s.subscribed == _MonitoredSensor.Subscribed.UNKNOWN
                    or (s.subscribed == _MonitoredSensor.Subscribed.YES and not s.watchers)
                    or (s.subscribed == _MonitoredSensor.Subscribed.NO and s.watchers)
                ):
                    expected.add(s)
            assert self.client._sensor_monitor._need_subscribe == expected

    @invariant()
    def check_state_invariant(self) -> None:
        """Check that state is consistent with the update event."""
        monitor = self.client._sensor_monitor
        if monitor is not None:
            assert (monitor._state == SyncState.SYNCING) == (monitor._update_event.is_set())

    # This is a @rule rather than @invariant, to allow multiple rules to run
    # in between without the event loop running.
    @rule()
    @run_in_loop
    async def check_consistency(self) -> None:
        """Check that mirrored state converges."""

        def sensor_key(sensor: Sensor) -> Tuple[type, str, str, str, float, Sensor.Status, Any]:
            """Map a sensor to something that can be checked for equality."""
            return (
                sensor.stype,
                sensor.name,
                sensor.description,
                sensor.units,
                sensor.timestamp,
                sensor.status,
                sensor.value,
            )

        # Allow everything to reach steady state (including automatic
        # reconnection after `disconnect`).
        await asyncio.sleep(1)
        # Check that each watcher has the correct sensor information
        for watcher in self.watchers:
            expected = {
                sensor_key(sensor)
                for sensor in self.server.sensors.values()
                if watcher.filter(sensor.name, sensor.description, sensor.units, sensor.type_name)
            }
            actual = {sensor_key(sensor) for sensor in watcher.sensors.values()}
            assert actual == expected

        # Check that we don't have any unwanted subscriptions
        conn = next(iter(self.server._connections))
        for sensor in self.server.sensors.values():
            if not any(
                watcher.filter(sensor.name, sensor.description, sensor.units, sensor.type_name)
                for watcher in self.watchers
            ):
                assert conn.get_sampler(sensor) is None


TestSensorWatcherStateMachine = SensorWatcherStateMachine.TestCase


@pytest.mark.channel_args(auto_reconnect=False)
class TestClientNoReconnect:
    async def test_unparsable_protocol(self, channel) -> None:
        channel.writer.write(b"#version-connect katcp-protocol notvalid\n")
        assert await channel.reader.read() == b""
        with pytest.raises(ProtocolError):
            await channel.client.wait_connected()

    async def test_bad_protocol(self, channel) -> None:
        # Different approach to test_unparsable_protocol, to get more coverage
        wait_task = asyncio.create_task(channel.client.wait_connected())
        channel.writer.write(b"#version-connect katcp-protocol 4.0-I\n")
        assert await channel.reader.read() == b""
        with pytest.raises(ProtocolError):
            await wait_task

    async def test_disconnected(self, channel) -> None:
        await channel.wait_connected()
        channel.writer.write(b"#disconnect Server\\_exiting\n")
        await channel.client.wait_disconnected()
        with pytest.raises(BrokenPipeError):
            await channel.client.request("help")
        with pytest.raises(ConnectionResetError):
            await channel.client.wait_connected()

    async def test_connect_failed(self, server, client_queue) -> None:
        host, port = server.sockets[0].getsockname()[:2]
        client_task = asyncio.create_task(DummyClient.connect(host, port, auto_reconnect=False))
        (reader, writer) = await client_queue.get()
        await asyncio.sleep(1)
        assert not client_task.done()
        writer.close()
        with pytest.raises(ConnectionAbortedError):
            await client_task


class TestClientNoMidSupport:
    async def test_single(self, channel) -> None:
        channel.writer.write(b"#version-connect katcp-protocol 5.1-M\n")
        await channel.client.wait_connected()
        future = asyncio.create_task(channel.client.request("echo"))
        assert await channel.reader.readline() == b"?echo\n"
        channel.writer.write(b"#echo an\\_inform\n")
        channel.writer.write(b"!echo ok\n")
        result = await future
        assert result == ([], [Message.inform("echo", b"an inform")])

    async def test_concurrent(self, channel) -> None:
        channel.writer.write(b"#version-connect katcp-protocol 5.1-M\n")
        await channel.client.wait_connected()
        future1 = asyncio.create_task(channel.client.request("echo", 1))
        future2 = asyncio.create_task(channel.client.request("echo", 2))
        for i in range(2):
            line = await channel.reader.readline()
            match = re.fullmatch(rb"\?echo (1|2)\n", line)
            assert match
            channel.writer.write(b"#echo value " + match.group(1) + b"\n")
            channel.writer.write(b"!echo ok " + match.group(1) + b"\n")
        result1 = await future1
        assert result1 == ([b"1"], [Message.inform("echo", b"value", b"1")])
        result2 = await future2
        assert result2 == ([b"2"], [Message.inform("echo", b"value", b"2")])


# Workaround for https://github.com/python/cpython/issues/109538
@pytest.mark.filterwarnings("ignore:.*StreamWriter.__del__:pytest.PytestUnraisableExceptionWarning")
class TestUnclosedClient:
    async def body(self) -> None:
        # We can't use the existing fixtures, because their cleanup depends
        # on the event loop still running, and we're going to close the loop
        # during the test.
        def callback(reader, writer):
            client_queue.put_nowait((reader, writer))

        client_queue: _ClientQueue = asyncio.Queue()
        server = await asyncio.start_server(callback, "::1", 7777)
        DummyClient("::1", 7777)
        (reader, writer) = await client_queue.get()
        writer.close()
        server.close()
        await server.wait_closed()

    @pytest.mark.filterwarnings("ignore:unclosed transport:ResourceWarning")
    @pytest.mark.filterwarnings("ignore:loop is closed:ResourceWarning")
    def test(self) -> None:
        loop = async_solipsism.EventLoop()
        with pytest.warns(ResourceWarning, match="unclosed Client"):
            loop.run_until_complete(self.body())
            loop.close()
            # Run a few times for PyPy's benefit
            gc.collect()
            gc.collect()
