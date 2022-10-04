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

import asyncio.base_events
import enum
import gc
import logging
import re
import unittest
import unittest.mock
from typing import AsyncGenerator, Tuple, Type, Union, cast
from unittest.mock import call

import async_solipsism
import pytest

from aiokatcp import (
    AbstractSensorWatcher,
    Client,
    FailReply,
    InvalidReply,
    Message,
    ProtocolError,
    Sensor,
    SensorWatcher,
    SyncState,
    encode,
)

_ClientQueue = Union["asyncio.Queue[Tuple[asyncio.StreamReader, asyncio.StreamWriter]]"]


@pytest.fixture
def event_loop():
    loop = async_solipsism.EventLoop()
    yield loop
    loop.close()


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
def client_queue() -> _ClientQueue:
    """Queue to which client connections are added as they connection to :meth:`server`."""
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


async def test_request_ok(channel, event_loop) -> None:
    await channel.wait_connected()
    future = event_loop.create_task(channel.client.request("echo"))
    assert await channel.reader.readline() == b"?echo[1]\n"
    channel.writer.write(b"!echo[1] ok\n")
    result = await future
    assert result == ([], [])
    # Again, with arguments. This also tests MID incrementing, non-ASCII
    # characters, and null escaping.
    arg = b"h\xaf\xce\0"
    arg_esc = b"h\xaf\xce\\0"  # katcp escaping
    future = event_loop.create_task(channel.client.request("echo", b"123", arg))
    assert await channel.reader.readline() == b"?echo[2] 123 " + arg_esc + b"\n"
    channel.writer.write(b"!echo[2] ok 123 " + arg_esc + b"\n")
    result = await future
    assert result == ([b"123", arg], [])


async def test_request_fail(channel, event_loop) -> None:
    await channel.wait_connected()
    future = event_loop.create_task(channel.client.request("failme"))
    assert await channel.reader.readline() == b"?failme[1]\n"
    channel.writer.write(b"!failme[1] fail Error\\_message\n")
    with pytest.raises(FailReply, match="^Error message$"):
        await future


async def test_request_fail_no_msg(channel, event_loop) -> None:
    await channel.wait_connected()
    future = event_loop.create_task(channel.client.request("failme"))
    assert await channel.reader.readline() == b"?failme[1]\n"
    channel.writer.write(b"!failme[1] fail\n")
    with pytest.raises(FailReply, match="^$"):
        await future


async def test_request_fail_msg_bad_encoding(channel, event_loop) -> None:
    await channel.wait_connected()
    future = event_loop.create_task(channel.client.request("failme"))
    assert await channel.reader.readline() == b"?failme[1]\n"
    channel.writer.write(b"!failme[1] fail \xaf\n")
    with pytest.raises(FailReply, match="^\uFFFD$"):
        await future


async def test_request_invalid(channel, event_loop) -> None:
    await channel.wait_connected()
    future = event_loop.create_task(channel.client.request("invalid-request"))
    assert await channel.reader.readline() == b"?invalid-request[1]\n"
    channel.writer.write(b"!invalid-request[1] invalid Unknown\\_request\n")
    with pytest.raises(InvalidReply, match="^Unknown request$"):
        await future


async def test_request_no_code(channel, event_loop) -> None:
    await channel.wait_connected()
    future = event_loop.create_task(channel.client.request("invalid-request"))
    assert await channel.reader.readline() == b"?invalid-request[1]\n"
    channel.writer.write(b"!invalid-request[1]\n")
    with pytest.raises(InvalidReply, match="^$"):
        await future


async def test_request_with_informs(channel, event_loop) -> None:
    await channel.wait_connected()
    future = event_loop.create_task(channel.client.request("help"))
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


async def test_unsolicited_reply(channel, event_loop, caplog) -> None:
    await channel.wait_connected()
    future = event_loop.create_task(channel.client.request("echo"))
    with caplog.at_level(logging.DEBUG, "aiokatcp.client"):
        channel.writer.write(b"!surprise[3]\n!echo[1] ok\n")
        await future
    assert re.search("Received .* with unknown message ID", caplog.text)


async def test_receive_request(channel, event_loop, caplog) -> None:
    await channel.wait_connected()
    future = event_loop.create_task(channel.client.request("echo"))
    with caplog.at_level(logging.INFO, "aiokatcp.client"):
        channel.writer.write(b"?surprise\n!echo[1] ok\n")
        await future
    assert re.search("Received unexpected request", caplog.text)


async def test_reply_no_mid(channel, event_loop, caplog) -> None:
    await channel.wait_connected()
    future = event_loop.create_task(channel.client.request("echo"))
    with caplog.at_level(logging.INFO, "aiokatcp.client"):
        channel.writer.write(b"!surprise ok\n!echo[1] ok\n")
        await future
    assert re.search("Received unexpected REPLY .* without message ID", caplog.text)


async def test_context_manager(channel) -> None:
    async with channel.client:
        pass
    await channel.client.wait_closed()


async def test_connect(server, client_queue, event_loop) -> None:
    host, port = server.sockets[0].getsockname()[:2]
    client_task = event_loop.create_task(DummyClient.connect(host, port))
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


async def test_bad_address(event_loop, caplog) -> None:
    client = DummyClient("invalid.invalid", 1)
    try:
        with caplog.at_level(logging.WARNING, "aiokatcp.client"):
            task = event_loop.create_task(client.wait_connected())
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
        assert await channel.reader.readline() == b"?sensor-sampling[4] temp,pressure auto\n"
        assert channel.watcher.mock_calls == [
            call.batch_start(),
            call.sensor_added("temp", "Temperature", "F", "float"),
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
        assert await channel.reader.readline() == b"?sensor-sampling[4] temp,pressure auto\n"

        channel.writer.write(rb"!sensor-sampling[4] fail Unknown\_sensor\_'pressure'" + b"\n")
        assert await channel.reader.readline() == b"?sensor-sampling[5] temp auto\n"
        channel.writer.write(
            b"#sensor-status 123456790.0 1 temp warn 451.0\n" b"!sensor-sampling[5] ok temp auto\n"
        )
        assert await channel.reader.readline() == b"?sensor-sampling[6] pressure auto\n"
        channel.writer.write(rb"!sensor-sampling[6] fail Unknown\_sensor\_'pressure'" + b"\n")
        await asyncio.sleep(1)

        assert channel.watcher.mock_calls == [
            call.batch_start(),
            call.sensor_added("temp", "Temperature", "F", "float"),
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
            call.state_updated(SyncState.SYNCING),
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


class DummySensorWatcher(SensorWatcher):
    def rewrite_name(self, name: str) -> str:
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
        watcher.batch_stop()
        assert len(watcher.sensors) == 1
        sensor = watcher.sensors["test_foo"]
        assert sensor.name == "test_foo"
        assert sensor.description == "A sensor"
        assert sensor.units == "F"
        assert sensor.stype == float
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
        watcher.batch_stop()
        assert len(watcher.sensors) == 0

    def test_sensor_updated(self, watcher: DummySensorWatcher) -> None:
        self.test_sensor_added(watcher)

        watcher.batch_start()
        watcher.sensor_updated("foo", b"12.5", Sensor.Status.WARN, 1234567890.0)
        watcher.batch_stop()
        sensor = watcher.sensors["test_foo"]
        assert sensor.value == 12.5
        assert sensor.status == Sensor.Status.WARN
        assert sensor.timestamp == 1234567890.0

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
        watcher.sensor_updated("bar", b"123.0", Sensor.Status.WARN, 1234567890.0)
        watcher.batch_stop()
        watcher.logger.warning.assert_called_once_with(  # type: ignore
            "Received update for unknown sensor %s", "bar"
        )

    def test_state_updated(self, watcher: DummySensorWatcher) -> None:
        self.test_sensor_added(watcher)

        watcher.state_updated(SyncState.SYNCING)
        assert not watcher.synced.is_set()
        assert len(watcher.sensors) == 1
        assert watcher.sensors["test_foo"].status == Sensor.Status.UNKNOWN

        watcher.state_updated(SyncState.SYNCED)
        assert watcher.synced.is_set()
        assert len(watcher.sensors) == 1
        assert watcher.sensors["test_foo"].status == Sensor.Status.UNKNOWN

        watcher.state_updated(SyncState.DISCONNECTED)
        assert not watcher.synced.is_set()
        # Disconnecting should set all sensors to UNREACHABLE
        assert watcher.sensors["test_foo"].status == Sensor.Status.UNREACHABLE


@pytest.mark.channel_args(auto_reconnect=False)
class TestClientNoReconnect:
    async def test_unparsable_protocol(self, channel) -> None:
        channel.writer.write(b"#version-connect katcp-protocol notvalid\n")
        assert await channel.reader.read() == b""
        with pytest.raises(ProtocolError):
            await channel.client.wait_connected()

    async def test_bad_protocol(self, channel, event_loop) -> None:
        # Different approach to test_unparsable_protocol, to get more coverage
        wait_task = event_loop.create_task(channel.client.wait_connected())
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

    async def test_connect_failed(self, server, client_queue, event_loop) -> None:
        host, port = server.sockets[0].getsockname()[:2]
        client_task = event_loop.create_task(DummyClient.connect(host, port, auto_reconnect=False))
        (reader, writer) = await client_queue.get()
        await asyncio.sleep(1)
        assert not client_task.done()
        writer.close()
        with pytest.raises(ConnectionAbortedError):
            await client_task


class TestClientNoMidSupport:
    async def test_single(self, channel, event_loop) -> None:
        channel.writer.write(b"#version-connect katcp-protocol 5.1-M\n")
        await channel.client.wait_connected()
        future = event_loop.create_task(channel.client.request("echo"))
        assert await channel.reader.readline() == b"?echo\n"
        channel.writer.write(b"#echo an\\_inform\n")
        channel.writer.write(b"!echo ok\n")
        result = await future
        assert result == ([], [Message.inform("echo", b"an inform")])

    async def test_concurrent(self, channel, event_loop) -> None:
        channel.writer.write(b"#version-connect katcp-protocol 5.1-M\n")
        await channel.client.wait_connected()
        future1 = event_loop.create_task(channel.client.request("echo", 1))
        future2 = event_loop.create_task(channel.client.request("echo", 2))
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

    def test(self) -> None:
        loop = async_solipsism.EventLoop()
        with pytest.warns(ResourceWarning, match="unclosed Client"):
            loop.run_until_complete(self.body())
            loop.close()
            # Run a few times for PyPy's benefit
            gc.collect()
            gc.collect()
