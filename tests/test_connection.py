# Copyright 2017, 2020, 2022 National Research Foundation (SARAO)
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
import logging
import re
from typing import Optional, Tuple  # noqa: F401
from unittest import mock

import async_solipsism
import pytest

from aiokatcp.connection import Connection
from aiokatcp.core import Message


@pytest.fixture
def event_loop_policy():
    return async_solipsism.EventLoopPolicy()


@pytest.fixture
def owner(event_loop):
    owner = mock.MagicMock()
    owner.loop = event_loop
    owner.handle_message = mock.MagicMock(side_effect=_ok_handler)
    # Close the transport on EOF
    owner._eof_received = mock.MagicMock(return_value=False)
    return owner


@pytest.fixture
def connection_queue() -> "asyncio.Queue[Connection]":
    return asyncio.Queue()


@pytest.fixture
async def server(owner, connection_queue):
    def protocol_factory():
        # Very low limit to test overrun handling
        conn = Connection(owner, is_server=True, limit=25)
        connection_queue.put_nowait(conn)
        return conn

    server = await asyncio.get_running_loop().create_server(protocol_factory, "::1", 7777)
    yield server
    server.close()
    await server.wait_closed()


# Note: we don't reference the 'client_reader_writer' fixture, but it must
# be listed as a dependency so that the connection_queue will be populated.
@pytest.fixture
async def server_connection(client_reader_writer, connection_queue):
    conn = await connection_queue.get()
    # connection_queue is populated as soon as the Connection is created, but
    # we need to wait at least until connection_made has been called before we
    # can actually use it.
    await asyncio.sleep(1)
    yield conn
    conn.close()
    await conn.wait_closed()


async def _close_writer(writer):
    writer.close()
    try:
        await writer.wait_closed()
    except ConnectionError:
        # If the stream closed due to an exception, wait_closed
        # will raise that exception.
        pass


@pytest.fixture
async def client_reader_writer(server):
    reader, writer = await asyncio.open_connection("::1", 7777)
    yield reader, writer
    await _close_writer(writer)


@pytest.fixture
def client_reader(client_reader_writer):
    return client_reader_writer[0]


@pytest.fixture
def client_writer(client_reader_writer):
    return client_reader_writer[1]


async def _ok_reply(conn, msg):
    # Give test code virtual time to run between request and reply.
    await asyncio.sleep(0.5)
    conn.write_message(Message.reply_to_request(msg, "ok"))
    await conn.drain()


def _ok_handler(conn, msg):
    asyncio.get_event_loop().create_task(_ok_reply(conn, msg))


async def test_write_message(server_connection, client_reader) -> None:
    conn = server_connection
    conn.write_message(Message.reply("ok", mid=1))
    await conn.drain()
    line = await client_reader.readline()
    assert line == b"!ok[1]\n"


async def test_basic(owner, server_connection, client_reader, client_writer) -> None:
    conn = server_connection
    client_writer.write(b"?watchdog[2]\n")
    await client_writer.drain()
    await asyncio.sleep(1)
    owner.handle_message.assert_called_with(conn, Message.request("watchdog", mid=2))
    reply = await client_reader.readline()
    assert reply == b"!watchdog[2] ok\n"
    # Check that it exits when the client disconnects its write end
    client_writer.write_eof()
    await client_writer.drain()
    await conn.wait_closed()


async def test_disconnected(owner, server_connection, client_writer, caplog) -> None:
    conn = server_connection
    client_writer.write(b"?watchdog[2]\n?watchdog[3]\n?watchdog[4]\n")
    # Close the socket before the replies can be sent.
    client_writer.close()
    with caplog.at_level(logging.WARNING, logger="aiokatcp.connection"):
        # Give time for the first two watchdogs and the close to go through
        await asyncio.sleep(1.25)
        await conn.wait_closed()
    owner.handle_message.assert_called_with(conn, Message.request("watchdog", mid=4))
    # Note: should only be one warning, not two
    assert 1 == len(caplog.records)
    assert re.fullmatch(
        r"Connection closed before message could be sent \[.*\]", caplog.records[0].message
    )
    # Allow the final watchdog to go through. This just provides test coverage
    # that Connection.write_message handles the writer having already gone away.
    await asyncio.sleep(10)


async def test_malformed(owner, server_connection, client_writer, caplog) -> None:
    conn = server_connection
    client_writer.write(b"malformed\n")
    client_writer.write_eof()
    with caplog.at_level(logging.WARNING, "aiokatcp.connection"):
        # Wait for the close to go through
        await conn.wait_closed()
        owner.handle_message.assert_not_called()
    assert len(caplog.records) == 1
    assert re.match("Malformed message received", caplog.records[0].message)


async def test_read_overrun(owner, server_connection, client_writer, caplog) -> None:
    client_writer.write(b"?foo a_string_that_doesnt_fit_in_the_buffer\n?foo short_string\n")
    with caplog.at_level(logging.WARNING, "aiokatcp.connection"):
        await asyncio.sleep(10)
    owner.handle_message.assert_called_once_with(
        server_connection, Message.request("foo", b"short_string")
    )
    assert len(caplog.records) == 1
    assert re.match("Malformed message received", caplog.records[0].message)


async def test_read_partial(owner, server_connection, client_writer, caplog) -> None:
    client_writer.write(b"?foo nonewline")
    client_writer.write_eof()
    with caplog.at_level(logging.WARNING, "aiokatcp.connection"):
        await asyncio.sleep(10)
    owner.handle_message.assert_not_called()
    assert len(caplog.records) == 1
    assert re.match("EOF received on connection with partial message", caplog.records[0].message)


async def test_close_early(server_connection) -> None:
    conn = server_connection
    conn.close()
    await conn.wait_closed()


async def test_close(server_connection) -> None:
    conn = server_connection
    await asyncio.sleep(1)
    conn.close()
    await conn.wait_closed()


async def test_exception(owner, server_connection, client_writer, caplog) -> None:
    owner.handle_message.side_effect = RuntimeError("test error")
    conn = server_connection
    client_writer.write(b"?watchdog[2]\n")
    client_writer.close()
    with caplog.at_level(logging.ERROR, "aiokatcp.connection"):
        await conn.wait_closed()
    assert len(caplog.records) == 1
    assert re.match("Exception in message handler", caplog.records[0].message)
    assert re.search("test error", caplog.text)
