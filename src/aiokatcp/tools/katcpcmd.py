#!/usr/bin/env python3

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

"""Command-line request tool"""

import asyncio
import logging
import signal
import argparse
import contextlib

import async_timeout

import aiokatcp


def text(msg: aiokatcp.Message) -> str:
    return bytes(msg).decode('utf-8', errors='backslashreplace')


class CmdClient(aiokatcp.Client):
    def unhandled_inform(self, msg: aiokatcp.Message) -> None:
        print(text(msg), end='')


async def async_main(args, host, port) -> int:
    try:
        with async_timeout.timeout(args.connect_timeout):
            client = await CmdClient.connect(host, port, auto_reconnect=False)
    except (OSError, aiokatcp.client.ProtocolError) as error:
        logging.error('Connection error: %s', error)
        return 1
    except asyncio.TimeoutError:
        logging.error('Timed out connecting to %s:%s', host, port)
        return 1
    async with client:
        try:
            with async_timeout.timeout(args.request_timeout):
                reply, informs = await client.request_raw(args.command, *args.args)
        except (OSError, aiokatcp.client.ProtocolError) as error:
            logging.error('Connection error: %s', error)
            return 1
        except asyncio.TimeoutError:
            logging.error('Request timed out after %gs', args.request_timeout)
        else:
            for msg in informs + [reply]:
                print(text(msg), end='')
            if not reply.reply_ok():
                return 2
    return 0


def main() -> int:
    logging.basicConfig(level=logging.WARNING)

    parser = argparse.ArgumentParser(
        description='Send a single katcp request and print the reply',
        usage='%(prog)s host:port command [args...]')
    parser.add_argument('endpoint')
    parser.add_argument('command')
    parser.add_argument('args', nargs='*')
    parser.add_argument('--connect-timeout', type=float, metavar='TIME', default=30,
                        help='Time to wait for a connection to be established')
    parser.add_argument('--request-timeout', type=float, metavar='TIME',
                        help='Time to wait for the request to complete')
    args = parser.parse_args()
    host_port = args.endpoint.rsplit(':', 1)
    if len(host_port) != 2:
        parser.error('missing port number in {}'.format(host_port))

    loop = asyncio.get_event_loop()
    with contextlib.closing(loop):
        main_task = loop.create_task(async_main(args, *host_port))
        loop.add_signal_handler(signal.SIGINT, main_task.cancel)
        try:
            return loop.run_until_complete(main_task)
        except asyncio.CancelledError:
            return 1
