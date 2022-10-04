#!/usr/bin/env python3

# Copyright 2019 National Research Foundation (SARAO)
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

import argparse
import asyncio
import logging
import signal

import aiokatcp


class MirrorServer(aiokatcp.DeviceServer):
    VERSION = "mirror-1.0"
    BUILD_STATE = "mirror-1.0.0"


class MirrorWatcher(aiokatcp.SensorWatcher):
    def __init__(self, client: aiokatcp.Client, server: aiokatcp.DeviceServer):
        super().__init__(client)
        self.server = server
        self._interface_stale = False

    def sensor_added(
        self, name: str, description: str, units: str, type_name: str, *args: bytes
    ) -> None:
        super().sensor_added(name, description, units, type_name, *args)
        self.server.sensors.add(self.sensors[name])
        self._interface_stale = True
        logging.info("Added sensor %s", name)

    def sensor_removed(self, name: str) -> None:
        del self.server.sensors[name]
        self._interface_stale = True
        super().sensor_removed(name)
        logging.info("Removed sensor %s", name)

    def batch_stop(self) -> None:
        if self._interface_stale:
            self.server.mass_inform("interface-changed", "sensor-list")
            self._interface_stale = False
            logging.info("Sent interface-changed")


async def main(args: argparse.Namespace):
    client = aiokatcp.Client(args.host, args.port)
    server = MirrorServer("127.0.0.1", args.local_port)
    await server.start()

    watcher = MirrorWatcher(client, server)
    client.add_sensor_watcher(watcher)

    asyncio.get_event_loop().add_signal_handler(signal.SIGINT, server.halt)
    await server.join()
    client.close()
    await client.wait_closed()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mirror another server's sensors")
    parser.add_argument("host")
    parser.add_argument("port", type=int)
    parser.add_argument("local_port", type=int)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args))
    loop.close()
