#!/usr/bin/env python3

import asyncio
import logging
import enum
from typing import Tuple

import aiokatcp


class Foo(enum.Enum):
    ABC_DEF = 1
    GHI_K = 2


class Server(aiokatcp.DeviceServer):
    VERSION = 'testapi-1.0'
    BUILD_STATE = 'testapi-1.0.1'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        sensor = aiokatcp.Sensor(int, 'counter-queries', 'number of ?counter queries',
                                 default=0,
                                 initial_status=aiokatcp.Sensor.Status.NOMINAL)
        self.sensors.add(sensor)
        sensor = aiokatcp.Sensor(Foo, 'foo', 'nonsense')
        self.sensors.add(sensor)

    async def request_echo(self, ctx, *args: str) -> Tuple:
        """Return the arguments to the caller"""
        return tuple(args)

    async def request_sleep(self, ctx, time: float) -> None:
        """Sleep for some amount of time"""
        await asyncio.sleep(time, loop=self.loop)

    async def request_fail(self, ctx, arg: str) -> None:
        """Request that always returns a failure reply"""
        raise aiokatcp.FailReply(arg + ' is no good')

    async def request_crash(self, ctx) -> None:
        """Request that always raises an exception"""
        raise RuntimeError("help I've fallen over and can't get up")

    async def request_counter(self, ctx) -> None:
        """Increment counter-queries"""
        self.sensors['counter-queries'].value += 1


async def main():
    server = Server('localhost', 4444)
    handler = Server.LogHandler(server)
    logging.getLogger().addHandler(handler)
    await server.start()
    await server.join()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    loop = asyncio.get_event_loop()
    loop.set_debug(True)
    loop.run_until_complete(main())
    loop.close()
