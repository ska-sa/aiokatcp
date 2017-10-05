"""Tests for :mod:`aiokatcp.sensor`. Most of the testing is done indirectly
in :mod:`aiokatcp.test.test_server`.
"""

import gc

import asynctest

from aiokatcp.sensor import Sensor, SensorSampler


class TestSensorSampling(asynctest.TestCase):
    async def test_unclosed_sampler(self):
        sensor = Sensor(int, 'sensor')
        sampler = SensorSampler.factory(
            sensor, lambda sensor, reading: None,
            self.loop, SensorSampler.Strategy.EVENT)
        with self.assertWarns(ResourceWarning):
            del sensor
            del sampler
            # Run gc twice because PyPy sometimes needs this.
            gc.collect()
            gc.collect()
