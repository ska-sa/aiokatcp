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

"""Tests for :mod:`aiokatcp.sensor`. Most of the testing is done indirectly
in :mod:`aiokatcp.test.test_server`.
"""

import gc
import unittest

import asynctest

from aiokatcp.sensor import Sensor, SensorSampler, SensorSet


class TestSensorState(unittest.TestCase):
    def test_valid_value(self):
        Status = Sensor.Status
        self.assertFalse(Status.UNKNOWN.valid_value())
        self.assertTrue(Status.NOMINAL.valid_value())
        self.assertTrue(Status.WARN.valid_value())
        self.assertTrue(Status.ERROR.valid_value())
        self.assertFalse(Status.FAILURE.valid_value())
        self.assertFalse(Status.UNREACHABLE.valid_value())
        self.assertFalse(Status.INACTIVE.valid_value())


class TestSensor(unittest.TestCase):
    def test_status_func(self):
        def status_func(value):
            return Sensor.Status.WARN if value & 1 else Sensor.Status.ERROR

        sensor = Sensor(int, 'sensor', status_func=status_func)
        self.assertEqual(sensor.status, Sensor.Status.UNKNOWN)
        sensor.value = 1
        self.assertEqual(sensor.status, Sensor.Status.WARN)
        sensor.value = 2
        self.assertEqual(sensor.status, Sensor.Status.ERROR)
        sensor.set_value(1, Sensor.Status.NOMINAL)
        self.assertEqual(sensor.status, Sensor.Status.NOMINAL)


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


class TestSensorSet(unittest.TestCase):
    def setUp(self):
        self.ss = SensorSet()
        self.callback = unittest.mock.MagicMock()
        self.ss.add_remove_callback(self.callback)
        self.sensors = [Sensor(int, 'name{}'.format(i)) for i in range(5)]
        # A different set of sensors with the same names
        self.alt_sensors = [Sensor(float, 'name{}'.format(i)) for i in range(5)]
        self.ss.add(self.sensors[0])

    def _assert_sensors(self, ss, sensors):
        """Assert that `ss` has the same sensors as `sensors`"""
        ordered = sorted(ss.values(), key=lambda x: x.name)
        self.assertEqual(ordered, sensors)

    def test_construct(self):
        """Test that setUp put things into the right state."""
        self._assert_sensors(self.ss, [self.sensors[0]])

    def test_add(self):
        # Add a new one
        self.ss.add(self.sensors[1])
        self._assert_sensors(self.ss, [self.sensors[0], self.sensors[1]])
        # Add the same one
        self.ss.add(self.sensors[0])
        self._assert_sensors(self.ss, [self.sensors[0], self.sensors[1]])
        # Replace one
        self.callback.assert_not_called()
        self.ss.add(self.alt_sensors[1])
        self._assert_sensors(self.ss, [self.sensors[0], self.alt_sensors[1]])
        self.callback.assert_called_once_with(self.sensors[1])

    def test_remove(self):
        # Try to remove non-existent name
        with self.assertRaises(KeyError):
            self.ss.remove(self.sensors[4])
        # Try to remove one with the same name as an existing one
        with self.assertRaises(KeyError):
            self.ss.remove(self.alt_sensors[0])
        self._assert_sensors(self.ss, [self.sensors[0]])
        # Remove one
        self.callback.assert_not_called()
        self.ss.remove(self.sensors[0])
        self._assert_sensors(self.ss, [])
        self.callback.assert_called_once_with(self.sensors[0])

    def test_discard(self):
        # Try to remove non-existent name
        self.ss.discard(self.sensors[4])
        # Try to remove one with the same name as an existing one
        self.ss.discard(self.alt_sensors[0])
        self._assert_sensors(self.ss, [self.sensors[0]])
        # Remove one
        self.callback.assert_not_called()
        self.ss.discard(self.sensors[0])
        self._assert_sensors(self.ss, [])
        self.callback.assert_called_once_with(self.sensors[0])

    def test_clear(self):
        self.ss.add(self.sensors[1])
        self.ss.clear()
        self._assert_sensors(self.ss, [])
        self.callback.assert_any_call(self.sensors[0])
        self.callback.assert_any_call(self.sensors[1])

    def test_popitem(self):
        self.ss.add(self.sensors[1])
        items = []
        try:
            for _ in range(100):   # To prevent infinite loop if it's broken
                items.append(self.ss.popitem())
        except KeyError:
            pass
        items.sort(key=lambda x: x[0])
        self.assertEqual(items, [('name0', self.sensors[0]), ('name1', self.sensors[1])])
        self.callback.assert_any_call(self.sensors[0])
        self.callback.assert_any_call(self.sensors[1])

    def test_pop_absent(self):
        # Non-existent name
        with self.assertRaises(KeyError):
            self.ss.pop('name4')
        # Non-existent with defaults
        self.assertIsNone(self.ss.pop('name4', None))
        self.assertEqual(self.ss.pop('name4', 'foo'), 'foo')
        # Remove one
        self.callback.assert_not_called()
        self.assertIs(self.ss.pop('name0'), self.sensors[0])
        self._assert_sensors(self.ss, [])
        self.callback.assert_called_once_with(self.sensors[0])

    def test_delitem(self):
        # Try to remove non-existent name
        with self.assertRaises(KeyError):
            del self.ss['name4']
        self._assert_sensors(self.ss, [self.sensors[0]])
        # Remove one
        self.callback.assert_not_called()
        del self.ss['name0']
        self._assert_sensors(self.ss, [])
        self.callback.assert_called_once_with(self.sensors[0])

    def test_getitem(self):
        # Non-existing name
        with self.assertRaises(KeyError):
            self.ss['name4']
        # Existing name
        self.assertIs(self.ss['name0'], self.sensors[0])

    def test_get(self):
        # Non-existing name
        self.assertIsNone(self.ss.get('name4'))
        self.assertIsNone(self.ss.get('name4', None))
        self.assertEqual(self.ss.get('name4', 'foo'), 'foo')
        # Existing name
        self.assertIs(self.ss.get('name0'), self.sensors[0])

    def test_len(self):
        self.assertEqual(len(self.ss), 1)
        self.ss.add(self.sensors[1])
        self.assertEqual(len(self.ss), 2)

    def test_contains(self):
        self.assertIn(self.sensors[0], self.ss)
        self.assertNotIn(self.alt_sensors[0], self.ss)
        self.assertNotIn(self.sensors[1], self.ss)

    def test_bool(self):
        self.assertTrue(self.ss)
        self.ss.clear()
        self.assertFalse(self.ss)

    def test_keys(self):
        self.ss.add(self.sensors[1])
        self.assertEqual(sorted(self.ss.keys()), ['name0', 'name1'])

    def test_values(self):
        self.ss.add(self.sensors[1])
        self.assertEqual(sorted(self.ss.values(), key=lambda x: x.name),
                         [self.sensors[0], self.sensors[1]])

    def test_items(self):
        self.ss.add(self.sensors[1])
        self.assertEqual(sorted(self.ss.items()), [
            ('name0', self.sensors[0]),
            ('name1', self.sensors[1])
        ])

    def test_iter(self):
        self.assertEqual(sorted(iter(self.ss)), ['name0'])

    def test_copy(self):
        self.assertEqual(self.ss.copy(), {'name0': self.sensors[0]})
