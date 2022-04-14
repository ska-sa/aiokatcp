# Copyright 2017, 2022 National Research Foundation (SARAO)
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

import pytest

from aiokatcp.sensor import Sensor, SensorSampler, SensorSet


@pytest.mark.parametrize(
    'status,valid',
    [
        (Sensor.Status.UNKNOWN, False),
        (Sensor.Status.NOMINAL, True),
        (Sensor.Status.WARN, True),
        (Sensor.Status.ERROR, True),
        (Sensor.Status.FAILURE, False),
        (Sensor.Status.UNREACHABLE, False),
        (Sensor.Status.INACTIVE, False)
    ]
)
def test_sensor_state_valid_value(status, valid):
    assert status.valid_value() is valid


def test_sensor_status_func():
    def status_func(value):
        return Sensor.Status.WARN if value & 1 else Sensor.Status.ERROR

    sensor = Sensor(int, 'sensor', status_func=status_func)
    assert sensor.status == Sensor.Status.UNKNOWN
    sensor.value = 1
    assert sensor.status == Sensor.Status.WARN
    sensor.value = 2
    assert sensor.status == Sensor.Status.ERROR
    sensor.set_value(1, Sensor.Status.NOMINAL)
    assert sensor.status == Sensor.Status.NOMINAL


async def test_unclosed_sampler(event_loop):
    sensor = Sensor(int, 'sensor')
    sampler = SensorSampler.factory(
        sensor, lambda sensor, reading: None,
        event_loop, SensorSampler.Strategy.EVENT)
    with pytest.warns(ResourceWarning):
        del sensor
        del sampler
        # Run gc twice because PyPy sometimes needs this.
        gc.collect()
        gc.collect()


@pytest.fixture
def add_callback():
    return unittest.mock.MagicMock()


@pytest.fixture
def remove_callback():
    return unittest.mock.MagicMock()


@pytest.fixture
def sensors():
    return [Sensor(int, f'name{i}') for i in range(5)]


@pytest.fixture
def alt_sensors():
    # A different set of sensors with the same names
    return [Sensor(float, f'name{i}') for i in range(5)]


@pytest.fixture
def ss(add_callback, remove_callback, sensors):
    ss = SensorSet()
    ss.add(sensors[0])
    ss.add_add_callback(add_callback)
    ss.add_remove_callback(remove_callback)
    return ss


def _get_sensors(ss):
    """Return sensors in a :class:`SensorSet` sorted by name."""
    return sorted(ss.values(), key=lambda x: x.name)


class TestSensorSet:
    def test_construct(self, ss, sensors):
        """Test that constructor put things into the right state."""
        assert _get_sensors(ss) == [sensors[0]]

    def test_add(self, ss, sensors, alt_sensors, add_callback, remove_callback):
        # Add a new one
        ss.add(sensors[1])
        assert _get_sensors(ss) == [sensors[0], sensors[1]]
        add_callback.assert_called_with(sensors[1])
        # Add the same one
        add_callback.reset_mock()
        ss.add(sensors[0])
        assert _get_sensors(ss) == [sensors[0], sensors[1]]
        add_callback.assert_not_called()
        # Replace one
        remove_callback.assert_not_called()
        ss.add(alt_sensors[1])
        assert _get_sensors(ss) == [sensors[0], alt_sensors[1]]
        remove_callback.assert_called_once_with(sensors[1])
        add_callback.assert_called_once_with(alt_sensors[1])

    def test_remove(self, ss, sensors, alt_sensors, remove_callback):
        # Try to remove non-existent name
        with pytest.raises(KeyError):
            ss.remove(sensors[4])
        # Try to remove one with the same name as an existing one
        with pytest.raises(KeyError):
            ss.remove(alt_sensors[0])
        assert _get_sensors(ss) == [sensors[0]]
        # Remove one
        remove_callback.assert_not_called()
        ss.remove(sensors[0])
        assert _get_sensors(ss) == []
        remove_callback.assert_called_once_with(sensors[0])

    def test_discard(self, ss, sensors, alt_sensors, remove_callback):
        # Try to remove non-existent name
        ss.discard(sensors[4])
        # Try to remove one with the same name as an existing one
        ss.discard(alt_sensors[0])
        assert _get_sensors(ss) == [sensors[0]]
        # Remove one
        remove_callback.assert_not_called()
        ss.discard(sensors[0])
        assert _get_sensors(ss) == []
        remove_callback.assert_called_once_with(sensors[0])

    def test_clear(self, ss, sensors, remove_callback):
        ss.add(sensors[1])
        ss.clear()
        assert _get_sensors(ss) == []
        remove_callback.assert_any_call(sensors[0])
        remove_callback.assert_any_call(sensors[1])

    def test_popitem(self, ss, sensors, remove_callback):
        ss.add(sensors[1])
        items = []
        try:
            for _ in range(100):   # To prevent infinite loop if it's broken
                items.append(ss.popitem())
        except KeyError:
            pass
        items.sort(key=lambda x: x[0])
        assert items == [('name0', sensors[0]), ('name1', sensors[1])]
        remove_callback.assert_any_call(sensors[0])
        remove_callback.assert_any_call(sensors[1])

    def test_pop_absent(self, ss, sensors, remove_callback):
        # Non-existent name
        with pytest.raises(KeyError):
            ss.pop('name4')
        # Non-existent with defaults
        assert ss.pop('name4', None) is None
        assert ss.pop('name4', 'foo') == 'foo'
        # Remove one
        remove_callback.assert_not_called()
        assert ss.pop('name0') is sensors[0]
        assert _get_sensors(ss) == []
        remove_callback.assert_called_once_with(sensors[0])

    def test_delitem(self, ss, sensors, remove_callback):
        # Try to remove non-existent name
        with pytest.raises(KeyError):
            del ss['name4']
        assert _get_sensors(ss) == [sensors[0]]
        # Remove one
        remove_callback.assert_not_called()
        del ss['name0']
        assert _get_sensors(ss) == []
        remove_callback.assert_called_once_with(sensors[0])

    def test_getitem(self, ss, sensors):
        # Non-existing name
        with pytest.raises(KeyError):
            ss['name4']
        # Existing name
        assert ss['name0'] is sensors[0]

    def test_get(self, ss, sensors):
        # Non-existing name
        assert ss.get('name4') is None
        assert ss.get('name4', None) is None
        assert ss.get('name4', 'foo') == 'foo'
        # Existing name
        assert ss.get('name0') is sensors[0]

    def test_len(self, ss, sensors):
        assert len(ss) == 1
        ss.add(sensors[1])
        assert len(ss) == 2

    def test_contains(self, ss, sensors, alt_sensors):
        assert sensors[0] in ss
        assert alt_sensors[0] not in ss
        assert sensors[1] not in ss

    def test_bool(self, ss):
        assert ss
        ss.clear()
        assert not ss

    def test_keys(self, ss, sensors):
        ss.add(sensors[1])
        assert sorted(ss.keys()) == ['name0', 'name1']

    def test_values(self, ss, sensors):
        ss.add(sensors[1])
        assert sorted(ss.values(), key=lambda x: x.name) == [sensors[0], sensors[1]]

    def test_items(self, ss, sensors):
        ss.add(sensors[1])
        assert sorted(ss.items()) == [
            ('name0', sensors[0]),
            ('name1', sensors[1])
        ]

    def test_iter(self, ss):
        assert sorted(iter(ss)) == ['name0']

    def test_copy(self, ss, sensors):
        assert ss.copy() == {'name0': sensors[0]}

    def test_remove_callbacks(self, ss, sensors, add_callback, remove_callback):
        ss.remove_remove_callback(remove_callback)
        ss.remove_add_callback(add_callback)
        ss.add(sensors[0])
        ss.remove(sensors[0])
        remove_callback.assert_not_called()
        add_callback.assert_not_called()
