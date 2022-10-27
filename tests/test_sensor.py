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
from typing import Optional
from unittest.mock import create_autospec

import pytest

from aiokatcp.sensor import AggregateSensor, Reading, Sensor, SensorSampler, SensorSet


@pytest.mark.parametrize(
    "status,valid",
    [
        (Sensor.Status.UNKNOWN, False),
        (Sensor.Status.NOMINAL, True),
        (Sensor.Status.WARN, True),
        (Sensor.Status.ERROR, True),
        (Sensor.Status.FAILURE, False),
        (Sensor.Status.UNREACHABLE, False),
        (Sensor.Status.INACTIVE, False),
    ],
)
def test_sensor_state_valid_value(status, valid):
    assert status.valid_value() is valid


def test_sensor_status_func():
    def status_func(value):
        return Sensor.Status.WARN if value & 1 else Sensor.Status.ERROR

    sensor = Sensor(int, "sensor", status_func=status_func)
    assert sensor.status == Sensor.Status.UNKNOWN
    sensor.value = 1
    assert sensor.status == Sensor.Status.WARN
    sensor.value = 2
    assert sensor.status == Sensor.Status.ERROR
    sensor.set_value(1, Sensor.Status.NOMINAL)
    assert sensor.status == Sensor.Status.NOMINAL


@pytest.fixture
def classic_observer():
    def classic(sensor, reading):
        pass

    return create_autospec(classic)


@pytest.fixture
def change_aware_observer():
    def change_aware(sensor, reading, old_reading):
        pass

    return create_autospec(change_aware)


def test_observer_sorting(classic_observer, change_aware_observer):
    """Check whether change-aware and classic ovserver callbacks are sorted appropriately."""
    sensor = Sensor(int, "my-sensor")
    sensor.value = 7

    sensor.attach(classic_observer)
    sensor.attach(change_aware_observer)

    old_reading = sensor.reading
    sensor.value = 12
    new_reading = sensor.reading

    classic_observer.assert_called_with(sensor, new_reading)
    change_aware_observer.assert_called_with(sensor, new_reading, old_reading)

    classic_observer.reset_mock()
    change_aware_observer.reset_mock()

    sensor.detach(classic_observer)
    sensor.detach(change_aware_observer)

    sensor.value = 42

    classic_observer.assert_not_called()
    change_aware_observer.assert_not_called()


async def test_unclosed_sampler(event_loop):
    sensor = Sensor(int, "sensor")
    sampler = SensorSampler.factory(
        sensor, lambda sensor, reading: None, event_loop, SensorSampler.Strategy.EVENT
    )
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
    return [Sensor(int, f"name{i}") for i in range(5)]


@pytest.fixture
def alt_sensors():
    # A different set of sensors with the same names
    return [Sensor(float, f"name{i}") for i in range(5)]


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
            for _ in range(100):  # To prevent infinite loop if it's broken
                items.append(ss.popitem())
        except KeyError:
            pass
        items.sort(key=lambda x: x[0])
        assert items == [("name0", sensors[0]), ("name1", sensors[1])]
        remove_callback.assert_any_call(sensors[0])
        remove_callback.assert_any_call(sensors[1])

    def test_pop_absent(self, ss, sensors, remove_callback):
        # Non-existent name
        with pytest.raises(KeyError):
            ss.pop("name4")
        # Non-existent with defaults
        assert ss.pop("name4", None) is None
        assert ss.pop("name4", "foo") == "foo"
        # Remove one
        remove_callback.assert_not_called()
        assert ss.pop("name0") is sensors[0]
        assert _get_sensors(ss) == []
        remove_callback.assert_called_once_with(sensors[0])

    def test_delitem(self, ss, sensors, remove_callback):
        # Try to remove non-existent name
        with pytest.raises(KeyError):
            del ss["name4"]
        assert _get_sensors(ss) == [sensors[0]]
        # Remove one
        remove_callback.assert_not_called()
        del ss["name0"]
        assert _get_sensors(ss) == []
        remove_callback.assert_called_once_with(sensors[0])

    def test_getitem(self, ss, sensors):
        # Non-existing name
        with pytest.raises(KeyError):
            ss["name4"]
        # Existing name
        assert ss["name0"] is sensors[0]

    def test_get(self, ss, sensors):
        # Non-existing name
        assert ss.get("name4") is None
        assert ss.get("name4", None) is None
        assert ss.get("name4", "foo") == "foo"
        # Existing name
        assert ss.get("name0") is sensors[0]

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
        assert sorted(ss.keys()) == ["name0", "name1"]

    def test_values(self, ss, sensors):
        ss.add(sensors[1])
        assert sorted(ss.values(), key=lambda x: x.name) == [sensors[0], sensors[1]]

    def test_items(self, ss, sensors):
        ss.add(sensors[1])
        assert sorted(ss.items()) == [("name0", sensors[0]), ("name1", sensors[1])]

    def test_iter(self, ss):
        assert sorted(iter(ss)) == ["name0"]

    def test_copy(self, ss, sensors):
        assert ss.copy() == {"name0": sensors[0]}

    def test_remove_callbacks(self, ss, sensors, add_callback, remove_callback):
        ss.remove_remove_callback(remove_callback)
        ss.remove_add_callback(add_callback)
        ss.add(sensors[0])
        ss.remove(sensors[0])
        remove_callback.assert_not_called()
        add_callback.assert_not_called()


class MyAgg(AggregateSensor):
    """A simple AggregateSensor subclass for testing."""

    def update_aggregate(
        self,
        updated_sensor: Optional[Sensor],
        reading: Optional[Reading],
        old_reading: Optional[Reading],
    ) -> Reading:
        """Return a known Reading."""
        return Reading(0, Sensor.Status.NOMINAL, 7)


@pytest.fixture
def agg_sensor(mocker, ss):
    """Mock out update_aggregate so we can check it's called appropriately."""

    def return_a_reading(*args, **kwargs):
        """Return an arbitrary Reading."""
        return Reading(0, Sensor.Status.WARN, 8)

    my_agg = MyAgg(target=ss, sensor_type=int, name="good-bad-ugly")
    mocker.patch.object(my_agg, "update_aggregate", side_effect=return_a_reading)
    return my_agg


class TestAggregateSensor:
    """Test operation of AggregateSensor."""

    def test_creation(self, agg_sensor, ss, sensors):
        """Check that creation happens properly, and correct initial value is set."""
        assert agg_sensor.target is ss
        assert agg_sensor.reading.timestamp == 0
        assert agg_sensor.reading.status == Sensor.Status.NOMINAL
        assert agg_sensor.reading.value == 7

    def test_sensor_added(self, agg_sensor, ss, sensors):
        """Check that the update function is called when a sensor is added."""
        ss.add(sensors[1])
        agg_sensor.update_aggregate.assert_called_with(sensors[1], sensors[1].reading, None)
        assert agg_sensor.reading.timestamp == 0
        assert agg_sensor.reading.status == Sensor.Status.WARN
        assert agg_sensor.reading.value == 8

    def test_sensor_removed(self, agg_sensor, ss, sensors):
        """Check that the update function is called for a removed sensor."""
        ss.remove(sensors[0])
        agg_sensor.update_aggregate.assert_called_with(sensors[0], None, sensors[0].reading)
        # Reset the counters to check subsequent changes do nothing.
        agg_sensor.update_aggregate.reset_mock()
        sensors[0].set_value(5, Sensor.Status.ERROR)
        agg_sensor.update_aggregate.assert_not_called()

    def test_sensor_value_changed(self, agg_sensor, ss, sensors):
        """Check that the update function is called for a sensor whose value has changed."""
        old_reading = sensors[0].reading
        sensors[0].set_value(7, Sensor.Status.WARN)
        agg_sensor.update_aggregate.assert_called_with(sensors[0], sensors[0].reading, old_reading)

    def test_aggregate_sensor_excluded(self, agg_sensor, mocker, ss, sensors):
        """Check that the aggregate sensor gets excluded if it's in the set itself."""
        mocker.patch.object(agg_sensor, "attach")
        mocker.patch.object(agg_sensor, "detach")
        ss.add(agg_sensor)
        agg_sensor.attach.assert_not_called()
        ss.remove(agg_sensor)
        agg_sensor.detach.assert_not_called()
