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
import weakref
from typing import List, Optional
from unittest import mock
from unittest.mock import create_autospec

import pytest

from aiokatcp.sensor import (
    AggregateSensor,
    Reading,
    Sensor,
    SensorSampler,
    SensorSet,
    SimpleAggregateSensor,
    _weak_callback,
)


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
def delta_observer():
    def delta(sensor, reading, old_reading):
        pass

    return create_autospec(delta)


def test_observer_sorting(classic_observer, delta_observer):
    """Check whether delta and classic observer callbacks are classified appropriately."""
    sensor = Sensor(int, "my-sensor")
    sensor.value = 7

    sensor.attach(classic_observer)
    sensor.attach(delta_observer)

    old_reading = sensor.reading
    sensor.value = 12
    new_reading = sensor.reading

    classic_observer.assert_called_with(sensor, new_reading)
    delta_observer.assert_called_with(sensor, new_reading, old_reading)

    classic_observer.reset_mock()
    delta_observer.reset_mock()

    sensor.detach(classic_observer)
    sensor.detach(delta_observer)

    sensor.value = 42

    classic_observer.assert_not_called()
    delta_observer.assert_not_called()


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
    ) -> Optional[Reading]:
        """Return a known Reading."""
        return None


@pytest.fixture
def agg_sensor(mocker, ss):
    """Mock out update_aggregate so we can check it's called appropriately."""

    mocker.patch.object(
        MyAgg,
        "update_aggregate",
        autospec=True,
        return_value=Reading(0, Sensor.Status.NOMINAL, 7),
    )
    my_agg = MyAgg(target=ss, sensor_type=int, name="good-bad-ugly")
    return my_agg


class TestAggregateSensor:
    """Test operation of AggregateSensor."""

    def test_creation(self, agg_sensor, ss, sensors):
        """Check that creation happens properly, and correct initial value is set."""
        agg_sensor.update_aggregate.assert_called_with(agg_sensor, None, None, None)
        assert agg_sensor.target is ss
        assert agg_sensor.reading.timestamp == 0
        assert agg_sensor.reading.status == Sensor.Status.NOMINAL
        assert agg_sensor.reading.value == 7

    def test_sensor_added(self, agg_sensor, ss, sensors):
        """Check that the update function is called when a sensor is added."""
        MyAgg.update_aggregate.return_value = Reading(7, Sensor.Status.WARN, 42)
        ss.add(sensors[1])
        agg_sensor.update_aggregate.assert_called_with(
            agg_sensor, sensors[1], sensors[1].reading, None
        )
        assert agg_sensor.reading.timestamp == 7
        assert agg_sensor.reading.status == Sensor.Status.WARN
        assert agg_sensor.reading.value == 42

    def test_sensor_removed(self, agg_sensor, ss, sensors):
        """Check that the update function is called for a removed sensor."""
        ss.remove(sensors[0])
        agg_sensor.update_aggregate.assert_called_with(
            agg_sensor, sensors[0], None, sensors[0].reading
        )
        # Reset the counters to check subsequent changes do nothing.
        agg_sensor.update_aggregate.reset_mock()
        sensors[0].set_value(5, Sensor.Status.ERROR)
        agg_sensor.update_aggregate.assert_not_called()

    def test_sensor_value_changed(self, agg_sensor, ss, sensors):
        """Check that the update function is called for a sensor whose value has changed."""
        old_reading = sensors[0].reading
        sensors[0].set_value(7, Sensor.Status.WARN)
        agg_sensor.update_aggregate.assert_called_with(
            agg_sensor, sensors[0], sensors[0].reading, old_reading
        )

    def test_sensor_value_unchanged(self, agg_sensor, ss, sensors):
        """Check that the update function is called for a sensor whose value has changed."""
        old_reading = agg_sensor.reading
        MyAgg.update_aggregate.return_value = None
        sensors[0].set_value(7, Sensor.Status.WARN)
        assert agg_sensor.reading is old_reading

    def test_aggregate_sensor_excluded(self, agg_sensor, mocker, ss, sensors):
        """Check that the aggregate sensor gets excluded if it's in the set itself."""
        mocker.patch.object(agg_sensor, "attach")
        mocker.patch.object(agg_sensor, "detach")
        ss.add(agg_sensor)
        agg_sensor.attach.assert_not_called()
        ss.remove(agg_sensor)
        agg_sensor.detach.assert_not_called()

    def test_aggregate_garbage_collection(self, ss, sensors):
        """Check that the aggregate can be garbage collected."""
        # Don't use the agg_sensor fixture, because pytest will hold its own
        # references to it.
        my_agg = MyAgg(target=ss, sensor_type=int, name="garbage")
        ss.add(sensors[1])
        weak = weakref.ref(my_agg)
        del my_agg
        # Some Python implementations need multiple rounds to garbage-collect
        # everything.
        for _ in range(5):
            gc.collect()
        assert weak() is None  # i.e. my_agg was garbage-collected
        sensors[0].value = 12  # Check that it doesn't fail

    def test_sensor_garbage_collection(self):
        """Check that sensors can be garbage-collected once removed from the aggregate."""
        # Don't use the fixtures, because they have mocks that might
        # record things and keep them alive.
        # The noqa is to suppress
        # "local variable 'my_agg' is assigned to but never used"
        # (we need to give it a name just to keep it alive)
        ss = SensorSet()
        my_agg = MyAgg(target=ss, sensor_type=int, name="agg")  # noqa: F841
        sensor = Sensor(int, "rubbish")
        ss.add(sensor)
        ss.remove(sensor)
        weak = weakref.ref(sensor)
        del sensor
        # Some Python implementations need multiple rounds to garbage-collect
        # everything.
        for _ in range(5):
            gc.collect()
        assert weak() is None

    def test_weak_callback_failures(self, agg_sensor, monkeypatch):
        """Ensure code coverage of :class:`._weak_callback`."""
        assert isinstance(MyAgg._sensor_added, _weak_callback)
        wc = _weak_callback(lambda x: x)
        monkeypatch.setattr(MyAgg, "bad_weak_callback", wc, raising=False)
        with pytest.raises(TypeError):
            agg_sensor.bad_weak_callback


class MySimpleAgg(SimpleAggregateSensor[int]):
    """Example sensor that collects the sum of valid readings."""

    def __init__(self, *args, **kwargs):
        self._total = 0
        super().__init__(*args, **kwargs)

    def aggregate_add(self, sensor, reading):
        if reading.status.valid_value():
            self._total += reading.value
            return True
        return False

    def aggregate_remove(self, sensor, reading):
        if reading.status.valid_value():
            self._total -= reading.value
            return True
        return False

    def aggregate_compute(self):
        return (Sensor.Status.NOMINAL if self._total <= 10 else Sensor.Status.WARN, self._total)


class TestSimpleAggregateSensor:
    @pytest.fixture
    def mock_time(self, mocker) -> mock.Mock:
        return mocker.patch("time.time", return_value=1234567890.0)

    @pytest.fixture
    def agg(self, mock_time, sensors: List[Sensor], ss: SensorSet) -> MySimpleAgg:
        sensors[0].set_value(3, timestamp=1234512345.0)
        return MySimpleAgg(ss, int, "simple-agg", "Test sensor")

    def test_initial_state(self, agg: MySimpleAgg, mock_time: mock.Mock) -> None:
        assert agg.value == 3
        assert agg.timestamp == mock_time.return_value
        assert agg.status == Sensor.Status.NOMINAL

    def test_update_sensor(self, agg: MySimpleAgg, sensors: List[Sensor]) -> None:
        sensors[0].set_value(11, timestamp=1234512346.0)
        assert agg.value == 11
        assert agg.timestamp == 1234567890.0  # Time must not go backwards
        assert agg.status == Sensor.Status.WARN
        # Set to invalid state - should effectively remove the reading
        sensors[0].set_value(12, timestamp=1400567891.0, status=Sensor.Status.UNKNOWN)
        assert agg.value == 0
        assert agg.timestamp == 1400567891.0
        assert agg.status == Sensor.Status.NOMINAL
        # Return to a valid state
        sensors[0].set_value(5, timestamp=1400567892.0)
        assert agg.value == 5
        assert agg.timestamp == 1400567892.0
        assert agg.status == Sensor.Status.NOMINAL

    def test_add_remove_sensor(
        self, agg: MySimpleAgg, sensors: List[Sensor], ss: SensorSet, mock_time: mock.Mock
    ) -> None:
        mock_time.return_value = 1234567891.5
        sensors[1].set_value(8, timestamp=1400567890.0)
        ss.add(sensors[1])
        assert agg.value == 11
        assert agg.timestamp == mock_time.return_value
        assert agg.status == Sensor.Status.WARN

        mock_time.return_value = 1234567892.0
        ss.remove(sensors[0])
        assert agg.value == 8
        assert agg.timestamp == mock_time.return_value
        assert agg.status == Sensor.Status.NOMINAL

    def test_filter_updates(self, agg: MySimpleAgg, sensors: List[Sensor]) -> None:
        sensors[0].set_value(5, timestamp=1400567890.0, status=Sensor.Status.UNKNOWN)
        assert agg.value == 0
        assert agg.timestamp == 1400567890.0
        # Make an update that doesn't change anything
        callback = mock.Mock()
        agg.attach(callback)
        sensors[0].set_value(6, timestamp=1400567891.0, status=Sensor.Status.UNKNOWN)
        assert agg.value == 0
        assert agg.timestamp == 1400567890.0
        callback.assert_not_called()
