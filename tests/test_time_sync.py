# Copyright 2017, 2022, 2023 National Research Foundation (SARAO)
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

"""Tests for :mod:`aiokatcp.time_sync` and (indirectly) :mod:`aiokatcp.adjtimex`."""

import sys

import pytest

import aiokatcp.adjtimex
from aiokatcp import Sensor, SensorSet
from aiokatcp.time_sync import ClockState, TimeSyncUpdater


@pytest.fixture
def sensors() -> SensorSet:
    sensors = SensorSet()
    sensors.add(Sensor(float, "ntp.esterror", ""))
    sensors.add(Sensor(float, "ntp.maxerror", ""))
    sensors.add(Sensor(ClockState, "ntp.state", ""))
    return sensors


@pytest.fixture
def updater(sensors: SensorSet) -> TimeSyncUpdater:
    return TimeSyncUpdater({sensor.name[4:]: sensor for sensor in sensors.values()})


@pytest.fixture(params=[False, True])
def pretend_no_adjtimex(mocker, request) -> bool:
    """Optionally replace adjtimex with _no_adjtimex (i.e. pretend it is absent)."""
    if request.param:
        mocker.patch("aiokatcp.adjtimex.adjtimex", side_effect=aiokatcp.adjtimex._no_adjtimex)
    return request.param


@pytest.fixture(params=[False, True])
def mock_adjtimex(mocker, request) -> None:
    """Replace get_adjtimex with a mock version with known values."""
    timex = aiokatcp.adjtimex.Timex()
    timex.modes = 0
    timex.esterror = 123
    timex.maxerror = 4567
    timex.time.tv_sec = 1234567890
    timex.time.tv_usec = 654321
    # Check both microsecond and nanosecond versions of the timex struct
    if request.param:
        timex.status |= aiokatcp.adjtimex.STA_NANO
        timex.time.tv_usec *= 1000
    return_value = (aiokatcp.adjtimex.TIME_OK, timex)
    mocker.patch("aiokatcp.adjtimex.get_adjtimex", return_value=return_value)


def test_smoke(sensors: SensorSet, updater: TimeSyncUpdater, pretend_no_adjtimex) -> None:
    """Test with real adjtimex, to make sure it interacts cleanly with the kernel.

    On non-Linux systems, check that the sensors are inactive instead. Also
    pretend that we don't have adjtimex just to check both code paths on Linux.
    """
    updater.update()
    assert isinstance(sensors["ntp.esterror"].value, float)
    assert isinstance(sensors["ntp.maxerror"].value, float)
    assert isinstance(sensors["ntp.state"].value, ClockState)
    if not pretend_no_adjtimex and sys.platform == "linux":
        expected_status = Sensor.Status.NOMINAL
    else:
        expected_status = Sensor.Status.INACTIVE
    for sensor in sensors.values():
        # Check that it actually got updated
        assert sensor.status == expected_status


def test_bad_key(sensors) -> None:
    with pytest.raises(KeyError, match="Key 'foo' is not valid"):
        TimeSyncUpdater({"foo": sensors["ntp.esterror"]})


def test_mocked(sensors: SensorSet, updater: TimeSyncUpdater, mock_adjtimex) -> None:
    """Test with mocked adjtimex, to check the values."""
    updater.update()
    assert sensors["ntp.esterror"].value == pytest.approx(0.000123)
    assert sensors["ntp.maxerror"].value == pytest.approx(0.004567)
    assert sensors["ntp.state"].value == ClockState.OK
    for sensor in sensors.values():
        assert sensor.timestamp == 1234567890.654321
