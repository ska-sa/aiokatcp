# Copyright 2022, 2023 National Research Foundation (SARAO)
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

"""Utilities for creating time synchronisation sensors.

These use the Linux-specific :func:`adjtimex` system call to get information
about time synchronisation.  This does not give access to all the statistics
that an NTP or PTP server would provide, but should be sufficient to check that
time is being kept synchronised, and (at least on Linux) is accessible from
inside a container with no privileges required.
"""

from enum import Enum
from typing import Callable, Mapping

from . import adjtimex
from .sensor import Sensor


class ClockState(Enum):
    # Note: values must match TIME_* constants in adjtimex.py
    OK = 0
    INS = 1
    DEL = 2
    OOP = 3
    WAIT = 4
    ERROR = 5


def _us_to_s(value):
    return 1e-6 * value


_TRANSFORMS: Mapping[str, Callable] = {
    "maxerror": _us_to_s,
    "esterror": _us_to_s,
    "state": ClockState,
}


class TimeSyncUpdater:
    """Maps raw adjtimex(2) fields to sensor values.

    The sensors to populate are specified by a mapping, whose keys can be any
    subset of:

    state
        Return value of the :c:func:`!adjtimex` call (as a :class:`ClockState`)
    maxerror
        Maximum error, in seconds
    esterror
        Estimated error, in seconds
    """

    def __init__(self, sensor_map: Mapping[str, Sensor]) -> None:
        self.sensor_map = dict(sensor_map)
        for key in self.sensor_map.keys():
            if key not in _TRANSFORMS:
                raise KeyError(f"Key {key!r} is not valid")

    def update(self) -> None:
        """Update the sensors now."""
        try:
            state, timex = adjtimex.get_adjtimex()
        except NotImplementedError:
            for sensor in self.sensor_map.values():
                sensor.set_value(sensor.value, Sensor.Status.INACTIVE)
            return

        if timex.status & adjtimex.STA_NANO:
            scale = 1e-9
        else:
            scale = 1e-6
        now = timex.time.tv_sec + scale * timex.time.tv_usec
        for key, sensor in self.sensor_map.items():
            if key == "state":
                value = state
            else:
                value = getattr(timex, key)
            value = _TRANSFORMS[key](value)
            sensor.set_value(value, timestamp=now)
