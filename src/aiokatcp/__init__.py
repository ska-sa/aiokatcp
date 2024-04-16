# Copyright 2017, 2022-2024 National Research Foundation (SARAO)
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

from ._version import __version__
from .client import (  # noqa: F401
    AbstractSensorWatcher,
    Client,
    ProtocolError,
    SensorWatcher,
    SyncState,
)
from .connection import Connection, FailReply, InvalidReply  # noqa: F401
from .core import (  # noqa: F401
    Address,
    DeviceStatus,
    KatcpSyntaxError,
    Message,
    Now,
    Timestamp,
    TimestampOrNow,
    TypeInfo,
    decode,
    encode,
    get_type,
    register_type,
)
from .sensor import (  # noqa: F401
    AggregateSensor,
    Reading,
    Sensor,
    SensorSampler,
    SensorSet,
    SimpleAggregateSensor,
)
from .server import DeviceServer, RequestContext  # noqa: F401
from .time_sync import ClockState, TimeSyncUpdater  # noqa: F401


def minor_version():
    return ".".join(__version__.split(".")[:2])
