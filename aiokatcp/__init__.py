# BEGIN VERSION CHECK
# Get package version when locally imported from repo or via -e develop install
try:
    import katversion as _katversion
except ImportError:
    import time as _time
    __version__ = "0.0+unknown.{}".format(_time.strftime('%Y%m%d%H%M'))
else:
    __version__ = _katversion.get_version(__path__[0])
# END VERSION CHECK

__minor_version__ = '.'.join(__version__.split('.')[:2])

from .core import (                                   # noqa: F401
    Message, KatcpSyntaxError, Address, Timestamp,
    encode, decode, register_type, get_type, TypeInfo)
from .connection import Connection, FailReply         # noqa: F401
from .server import DeviceServer, RequestContext, SensorSet      # noqa: F401
from .sensor import Reading, Sensor, SensorSampler    # noqa: F401
