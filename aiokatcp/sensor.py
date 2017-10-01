import enum
import time
import abc
import asyncio
from typing import Generic, TypeVar, Type, List, Tuple, Iterable, Optional, Any, Callable, cast
# Imports only used for type comments, otherwise unused
from typing import Set, Dict     # noqa: F401

from . import core


_T = TypeVar('_T')


class Reading(Generic[_T]):
    """Sensor reading

    Parameters
    ----------
    timestamp
        The UNIX timestamp at which the sensor value was determined
    status
        Sensor status at `timestamp`
    value
        Sensor value at `timestamp`
    """
    # Note: can't use slots in 3.5 due to https://bugs.python.org/issue28790

    def __init__(self, timestamp: float, status: 'Sensor.Status', value: _T) -> None:
        self.timestamp = timestamp
        self.status = status
        self.value = value


class Sensor(Generic[_T]):
    class Status(enum.Enum):
        UNKNOWN = 0
        NOMINAL = 1
        WARN = 2
        ERROR = 3
        FAILURE = 4
        UNREACHABLE = 5
        INACTIVE = 6

    def __init__(self, sensor_type: Type[_T],
                 name: str,
                 description: str = None,
                 units: str = '',
                 default: _T = None,
                 initial_status: Status = Status.UNKNOWN) -> None:
        self.stype = sensor_type
        type_info = core.get_type(sensor_type)
        self.type_name = type_info.name
        self._observers = set()           # type: Set[Callable[[Sensor[_T], Reading[_T]], None]]
        self.name = name
        self.description = description
        self.units = units
        if default is None:
            value = type_info.default(sensor_type)   # type: _T
        else:
            value = default
        self._reading = Reading(time.time(), initial_status, value)

    def notify(self, reading: Reading[_T]) -> None:
        """Notify all observers of changes to this sensor."""
        for observer in self._observers:
            observer(self, reading)

    def set_value(self, value: _T, status: Status = Status.NOMINAL,
                  timestamp: float = None) -> None:
        """Set the current value of the sensor.

        Parameters
        ----------
        timestamp
           The time at which the sensor value was determined (seconds).
        status
            Whether the value represents an error condition or not.
        value
            The value of the sensor (the type should be appropriate to the
            sensor's type).
        """
        if timestamp is None:
            timestamp = time.time()
        reading = Reading(timestamp, status, value)
        self._reading = reading
        self.notify(reading)

    @property
    def value(self) -> _T:
        return self.reading.value

    @value.setter
    def value(self, value: _T) -> None:
        self.set_value(value)

    @property
    def timestamp(self) -> float:
        return self.reading.timestamp

    @property
    def status(self) -> Status:
        return self.reading.status

    @property
    def reading(self) -> Reading[_T]:
        return self._reading

    @property
    def params(self) -> List[bytes]:
        if self.type_name == 'discrete':
            return [core.encode(value) for value in cast(Iterable, self.stype)]
        else:
            return []

    def attach(self, observer: Callable[['Sensor[_T]', Reading[_T]], None]) -> None:
        self._observers.add(observer)

    def detach(self, observer: Callable[['Sensor[_T]', Reading[_T]], None]) -> None:
        self._observers.discard(observer)


class SensorSampler(Generic[_T], metaclass=abc.ABCMeta):
    class Strategy(enum.Enum):
        NONE = 0
        AUTO = 1
        PERIOD = 2
        EVENT = 3
        DIFFERENTIAL = 4
        EVENT_RATE = 5
        DIFFERENTIAL_RATE = 6

    def __init__(self, sensor: Sensor[_T], observer: Callable[[Sensor[_T], Reading[_T]], None],
                 loop: asyncio.AbstractEventLoop,
                 difference: Optional[_T] = None,
                 shortest: core.Timestamp = core.Timestamp(0),
                 longest: core.Timestamp = None,
                 *, always_update: bool = False) -> None:
        self.sensor = sensor
        self.observer = observer
        self.shortest = float(shortest)
        if longest is not None:
            self.longest = float(longest)  # type: Optional[float]
            if self.longest <= 0:
                raise ValueError('period must be positive')
        else:
            self.longest = None
        self.difference = difference
        self.always_update = always_update
        self.loop = loop
        self._callback_handle = None    # type: Optional[asyncio.Handle]
        self._last_time = 0.0
        self._last_value = None         # type: _T
        self._last_status = None        # type: Sensor.Status
        self._changed = False
        self.sensor.attach(self._receive_update)
        self._send_update(loop.time(), sensor.reading)

    def _clear_callback(self) -> None:
        if self._callback_handle is not None:
            self._callback_handle.cancel()
            self._callback_handle = None

    def _send_update(self, sched_time: float, reading: Optional[Reading[_T]]) -> None:
        if reading is None:
            reading = self.sensor.reading
        self.observer(self.sensor, reading)
        self._last_time = sched_time
        self._last_value = reading.value
        self._last_status = reading.status
        self._changed = False
        self._clear_callback()
        if self.longest is not None:
            next_time = max(self.loop.time(), sched_time + self.longest)
            self._callback_handle = self.loop.call_at(
                next_time, self._send_update, next_time, None)

    def _receive_update(self, sensor: Sensor[_T], reading: Reading[_T]) -> None:
        if self._changed:
            # We already know the value changed, we're waiting for time-based callback
            return

        if self.always_update:
            changed = True
        elif self.difference is None:
            changed = reading.value != self._last_value
        else:
            assert sensor.stype in (int, float)
            changed = abs(cast(Any, reading.value) - self._last_value) > self.difference
        if reading.status != self._last_status:
            changed = True

        if changed:
            self._changed = True
            self._clear_callback()
            sched_time = self._last_time + self.shortest
            now = self.loop.time()
            if not self.shortest or now >= sched_time:
                self._send_update(now, reading)
            else:
                self._callback_handle = self.loop.call_at(
                    sched_time, self._send_update, sched_time, None)

    def close(self) -> None:
        """Stop monitoring the sensor.

        This should be called when the sampler is no longer needed. It is not
        valid to call any methods on the sampler after this.
        """
        self._clear_callback()
        self.sensor.detach(self._receive_update)
        self.sensor = None
        self.observer = None

    @abc.abstractmethod
    def parameters(self) -> Tuple:
        pass       # pragma: no cover

    @classmethod
    def factory(cls, sensor: Sensor[_T], observer: Callable[[Sensor[_T], Reading[_T]], None],
                loop: asyncio.AbstractEventLoop,
                strategy: 'SensorSampler.Strategy', *args: bytes) -> Optional['SensorSampler[_T]']:
        classes_types = {
            cls.Strategy.NONE: (None, []),
            cls.Strategy.AUTO: (_SensorSamplerAuto, []),
            cls.Strategy.PERIOD: (_SensorSamplerPeriod, [core.Timestamp]),
            cls.Strategy.EVENT: (_SensorSamplerEvent, []),
            cls.Strategy.DIFFERENTIAL: (_SensorSamplerDifferential, [sensor.stype]),
            cls.Strategy.EVENT_RATE: (_SensorSamplerEventRate, [core.Timestamp, core.Timestamp]),
            cls.Strategy.DIFFERENTIAL_RATE:
                (_SensorSamplerDifferentialRate, [sensor.stype, core.Timestamp, core.Timestamp])
        }   # type: Dict[SensorSampler.Strategy, Tuple[Optional[Type[SensorSampler]], List[Type]]]
        if strategy in (cls.Strategy.DIFFERENTIAL, cls.Strategy.DIFFERENTIAL_RATE):
            if sensor.stype not in (int, float):
                raise TypeError('differential strategies only valid for integer and float sensors')
        out_cls, types = classes_types[strategy]
        if len(types) != len(args):
            raise ValueError('expected {} strategy arguments, found {}'.format(
                             len(types), len(args)))
        decoded_args = [core.decode(type_, arg) for type_, arg in zip(types, args)]
        if out_cls is None:
            return None
        else:
            return out_cls(sensor, observer, loop, *decoded_args)


class _SensorSamplerAuto(SensorSampler[_T]):
    def __init__(self, sensor: Sensor[_T], observer: Callable[[Sensor[_T], Reading[_T]], None],
                 loop: asyncio.AbstractEventLoop) -> None:
        super().__init__(sensor, observer, loop, always_update=True)

    def parameters(self) -> Tuple[SensorSampler.Strategy]:
        return (SensorSampler.Strategy.AUTO,)


class _SensorSamplerPeriod(SensorSampler[_T]):
    def __init__(self, sensor: Sensor[_T], observer: Callable[[Sensor[_T], Reading[_T]], None],
                 loop: asyncio.AbstractEventLoop, period: core.Timestamp) -> None:
        super().__init__(sensor, observer, loop, shortest=period, longest=period)

    def parameters(self) -> Tuple[SensorSampler.Strategy, core.Timestamp]:
        return (SensorSampler.Strategy.PERIOD, core.Timestamp(self.shortest))


class _SensorSamplerEvent(SensorSampler[_T]):
    def __init__(self, sensor: Sensor[_T], observer: Callable[[Sensor[_T], Reading[_T]], None],
                 loop: asyncio.AbstractEventLoop) -> None:
        super().__init__(sensor, observer, loop)

    def parameters(self) -> Tuple[SensorSampler.Strategy]:
        return (SensorSampler.Strategy.EVENT,)


class _SensorSamplerDifferential(SensorSampler[_T]):
    def __init__(self, sensor: Sensor[_T], observer: Callable[[Sensor[_T], Reading[_T]], None],
                 loop: asyncio.AbstractEventLoop, difference: _T) -> None:
        super().__init__(sensor, observer, loop, difference=difference)

    def parameters(self) -> Tuple[SensorSampler.Strategy, _T]:
        return (SensorSampler.Strategy.DIFFERENTIAL, self.difference)


class _SensorSamplerEventRate(SensorSampler[_T]):
    def __init__(self, sensor: Sensor[_T], observer: Callable[[Sensor[_T], Reading[_T]], None],
                 loop: asyncio.AbstractEventLoop,
                 shortest: core.Timestamp,
                 longest: core.Timestamp) -> None:
        super().__init__(sensor, observer, loop, shortest=shortest, longest=longest)

    def parameters(self) -> Tuple[SensorSampler.Strategy, core.Timestamp, core.Timestamp]:
        return (SensorSampler.Strategy.EVENT_RATE,
                core.Timestamp(self.shortest),
                core.Timestamp(self.longest))


class _SensorSamplerDifferentialRate(SensorSampler[_T]):
    def __init__(self, sensor: Sensor[_T], observer: Callable[[Sensor[_T], Reading[_T]], None],
                 loop: asyncio.AbstractEventLoop,
                 difference: _T,
                 shortest: core.Timestamp,
                 longest: core.Timestamp) -> None:
        super().__init__(sensor, observer, loop,
                         difference=difference, shortest=shortest, longest=longest)

    def parameters(self) -> Tuple[SensorSampler.Strategy, _T, core.Timestamp, core.Timestamp]:
        return (SensorSampler.Strategy.DIFFERENTIAL_RATE,
                self.difference,
                core.Timestamp(self.shortest),
                core.Timestamp(self.longest))
