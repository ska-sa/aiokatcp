# Copyright 2017, 2019, 2022 National Research Foundation (SARAO)
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

import abc
import asyncio
import enum
import functools
import inspect
import time
import warnings
import weakref
from abc import ABCMeta, abstractmethod
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    ItemsView,
    Iterable,
    Iterator,
    KeysView,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    ValuesView,
    cast,
    overload,
)

from typing_extensions import Protocol

from . import core

_T = TypeVar("_T")
_U = TypeVar("_U")


class DeltaObserver(Protocol[_T]):
    def __call__(
        self, __sensor: "Sensor[_T]", __reading: "Reading[_T]", *, old_reading: "Reading[_T]"
    ) -> None:
        ...


class ClassicObserver(Protocol[_T]):
    def __call__(self, __sensor: "Sensor[_T]", __reading: "Reading[_T]") -> None:
        ...


Observer = Union[ClassicObserver[_T], DeltaObserver[_T]]


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

    __slots__ = ("timestamp", "status", "value")

    def __init__(self, timestamp: float, status: "Sensor.Status", value: _T) -> None:
        self.timestamp = timestamp
        self.status = status
        self.value = value


def _default_status_func(value) -> "Sensor.Status":
    return Sensor.Status.NOMINAL


class Sensor(Generic[_T]):
    """A sensor in a :class:`DeviceServer`.

    A sensor has some static configuration (name, description, units etc) and
    dynamic state consisting of a :class:`Reading` (value, status and
    timestamp). Other code can attach observers to the sensor to be informed of
    updates.

    Parameters
    ----------
    sensor_type
        The type of the sensor.
    name
        Sensor name
    description
        More detailed explanation of the sensor
    units
        Physical units of the sensor
    default
        Initial value of the sensor. When setting this, it may be desirable to
        specify `initial_status` too.
    initial_status
        Initial status of the sensor
    status_func
        Function that maps a value to a status in :meth:`set_value` if none is given.
        The default is a function that always returns NOMINAL.
    auto_strategy
        Sampling strategy to use when a client requests the ``auto`` strategy.
        The default is to send all updates of the value to the client
        immediately.
    auto_strategy_parameters
        Parameters to use with `auto_strategy`. They must be already-decoded values.
    """

    class Status(enum.IntEnum):
        UNKNOWN = 0
        NOMINAL = 1
        WARN = 2
        ERROR = 3
        FAILURE = 4
        UNREACHABLE = 5
        INACTIVE = 6

        def valid_value(self) -> bool:
            """True if this state is one where the value provided is valid."""
            return self in {Sensor.Status.NOMINAL, Sensor.Status.WARN, Sensor.Status.ERROR}

    def __init__(
        self,
        sensor_type: Type[_T],
        name: str,
        description: str = "",
        units: str = "",
        default: Optional[_T] = None,
        initial_status: Status = Status.UNKNOWN,
        *,
        status_func: Callable[[_T], Status] = _default_status_func,
        auto_strategy: Optional["SensorSampler.Strategy"] = None,
        auto_strategy_parameters: Iterable[Any] = (),
    ) -> None:
        self.stype = sensor_type
        type_info = core.get_type(sensor_type)
        self.type_name = type_info.name
        self._classic_observers: Set[ClassicObserver[_T]] = set()
        self._delta_observers: Set[DeltaObserver[_T]] = set()
        self.name = name
        self.description = description
        self.units = units
        self.status_func = status_func
        if default is None:
            value: _T = type_info.default(sensor_type)
        else:
            value = default
        self._reading = Reading(time.time(), initial_status, value)
        if auto_strategy is None:
            self.auto_strategy = SensorSampler.Strategy.AUTO
        else:
            self.auto_strategy = auto_strategy
        self.auto_strategy_parameters = tuple(auto_strategy_parameters)
        # TODO: should validate the parameters against the strategy.

    def notify(self, reading: Reading[_T], old_reading: Reading[_T]) -> None:
        """Notify all observers of changes to this sensor.

        Users should not usually call this directly. It is called automatically
        by :meth:`set_value`.
        """
        for classic_observer in self._classic_observers:
            classic_observer(self, reading)
        for delta_observer in self._delta_observers:
            delta_observer(self, reading, old_reading=old_reading)

    def set_value(
        self, value: _T, status: Optional[Status] = None, timestamp: Optional[float] = None
    ) -> None:
        """Set the current value of the sensor.

        Parameters
        ----------
        value
            The value of the sensor (the type should be appropriate to the
            sensor's type).
        status
            Whether the value represents an error condition or not. If not
            given, the `status_func` given to the constructor is used to
            determine the status from the value.
        timestamp
            The time at which the sensor value was determined (seconds).
            If not given, it defaults to :func:`time.time`.
        """
        if timestamp is None:
            timestamp = time.time()
        if status is None:
            status = self.status_func(value)
        reading = Reading(timestamp, status, value)
        old_reading = self._reading
        self._reading = reading
        self.notify(reading, old_reading)

    @property
    def value(self) -> _T:
        """The current value of the sensor.

        Modifying it invokes :meth:`set_value`.
        """
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
        if self.type_name == "discrete":
            return [core.encode(value) for value in cast(Iterable, self.stype)]
        else:
            return []

    def attach(self, observer: Observer[_T]) -> None:
        sig = inspect.signature(observer)
        if "old_reading" in sig.parameters:
            self._delta_observers.add(observer)  # type: ignore
        else:
            self._classic_observers.add(observer)  # type: ignore

    def detach(self, observer: Observer[_T]) -> None:
        # It's simpler to just discard from both sets (and ignore the type) than to do inspection.
        self._delta_observers.discard(observer)  # type: ignore
        self._classic_observers.discard(observer)  # type: ignore


class SensorSampler(Generic[_T], metaclass=abc.ABCMeta):
    """Implement the strategies defined by the ``sensor-sampling`` request.

    This is an abstract base class. Instances should be constructed by
    calling :meth:`factory`.

    It takes an "observer", which is a callback function that is called when
    a sensor update should be sent to the subscribed client. When the sampler
    is constructed, the observer is called immediately, and then again when
    appropriate to the strategy.

    It is possible to construct this class without an observer, and set it
    later. This is used by the ``sensor-sampling`` implementation to first
    validate the parameters before sending any readings.

    Parameters
    ----------
    sensor
        The sensor to observe
    observer
        Callback function to invoke
    loop
        Asyncio event loop
    difference
        Minimum change in value before sending an update
    shortest
        Minimum time between updates
    longest
        Maximum time between updates (or None for no maximum)
    always_update
        If true, update on every sensor value assignment
    is_auto
        True if this sampler was created from the "auto" strategy
    """

    class Strategy(enum.Enum):
        NONE = 0
        AUTO = 1
        PERIOD = 2
        EVENT = 3
        DIFFERENTIAL = 4
        EVENT_RATE = 5
        DIFFERENTIAL_RATE = 6

    def __init__(
        self,
        sensor: Sensor[_T],
        observer: Optional[Callable[[Sensor[_T], Reading[_T]], None]],
        loop: asyncio.AbstractEventLoop,
        difference: Optional[_T] = None,
        shortest: core.Timestamp = core.Timestamp(0),
        longest: Optional[core.Timestamp] = None,
        *,
        always_update: bool = False,
        is_auto: bool = False,
    ) -> None:
        if longest is not None:
            self.longest: Optional[float] = float(longest)
            if self.longest <= 0:
                raise ValueError("period must be positive")
        else:
            self.longest = None
        self.shortest = float(shortest)
        self.sensor: Optional[Sensor[_T]] = sensor
        self._observer: Optional[Callable[[Sensor[_T], Reading[_T]], None]] = observer
        self.difference = difference
        self.always_update = always_update
        self.is_auto = is_auto
        self.loop = loop
        self._callback_handle: Optional[asyncio.Handle] = None
        self._last_time = 0.0
        self._last_value: Optional[_T] = None
        self._last_status: Optional[Sensor.Status] = None
        self._changed = False
        self.sensor.attach(self._receive_update)
        self._send_update(loop.time(), sensor.reading)

    def __del__(self) -> None:
        if getattr(self, "sensor", None) is not None:
            warnings.warn(f"unclosed SensorSampler {self!r}", ResourceWarning)
            if not self.loop.is_closed():
                self.loop.call_soon_threadsafe(self.close)

    @property
    def observer(self) -> Optional[Callable[[Sensor[_T], Reading[_T]], None]]:
        return self._observer

    @observer.setter
    def observer(self, observer: Optional[Callable[[Sensor[_T], Reading[_T]], None]]) -> None:
        assert self.sensor is not None
        self._observer = observer
        self._send_update(self.loop.time(), self.sensor.reading)

    def _clear_callback(self) -> None:
        if self._callback_handle is not None:
            self._callback_handle.cancel()
            self._callback_handle = None

    def _send_update(self, sched_time: float, reading: Optional[Reading[_T]]) -> None:
        assert self.sensor is not None
        if self._observer is None:
            return
        if reading is None:
            reading = self.sensor.reading
        self._observer(self.sensor, reading)
        self._last_time = sched_time
        self._last_value = reading.value
        self._last_status = reading.status
        self._changed = False
        self._clear_callback()
        if self.longest is not None:
            next_time = max(self.loop.time(), sched_time + self.longest)
            self._callback_handle = self.loop.call_at(next_time, self._send_update, next_time, None)

    def _receive_update(self, sensor: Sensor[_T], reading: Reading[_T]) -> None:
        if self._changed:
            # We already know the value changed, we're waiting for time-based callback
            return
        if self._observer is None:
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
                    sched_time, self._send_update, sched_time, None
                )

    def close(self) -> None:
        """Stop monitoring the sensor.

        This should be called when the sampler is no longer needed. It is not
        valid to call any methods on the sampler after this.
        """
        self._clear_callback()
        if self.sensor is not None:
            self.sensor.detach(self._receive_update)
            self.sensor = None
        self._observer = None

    @abc.abstractmethod
    def _parameters(self) -> tuple:
        pass  # pragma: no cover

    def parameters(self) -> tuple:
        """Return the parameters with which the sensor was created."""
        if self.is_auto:
            return (SensorSampler.Strategy.AUTO,)
        else:
            return self._parameters()

    @classmethod
    def factory(
        cls,
        sensor: Sensor[_T],
        observer: Optional[Callable[[Sensor[_T], Reading[_T]], None]],
        loop: asyncio.AbstractEventLoop,
        strategy: "SensorSampler.Strategy",
        *args: bytes,
    ) -> Optional["SensorSampler[_T]"]:
        if strategy == cls.Strategy.AUTO:
            strategy = sensor.auto_strategy
            decoded_args = sensor.auto_strategy_parameters
            out_cls = _SAMPLER_CLASSES_TYPES[strategy][0]
            is_auto = True
        else:
            out_cls, types = _SAMPLER_CLASSES_TYPES[strategy]
            if strategy in (cls.Strategy.DIFFERENTIAL, cls.Strategy.DIFFERENTIAL_RATE):
                if sensor.stype not in (int, float):
                    raise TypeError(
                        "differential strategies only valid for integer and float sensors"
                    )
                types = list(types)
                types[0] = sensor.stype
            if len(types) != len(args):
                raise ValueError(
                    "expected {} strategy arguments, found {}".format(len(types), len(args))
                )
            decoded_args = tuple(core.decode(type_, arg) for type_, arg in zip(types, args))
            is_auto = False

        if out_cls is None:
            return None
        else:
            return out_cls(sensor, observer, loop, *decoded_args, is_auto=is_auto)


class _SensorSamplerEventAlways(SensorSampler[_T]):
    def __init__(
        self,
        sensor: Sensor[_T],
        observer: Callable[[Sensor[_T], Reading[_T]], None],
        loop: asyncio.AbstractEventLoop,
        *,
        is_auto: bool,
    ) -> None:
        super().__init__(sensor, observer, loop, always_update=True, is_auto=is_auto)

    def _parameters(self) -> Tuple[SensorSampler.Strategy]:
        return (SensorSampler.Strategy.AUTO,)


class _SensorSamplerPeriod(SensorSampler[_T]):
    def __init__(
        self,
        sensor: Sensor[_T],
        observer: Callable[[Sensor[_T], Reading[_T]], None],
        loop: asyncio.AbstractEventLoop,
        period: core.Timestamp,
        *,
        is_auto: bool,
    ) -> None:
        super().__init__(sensor, observer, loop, shortest=period, longest=period, is_auto=is_auto)

    def _parameters(self) -> Tuple[SensorSampler.Strategy, core.Timestamp]:
        return (SensorSampler.Strategy.PERIOD, core.Timestamp(self.shortest))


class _SensorSamplerEvent(SensorSampler[_T]):
    def _parameters(self) -> Tuple[SensorSampler.Strategy]:
        return (SensorSampler.Strategy.EVENT,)


class _SensorSamplerDifferential(SensorSampler[_T]):
    def __init__(
        self,
        sensor: Sensor[_T],
        observer: Callable[[Sensor[_T], Reading[_T]], None],
        loop: asyncio.AbstractEventLoop,
        difference: _T,
        *,
        is_auto: bool,
    ) -> None:
        super().__init__(sensor, observer, loop, difference=difference, is_auto=is_auto)

    def _parameters(self) -> Tuple[SensorSampler.Strategy, _T]:
        assert self.difference is not None  # To keep mypy happy
        return (SensorSampler.Strategy.DIFFERENTIAL, self.difference)


class _SensorSamplerEventRate(SensorSampler[_T]):
    def __init__(
        self,
        sensor: Sensor[_T],
        observer: Callable[[Sensor[_T], Reading[_T]], None],
        loop: asyncio.AbstractEventLoop,
        shortest: core.Timestamp,
        longest: core.Timestamp,
        *,
        is_auto: bool,
    ) -> None:
        super().__init__(
            sensor, observer, loop, shortest=shortest, longest=longest, is_auto=is_auto
        )

    def _parameters(self) -> Tuple[SensorSampler.Strategy, core.Timestamp, core.Timestamp]:
        # assertions to keep mypy happy
        assert self.shortest is not None
        assert self.longest is not None
        return (
            SensorSampler.Strategy.EVENT_RATE,
            core.Timestamp(self.shortest),
            core.Timestamp(self.longest),
        )


class _SensorSamplerDifferentialRate(SensorSampler[_T]):
    def __init__(
        self,
        sensor: Sensor[_T],
        observer: Callable[[Sensor[_T], Reading[_T]], None],
        loop: asyncio.AbstractEventLoop,
        difference: _T,
        shortest: core.Timestamp,
        longest: core.Timestamp,
        *,
        is_auto: bool,
    ) -> None:
        super().__init__(
            sensor,
            observer,
            loop,
            difference=difference,
            shortest=shortest,
            longest=longest,
            is_auto=is_auto,
        )

    def _parameters(
        self,
    ) -> Tuple[SensorSampler.Strategy, _T, core.Timestamp, core.Timestamp]:
        # assertions to keep mypy happy
        assert self.difference is not None
        assert self.shortest is not None
        assert self.longest is not None
        return (
            SensorSampler.Strategy.DIFFERENTIAL_RATE,
            self.difference,
            core.Timestamp(self.shortest),
            core.Timestamp(self.longest),
        )


# float is used as a placeholder for the sensor value type
_SAMPLER_CLASSES_TYPES: Dict[
    SensorSampler.Strategy, Tuple[Optional[Type[SensorSampler]], List[Type]]
] = {
    SensorSampler.Strategy.NONE: (None, []),
    SensorSampler.Strategy.AUTO: (_SensorSamplerEventAlways, []),
    SensorSampler.Strategy.PERIOD: (_SensorSamplerPeriod, [core.Timestamp]),
    SensorSampler.Strategy.EVENT: (_SensorSamplerEvent, []),
    SensorSampler.Strategy.DIFFERENTIAL: (_SensorSamplerDifferential, [float]),
    SensorSampler.Strategy.EVENT_RATE: (
        _SensorSamplerEventRate,
        [core.Timestamp, core.Timestamp],
    ),
    SensorSampler.Strategy.DIFFERENTIAL_RATE: (
        _SensorSamplerDifferentialRate,
        [float, core.Timestamp, core.Timestamp],
    ),
}


class SensorSet(Mapping[str, Sensor]):
    """A dict-like and set-like collection of sensors.

    It is possible to monitor for removal of sensors using
    :meth:`add_remove_callback`.
    """

    class _Sentinel(enum.Enum):
        """Internal enum used to signal that no default is provided to pop"""

        NO_DEFAULT = 0

    def __init__(self) -> None:
        self._sensors: Dict[str, Sensor] = {}
        self._add_callbacks: List[Callable[[Sensor], None]] = []
        self._remove_callbacks: List[Callable[[Sensor], None]] = []

    def _removed(self, s: Sensor) -> None:
        """Call all the remove callbacks."""
        for callback in list(self._remove_callbacks):
            callback(s)

    def _added(self, s: Sensor) -> None:
        """Call all the add callbacks."""
        for callback in list(self._add_callbacks):
            callback(s)

    def add_remove_callback(self, callback: Callable[[Sensor], None]) -> None:
        """Add a callback that will be passed any sensor removed from the set."""
        self._remove_callbacks.append(callback)

    def add_add_callback(self, callback: Callable[[Sensor], None]) -> None:
        """Add a callback that will be passed any sensor added to the set."""
        self._add_callbacks.append(callback)

    def remove_remove_callback(self, callback: Callable[[Sensor], None]) -> None:
        """Remove a callback registered with :meth:`add_remove_callback`."""
        self._remove_callbacks.remove(callback)

    def remove_add_callback(self, callback: Callable[[Sensor], None]) -> None:
        """Remove a callback registered with :meth:`add_add_callback`."""
        self._add_callbacks.remove(callback)

    def add(self, elem: Sensor) -> None:
        if elem.name in self._sensors:
            if self._sensors[elem.name] is not elem:
                del self[elem.name]
            else:
                return
        self._sensors[elem.name] = elem
        self._added(elem)

    def remove(self, elem: Sensor) -> None:
        if elem not in self:
            raise KeyError(elem.name)
        del self[elem.name]

    def discard(self, elem: Sensor) -> None:
        try:
            self.remove(elem)
        except KeyError:
            pass

    def clear(self) -> None:
        while self._sensors:
            self.popitem()

    def popitem(self) -> Tuple[str, Sensor]:
        name, value = self._sensors.popitem()
        self._removed(value)
        return name, value

    def pop(
        self, key: str, default: Union[Sensor, None, _Sentinel] = _Sentinel.NO_DEFAULT
    ) -> Optional[Sensor]:
        if key not in self._sensors:
            if isinstance(default, self._Sentinel):
                raise KeyError(key)
            else:
                return default
        else:
            s = self._sensors.pop(key)
            self._removed(s)
            return s

    def __delitem__(self, key: str) -> None:
        s = self._sensors.pop(key)
        self._removed(s)

    def __getitem__(self, name: str) -> Sensor:
        return self._sensors[name]

    @overload
    def get(self, name: str) -> Optional[Sensor]:
        ...

    @overload  # noqa: F811
    def get(self, name: str, default: Union[Sensor, _T]) -> Union[Sensor, _T]:
        ...

    def get(self, name: str, default: Optional[object] = None) -> object:  # noqa: F811
        return self._sensors.get(name, default)

    def __contains__(self, s: object) -> bool:
        if isinstance(s, Sensor):
            return s.name in self._sensors and self._sensors[s.name] is s
        else:
            return s in self._sensors

    def __len__(self) -> int:
        return len(self._sensors)

    def __bool__(self) -> bool:
        return bool(self._sensors)

    def __iter__(self) -> Iterator[str]:
        return iter(self._sensors)

    def keys(self) -> KeysView[str]:
        return self._sensors.keys()

    def values(self) -> ValuesView[Sensor]:
        return self._sensors.values()

    def items(self) -> ItemsView[str, Sensor]:
        return self._sensors.items()

    def copy(self) -> Dict[str, Sensor]:
        return self._sensors.copy()

    __hash__ = None  # type: ignore     # mypy can't handle this

    add.__doc__ = set.add.__doc__
    remove.__doc__ = set.remove.__doc__
    discard.__doc__ = set.discard.__doc__
    clear.__doc__ = dict.clear.__doc__
    popitem.__doc__ = dict.popitem.__doc__
    pop.__doc__ = dict.pop.__doc__
    get.__doc__ = dict.get.__doc__
    keys.__doc__ = dict.keys.__doc__
    values.__doc__ = dict.values.__doc__
    items.__doc__ = dict.items.__doc__
    copy.__doc__ = dict.copy.__doc__


class _weak_callback:
    """Method decorator that makes it hold only a weak reference to self.

    Calling the method will be a no-op if the object has been deleted.

    The return value is cached, so accessing it multiple times will yield the
    same wrapper object each time. However, this is *not* thread-safe.
    """

    def __init__(self, func: Callable) -> None:
        self._func = func
        self._name: Optional[str] = None
        self.__doc__ = func.__doc__

    def __set_name__(self, owner: type, name: str) -> None:
        self._name = name

    def __get__(self, instance: object, owner: Optional[type] = None):
        # __get__ is magic in Python: it makes this class a "descriptor".
        # Refer to the language guide for an explanation of what that means.
        # In short, when one calls `obj.method` where `method` was
        # decorated with this descriptor, it's resolved with
        # `__get__(obj, type(obj))`.
        if instance is None:
            return self
        if self._name is None:
            raise TypeError("name was not set for weak callback")
        cache = instance.__dict__
        weak_instance = weakref.ref(instance)
        func = self._func

        @functools.wraps(self._func)
        def wrapper(*args, **kwargs):
            strong_instance = weak_instance()
            if strong_instance is not None:
                return func(strong_instance, *args, **kwargs)

        # Note: this overrides the descriptor, so that future accesses
        # will use the value directly.
        assert self._name not in cache
        cache[self._name] = wrapper
        return wrapper


class AggregateSensor(Sensor[_T], metaclass=ABCMeta):
    """A Sensor with its reading determined by several other Sensors.

    This is an abstract class: the user must implement :meth:`update_aggregate`.
    This method is called whenever the target :class:`SensorSet` has a sensor
    added, removed, or one of the sensors changes its reading. The user-defined
    :meth:`update_aggregate` returns the new reading for the AggregateSensor.

    Parameters are all as per :class:`Sensor`, with the exception of
    `target`, which is the :class:`SensorSet` from which the aggregated sensor
    will determine its own reading.

    Attributes
    ----------
    target : SensorSet
        The set of sensors which will determine the reading of the aggregate
        one. The aggregate sensor may be included in the set (e.g. in
        `self.sensors` of a server) but it will not affect its own value.
    """

    def __init__(
        self,
        target: SensorSet,
        sensor_type: Type[_T],
        name: str,
        description: str = "",
        units: str = "",
        *,
        auto_strategy: Optional["SensorSampler.Strategy"] = None,
        auto_strategy_parameters: Iterable[Any] = (),
    ) -> None:
        super().__init__(
            sensor_type=sensor_type,
            name=name,
            description=description,
            units=units,
            auto_strategy=auto_strategy,
            auto_strategy_parameters=auto_strategy_parameters,
        )
        self.target = target
        for sensor in self.target.values():
            if self.filter_aggregate(sensor):
                sensor.attach(self._update_aggregate_callback)
                # We don't use weakref.finalize to detach, because that
                # finalizer would live until `self` is destroyed and thus
                # keep that `sensor` alive, even if `sensor` is removed from
                # the sensor set and could otherwise be destroyed. Instead,
                # __del__ cleans up the attachments.

        self.target.add_add_callback(self._sensor_added)
        weakref.finalize(self, self.target.remove_add_callback, self._sensor_added)
        self.target.add_remove_callback(self._sensor_removed)
        weakref.finalize(self, self.target.remove_remove_callback, self._sensor_removed)
        self._update_aggregate(None, None, None)

    def __del__(self):
        # Protect against an exception early in __init__
        for sensor in getattr(self, "target", []).values():
            # Could use filter_aggregate, but it might not work during
            # destruction, and its a no-op if there is no attachment.
            sensor.detach(self._update_aggregate_callback)

    @abstractmethod
    def update_aggregate(
        self,
        updated_sensor: Optional[Sensor[_U]],
        reading: Optional[Reading[_U]],
        old_reading: Optional[Reading[_U]],
    ) -> Optional[Reading[_T]]:
        """Update the aggregated sensor.

        The user is required to override this function, which must return the
        updated :class:`Reading` (i.e. value, status and timestamp) which will
        be reflected in the `Reading` of the aggregated sensor.

        Parameters
        ----------
        updated_sensor
            The sensor in the target :class:`SensorSet` which has changed in
            some way.
        reading
            The current reading of the `updated_sensor`. This is `None` if the
            sensor is being removed from the set.
        old_reading
            The previous reading of the `updated_sensor`. This is `None` if the
            sensor is being added to the set.

        Returns
        -------
        Optional[Reading]
            The reading (value, status, timestamp) that should be shown by the
            `AggregatedSensor` as a result of the change. If None is returned,
            the sensor's reading is not modified.
        """
        pass  # pragma: nocover

    def filter_aggregate(self, sensor: Sensor) -> bool:
        """Decide whether another sensor is part of the aggregation.

        Users can override this function to exclude certain categories of
        sensors, such as other aggregates, to prevent circular references.

        Returns
        -------
        bool
            True if `sensor` should be included in calculation of the
            aggregate, False if not.
        """
        return sensor is not self

    def _update_aggregate(
        self,
        updated_sensor: Optional[Sensor[_U]],
        reading: Optional[Reading[_U]],
        old_reading: Optional[Reading[_U]],
    ) -> None:
        updated_reading = self.update_aggregate(updated_sensor, reading, old_reading)
        if updated_reading is not None:
            self.set_value(updated_reading.value, updated_reading.status, updated_reading.timestamp)

    # We create a separate name for the _weak_callback version rather than
    # decorating _update_aggregate. This lets us call the original directly
    # without incurring the decorator overheads.
    _update_aggregate_callback = _weak_callback(_update_aggregate)

    @_weak_callback
    def _sensor_added(self, sensor: Sensor) -> None:
        """Add the update callback to a new sensor in the set."""
        if self.filter_aggregate(sensor):
            sensor.attach(self._update_aggregate_callback)
            self._update_aggregate(sensor, sensor.reading, None)

    @_weak_callback
    def _sensor_removed(self, sensor: Sensor) -> None:
        """Remove the update callback from a sensor no longer in the set."""
        if self.filter_aggregate(sensor):
            sensor.detach(self._update_aggregate_callback)
            self._update_aggregate(sensor, None, sensor.reading)


class SimpleAggregateSensor(AggregateSensor[_T]):
    """A simplified version of :class:`AggregateSensor` for common use cases.

    This class is suitable when:

    - You don't need direct control of the returned timestamp. It will be
      determined automatically.
    - The aggregate value and status can be determined from an internal state
      which is updated by adding or removing readings.

    Subclasses must override :meth:`aggregate_add`, :meth:`aggregate_remove`,
    :meth:`aggregate_compute` and optionally :meth:`filter_aggregate`.

    Currently, the timestamp will be set to the larger of the last sensor
    update time (using the sensor timestamp) and the last addition or removal
    time (wallclock time). This may be changed in future releases based on
    implementation experience. This class is best suited to cases where child
    sensors are only added on startup and removed on teardown, rather than
    continuously added and removed.
    """

    @abstractmethod
    def aggregate_add(self, sensor: Sensor[_U], reading: Reading[_U]) -> bool:
        """Update internal state with an additional reading.

        Returns
        -------
        bool
            True if the new reading should result in a state update.
        """

    @abstractmethod
    def aggregate_remove(self, sensor: Sensor[_U], reading: Reading[_U]) -> bool:
        """Update internal state by removing a reading.

        Returns
        -------
        bool
            True if removing the reading should result in a state update.
        """

    @abstractmethod
    def aggregate_compute(self) -> Tuple[Sensor.Status, _T]:
        """Compute aggregate status and value from the internal state."""

    def update_aggregate(
        self,
        updated_sensor: Optional[Sensor[_U]],
        reading: Optional[Reading[_U]],
        old_reading: Optional[Reading[_U]],
    ) -> Optional[Reading[_T]]:
        if updated_sensor is None:
            # We're being called from the constructor. Add any existing sensors.
            for s in self.target.values():
                if self.filter_aggregate(s) and s.reading.status.valid_value():
                    self.aggregate_add(s, s.reading)
            timestamp = time.time()
        else:
            # Update the internal state
            update = False
            if old_reading is not None:
                update |= self.aggregate_remove(updated_sensor, old_reading)
            if reading is not None:
                update |= self.aggregate_add(updated_sensor, reading)
            if not update:
                return None

            # Calculate the timestamp
            if old_reading is not None and reading is not None:
                timestamp = reading.timestamp
            else:
                timestamp = time.time()
        timestamp = max(timestamp, self.timestamp)  # Ensure time doesn't go backwards
        status, value = self.aggregate_compute()
        return Reading(timestamp, status, value)
