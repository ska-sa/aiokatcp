import enum
import time
import abc
from typing import Generic, TypeVar, Type, List, Iterable, cast
# Imports only used for type comments, otherwise unused
from typing import Set     # noqa: F401

from . import core


_T = TypeVar('_T')


class SensorObserver(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def update(self, sensor: 'Sensor') -> None:
        pass


class Sensor(Generic[_T]):
    class Status(enum.Enum):
        UNKNOWN = 'unknown'
        NOMINAL = 'nominal'
        WARN = 'warn'
        ERROR = 'error'
        FAILURE = 'failure'
        UNREACHABLE = 'unreachable'
        INACTIVE = 'inactive'

    def __init__(self, sensor_type: Type[_T],
                 name: str,
                 description: str = None,
                 units: str = '',
                 default: _T = None,
                 initial_status: Status = Status.UNKNOWN) -> None:
        self.stype = sensor_type
        type_info = core.get_type(sensor_type)
        self.type_name = type_info.name
        self._observers = set()           # type: Set[SensorObserver]
        self.name = name
        self.description = description
        self.units = units
        self._timestamp = time.time()
        self._status = initial_status
        if default is None:
            self._value = type_info.default(sensor_type)   # type: _T
        else:
            self._value = default

    def notify(self) -> None:
        """Notify all observers of changes to this sensor."""
        for observer in self._observers:
            observer.update(self)

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
        self._timestamp = timestamp if timestamp is not None else time.time()
        self._status = status
        self._value = value
        self.notify()

    @property
    def value(self) -> _T:
        return self._value

    @value.setter
    def value(self, value: _T) -> None:
        self.set_value(value)

    @property
    def timestamp(self) -> float:
        return self._timestamp

    @property
    def status(self) -> Status:
        return self._status

    @property
    def params(self) -> List:
        if self.type_name == 'discrete':
            return [core.encode(value) for value in cast(Iterable, self.stype)]
        else:
            return []

    def attach(self, observer: SensorObserver) -> None:
        self._observers.add(observer)

    def detach(self, observer: SensorObserver) -> None:
        self._observers.discard(observer)
