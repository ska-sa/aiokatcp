Strings versus bytes
====================

Python 3 has a strict separation between strings (Unicode text) and bytes
(binary data). Since the katcp specification does not have this strong
separation and does not discuss character encodings, aiokatcp has to make some
judgement calls about how to treat various parts of katcp messages.

The katcp specification identifies a number of fields as "human-readable" and
says that they should consist of "printable ASCII plus the escape characters
for horizontal tab, line feed and carriage return". This is not currently
enforced by katcp, but may be in future. Applications should adhere to this
limitation as failing to do so may lead to errors with future versions of
aiokatcp, as well as problems interoperating with other katcp
implementations.

Message names
-------------
In the :class:`.Message` class, the name of the message is a string. However,
the katcp specification limits the character set to a subset of ASCII
characters. This is enforced both on encoding and decoding.

Message arguments
-----------------
In the :class:`.Message` class, the arguments are stored as :class:`bytes`.
:ref:`Type converters <type-conversions>` convert the arguments to and from
their semantics types. The built-in type converter for :class:`str` converts
using UTF-8 encoding.

If a request handler wishes to use a different encoding (such as ASCII), it
can take the argument as type :class:`bytes` and apply its own conversion.

Sensors
-------
Sensor names, descriptions and units are all strings. At present they are
encoded as UTF-8 for transmission, but may in future be restricted to
human-readable ASCII.
