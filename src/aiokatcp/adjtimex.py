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

"""Python wrapper for the Linux-specific :func:`adjtimex` system call."""

import ctypes
import os
from typing import Tuple

TIME_OK = 0
TIME_INS = 1
TIME_DEL = 2
TIME_OOP = 3
TIME_WAIT = 4
TIME_ERROR = 5
TIME_BAD = TIME_ERROR

ADJ_OFFSET = 0x0001
ADJ_FREQUENCY = 0x0002
ADJ_MAXERROR = 0x0004
ADJ_ESTERROR = 0x0008
ADJ_STATUS = 0x0010
ADJ_TIMECONST = 0x0020
ADJ_TAI = 0x0080
ADJ_SETOFFSET = 0x0100
ADJ_MICRO = 0x1000
ADJ_NANO = 0x2000
ADJ_TICK = 0x4000
ADJ_OFFSET_SINGLESHOT = 0x8001
ADJ_OFFSET_SS_READ = 0xA001

STA_PLL = 0x0001
STA_PPSFREQ = 0x0002
STA_PPSTIME = 0x0004
STA_FLL = 0x0008
STA_INS = 0x0010
STA_DEL = 0x0020
STA_UNSYNC = 0x0040
STA_FREQHOLD = 0x0080
STA_PPSSIGNAL = 0x0100
STA_PPSJITTER = 0x0200
STA_PPSWANDER = 0x0400
STA_PPSERROR = 0x0800
STA_CLOCKERR = 0x1000
STA_NANO = 0x2000
STA_MODE = 0x4000
STA_CLK = 0x8000
STA_RONLY = (
    STA_PPSSIGNAL
    | STA_PPSJITTER
    | STA_PPSWANDER
    | STA_PPSERROR
    | STA_CLOCKERR
    | STA_NANO
    | STA_MODE
    | STA_CLK
)


class Timeval(ctypes.Structure):
    """See https://man7.org/linux/man-pages/man3/adjtime.3.html."""

    _fields_ = [
        ("tv_sec", ctypes.c_long),
        ("tv_usec", ctypes.c_long),
    ]


class Timex(ctypes.Structure):
    """See https://man7.org/linux/man-pages/man2/adjtimex.2.html."""

    _fields_ = [
        ("modes", ctypes.c_int),
        ("offset", ctypes.c_long),
        ("freq", ctypes.c_long),
        ("maxerror", ctypes.c_long),
        ("esterror", ctypes.c_long),
        ("status", ctypes.c_int),
        ("constant", ctypes.c_long),
        ("precision", ctypes.c_long),
        ("tolerance", ctypes.c_long),
        ("time", Timeval),
        ("tick", ctypes.c_long),
        ("ppsfreq", ctypes.c_long),
        ("jitter", ctypes.c_long),
        ("shift", ctypes.c_int),
        ("stabil", ctypes.c_long),
        ("jitcnt", ctypes.c_long),
        ("calcnt", ctypes.c_long),
        ("errcnt", ctypes.c_long),
        ("stbcnt", ctypes.c_long),
        ("tai", ctypes.c_int),
        ("_padding", ctypes.c_int * 11),
    ]


def _no_adjtimex(timex: Timex) -> int:
    raise NotImplementedError("System call 'adjtimex' is only available on Linux")


def _errcheck(result, func, args):
    if result == -1:
        e = ctypes.get_errno()
        raise OSError(e, os.strerror(e))
    return result


try:
    _libc = ctypes.CDLL("libc.so.6", use_errno=True)
except OSError:
    adjtimex = _no_adjtimex
else:
    _real_adjtimex = _libc.adjtimex
    _real_adjtimex.argtypes = [ctypes.POINTER(Timex)]
    _real_adjtimex.restype = ctypes.c_int
    _real_adjtimex.errcheck = _errcheck
    adjtimex = _real_adjtimex


def get_adjtimex() -> Tuple[int, Timex]:
    """Read-only adjtimex call.

    Returns
    -------
    int
        Clock state (one of the ``TIME_*`` constants)
    Timex
        Clock information
    """
    timex = Timex()
    timex.modes = 0
    state = adjtimex(timex)
    return state, timex
