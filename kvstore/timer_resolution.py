"""Make asyncio timers accurate on Windows.

Windows' default system timer ticks every ~15.6 ms, and asyncio.sleep() wakes
on the next tick. The shim's injected delay (uniform 5-50 ms) would then be
rounded up to ~16, 31, 47 or 62 ms: on average ~8 ms too long, and no longer
uniform. Measured on the development laptop: mean overshoot 7.96 ms by
default, 0.98 ms after timeBeginPeriod(1).

Call use_1ms_timer() once at process start in every node and experiment
process. It is a no-op on other platforms.
"""

from __future__ import annotations

import sys


def use_1ms_timer() -> bool:
    """Request 1 ms timer resolution for this process. Returns True if applied."""
    if sys.platform != "win32":
        return False
    try:
        import ctypes
        return ctypes.WinDLL("winmm").timeBeginPeriod(1) == 0
    except (OSError, AttributeError):
        return False
