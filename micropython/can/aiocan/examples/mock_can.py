# MockCAN — in-process CAN-like object for testing aiocan on CPython.
#
# Implements the same interface that aiocan.Bus expects from machine.CAN,
# so tests can run on desktop without MicroPython or real hardware.
#
# Usage::
#
#   import asyncio
#   from mock_can import MockCAN
#   import aiocan
#
#   async def test():
#       can = MockCAN()                         # loopback by default
#       bus = aiocan.Bus(can)
#
#       await bus.send(0x123, b'\x01\x02')      # also injects into rx
#       msg = await bus.recv(0x123)
#       assert msg.data == b'\x01\x02'
#
#       can.inject(0x456, b'\xAB')              # inject without sending
#       msg = await bus.recv(0x456)
#
#       can.set_state(MockCAN.STATE_BUS_OFF)    # trigger a state change
#       await bus.deinit()
#
#   asyncio.run(test())


class MockCAN:
    # Protocol constants. Must be non-overlapping bits within each group
    # so they can be combined with | in irq() trigger masks.
    IRQ_RX    = 0x01
    IRQ_STATE = 0x02

    FLAG_RTR    = 0x01
    FLAG_EXT_ID = 0x02

    STATE_STOPPED = 0
    STATE_ACTIVE  = 1
    STATE_WARNING = 2
    STATE_PASSIVE = 3
    STATE_BUS_OFF = 4

    MODE_NORMAL          = 0
    MODE_LOOPBACK        = 1
    MODE_SILENT          = 2
    MODE_SILENT_LOOPBACK = 3

    def __init__(self, loopback: bool = True) -> None:
        """
        Args:
            loopback: When True (default), frames passed to send() are also
                      pushed into the receive queue, mirroring MODE_LOOPBACK.
                      Set to False to test one-way transmission; use inject()
                      to feed frames into the receive path independently.
        """
        self._loopback = loopback
        self._handler = None
        self._trigger = 0
        self._rx_queue = []
        self._state = self.STATE_ACTIVE
        self._filters = None    # None = accept all

        # Public log of every frame passed to send(), as (id, data, flags).
        # Tests can inspect or clear this freely.
        self.tx_log: list = []

    # ------------------------------------------------------------------ #
    # aiocan.Bus interface                                                 #
    # ------------------------------------------------------------------ #

    def irq(self, trigger: int, handler, hard: bool = False) -> None:
        self._trigger = trigger
        self._handler = handler

    def send(self, id: int, data: bytes | bytearray, flags: int = 0) -> int | None:
        """Record the frame and, in loopback mode, inject it into the rx path."""
        if self._state == self.STATE_BUS_OFF:
            return None
        idx = len(self.tx_log)
        self.tx_log.append((id, bytes(data), flags))
        if self._loopback:
            self.inject(id, data, flags)
        return idx

    def recv(self) -> tuple | None:
        """Return the next queued frame as (id, data, flags, error_flags) or None."""
        return self._rx_queue.pop(0) if self._rx_queue else None

    def state(self) -> int:
        return self._state

    def set_filters(self, filters) -> None:
        self._filters = filters

    def get_counters(self) -> list:
        return [0, 0, 0, 0]

    def restart(self) -> None:
        if self._state == self.STATE_BUS_OFF:
            self.set_state(self.STATE_ACTIVE)

    def deinit(self) -> None:
        self._handler = None

    # ------------------------------------------------------------------ #
    # Test helpers                                                         #
    # ------------------------------------------------------------------ #

    def inject(self, id: int, data: bytes | bytearray, flags: int = 0, error_flags: int = 0) -> None:
        """Push a frame directly into the receive queue.

        Simulates a frame arriving from the bus, independent of send().
        Skipped if the frame does not pass the active hardware filters.
        Calls the registered IRQ handler with IRQ_RX so that aiocan.Bus
        wakes its receive task immediately.
        """
        if not self._passes_filter(id, flags):
            return
        self._rx_queue.append((id, bytes(data), flags, error_flags))
        self._notify(self.IRQ_RX)

    def set_state(self, state: int) -> None:
        """Change the bus state and fire IRQ_STATE if a handler is registered."""
        self._state = state
        self._notify(self.IRQ_STATE)

    def clear_tx_log(self) -> None:
        """Reset the transmit log between test cases."""
        self.tx_log.clear()

    # ------------------------------------------------------------------ #
    # Internal                                                             #
    # ------------------------------------------------------------------ #

    def _notify(self, event: int) -> None:
        if self._handler and (self._trigger & event):
            self._handler(self, event)

    def _passes_filter(self, id: int, flags: int) -> bool:
        if self._filters is None:
            return True     # accept all
        if not self._filters:
            return False    # reject all
        for f_id, f_mask, f_flags in self._filters:
            if (id & f_mask) == (f_id & f_mask):
                return True
        return False
