# MicroPython aiocan module
# MIT license; Copyright (c) 2026 Matt Trentini

import asyncio
from machine import CAN


log_level = 1


def log_error(*args):
    if log_level > 0:
        print("[aiocan] E:", *args)


def log_warn(*args):
    if log_level > 1:
        print("[aiocan] W:", *args)


def log_info(*args):
    if log_level > 2:
        print("[aiocan] I:", *args)


class CanError(Exception):
    pass


class BusOffError(CanError):
    pass


class TxError(CanError):
    pass


class Message:
    def __init__(self, id, data, flags=0, error_flags=0):
        self.id = id
        self.data = data
        self.flags = flags
        self.error_flags = error_flags

    @property
    def rtr(self):
        return bool(self.flags & CAN.FLAG_RTR)

    @property
    def extid(self):
        return bool(self.flags & CAN.FLAG_EXT_ID)

    def __repr__(self):
        return "Message(id=0x{:03X}, data={}, rtr={})".format(
            self.id, self.data, self.rtr
        )


class PeriodicTask:
    def __init__(self, can_id, period_ms, flags):
        self.can_id = can_id
        self.period_ms = period_ms
        self.flags = flags
        self._data = bytearray()
        self._task = None

    def update(self, data):
        """Update the payload sent on each cycle."""
        if len(data) != len(self._data):
            self._data = bytearray(data)
        else:
            self._data[:] = data

    def cancel(self):
        """Stop the periodic transmission."""
        if self._task:
            self._task.cancel()
            self._task = None


class _Subscription:
    def __init__(self, bus, can_id, maxsize=4):
        self._bus = bus
        self._can_id = can_id
        self._queue = asyncio.Queue(maxsize)

    async def __aenter__(self):
        self._bus._subscribers.setdefault(self._can_id, []).append(self._queue)
        return self._queue

    async def __aexit__(self, *_):
        subs = self._bus._subscribers.get(self._can_id, [])
        if self._queue in subs:
            subs.remove(self._queue)


class Bus:
    """Async wrapper around machine.CAN.

    Usage::

        bus = aiocan.Bus(0, bitrate=500_000)
        msg = await bus.recv(0x581, timeout_ms=300)
        await bus.send(0x601, b'\\x40\\x00\\x10\\x00\\x00\\x00\\x00\\x00')
        await bus.deinit()
    """

    STATE_STOPPED = CAN.STATE_STOPPED
    STATE_ACTIVE = CAN.STATE_ACTIVE
    STATE_WARNING = CAN.STATE_WARNING
    STATE_PASSIVE = CAN.STATE_PASSIVE
    STATE_BUS_OFF = CAN.STATE_BUS_OFF

    MODE_NORMAL = CAN.MODE_NORMAL
    MODE_LOOPBACK = CAN.MODE_LOOPBACK
    MODE_SILENT = CAN.MODE_SILENT
    MODE_SILENT_LOOPBACK = CAN.MODE_SILENT_LOOPBACK

    def __init__(self, id, bitrate=250_000, mode=None, **kwargs):
        if mode is None:
            mode = CAN.MODE_NORMAL
        self._can = CAN(id, bitrate, mode=mode, **kwargs)
        self._rx_flag = asyncio.ThreadSafeFlag()
        self._state_flag = asyncio.ThreadSafeFlag()
        self._subscribers = {}  # {can_id: [asyncio.Queue, ...]}
        self._can.irq(CAN.IRQ_RX | CAN.IRQ_STATE, self._irq)
        self._recv_task = asyncio.create_task(self._run())

    def _irq(self, can, event):
        # Called from IRQ context — must not allocate.
        if event & CAN.IRQ_RX:
            self._rx_flag.set()
        if event & CAN.IRQ_STATE:
            self._state_flag.set()

    async def _run(self):
        while True:
            await self._rx_flag.wait()
            while True:
                frame = self._can.recv()
                if frame is None:
                    break
                id, data, flags, error_flags = frame
                msg = Message(id, bytes(data), flags, error_flags)
                self._dispatch(msg)

    def _dispatch(self, msg):
        for q in self._subscribers.get(msg.id, ()):
            try:
                q.put_nowait(msg)
            except Exception:
                log_warn("Queue full, dropping 0x{:03X}".format(msg.id))

    async def send(self, id, data, flags=0):
        """Send a CAN message. Raises TxError if the TX queue is full."""
        result = self._can.send(id, data, flags=flags)
        if result is None:
            raise TxError("TX queue full (id=0x{:03X})".format(id))
        return result

    def subscribe(self, can_id, maxsize=4):
        """Async context manager yielding a Queue for the given CAN ID.

        Usage::

            async with bus.subscribe(0x181) as q:
                while True:
                    msg = await q.get()
                    process(msg)
        """
        return _Subscription(self, can_id, maxsize)

    async def recv(self, can_id, timeout_ms=None):
        """Receive a single message matching can_id, optionally with timeout."""
        async with _Subscription(self, can_id, maxsize=1) as q:
            if timeout_ms is None:
                return await q.get()
            try:
                return await asyncio.wait_for(q.get(), timeout_ms / 1000)
            except asyncio.TimeoutError:
                raise CanError("Timeout waiting for 0x{:03X}".format(can_id))

    def send_periodic(self, can_id, data, period_ms, flags=0):
        """Start a periodic transmit task.

        Returns a PeriodicTask. Call .update(data) to change the payload or
        .cancel() to stop.
        """
        pt = PeriodicTask(can_id, period_ms, flags)
        pt._data = bytearray(data)
        pt._task = asyncio.create_task(self._periodic(pt))
        return pt

    async def _periodic(self, pt):
        while True:
            try:
                self._can.send(pt.can_id, pt._data, flags=pt.flags)
            except Exception as e:
                log_warn("Periodic send failed:", e)
            await asyncio.sleep_ms(pt.period_ms)

    def state(self):
        """Return the current bus state (one of the Bus.STATE_* constants)."""
        return self._can.state()

    async def wait_state_change(self):
        """Suspend until the bus state changes, then return the new state."""
        await self._state_flag.wait()
        return self._can.state()

    def set_filters(self, filters):
        """Configure hardware receive filters.

        - None                      : accept all messages (default)
        - []                        : reject all messages
        - [(id, mask, flags), ...]  : accept messages matching any entry
        """
        self._can.set_filters(filters)

    def get_counters(self):
        """Return controller counters (TEC, REC, pending TX/RX, overruns)."""
        return self._can.get_counters()

    async def restart(self):
        """Request recovery from BUS_OFF state.

        Calls machine.CAN.restart() then polls until the state is no longer
        BUS_OFF, returning the new state.
        """
        self._can.restart()
        while True:
            state = self._can.state()
            if state != self.STATE_BUS_OFF:
                return state
            await asyncio.sleep_ms(10)

    async def deinit(self):
        """Cancel the receive task and deinitialise the CAN controller."""
        self._recv_task.cancel()
        self._can.deinit()
