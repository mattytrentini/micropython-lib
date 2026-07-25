# MicroPython aiocan module
# MIT license; Copyright (c) 2026 Matt Trentini

import asyncio


log_level: int = 1


def log_error(*args) -> None:
    if log_level > 0:
        print("[aiocan] E:", *args)


def log_warn(*args) -> None:
    if log_level > 1:
        print("[aiocan] W:", *args)


def log_info(*args) -> None:
    if log_level > 2:
        print("[aiocan] I:", *args)


class CanError(Exception):
    pass


class BusOffError(CanError):
    pass


class TxError(CanError):
    pass


class Message:
    def __init__(self, id: int, data: bytes, rtr: bool = False, extid: bool = False, error_flags: int = 0) -> None:
        self.id: int = id
        self.data: bytes = data
        self.rtr: bool = rtr
        self.extid: bool = extid
        self.error_flags: int = error_flags

    def __repr__(self) -> str:
        return "Message(id=0x{:03X}, data={}, rtr={})".format(
            self.id, self.data, self.rtr
        )


class PeriodicTask:
    def __init__(self, can_id: int, period_ms: int, flags: int) -> None:
        self.can_id: int = can_id
        self.period_ms: int = period_ms
        self.flags: int = flags
        self._data: bytearray = bytearray()
        self._task: asyncio.Task | None = None

    def update(self, data: bytes | bytearray) -> None:
        """Update the payload sent on each cycle."""
        if len(data) != len(self._data):
            self._data = bytearray(data)
        else:
            self._data[:] = data

    def cancel(self) -> None:
        """Stop the periodic transmission."""
        if self._task:
            self._task.cancel()
            self._task = None


class _Subscription:
    def __init__(self, bus: "Bus", can_id: int | list[int] | tuple[int, ...] | None, maxsize: int = 4) -> None:
        self._bus: Bus = bus
        if can_id is None:
            self._can_ids: tuple[int, ...] | None = None
        elif isinstance(can_id, int):
            self._can_ids = (can_id,)
        else:
            self._can_ids = tuple(can_id)
        self._queue: asyncio.Queue = asyncio.Queue(maxsize)

    async def __aenter__(self) -> asyncio.Queue:
        if self._can_ids is None:
            self._bus._wildcard_subscribers.append(self._queue)
        else:
            for can_id in self._can_ids:
                self._bus._subscribers.setdefault(can_id, []).append(self._queue)
        return self._queue

    async def __aexit__(self, *_) -> None:
        if self._can_ids is None:
            subs = self._bus._wildcard_subscribers
            if self._queue in subs:
                subs.remove(self._queue)
        else:
            for can_id in self._can_ids:
                subs = self._bus._subscribers.get(can_id, [])
                if self._queue in subs:
                    subs.remove(self._queue)


class Bus:
    """Async CAN bus wrapper.

    Accepts any CAN-like object that implements the expected interface:
    irq(), send(), recv(), state(), set_filters(), get_counters(),
    restart(), deinit(), plus class attributes IRQ_RX, IRQ_STATE,
    FLAG_RTR, FLAG_EXT_ID.

    Usage::

        from machine import CAN
        can = CAN(0, bitrate=500_000)
        bus = aiocan.Bus(can)
        msg = await bus.recv(0x581, timeout_ms=300)
        await bus.send(0x601, b'\\x40\\x00\\x10\\x00\\x00\\x00\\x00\\x00')
        await bus.deinit()
    """

    def __init__(self, can) -> None:
        self._can = can
        self._rx_flag: asyncio.ThreadSafeFlag = asyncio.ThreadSafeFlag()
        self._state_flag: asyncio.ThreadSafeFlag = asyncio.ThreadSafeFlag()
        self._subscribers: dict[int, list[asyncio.Queue]] = {}
        self._wildcard_subscribers: list[asyncio.Queue] = []
        self._can.irq(self._can.IRQ_RX | self._can.IRQ_STATE, self._irq)
        self._recv_task: asyncio.Task = asyncio.create_task(self._run())

    def _irq(self, can, event: int) -> None:
        # Called from IRQ context — must not allocate.
        if event & self._can.IRQ_RX:
            self._rx_flag.set()
        if event & self._can.IRQ_STATE:
            self._state_flag.set()

    async def _run(self) -> None:
        while True:
            await self._rx_flag.wait()
            while True:
                frame = self._can.recv()
                if frame is None:
                    break
                id, data, flags, error_flags = frame
                msg = Message(
                    id,
                    bytes(data),
                    rtr=bool(flags & self._can.FLAG_RTR),
                    extid=bool(flags & self._can.FLAG_EXT_ID),
                    error_flags=error_flags,
                )
                self._dispatch(msg)

    def _dispatch(self, msg: Message) -> None:
        for q in self._subscribers.get(msg.id, ()):
            self._enqueue(q, msg)
        for q in self._wildcard_subscribers:
            self._enqueue(q, msg)

    @staticmethod
    def _enqueue(q: asyncio.Queue, msg: Message) -> None:
        try:
            q.put_nowait(msg)
        except Exception:
            log_warn("Queue full, dropping 0x{:03X}".format(msg.id))

    async def send(self, id: int, data: bytes | bytearray, flags: int = 0) -> int:
        """Send a CAN message. Raises TxError if the TX queue is full."""
        result = self._can.send(id, data, flags=flags)
        if result is None:
            raise TxError("TX queue full (id=0x{:03X})".format(id))
        return result

    def subscribe(
        self, can_id: int | list[int] | tuple[int, ...], maxsize: int = 4
    ) -> _Subscription:
        """Async context manager yielding a Queue for the given CAN ID(s).

        ``can_id`` may be a single ID or a list/tuple of IDs — frames
        matching any of them are delivered to the same queue.

        Usage::

            async with bus.subscribe(0x181) as q:
                while True:
                    msg = await q.get()
                    process(msg)

            async with bus.subscribe([0x181, 0x182, 0x183]) as q:
                while True:
                    msg = await q.get()
                    process(msg)
        """
        return _Subscription(self, can_id, maxsize)

    def subscribe_all(self, maxsize: int = 4) -> _Subscription:
        """Async context manager yielding a Queue that receives every frame,
        regardless of arbitration ID.

        Usage::

            async with bus.subscribe_all() as q:
                while True:
                    msg = await q.get()
                    process(msg)
        """
        return _Subscription(self, None, maxsize)

    async def recv(self, can_id: int, timeout_ms: int | None = None) -> Message:
        """Receive a single message matching can_id, optionally with timeout."""
        async with _Subscription(self, can_id, maxsize=1) as q:
            if timeout_ms is None:
                return await q.get()
            try:
                return await asyncio.wait_for(q.get(), timeout_ms / 1000)
            except asyncio.TimeoutError:
                raise CanError("Timeout waiting for 0x{:03X}".format(can_id))

    def send_periodic(
        self, can_id: int, data: bytes | bytearray, period_ms: int, flags: int = 0
    ) -> PeriodicTask:
        """Start a periodic transmit task.

        Returns a PeriodicTask. Call .update(data) to change the payload or
        .cancel() to stop.
        """
        pt = PeriodicTask(can_id, period_ms, flags)
        pt._data = bytearray(data)
        pt._task = asyncio.create_task(self._periodic(pt))
        return pt

    async def _periodic(self, pt: PeriodicTask) -> None:
        while True:
            try:
                self._can.send(pt.can_id, pt._data, flags=pt.flags)
            except Exception as e:
                log_warn("Periodic send failed:", e)
            await asyncio.sleep_ms(pt.period_ms)

    def state(self) -> int:
        """Return the current bus state (one of the Bus.STATE_* constants)."""
        return self._can.state()

    async def wait_state_change(self) -> int:
        """Suspend until the bus state changes, then return the new state."""
        await self._state_flag.wait()
        return self._can.state()

    def set_filters(self, filters: list[tuple[int, int, int]] | None) -> None:
        """Configure hardware receive filters.

        - None                      : accept all messages (default)
        - []                        : reject all messages
        - [(id, mask, flags), ...]  : accept messages matching any entry
        """
        self._can.set_filters(filters)

    def get_counters(self) -> list[int]:
        """Return controller counters (TEC, REC, pending TX/RX, overruns)."""
        return self._can.get_counters()

    async def restart(self) -> int:
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

    async def deinit(self) -> None:
        """Cancel the receive task and deinitialise the CAN controller."""
        self._recv_task.cancel()
        self._can.deinit()
