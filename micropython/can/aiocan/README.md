# aiocan

An async CAN bus library for MicroPython, built on top of `machine.CAN` and
`asyncio`. Modelled after [aioble](../bluetooth/aioble/), it bridges the
`machine.CAN` IRQ callback to cooperative tasks via `asyncio.ThreadSafeFlag`,
so CAN traffic can be processed without threads or polling loops.

## Installation

```python
import mip
mip.install("github:micropython/micropython-lib/micropython/can/aiocan")
```

Or copy the `aiocan/` directory to your device. `mip install`/the manifest
also pulls in [`aioqueue`](../../aioqueue/) (see
[Dependencies](#dependencies)).

## Quick start

```python
import asyncio
from machine import CAN
import aiocan

async def main():
    # Create a machine.CAN object and wrap it with aiocan.Bus
    bus = aiocan.Bus(CAN(0, bitrate=500_000))

    # Send a raw CAN frame
    await bus.send(0x123, b'\x01\x02\x03')

    # Receive a single frame (blocks until one arrives)
    msg = await bus.recv(0x456, timeout_ms=500)
    print(hex(msg.id), msg.data)

    # Subscribe to repeated messages (e.g. a PDO)
    async with bus.subscribe(0x181) as q:
        for _ in range(10):
            msg = await q.get()
            print(msg)

    # Subscribe to several IDs on one queue
    async with bus.subscribe([0x181, 0x182, 0x183]) as q:
        for _ in range(10):
            msg = await q.get()
            print(msg)

    # Subscribe to every frame on the bus
    async with bus.subscribe_all() as q:
        for _ in range(10):
            msg = await q.get()
            print(msg)

    # Periodic transmit — returns a handle to update or cancel
    heartbeat = bus.send_periodic(0x700 | 0x01, b'\x05', period_ms=1000)
    await asyncio.sleep(5)
    heartbeat.update(b'\x7F')   # change payload
    heartbeat.cancel()           # stop

    await bus.deinit()

asyncio.run(main())
```

## API

### `aiocan.Bus(can)`

Wrap a `machine.CAN`-compatible object with async receive dispatching.
`can` must already be constructed and configured (bitrate, mode, etc.) before
being passed in. Any `machine.CAN`-like object is accepted — useful for
passing mock objects in tests.

```python
from machine import CAN
import aiocan

# Normal operation
bus = aiocan.Bus(CAN(0, bitrate=250_000))

# Loopback (no transceiver needed — for testing)
bus = aiocan.Bus(CAN(0, bitrate=250_000, mode=CAN.MODE_LOOPBACK))
```

Mode constants come from `machine.CAN` directly (e.g. `CAN.MODE_LOOPBACK`),
since they're only needed when constructing the `machine.CAN` object itself.
State constants are mirrored onto the `Bus` instance at construction time
(e.g. `bus.STATE_ACTIVE`, `bus.STATE_BUS_OFF`) for convenience.

`Bus` must be constructed from within a running asyncio event loop — it
starts a background receive task immediately — typically as the first thing
done inside the `async def main()` passed to `asyncio.run()`.

---

#### `await bus.send(can_id, data, flags=0)`

Send a CAN frame. `flags` may include `machine.CAN.FLAG_RTR` and/or
`machine.CAN.FLAG_EXT_ID`. Raises `BusOffError` if the controller is in the
`BUS_OFF` state, or `TxError` if the hardware TX queue is full.

---

#### `await bus.recv(can_id, timeout_ms=None)`

Wait for a single frame with the given arbitration ID. Returns a `Message`.
Raises `CanTimeoutError` if `timeout_ms` elapses without a matching frame.

---

#### `bus.subscribe(can_id, maxsize=4)`

Async context manager that yields an `aioqueue.Queue` (see
[Dependencies](#dependencies)) pre-filled as matching frames arrive. Frames
are dropped (with a warning) if the queue is full.

```python
async with bus.subscribe(0x181) as q:
    msg = await q.get()
```

`can_id` may also be a list or tuple of IDs, in which case frames matching
any of them are delivered to the same queue:

```python
async with bus.subscribe([0x181, 0x182, 0x183]) as q:
    msg = await q.get()
```

Multiple subscribers for the same `can_id` are supported; each receives a
copy of every matching frame.

---

#### `bus.subscribe_all(maxsize=4)`

Async context manager that yields an `aioqueue.Queue` receiving every frame on
the bus, regardless of arbitration ID. Behaves like `subscribe()` in all
other respects (dropped frames on overflow, multiple subscribers allowed) and
can be used alongside ID-specific subscriptions — both receive a copy of any
matching frame.

```python
async with bus.subscribe_all() as q:
    msg = await q.get()
```

---

#### `bus.send_periodic(can_id, data, period_ms, flags=0)` → `PeriodicTask`

Start transmitting `data` on `can_id` every `period_ms` milliseconds.

---

#### `bus.state()` → `int`

Return the current bus state (one of the `bus.STATE_*` constants).

---

#### `await bus.wait_state_change()` → `int`

Suspend until the bus state changes, then return the new state.

```python
state = await bus.wait_state_change()
if state == bus.STATE_BUS_OFF:
    await bus.restart()
```

---

#### `bus.set_filters(filters)`

Configure hardware receive filters. Frames that do not match are discarded by
the controller before reaching the CPU.

| Value | Effect |
|---|---|
| `None` | Accept all (default) |
| `[]` | Reject all |
| `[(id, mask, flags), ...]` | Accept frames where `frame_id & mask == id & mask` |

Filters are applied before subscription matching: a frame that a filter
rejects never reaches the controller, so an active `subscribe()` /
`subscribe_all()` for that ID will silently see nothing.

---

#### `bus.get_counters()`

Return controller error/activity counters (TEC, REC, pending TX/RX, overruns).

---

#### `await bus.restart()`

Request recovery from `BUS_OFF`. Polls until the controller is no longer in
the BUS_OFF state, then returns the new state.

---

#### `await bus.deinit()`

Cancel the background receive task and shut down the CAN controller.

---

### `aiocan.Message`

Received frames are returned as `Message` objects.

| Attribute | Type | Description |
|---|---|---|
| `id` | `int` | Arbitration ID |
| `data` | `bytes` | Payload (0–8 bytes) |
| `error_flags` | `int` | Error flags (`RECV_ERR_FULL`, `RECV_ERR_OVERRUN`) |
| `rtr` | `bool` | Remote transmission request frame |
| `extid` | `bool` | 29-bit extended ID frame |

---

### `aiocan.PeriodicTask`

Returned by `bus.send_periodic()`. Its `can_id`, `period_ms` and `flags`
attributes are public and safe to change directly between cycles — the
transmit loop re-reads them each time.

#### `task.update(data)`

Replace the payload for subsequent transmissions. If `len(data)` matches the
existing payload length the update is done in-place (no allocation); otherwise
a new `bytearray` is allocated.

#### `task.cancel()`

Stop the periodic transmission.

---

### Exceptions

| Exception | Description |
|---|---|
| `CanError` | Base class for aiocan errors |
| `BusOffError` | Raised by `send()` when the controller is in the BUS_OFF state |
| `TxError` | TX queue was full when `send()` was called |
| `CanTimeoutError` | `recv()` timed out without a matching frame |

`CanTimeoutError` is a subclass of `CanError`, so `except CanError` still
catches it.

---

## Logging

Call `aiocan.set_log_level(n)` to control verbosity — assigning directly to
`aiocan.log_level` has no effect, since it's a plain int copied into the
package namespace at import time, not a live reference:

| Value | Output |
|---|---|
| `0` | Silent |
| `1` | Errors only (default) |
| `2` | Errors + warnings |
| `3` | + info messages |

---

## Dependencies

`subscribe()`/`subscribe_all()`/`recv()` are backed by
[`aioqueue.Queue`](../../aioqueue/), a small CPython-`asyncio.Queue`-compatible
implementation. It's a separate micropython-lib package (pulled in via
`require("aioqueue")` in `manifest.py`) because core MicroPython's `asyncio`
doesn't have a `Queue` yet — see
[micropython/micropython#5828](https://github.com/micropython/micropython/issues/5828)
and the in-progress
[micropython/micropython#19459](https://github.com/micropython/micropython/pull/19459),
which would add one to core.

Once #19459 (or equivalent) lands in a MicroPython release, aiocan should
switch to `asyncio.Queue` directly and drop the `aioqueue` dependency — the
two implementations are API-compatible for everything aiocan uses
(`put_nowait()`, `get()`, `empty()`), so that's expected to be a small change
localised to `core.py` and `manifest.py`.

## Notes

- **machine.CAN API**: targets the unified `machine.CAN` API introduced in
  MicroPython 1.24 (`irq()`, `recv()` without FIFO argument, `send()` with
  flags). Older port-specific APIs (STM32 `rxcallback`/`recv(fifo)`) are not
  supported.
- **Threading**: aiocan is single-threaded. All interaction must happen from
  within `asyncio.run()`.
- **IRQ safety**: `Bus._irq()` only calls `ThreadSafeFlag.set()`, which is
  safe to call from hard IRQ context.
- **Filter auto-configuration**: automatic `set_filters()` based on active
  subscriptions is a planned enhancement; for now, filters must be set
  manually.
