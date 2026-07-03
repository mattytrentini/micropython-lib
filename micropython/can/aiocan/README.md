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

Or copy the `aiocan/` directory to your device.

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

Mode and state constants come from `machine.CAN` directly (e.g.
`CAN.MODE_LOOPBACK`, `CAN.STATE_ACTIVE`). `aiocan.Bus` does not re-export
them.

---

#### `await bus.send(id, data, flags=0)`

Send a CAN frame. `flags` may include `machine.CAN.FLAG_RTR` and/or
`machine.CAN.FLAG_EXT_ID`. Raises `TxError` if the hardware TX queue is full.

---

#### `await bus.recv(can_id, timeout_ms=None)`

Wait for a single frame with the given arbitration ID. Returns a `Message`.
Raises `CanError` if `timeout_ms` elapses without a matching frame.

---

#### `bus.subscribe(can_id, maxsize=4)`

Async context manager that yields an `asyncio.Queue` pre-filled as matching
frames arrive. Frames are dropped (with a warning) if the queue is full.

```python
async with bus.subscribe(0x181) as q:
    msg = await q.get()
```

Multiple subscribers for the same `can_id` are supported; each receives a
copy of every matching frame.

---

#### `bus.send_periodic(can_id, data, period_ms, flags=0)` → `PeriodicTask`

Start transmitting `data` on `can_id` every `period_ms` milliseconds.

---

#### `bus.state()` → `int`

Return the current bus state (one of the `Bus.STATE_*` constants).

---

#### `await bus.wait_state_change()` → `int`

Suspend until the bus state changes, then return the new state.

```python
state = await bus.wait_state_change()
if state == aiocan.Bus.STATE_BUS_OFF:
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
| `flags` | `int` | Raw flags from `machine.CAN.recv()` |
| `error_flags` | `int` | Error flags (`RECV_ERR_FULL`, `RECV_ERR_OVERRUN`) |
| `rtr` | `bool` | Remote transmission request frame |
| `extid` | `bool` | 29-bit extended ID frame |

---

### `aiocan.PeriodicTask`

Returned by `bus.send_periodic()`.

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
| `BusOffError` | Raised when the bus enters the BUS_OFF state |
| `TxError` | TX queue was full when `send()` was called |

`asyncio.TimeoutError` is re-raised as `CanError` by `recv()`.

---

## Logging

Set `aiocan.log_level` to control verbosity:

| Value | Output |
|---|---|
| `0` | Errors only |
| `1` | Errors + warnings (default) |
| `2` | + info messages |

---

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
