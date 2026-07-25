# aiocan vs python-can asyncio — comparison

Side-by-side examples covering the most common patterns.
python-can examples use `interface="virtual"` or `interface="socketcan"` (no hardware needed for virtual).
aiocan examples use `machine.CAN` with `MODE_LOOPBACK` where self-contained.

---

## Example 1 — Send and receive a single message

**python-can**
```python
import asyncio
import can

async def main():
    with can.Bus(interface="virtual", channel="test",
                 receive_own_messages=True) as bus:

        reader = can.AsyncBufferedReader()
        notifier = can.Notifier(bus, [reader], loop=asyncio.get_running_loop())

        bus.send(can.Message(arbitration_id=0x123, data=b'\x01\x02\x03'))

        # AsyncBufferedReader returns any message — caller must filter by ID
        while True:
            msg = await reader.get_message()
            if msg.arbitration_id == 0x123:
                print(hex(msg.arbitration_id), msg.data)
                break

        notifier.stop()

asyncio.run(main())
```

**aiocan**
```python
import asyncio
from machine import CAN
import aiocan

async def main():
    bus = aiocan.Bus(CAN(0, bitrate=500_000, mode=CAN.MODE_LOOPBACK))

    await bus.send(0x123, b'\x01\x02\x03')

    # recv() waits for exactly this ID — no manual filtering needed
    msg = await bus.recv(0x123, timeout_ms=500)
    print(hex(msg.id), msg.data)

    await bus.deinit()

asyncio.run(main())
```

---

## Example 2 — Subscribe to a stream of messages

**python-can**
```python
import asyncio
import can

async def main():
    with can.Bus(interface="socketcan", channel="vcan0") as bus:
        reader = can.AsyncBufferedReader()
        notifier = can.Notifier(bus, [reader], loop=asyncio.get_running_loop())

        # All frames pass through the single reader — must filter manually
        async def watch_pdo():
            while True:
                msg = await reader.get_message()
                if msg.arbitration_id == 0x181:
                    print("PDO:", msg.data)

        await asyncio.wait_for(watch_pdo(), timeout=10)
        notifier.stop()

asyncio.run(main())
```

**aiocan**
```python
import asyncio
from machine import CAN
import aiocan

async def main():
    bus = aiocan.Bus(CAN(0, bitrate=500_000))

    # Only 0x181 frames reach this queue
    async with bus.subscribe(0x181) as q:
        deadline = asyncio.get_event_loop().time() + 10
        while asyncio.get_event_loop().time() < deadline:
            msg = await asyncio.wait_for(q.get(), timeout=1)
            print("PDO:", msg.data)

    await bus.deinit()

asyncio.run(main())
```

---

## Example 3 — Periodic transmit with a runtime update

**python-can**
```python
import asyncio
import can

async def main():
    with can.Bus(interface="socketcan", channel="vcan0") as bus:
        msg = can.Message(arbitration_id=0x700, data=b'\x05',
                          is_extended_id=False)

        # period is in seconds; returns a thread-based PeriodicTask
        task = bus.send_periodic(msg, period=1.0)

        await asyncio.sleep(3)

        # Update: must replace the whole message and restart the task
        task.stop()
        msg = can.Message(arbitration_id=0x700, data=b'\x7F',
                          is_extended_id=False)
        task = bus.send_periodic(msg, period=1.0)

        await asyncio.sleep(3)
        task.stop()

asyncio.run(main())
```

**aiocan**
```python
import asyncio
from machine import CAN
import aiocan

async def main():
    bus = aiocan.Bus(CAN(0, bitrate=500_000))

    # period_ms is in milliseconds; task is asyncio-based
    task = bus.send_periodic(0x700, b'\x05', period_ms=1000)

    await asyncio.sleep(3)

    # Update payload in-place — no need to stop and restart
    task.update(b'\x7F')

    await asyncio.sleep(3)
    task.cancel()

    await bus.deinit()

asyncio.run(main())
```

---

## Summary of differences

| | python-can asyncio | aiocan |
|---|---|---|
| Bus setup | `can.Bus(interface=..., channel=...)` constructs everything | External CAN object passed in |
| Notifier | Explicit `can.Notifier(bus, listeners, loop=...)` required | Built into `Bus`, no setup needed |
| Receive | One `AsyncBufferedReader` gets all frames; caller filters by ID | `recv(id)` or `subscribe(id)` — filtering is built in |
| Send | Construct `can.Message(arbitration_id=..., data=...)` | `await bus.send(can_id, data)` |
| Periodic update | Stop task, rebuild `Message`, restart | `task.update(data)` in-place |
| Period units | Seconds (`period=1.0`) | Milliseconds (`period_ms=1000`) |
| Thread model | One background thread per bus | No threads — pure asyncio |

The filtering difference in Example 2 is the most significant in practice. In
python-can every listener sees every frame on the bus, so any routing logic is
the caller's responsibility. In aiocan the subscription *is* the routing —
frames for different COB-IDs never touch each other's queues, which maps
directly onto how CANopen partitions its message space.
