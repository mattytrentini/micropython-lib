# aiocan loopback self-test
#
# Runs entirely on a single board using MODE_LOOPBACK — no second node or
# transceiver required. Adjust the bus id and bitrate for your hardware.
#
# Usage:
#   import loopback_test

import asyncio
from machine import CAN
import aiocan

BUS_ID = 0
BITRATE = 250_000

_passed = 0
_failed = 0


def _ok(name):
    global _passed
    _passed += 1
    print("  PASS:", name)


def _fail(name, reason):
    global _failed
    _failed += 1
    print("  FAIL:", name, "—", reason)


async def test_basic_send_recv(bus):
    """send() followed by recv() returns the same frame."""
    await bus.send(0x123, b'\xDE\xAD\xBE\xEF')
    msg = await bus.recv(0x123, timeout_ms=200)
    assert msg.id == 0x123, "id mismatch"
    assert msg.data == b'\xDE\xAD\xBE\xEF', "data mismatch"
    _ok("basic send/recv")


async def test_recv_timeout(bus):
    """recv() raises CanError when no matching frame arrives in time."""
    try:
        await bus.recv(0x7FF, timeout_ms=100)
        _fail("recv timeout", "no exception raised")
    except aiocan.CanError:
        _ok("recv timeout")


async def test_subscribe_single(bus):
    """subscribe() queues frames as they arrive."""
    async with bus.subscribe(0x200) as q:
        await bus.send(0x200, b'\x01')
        await bus.send(0x200, b'\x02')
        # Allow the receive task to process both frames.
        await asyncio.sleep_ms(20)
        assert not q.empty(), "queue should not be empty"
        m1 = await q.get()
        m2 = await q.get()
        assert m1.data == b'\x01', "first frame data"
        assert m2.data == b'\x02', "second frame data"
    _ok("subscribe single consumer")


async def test_subscribe_multiple(bus):
    """Two subscribers on the same ID both receive the frame."""
    async with bus.subscribe(0x300) as q1:
        async with bus.subscribe(0x300) as q2:
            await bus.send(0x300, b'\xAB')
            await asyncio.sleep_ms(20)
            assert not q1.empty(), "q1 should have frame"
            assert not q2.empty(), "q2 should have frame"
            assert (await q1.get()).data == b'\xAB'
            assert (await q2.get()).data == b'\xAB'
    _ok("subscribe multiple consumers")


async def test_subscribe_no_crosstalk(bus):
    """A frame for 0x400 does not appear in a subscriber for 0x401."""
    async with bus.subscribe(0x401) as q:
        await bus.send(0x400, b'\xFF')
        await asyncio.sleep_ms(20)
        assert q.empty(), "wrong ID should not be queued"
    _ok("subscribe no crosstalk")


async def test_subscribe_multi_id(bus):
    """subscribe() accepts a list of IDs; only those IDs are queued."""
    async with bus.subscribe([0x210, 0x211]) as q:
        await bus.send(0x210, b'\x01')
        await bus.send(0x211, b'\x02')
        await bus.send(0x212, b'\x03')  # not subscribed — should be dropped
        await asyncio.sleep_ms(20)
        m1 = await q.get()
        m2 = await q.get()
        assert m1.id == 0x210 and m1.data == b'\x01', "first frame"
        assert m2.id == 0x211 and m2.data == b'\x02', "second frame"
        assert q.empty(), "unsubscribed ID should not be queued"
    _ok("subscribe multi-id")


async def test_subscribe_all(bus):
    """subscribe_all() receives frames for every arbitration ID."""
    async with bus.subscribe_all() as q:
        await bus.send(0x220, b'\xAA')
        await bus.send(0x221, b'\xBB')
        await asyncio.sleep_ms(20)
        m1 = await q.get()
        m2 = await q.get()
        assert m1.id == 0x220, "first frame id"
        assert m2.id == 0x221, "second frame id"
    _ok("subscribe_all")


async def test_subscribe_all_and_single_coexist(bus):
    """A subscribe_all() queue and a single-ID subscribe() both see a frame."""
    async with bus.subscribe(0x230) as q_single:
        async with bus.subscribe_all() as q_all:
            await bus.send(0x230, b'\x0A')
            await asyncio.sleep_ms(20)
            assert (await q_single.get()).id == 0x230, "single subscriber"
            assert (await q_all.get()).id == 0x230, "wildcard subscriber"
    _ok("subscribe_all coexists with subscribe")


async def test_message_properties(bus):
    """Message.id, .data, .rtr and .extid are populated correctly."""
    await bus.send(0x555, b'\x00\x01\x02\x03\x04\x05\x06\x07')
    msg = await bus.recv(0x555, timeout_ms=200)
    assert msg.id == 0x555
    assert msg.data == b'\x00\x01\x02\x03\x04\x05\x06\x07'
    assert not msg.rtr
    assert not msg.extid
    _ok("message properties")


async def test_empty_payload(bus):
    """Zero-length data frames are handled correctly."""
    await bus.send(0x600, b'')
    msg = await bus.recv(0x600, timeout_ms=200)
    assert msg.data == b'', "expected empty payload"
    _ok("empty payload")


async def test_periodic_task(bus):
    """send_periodic() transmits at roughly the requested interval."""
    received = []

    async def collect():
        async with bus.subscribe(0x700) as q:
            for _ in range(3):
                received.append(await asyncio.wait_for(q.get(), 0.5))

    task = bus.send_periodic(0x700, b'\x05', period_ms=50)
    try:
        await collect()
    finally:
        task.cancel()

    assert len(received) == 3, "expected 3 frames"
    assert all(m.data == b'\x05' for m in received), "unexpected payload"
    _ok("periodic task transmit")


async def test_periodic_update(bus):
    """PeriodicTask.update() changes the payload of subsequent frames."""
    received = []

    async def collect():
        async with bus.subscribe(0x701) as q:
            for _ in range(4):
                received.append(await asyncio.wait_for(q.get(), 0.5))

    task = bus.send_periodic(0x701, b'\x01', period_ms=50)
    # Let one frame through, then update the payload.
    await asyncio.sleep_ms(75)
    task.update(b'\x02')
    try:
        await collect()
    finally:
        task.cancel()

    # At least one frame should carry the updated payload.
    payloads = [bytes(m.data) for m in received]
    assert b'\x02' in payloads, "updated payload not seen: {}".format(payloads)
    _ok("periodic task update")


async def test_state(bus):
    """bus.state() returns STATE_ACTIVE (or WARNING) in loopback mode."""
    s = bus.state()
    assert s in (CAN.STATE_ACTIVE, CAN.STATE_WARNING), (
        "unexpected state: {}".format(s)
    )
    _ok("bus state")


async def run_all():
    print("aiocan loopback test (bus={}, bitrate={})".format(BUS_ID, BITRATE))
    print()

    can = CAN(BUS_ID, BITRATE, mode=CAN.MODE_LOOPBACK)
    bus = aiocan.Bus(can)

    tests = [
        test_basic_send_recv,
        test_recv_timeout,
        test_subscribe_single,
        test_subscribe_multiple,
        test_subscribe_no_crosstalk,
        test_subscribe_multi_id,
        test_subscribe_all,
        test_subscribe_all_and_single_coexist,
        test_message_properties,
        test_empty_payload,
        test_periodic_task,
        test_periodic_update,
        test_state,
    ]

    for t in tests:
        try:
            await t(bus)
        except AssertionError as e:
            _fail(t.__name__, str(e))
        except Exception as e:
            _fail(t.__name__, "{}: {}".format(type(e).__name__, e))

    await bus.deinit()

    print()
    print("Results: {} passed, {} failed".format(_passed, _failed))
    if _failed:
        raise SystemExit(1)


asyncio.run(run_all())
