# Exchange CAN frames between a LilyGo T-2CAN and a Pyboard v1.1.
#
# Copy this file to both boards. Set ROLE = "ping" on one board and
# ROLE = "echo" on the other, then run it on both.
#
# Both boards use machine.CAN controller 1 at 500 kbit/s. The T-2CAN uses
# its onboard CAN-B transceiver (GPIO7 TX, GPIO6 RX). On a Pyboard v1.1,
# CAN1 uses Y4/TX (PB9) and Y3/RX (PB8), connected to an external CAN
# transceiver. Connect CANH to CANH, CANL to CANL, and share transceiver GND.

import asyncio
from machine import CAN
from time import ticks_diff, ticks_ms

import aiocan


# Set to "ping" on one board and "echo" on the other.
ROLE = "ping"

BITRATE = 500_000
PING_ID = 0x120
PONG_ID = 0x121


def _u32_le(value):
    return bytes((value & 0xFF, (value >> 8) & 0xFF, (value >> 16) & 0xFF, (value >> 24) & 0xFF))


def _read_u32_le(data, offset=0):
    return (
        data[offset]
        | (data[offset + 1] << 8)
        | (data[offset + 2] << 16)
        | (data[offset + 3] << 24)
    )


async def run_ping(bus):
    sequence = 0
    while True:
        sent_at = ticks_ms()
        payload = bytes((sequence & 0xFF, (sequence >> 8) & 0xFF))
        await bus.send(PING_ID, payload)

        try:
            reply = await bus.recv(PONG_ID, timeout_ms=2000)
        except aiocan.CanTimeoutError:
            print("No reply; retrying sequence", sequence)
            continue

        if len(reply.data) != 6:
            print("Unexpected reply payload:", reply.data)
            continue

        reply_sequence = reply.data[0] | (reply.data[1] << 8)
        if reply_sequence != sequence:
            print("Ignoring stale reply for sequence", reply_sequence)
            continue

        echo_ticks = _read_u32_le(reply.data, 2)
        print(
            "PONG seq={} RTT={} ms; echo board ticks_ms={}".format(
                sequence, ticks_diff(ticks_ms(), sent_at), echo_ticks
            )
        )
        sequence = (sequence + 1) & 0xFFFF
        await asyncio.sleep_ms(500)


async def run_echo(bus):
    async with bus.subscribe(PING_ID) as queue:
        while True:
            ping = await queue.get()
            if len(ping.data) != 2:
                print("Ignoring unexpected ping payload:", ping.data)
                continue

            sequence = ping.data[0] | (ping.data[1] << 8)
            payload = ping.data + _u32_le(ticks_ms())
            await bus.send(PONG_ID, payload)
            print("PING seq={} -> PONG".format(sequence))


async def main():
    can = CAN(1, BITRATE)
    # machine.CAN starts with receive filters set to reject all frames.
    can.set_filters(None)
    bus = aiocan.Bus(can)

    try:
        if ROLE == "ping":
            await run_ping(bus)
        elif ROLE == "echo":
            await run_echo(bus)
        else:
            raise ValueError('ROLE must be "ping" or "echo"')
    finally:
        await bus.deinit()


asyncio.run(main())
