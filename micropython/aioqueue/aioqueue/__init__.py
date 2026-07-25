# MicroPython aioqueue module
# MIT license
#
# Queue class adapted from Peter Hinch's asyncio primitives library
# (https://github.com/peterhinch/micropython-async), itself based on
# Paul Sokolovsky's original uasyncio Queue.
# Copyright (c) 2018-2020 Peter Hinch, Paul Sokolovsky
# Copyright (c) 2026 Matt Trentini

"""A CPython asyncio.Queue-compatible Queue for MicroPython's asyncio.

Not provided by core MicroPython (see
https://github.com/micropython/micropython/issues/5828). This is a
standalone package, not an extension of the asyncio module itself:
MicroPython's import resolver does not merge a filesystem package
directory with a frozen/built-in package of the same name, so a bolt-on
asyncio/queue.py submodule would either silently fail to import or (if
placed earlier on sys.path) shadow the rest of asyncio entirely.

Usage::

    import asyncio
    from aioqueue import Queue

    async def producer(q):
        for i in range(5):
            await q.put(i)
        await q.join()

    async def consumer(q):
        while True:
            item = await q.get()
            print("got", item)
            q.task_done()

    async def main():
        q = Queue(maxsize=2)
        asyncio.create_task(consumer(q))
        await producer(q)

    asyncio.run(main())

Unlike CPython's asyncio.Queue (and Peter Hinch's), maxsize is required and
must be > 0: MicroPython's collections.deque has no true unbounded mode,
and unbounded growth is rarely what you want on a memory-constrained
device. Pick a bound.

Not safe to put_nowait() from a hard IRQ; all methods must be called from
asyncio task context.
"""

import asyncio
from collections import deque


class QueueEmpty(Exception):
    """Raised by get_nowait() when the queue is empty."""


class QueueFull(Exception):
    """Raised by put_nowait() when the queue is full."""


class Queue:
    def __init__(self, maxsize):
        if maxsize <= 0:
            raise ValueError("maxsize must be > 0")
        self.maxsize = maxsize
        self._queue = deque((), maxsize)
        self._evput = asyncio.Event()  # set (then cleared) each time an item is put
        self._evget = asyncio.Event()  # set (then cleared) each time an item is got
        self._unfinished = 0
        self._alldone = asyncio.Event()
        self._alldone.set()

    def _get(self):
        self._evget.set()
        self._evget.clear()
        return self._queue.popleft()

    async def get(self):
        """Remove and return an item, waiting if the queue is empty."""
        while self.empty():
            await self._evput.wait()
        return self._get()

    def get_nowait(self):
        """Remove and return an item. Raises QueueEmpty if none is available."""
        if self.empty():
            raise QueueEmpty
        return self._get()

    def _put(self, val):
        self._queue.append(val)
        self._unfinished += 1
        self._alldone.clear()
        self._evput.set()
        self._evput.clear()

    async def put(self, val):
        """Add an item, waiting if the queue is full."""
        while self.full():
            await self._evget.wait()
        self._put(val)

    def put_nowait(self, val):
        """Add an item. Raises QueueFull if the queue has no room."""
        if self.full():
            raise QueueFull
        self._put(val)

    def qsize(self):
        """Return the number of items currently queued."""
        return len(self._queue)

    def empty(self):
        return len(self._queue) == 0

    def full(self):
        return len(self._queue) >= self.maxsize

    def task_done(self):
        """Indicate that a previously retrieved item has been processed.

        Used with join() by producers/consumers coordinating shutdown.
        Raises ValueError if called more times than items have been put.
        """
        if self._unfinished <= 0:
            raise ValueError("task_done() called too many times")
        self._unfinished -= 1
        if self._unfinished == 0:
            self._alldone.set()

    async def join(self):
        """Wait until every put() item has had a matching task_done()."""
        await self._alldone.wait()
