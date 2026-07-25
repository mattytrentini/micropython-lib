# `aioqueue`

A `CPython`-compatible `asyncio.Queue` for MicroPython.

Core MicroPython's `asyncio` does not include a `Queue` class (it was present
in an early development build of the current asyncio implementation but was
dropped before release — see
[micropython/micropython#5828](https://github.com/micropython/micropython/issues/5828),
open since 2020). `aioqueue` fills that gap as a standalone package.

There's an open pull request,
[micropython/micropython#19459](https://github.com/micropython/micropython/pull/19459),
to add a `Queue` class to core `asyncio`. Once that (or an equivalent fix)
lands in a MicroPython release, packages depending on `aioqueue` — such as
[`aiocan`](../can/aiocan/) — should switch to `asyncio.Queue` directly, and
this package's usefulness will mostly be superseded.

This is *not* an extension of the `asyncio` module itself — you `import
aioqueue`, not `from asyncio import queue`. MicroPython's import resolver
does not merge a filesystem package directory with a frozen/built-in package
of the same name, so a bolt-on `asyncio/queue.py` submodule would either
silently fail to import, or — if it happened to resolve first — shadow the
rest of `asyncio` entirely. Shipping as an independent package avoids that
risk category altogether.

## Differences from CPython's `asyncio.Queue`

- `maxsize` is **required** and must be `> 0`. CPython (and Peter Hinch's
  `micropython-async` implementation, which this is adapted from) default to
  `maxsize=0`, meaning unbounded. MicroPython's `collections.deque` has no
  true unbounded mode, and unbounded growth is rarely what you want on a
  memory-constrained device — pick a bound.
- Not safe to call `put_nowait()` from a hard IRQ. All methods must be called
  from `asyncio` task context, same as CPython's `asyncio.Queue`.

## API reference

- class `Queue(maxsize)`

  - `async Queue.get()` / `Queue.get_nowait()`

    Remove and return an item. `get()` waits if the queue is empty;
    `get_nowait()` raises `QueueEmpty` instead.

  - `async Queue.put(item)` / `Queue.put_nowait(item)`

    Add an item. `put()` waits if the queue is full; `put_nowait()` raises
    `QueueFull` instead.

  - `Queue.qsize()` / `Queue.empty()` / `Queue.full()`

  - `Queue.task_done()` / `async Queue.join()`

    Coordinate producer/consumer shutdown, same as CPython.

## Example usage

```python
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
```
