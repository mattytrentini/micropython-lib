import asyncio
from aioqueue import Queue, QueueEmpty, QueueFull

# maxsize is required and must be positive.
try:
    Queue(0)
    assert False, "expected ValueError for maxsize=0"
except ValueError:
    pass

try:
    Queue(-1)
    assert False, "expected ValueError for negative maxsize"
except ValueError:
    pass


async def test_fifo_order():
    q = Queue(maxsize=4)
    for i in range(4):
        q.put_nowait(i)
    assert q.qsize() == 4
    assert q.full()
    for i in range(4):
        assert q.get_nowait() == i
    assert q.empty()


async def test_put_nowait_raises_when_full():
    q = Queue(maxsize=1)
    q.put_nowait("a")
    try:
        q.put_nowait("b")
        assert False, "expected QueueFull"
    except QueueFull:
        pass


async def test_get_nowait_raises_when_empty():
    q = Queue(maxsize=1)
    try:
        q.get_nowait()
        assert False, "expected QueueEmpty"
    except QueueEmpty:
        pass


async def test_put_blocks_until_space():
    q = Queue(maxsize=1)
    q.put_nowait("a")
    order = []

    async def putter():
        await q.put("b")
        order.append("put b")

    task = asyncio.create_task(putter())
    await asyncio.sleep_ms(20)
    assert order == [], "put() should still be blocked while queue is full"

    assert q.get_nowait() == "a"
    await asyncio.sleep_ms(20)
    assert order == ["put b"], "put() should unblock once space is available"
    await task


async def test_get_blocks_until_item():
    q = Queue(maxsize=1)
    order = []

    async def getter():
        item = await q.get()
        order.append(item)

    task = asyncio.create_task(getter())
    await asyncio.sleep_ms(20)
    assert order == [], "get() should still be blocked while queue is empty"

    await q.put("x")
    await asyncio.sleep_ms(20)
    assert order == ["x"], "get() should unblock once an item is available"
    await task


async def test_task_done_and_join():
    q = Queue(maxsize=4)
    for i in range(3):
        await q.put(i)

    joined = []

    async def joiner():
        await q.join()
        joined.append(True)

    task = asyncio.create_task(joiner())
    await asyncio.sleep_ms(20)
    assert joined == [], "join() should block while unfinished tasks remain"

    while not q.empty():
        q.get_nowait()
        q.task_done()

    await asyncio.sleep_ms(20)
    assert joined == [True], "join() should unblock once all tasks are done"
    await task


async def test_task_done_without_matching_put_raises():
    q = Queue(maxsize=1)
    try:
        q.task_done()
        assert False, "expected ValueError"
    except ValueError:
        pass


async def main():
    await test_fifo_order()
    await test_put_nowait_raises_when_full()
    await test_get_nowait_raises_when_empty()
    await test_put_blocks_until_space()
    await test_get_blocks_until_item()
    await test_task_done_and_join()
    await test_task_done_without_matching_put_raises()
    print("aioqueue: all tests passed")


asyncio.run(main())
