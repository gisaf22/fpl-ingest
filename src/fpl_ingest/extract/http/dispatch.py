"""Bounded dispatch loop that stops when the run aborts (#115), and the off-loop storage runner.

Built for the stages that send many small requests and route each response as it
arrives (the manager panel and entry-picks). It checks ``aborted`` before
each dispatch, so a blocked API is not hammered across thousands of requests
(#109); work already in flight finishes and is stored. event-live and
element-summary fetch everything with ``asyncio.gather`` before routing and do
not use it.
"""

from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, Sequence, TypeVar

Item = TypeVar("Item")
Result = TypeVar("Result")


async def dispatch_bounded(
    items: Sequence[Item],
    work: Callable[[Item], Awaitable[Result]],
    *,
    aborted: Callable[[], bool],
    limit: int,
) -> list[tuple[Item, Result]]:
    """Run ``work`` over ``items``, at most ``limit`` at once, none after ``aborted()`` turns true.

    Returns ``(item, result)`` for every item that was dispatched, in the order
    of ``items``; items never dispatched are absent.
    """
    pending = enumerate(items)
    done: dict[int, tuple[Item, Result]] = {}

    async def worker() -> None:
        for index, item in pending:  # shared: each worker takes the next undispatched item
            if aborted():
                return
            done[index] = (item, await work(item))

    await asyncio.gather(*(worker() for _ in range(min(limit, len(items)))))
    return [done[i] for i in sorted(done)]


class OffLoop:
    """Runs the writer's and backend's blocking calls on a thread, one at a time.

    boto3 is synchronous: called on the loop it freezes every in-flight
    request, whose 30 s timers keep running (runs 39365465979, 39366583920).
    The writer is not thread-safe, so the lock keeps a single thread inside it.
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()

    async def __call__(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        async with self._lock:
            return await asyncio.to_thread(fn, *args, **kwargs)
