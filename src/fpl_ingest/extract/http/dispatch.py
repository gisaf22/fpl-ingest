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
import contextvars
import functools
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Awaitable, Callable, Sequence, TypeVar

from fpl_ingest.extract.http.rate_config import MAX_CONCURRENT_REQUESTS

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

    workers = [asyncio.ensure_future(worker()) for _ in range(min(limit, len(items)))]
    try:
        await asyncio.gather(*workers)
    except BaseException:
        # One worker failed: stop the rest before the caller sees the error, so
        # nothing is dispatched after the stage has given up (#123).
        for task in workers:
            task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        raise
    return [done[i] for i in sorted(done)]


class OffLoop:
    """Runs the writer's and backend's blocking calls on a pool of threads, so calls for different items overlap.

    boto3 is synchronous: called on the loop it freezes every in-flight
    request, whose 30 s timers keep running (runs 39365465979, 39366583920).
    Calls are not serialized here: the writer guards its own state, and boto3
    clients and the backends are safe across threads. One lock here held every
    S3 call of every worker in a single queue, which capped run 39552536884 at
    1.8 requests/s (#123).

    The pool has ``workers`` threads, by default the dispatch limit. The loop's
    default executor sizes itself from the CPU count (8 threads on a 4-vCPU
    runner) and would cap S3 concurrency below the dispatch limit.

    Close it when the stage ends, or use it as a context manager: closing waits
    for calls in flight, so nothing is stored after the stage has returned.
    """

    def __init__(self, workers: int = MAX_CONCURRENT_REQUESTS) -> None:
        self._pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="offloop")

    def __enter__(self) -> OffLoop:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        self._pool.shutdown(wait=True)

    async def __call__(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        # As asyncio.to_thread does, but on this pool.
        call = functools.partial(contextvars.copy_context().run, fn, *args, **kwargs)
        return await asyncio.get_running_loop().run_in_executor(self._pool, call)
