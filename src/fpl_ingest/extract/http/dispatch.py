"""Bounded dispatch loop that stops when the run aborts (#115 stub)."""

from __future__ import annotations

from typing import Awaitable, Callable, Sequence, TypeVar

Item = TypeVar("Item")
Result = TypeVar("Result")


async def dispatch_bounded(
    items: Sequence[Item],
    work: Callable[[Item], Awaitable[Result]],
    *,
    aborted: Callable[[], bool],
    limit: int,
) -> list[tuple[Item, Result]]:
    """Run ``work`` over ``items``, at most ``limit`` at once, none after ``aborted()`` turns true."""
    raise NotImplementedError("#115")
