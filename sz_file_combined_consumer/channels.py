"""Bounded, closable FIFO queues — the Python stand-in for Rust mpsc channels.

``queue.Queue`` has no close; workers need to distinguish "empty for now" from
"closed and drained" (which is how they learn to exit). Locks are held only
inside the (non-blocking) queue ops, never across an engine call.
"""

from __future__ import annotations

import queue
import threading
from collections.abc import Callable
from enum import Enum, auto
from typing import Generic, TypeVar

T = TypeVar("T")


class TryState(Enum):
    ITEM = auto()
    EMPTY = auto()
    CLOSED = auto()


class ClosableQueue(Generic[T]):
    """Bounded FIFO with an explicit :meth:`close`."""

    def __init__(self, maxsize: int) -> None:
        self._q: queue.Queue[T] = queue.Queue(maxsize=max(1, maxsize))
        self._closed = threading.Event()

    def close(self) -> None:
        """Mark end-of-stream; consumers drain what is queued, then see CLOSED."""
        self._closed.set()

    @property
    def closed(self) -> bool:
        return self._closed.is_set()

    def qsize(self) -> int:
        return self._q.qsize()

    def try_get(self) -> tuple[TryState, T | None]:
        """Non-blocking dequeue."""
        try:
            return TryState.ITEM, self._q.get_nowait()
        except queue.Empty:
            pass
        if not self._closed.is_set():
            return TryState.EMPTY, None
        # Closed: an item may have landed between the get and the closed check.
        try:
            return TryState.ITEM, self._q.get_nowait()
        except queue.Empty:
            return TryState.CLOSED, None

    def get(self, timeout: float) -> tuple[TryState, T | None]:
        """Blocking dequeue bounded by ``timeout`` seconds."""
        try:
            return TryState.ITEM, self._q.get(timeout=timeout)
        except queue.Empty:
            pass
        if self._closed.is_set() and self._q.empty():
            return TryState.CLOSED, None
        return TryState.EMPTY, None

    def put(self, item: T, keep_going: Callable[[], bool], step: float = 0.1) -> bool:
        """Blocking enqueue that re-checks ``keep_going`` every ``step`` seconds.

        Returns ``False`` (item not enqueued) once the queue is closed or
        ``keep_going()`` is false — so a producer wedged on a full queue is
        released by shutdown. Backpressure against a full queue is the redo
        fetch gating valve and the file reader's flow control.
        """
        while keep_going() and not self._closed.is_set():
            try:
                self._q.put(item, timeout=step)
            except queue.Full:
                continue
            return True
        return False
