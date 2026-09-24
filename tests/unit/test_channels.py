from __future__ import annotations

import threading
import time

from sz_file_combined_consumer.channels import ClosableQueue, TryState


def test_try_get_states() -> None:
    q: ClosableQueue[int] = ClosableQueue(2)
    assert q.try_get() == (TryState.EMPTY, None)
    assert q.put(1, keep_going=lambda: True)
    assert q.qsize() == 1
    assert q.try_get() == (TryState.ITEM, 1)
    q.close()
    assert q.closed
    assert q.try_get() == (TryState.CLOSED, None)


def test_closed_queue_drains_before_reporting_closed() -> None:
    q: ClosableQueue[str] = ClosableQueue(4)
    q.put("a", keep_going=lambda: True)
    q.close()
    assert q.try_get() == (TryState.ITEM, "a")
    assert q.try_get() == (TryState.CLOSED, None)
    assert q.get(timeout=0.01) == (TryState.CLOSED, None)


def test_blocking_get_timeout_and_item() -> None:
    q: ClosableQueue[int] = ClosableQueue(1)
    assert q.get(timeout=0.01) == (TryState.EMPTY, None)
    q.put(7, keep_going=lambda: True)
    assert q.get(timeout=0.01) == (TryState.ITEM, 7)


def test_put_refuses_when_closed() -> None:
    q: ClosableQueue[int] = ClosableQueue(1)
    q.close()
    assert not q.put(1, keep_going=lambda: True)


def test_put_on_full_queue_released_by_keep_going() -> None:
    q: ClosableQueue[int] = ClosableQueue(1)
    assert q.put(1, keep_going=lambda: True)
    running = threading.Event()
    running.set()
    result: list[bool] = []

    def producer() -> None:
        result.append(q.put(2, keep_going=running.is_set, step=0.01))

    t = threading.Thread(target=producer)
    t.start()
    time.sleep(0.05)
    assert t.is_alive(), "producer must block on the full queue"
    running.clear()
    t.join(timeout=2)
    assert result == [False]


def test_put_on_full_queue_proceeds_when_room_appears() -> None:
    q: ClosableQueue[int] = ClosableQueue(1)
    q.put(1, keep_going=lambda: True)
    result: list[bool] = []
    t = threading.Thread(target=lambda: result.append(q.put(2, keep_going=lambda: True, step=0.01)))
    t.start()
    time.sleep(0.03)
    assert q.try_get() == (TryState.ITEM, 1)
    t.join(timeout=2)
    assert result == [True]
    assert q.try_get() == (TryState.ITEM, 2)
