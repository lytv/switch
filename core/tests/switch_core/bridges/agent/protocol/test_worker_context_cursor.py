"""The worker pages read_context by commit order, not by timestamps."""

import pytest

from tests.switch_core.bridges.agent.protocol.test_read_context import (
    _make_room,
    _service,
    _write,
)


async def test_forward_cursor_same_timestamp_and_late_old_timestamp(session_factory):
    async with session_factory() as session:
        room_id, client_id = await _make_room(session)
        await _write(session, room_id, client_id, "$root", at=10)
        for i in range(5):
            await _write(
                session, room_id, client_id, f"$reply-{i}", at=10, thread_root="$root"
            )
        await session.commit()
    service = _service(session_factory)
    cursor, seen = 0, []
    while True:
        page = await service.read_context("agent-1", room_id, limit=2, after_seq=cursor)
        cursor = page["next_seq"]
        for thread in page["threads"]:
            assert thread["root"]["id"] == "$root"
            seen.extend(entry["id"] for entry in thread["replies"])
        if not page["truncated"]:
            break
    assert seen == [f"$reply-{i}" for i in range(5)]
    assert cursor == 6
    async with session_factory() as session:
        await _write(session, room_id, client_id, "$late", at=0, thread_root="$root")
        await session.commit()
    page = await service.read_context("agent-1", room_id, after_seq=cursor)
    assert [entry["id"] for entry in page["threads"][0]["replies"]] == ["$late"]
    assert page["next_seq"] == 7
    empty = await service.read_context("agent-1", room_id, after_seq=7)
    assert empty["threads"] == [] and empty["next_seq"] == 7


async def test_worker_cursor_rejects_time_window_and_negative_cursor(session_factory):
    service = _service(session_factory)
    with pytest.raises(ValueError):
        await service.read_context("agent-1", "room-1", after_seq=-1)
    with pytest.raises(ValueError):
        await service.read_context("agent-1", "room-1", after_seq=0, since_ms=10)
