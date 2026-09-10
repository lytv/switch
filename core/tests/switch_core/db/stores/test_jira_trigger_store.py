from __future__ import annotations

from datetime import timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from switch_core.db.models import JiraTrigger, Room
from switch_core.db.stores.jira_trigger_store import JiraTriggerStore


async def _make_room(session: AsyncSession, name: str) -> Room:
    room = Room(matrix_room_id=f"!{name}:test", name=name, description=f"{name} desc")
    session.add(room)
    await session.flush()
    return room


@pytest.mark.asyncio
async def test_jira_trigger_crud(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    store = JiraTriggerStore()
    async with session_factory() as session:
        room = await _make_room(session, "feature-x")
        trigger = JiraTrigger(
            name="Start coder",
            instance="acme",
            fire_on="transition",
            target_status="In Progress",
            target_kind="room",
            target_room_id=room.id,
            agent_name="coder",
            message_template="{{issue.key}} is {{issue.status}}",
        )
        await store.create(session, trigger)
        await session.commit()
        trigger_id = trigger.id

    async with session_factory() as session:
        got = await store.get(session, trigger_id)
        assert got is not None
        assert got.enabled is True
        assert got.project_key == ""
        assert got.issue_type == ""
        assert got.created_at is not None
        assert got.updated_at is not None

        listed = await store.list(session, instance="acme", enabled_only=True)
        assert len(listed) == 1

        updated = await store.update(
            session, trigger_id, enabled=False, project_key="PROJ"
        )
        assert updated is not None
        assert updated.enabled is False
        assert updated.project_key == "PROJ"
        await session.commit()

    async with session_factory() as session:
        assert await store.list(session, instance="acme", enabled_only=True) == []
        assert await store.delete(session, trigger_id) is True
        await session.commit()
        assert await store.get(session, trigger_id) is None


@pytest.mark.asyncio
async def test_firing_dedupe_and_rate_count(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    store = JiraTriggerStore()
    async with session_factory() as session:
        room = await _make_room(session, "rate-room")
        trigger = JiraTrigger(
            name="rate",
            instance="acme",
            fire_on="transition",
            target_room_id=room.id,
            agent_name="coder",
            message_template="x",
        )
        await store.create(session, trigger)
        await session.commit()
        rule_id = trigger.id

    window = timedelta(seconds=300)
    async with session_factory() as session:
        assert (
            await store.try_record_firing(
                session,
                issue_key="PROJ-1",
                rule_id=rule_id,
                transition_key="To Do->In Progress",
                dedupe_window=window,
            )
            is True
        )
        await session.commit()

    async with session_factory() as session:
        assert (
            await store.try_record_firing(
                session,
                issue_key="PROJ-1",
                rule_id=rule_id,
                transition_key="To Do->In Progress",
                dedupe_window=window,
            )
            is False
        )
        count = await store.count_firings_in_window(
            session, rule_id=rule_id, window=window
        )
        assert count == 1
        await session.commit()
