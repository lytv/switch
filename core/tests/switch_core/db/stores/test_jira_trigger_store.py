from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from switch_core.db.models import JiraTrigger, JiraTriggerFiring, Room
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
        firing_id = await store.try_record_firing(
            session,
            issue_key="PROJ-1",
            rule_id=rule_id,
            transition_key="To Do->In Progress",
            dedupe_window=window,
            instance="acme",
            rule_name="rate",
            matched_rule_ids=[rule_id],
        )
        assert firing_id is not None
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
            is None
        )
        count = await store.count_firings_in_window(
            session, rule_id=rule_id, window=window
        )
        assert count == 1
        await session.commit()


@pytest.mark.asyncio
async def test_delivery_log_list_finalize_and_prune(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    store = JiraTriggerStore()
    async with session_factory() as session:
        room = await _make_room(session, "log-room")
        trigger = JiraTrigger(
            name="log",
            instance="acme",
            fire_on="transition",
            target_room_id=room.id,
            agent_name="coder",
            message_template="x",
        )
        await store.create(session, trigger)
        await session.commit()
        rule_id = trigger.id

    now = datetime(2026, 9, 10, 12, 0, tzinfo=UTC)
    async with session_factory() as session:
        firing_id = await store.try_record_firing(
            session,
            issue_key="PROJ-9",
            rule_id=rule_id,
            transition_key="created",
            dedupe_window=timedelta(seconds=60),
            instance="acme",
            rule_name="log",
            matched_rule_ids=[rule_id],
            now=now,
        )
        assert firing_id is not None
        await store.finalize_firing(
            session,
            firing_id,
            status="delivered",
            room_results=[
                {
                    "room_id": "r1",
                    "room_name": "log-room",
                    "status": "ok",
                    "event_id": "$e",
                    "attempts": 1,
                }
            ],
            error=None,
            attempt_count=1,
        )
        await store.record_suppressed_firing(
            session,
            issue_key="PROJ-9",
            rule_id=rule_id,
            transition_key="created",
            status="suppressed_dedupe",
            instance="acme",
            rule_name="log",
            matched_rule_ids=[rule_id],
            now=now,
        )
        # Old row outside retention.
        session.add(
            JiraTriggerFiring(
                issue_key="OLD-1",
                rule_id=rule_id,
                transition_key="created",
                instance="acme",
                rule_name="log",
                status="delivered",
                created_at=now - timedelta(days=30),
            )
        )
        await session.commit()

    async with session_factory() as session:
        rows = await store.list_deliveries(session, instance="acme", limit=10)
        assert len(rows) >= 2
        assert rows[0].created_at >= rows[-1].created_at
        deleted = await store.prune_firings(
            session,
            retain_seconds=7 * 24 * 3600,
            max_rows=5000,
            now=now,
        )
        assert deleted >= 1
        await session.commit()

    async with session_factory() as session:
        remaining = await store.list_deliveries(session, instance="acme", limit=50)
        assert all(r.issue_key != "OLD-1" for r in remaining)
