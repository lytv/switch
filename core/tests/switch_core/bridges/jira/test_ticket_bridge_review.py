"""Ticket repair eligibility and channel-failure provisioning boundaries."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from switch_core.bridges.jira.rooms import (
    REPORTER_ACCESS_REASON,
    RESTORE_HISTORY_COMMAND,
)
from switch_core.bridges.jira.worker import JiraWorkerScheduler
from switch_core.db.models import (
    CollaborationBridge,
    JiraWorkerOutbox,
    JiraWorkerTicket,
    Room,
)
from switch_core.room_service import RoomCreateConfig
from tests.switch_core.bridges.jira.test_rooms import _switch_config
from tests.switch_core.bridges.jira.test_ticket_bridge import sync
from tests.switch_core.bridges.jira.test_ticket_bridge import (
    ticket_bridge as ticket_bridge,
)


@pytest.mark.parametrize("wait_channel", ["switch", "jira_comments"])
@pytest.mark.parametrize(
    "status",
    [
        None,
        "pending",
        "claimed",
        "sending",
        "done",
        "cancelled",
        "failed",
        "parked",
        "uncertain",
    ],
)
async def test_restoration_respects_question_channel_and_delivery_status(
    session_factory, ticket_bridge, wait_channel, status
):
    rooms, _, adapter, _ = ticket_bridge
    async with session_factory() as session:
        bridge = await session.get(CollaborationBridge, "mm")
        bridge.is_default = False
        await session.commit()
    room_id = await sync(session_factory, rooms)
    async with session_factory() as session:
        bridge = await session.get(CollaborationBridge, "mm")
        bridge.is_default = True
        ticket = await session.scalar(select(JiraWorkerTicket))
        ticket.wait_channel = wait_channel
        ticket.intake_state = {
            "phase": "question",
            "history": [{"question": "Which customer?"}],
        }
        if status is not None:
            for created_at, delivery_status in [
                (datetime(2019, 1, 1, tzinfo=UTC), "done"),
                (datetime(2020, 1, 1, tzinfo=UTC), status),
            ]:
                session.add(
                    JiraWorkerOutbox(
                        instance="acme",
                        issue_key="KAN-1",
                        channel="switch" if wait_channel == "switch" else "jira",
                        command="intake_question"
                        if wait_channel == "switch"
                        else "comment",
                        status=delivery_status,
                        created_at=created_at,
                        payload={
                            "room_id": room_id,
                            "thread_id": "$card",
                            "data": "Which customer?",
                            "intake_generation": ticket.agent_generation,
                        },
                    )
                )
        await session.commit()
    rooms._protocol.send_message.reset_mock()
    rooms._protocol.send_message.return_value = "$new-card"
    assert await sync(session_factory, rooms) == room_id
    should_replay = wait_channel == "switch" and status == "done"
    assert rooms._protocol.send_message.await_count == (2 if should_replay else 1)
    if should_replay:
        rooms._protocol.send_message.assert_any_await(
            "jira", room_id, "Which customer?", thread_id="$new-card"
        )
    async with session_factory() as session:
        deliveries = list(
            await session.scalars(
                select(JiraWorkerOutbox)
                .where(JiraWorkerOutbox.command.in_(("intake_question", "comment")))
                .order_by(JiraWorkerOutbox.created_at)
            )
        )
        for delivery in deliveries:
            if wait_channel == "switch" and delivery.status in ("pending", "claimed"):
                assert delivery.payload["thread_id"] == "$new-card"
            else:
                assert delivery.payload["thread_id"] == "$card"
        history = await session.scalar(
            select(JiraWorkerOutbox).where(
                JiraWorkerOutbox.command == RESTORE_HISTORY_COMMAND
            )
        )
        assert history.status == "done"
    assert await sync(session_factory, rooms) == room_id
    assert rooms._protocol.send_message.await_count == (2 if should_replay else 1)
    adapter.create_channel.assert_awaited_once()


async def test_restoration_does_not_revive_a_superseded_question(
    session_factory, ticket_bridge
):
    rooms, _, _, _ = ticket_bridge
    async with session_factory() as session:
        bridge = await session.get(CollaborationBridge, "mm")
        bridge.is_default = False
        await session.commit()
    room_id = await sync(session_factory, rooms)
    async with session_factory() as session:
        bridge = await session.get(CollaborationBridge, "mm")
        bridge.is_default = True
        ticket = await session.scalar(select(JiraWorkerTicket))
        ticket.agent_generation = 1
        ticket.intake_state = {
            "phase": "question",
            "history": [{"question": "Superseded question"}],
        }
        session.add(
            JiraWorkerOutbox(
                instance="acme",
                issue_key="KAN-1",
                channel="switch",
                command="intake_question",
                status="done",
                payload={
                    "room_id": room_id,
                    "thread_id": "$card",
                    "intake_generation": 0,
                },
            )
        )
        await session.commit()
    rooms._protocol.send_message.reset_mock()
    assert await sync(session_factory, rooms) == room_id
    rooms._protocol.send_message.assert_awaited_once()


@pytest.mark.parametrize("terminal_status", ["Done", " Cancelled ", "CANCELED"])
async def test_terminal_pending_restoration_does_not_starve_sweep_and_resumes_on_reopen(
    session_factory, ticket_bridge, terminal_status
):
    rooms, _, adapter, matrix = ticket_bridge
    async with session_factory() as session:
        bridge = await session.get(CollaborationBridge, "mm")
        bridge.is_default = False
        await session.commit()
    room_id = await sync(session_factory, rooms)
    async with session_factory() as session:
        bridge = await session.get(CollaborationBridge, "mm")
        bridge.is_default = True
        await session.commit()
    rooms._protocol.send_message.side_effect = RuntimeError(
        "Agent client is not running"
    )
    with pytest.raises(RuntimeError, match="Agent client is not running"):
        await sync(session_factory, rooms)
    async with session_factory() as session:
        ticket = await session.scalar(select(JiraWorkerTicket))
        ticket.status = terminal_status
        session.add(
            JiraWorkerTicket(
                instance="acme", issue_key="KAN-2", project_key="KAN", status="To Do"
            )
        )
        await session.commit()
    rooms._protocol.send_message.side_effect = None
    matrix.create_room.return_value = "!newer:test"
    adapter.create_channel.return_value = "newer-channel"
    scheduler = JiraWorkerScheduler(
        session_factory=session_factory,
        ticket_rooms=rooms,
        config=_switch_config(jira_worker_enabled_projects={"acme": ["KAN"]}),
    )
    assert await scheduler.sweep_ticket_rooms(limit=1) == 1
    assert await scheduler.sweep_ticket_rooms(limit=1) == 1
    assert await scheduler.sweep_ticket_rooms(limit=1) == 0
    async with session_factory() as session:
        ticket = await session.scalar(
            select(JiraWorkerTicket).where(JiraWorkerTicket.issue_key == "KAN-1")
        )
        assert ticket.room_claimed is False
        assert (await session.get(Room, room_id)).archived_at is not None
        history = await session.scalar(
            select(JiraWorkerOutbox).where(
                JiraWorkerOutbox.command == RESTORE_HISTORY_COMMAND
            )
        )
        assert history.status == "pending"
        ticket.status = "To Do"
        await session.commit()
    rooms._protocol.send_message.return_value = "$reopened-card"
    assert await scheduler.sweep_ticket_rooms(limit=1) == 1
    async with session_factory() as session:
        assert (await session.get(JiraWorkerOutbox, history.id)).status == "done"
        ticket = await session.scalar(
            select(JiraWorkerTicket).where(JiraWorkerTicket.issue_key == "KAN-1")
        )
        assert ticket.card_event_id == "$reopened-card"
    assert matrix.create_room.await_count == 2
    assert adapter.create_channel.await_count == 2


@pytest.mark.parametrize(
    "error",
    [
        RuntimeError("Mattermost client not connected"),
        ConnectionError("Mattermost unreachable"),
    ],
)
async def test_channel_creation_failure_preserves_ticket_sync_and_repairs_one_room(
    session_factory, ticket_bridge, caplog, error
):
    rooms, _, adapter, matrix = ticket_bridge
    async with session_factory() as session:
        ticket = await session.scalar(select(JiraWorkerTicket))
        session.add(
            JiraWorkerOutbox(
                instance="acme",
                issue_key="KAN-1",
                channel="switch",
                command="sync_ticket_room",
                payload={"ticket_id": ticket.id},
            )
        )
        await session.commit()
    adapter.create_channel.side_effect = error
    room_id = await sync(session_factory, rooms)
    assert room_id is not None
    assert "channel creation failed" in caplog.text
    assert "reporter access failed" in caplog.text
    async with session_factory() as session:
        persisted = list(await session.scalars(select(Room)))
        assert len(persisted) == 1
        assert persisted[0].external_channel_id is None
        assert (
            persisted[0].read_visibility == persisted[0].write_visibility == "private"
        )
        ticket = await session.scalar(select(JiraWorkerTicket))
        assert ticket.card_event_id == "$card"
        assert str(error) in ticket.admin_reasons[REPORTER_ACCESS_REASON]
        follow_up = await session.scalar(
            select(JiraWorkerOutbox).where(
                JiraWorkerOutbox.command == "sync_ticket_room"
            )
        )
        assert follow_up.status == "done"
    matrix.create_room.assert_awaited_once()
    adapter.create_channel.side_effect = None
    rooms._protocol.send_message.return_value = "$repaired-card"
    await asyncio.gather(sync(session_factory, rooms), sync(session_factory, rooms))
    assert await sync(session_factory, rooms) == room_id
    matrix.create_room.assert_awaited_once()
    adapter.add_users_to_channel.assert_awaited()
    async with session_factory() as session:
        assert len(list(await session.scalars(select(Room)))) == 1
        ticket = await session.scalar(select(JiraWorkerTicket))
        assert ticket.card_event_id == "$repaired-card"
        assert REPORTER_ACCESS_REASON not in (ticket.admin_reasons or {})
        assert (
            await session.get(Room, room_id)
        ).external_channel_id == "private-channel"
    assert adapter.create_channel.await_count == 3


@pytest.mark.parametrize("stage", ["validation", "matrix", "capture", "agents"])
async def test_unrelated_provisioning_failures_do_not_retry_room_creation(
    session_factory, ticket_bridge, stage
):
    rooms, service, adapter, matrix = ticket_bridge
    error = RuntimeError(f"{stage} failure")
    if stage == "validation":
        service._resolve_agent_ids = AsyncMock(side_effect=error)
    elif stage == "matrix":
        matrix.create_room.side_effect = error
    elif stage == "capture":
        adapter.ensure_channel_subscriptions.side_effect = error
    else:
        adapter.add_agents_to_channel.side_effect = error
    with pytest.raises(RuntimeError, match=f"{stage} failure"):
        await sync(session_factory, rooms)
    assert matrix.create_room.await_count == (0 if stage == "validation" else 1)
    assert adapter.create_channel.await_count == (0 if stage == "validation" else 1)
    async with session_factory() as session:
        persisted = list(await session.scalars(select(Room)))
        assert len(persisted) == (1 if stage in ("capture", "agents") else 0)


async def test_committed_room_is_adopted_after_create_reports_failure(
    session_factory, ticket_bridge
):
    rooms, service, adapter, matrix = ticket_bridge
    create_room = service.create_room

    async def committed_then_failed(config):
        await create_room(config)
        raise RuntimeError("Failure after room commit")

    service.create_room = AsyncMock(side_effect=committed_then_failed)
    with pytest.raises(RuntimeError, match="Failure after room commit"):
        await sync(session_factory, rooms)
    service.create_room.assert_awaited_once()
    async with session_factory() as session:
        committed = await session.scalar(select(Room))
        committed_id = committed.id
    assert await sync(session_factory, rooms) == committed_id
    service.create_room.assert_awaited_once()
    matrix.create_room.assert_awaited_once()
    adapter.create_channel.assert_awaited_once()
    async with session_factory() as session:
        assert len(list(await session.scalars(select(Room)))) == 1


async def test_ordinary_room_creation_still_raises_channel_failure(
    session_factory, ticket_bridge
):
    _, service, adapter, matrix = ticket_bridge
    adapter.create_channel.side_effect = RuntimeError("Channel unavailable")
    with pytest.raises(RuntimeError, match="Channel unavailable"):
        await service.create_room(
            RoomCreateConfig(name="Ordinary", description="Room", agent_names=["jira"])
        )
    matrix.create_room.assert_not_awaited()
    async with session_factory() as session:
        assert list(await session.scalars(select(Room))) == []
