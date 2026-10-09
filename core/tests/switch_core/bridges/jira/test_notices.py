"""Wrong-place inputs use real PostgreSQL and mocked Jira/Switch effects."""

from __future__ import annotations

import asyncio
import json
from datetime import timedelta

import pytest
from sqlalchemy import select

from switch_core.bridges.jira.activity import TurnResult
from switch_core.bridges.jira.outbox import JiraOutboxSender
from switch_core.bridges.jira.rooms import SwitchTicketRooms
from switch_core.config import JiraWorkerStatuses
from switch_core.db.models import Client, JiraWorkerOutbox, JiraWorkerTicket
from tests.switch_core.bridges.jira.test_activity import (
    NOW,
    ThreadRooms,
    activity,
    events,
    message,
    seed,
    ticket,
)
from tests.switch_core.bridges.jira.test_outbox import JiraMock, client_for
from tests.switch_core.bridges.jira.test_worker import _config


async def commands(factory):
    async with factory() as session:
        return list((await session.scalars(select(JiraWorkerOutbox))).all())


async def test_reporter_comment_is_ignored_and_gets_room_link(session_factory):
    key = await seed(session_factory)
    rooms, jira = ThreadRooms(), JiraMock()
    jira.change("reporter-account", comment=True)
    reader = activity(
        session_factory, jira, rooms, frontend_base_url="https://switch.example.invalid"
    )
    await reader.read_once()
    inputs = await events(session_factory)
    assert [event.event_kind for event in inputs] == ["ignored_input"]
    assert inputs[0].payload["reporter_input"] is False
    assert inputs[0].payload["wrong_place_type"] == "jira_answer"
    assert (await ticket(session_factory, key)).agent_state == "waiting"
    queued = await commands(session_factory)
    assert len(queued) == 1 and queued[0].command == "comment"
    assert "https://switch.example.invalid/rooms/room-1" in queued[0].payload["data"]
    sender = JiraOutboxSender(
        session_factory=session_factory,
        config=reader._config,
        clients={"acme": client_for(jira)},
        clock=lambda: NOW,
    )
    await sender.run_once()
    assert len(jira.writes) == 1
    assert jira.writes[0].url.path.endswith("/comment")
    assert (
        "did not use this Jira comment as an answer"
        in json.loads(jira.writes[0].content)["body"]["content"][0]["content"][0][
            "text"
        ]
    )
    rooms.messages = [message(1)]
    turns = []

    async def turn(data):
        turns.append(data)
        return TurnResult(1, None)

    assert await reader.run_one_turn(turn) == key
    assert len(turns[0].untrusted_data["events"]) == 1
    assert turns[0].untrusted_data["events"][0]["untrusted_data"]["id"] == "message-1"
    ignored = [
        e for e in await events(session_factory) if e.event_kind == "ignored_input"
    ]
    assert len(ignored) == 1 and not ignored[0].consumed


@pytest.mark.parametrize("mapped", [False, True])
async def test_jira_edits_rate_limit_and_restart(session_factory, mapped):
    key = await seed(session_factory, mapped=mapped)
    rooms, jira = ThreadRooms(), JiraMock()
    reader = activity(session_factory, jira, rooms)
    await reader.read_ticket(key, force_jira=True)
    jira.change("reporter-account", comment=True)
    await asyncio.gather(
        reader.read_ticket(key, force_jira=True),
        activity(session_factory, jira, rooms).read_ticket(key, force_jira=True),
    )
    jira.updated = (NOW + timedelta(seconds=10)).isoformat()
    jira.comments[0]["updated"] = jira.updated
    await activity(session_factory, jira, rooms).read_ticket(key, force_jira=True)
    jira.change("reporter-account", comment=True)
    await activity(session_factory, jira, rooms).read_ticket(key, force_jira=True)
    logged = await events(session_factory)
    if not mapped:
        assert all(e.event_kind == "jira_comment" for e in logged)
        assert (await ticket(session_factory, key)).agent_state == "wake_requested"
        assert not await commands(session_factory)
        return
    assert len(logged) == 3
    assert all(e.event_kind == "ignored_input" for e in logged)
    assert (await ticket(session_factory, key)).agent_state == "waiting"
    assert len(await commands(session_factory)) == 1
    # A rolling window starts at the first claim, not at midnight.
    jira.change("reporter-account", comment=True)
    await activity(
        session_factory, jira, rooms, clock=lambda: NOW + timedelta(hours=24)
    ).read_ticket(key, force_jira=True)
    assert len(await commands(session_factory)) == 2
    jira.change("human-account", comment=True)
    await reader.read_ticket(key, force_jira=True)
    assert (
        await ticket(session_factory, key)
    ).worker_parked_reason == "Human Jira action"


@pytest.mark.parametrize(
    "body", ["Approve", "Reject", "Please change the status", "A normal answer"]
)
async def test_human_room_message_is_input_not_approval(session_factory, body):
    key = await seed(session_factory)
    async with session_factory() as session:
        row = await session.get(JiraWorkerTicket, key)
        row.status = "Awaiting review"
        await session.commit()
    rooms, jira = ThreadRooms(), JiraMock()
    rooms.messages = [
        message(1),
        message(2),
        message(3, "other-agent"),
        message(4, "admin-user"),
    ]
    rooms.messages[0]["body"] = body
    config = JiraWorkerStatuses(waiting_for_approval="Awaiting review")
    reader = activity(session_factory, jira, rooms, jira_worker_statuses=config)
    await asyncio.gather(reader.read_once(), reader.read_once())
    await activity(
        session_factory, jira, rooms, jira_worker_statuses=config
    ).read_once()
    logged = await events(session_factory)
    assert len(logged) == 4
    assert all(e.event_kind == "thread_message" for e in logged)
    assert [e.payload.get("wrong_place_type") for e in logged] == [
        "switch_approval",
        "switch_approval",
        None,
        "switch_approval",
    ]
    assert len(await commands(session_factory)) == 2
    sender = JiraOutboxSender(
        session_factory=session_factory,
        config=reader._config,
        clients={"acme": client_for(jira)},
        clock=lambda: NOW,
        rooms=rooms,
    )
    await asyncio.gather(sender.run_once(), sender.run_once())
    await JiraOutboxSender(
        session_factory=session_factory,
        config=reader._config,
        clients={},
        clock=lambda: NOW,
        rooms=rooms,
    ).run_once()
    assert len(rooms.notices) == 2
    assert all(
        "Approve or Reject" in body and "https://jira.example/browse/KAN-1" in body
        for _, body in rooms.notices
    )
    assert not jira.writes
    assert (await ticket(session_factory, key)).status == "Awaiting review"
    turns = []

    async def turn(data):
        turns.append(data)
        return TurnResult(1, None)

    assert await reader.run_one_turn(turn) == key
    assert len(turns[0].untrusted_data["events"]) == 4
    assert (await ticket(session_factory, key)).status == "Awaiting review"
    assert not jira.writes


async def test_switch_notice_window_and_unknown_send_survive_restart(session_factory):
    key = await seed(session_factory)
    async with session_factory() as session:
        row = await session.get(JiraWorkerTicket, key)
        row.status = "Waiting for approval"
        await session.commit()
    rooms, jira = ThreadRooms(), JiraMock()
    rooms.messages = [message(1)]
    reader = activity(
        session_factory, jira, rooms, jira_worker_notice_window_seconds=60
    )
    await reader.read_once()
    pending = (await commands(session_factory))[0]
    async with session_factory() as session:
        row = await session.get(JiraWorkerOutbox, pending.id)
        row.status = "sending"
        await session.commit()
    rooms.messages.append(message(2))
    await activity(
        session_factory,
        jira,
        rooms,
        clock=lambda: NOW + timedelta(seconds=59),
        jira_worker_notice_window_seconds=60,
    ).read_once()
    assert len(await commands(session_factory)) == 1
    sender = JiraOutboxSender(
        session_factory=session_factory,
        config=reader._config,
        clients={},
        clock=lambda: NOW,
        rooms=rooms,
    )
    await sender.run_once()
    assert (await commands(session_factory))[0].status == "uncertain"
    assert not rooms.notices
    rooms.messages.append(message(3))
    await activity(
        session_factory,
        jira,
        rooms,
        clock=lambda: NOW + timedelta(seconds=60),
        jira_worker_notice_window_seconds=60,
    ).read_once()
    assert len(await commands(session_factory)) == 2
    assert not jira.writes


async def test_disabled_project_has_no_wrong_place_effects(session_factory):
    key = await seed(session_factory)
    async with session_factory() as session:
        row = await session.get(JiraWorkerTicket, key)
        row.status = "Waiting for approval"
        await session.commit()
    rooms, jira = ThreadRooms(), JiraMock()
    rooms.messages = [message(1)]
    jira.change("reporter-account", comment=True)
    reader = activity(session_factory, jira, rooms)
    reader._config.jira_worker_enabled_projects = {}
    await reader.read_once()
    assert not await events(session_factory)
    assert not await commands(session_factory)
    assert not jira.requests and not rooms.reads
    assert (await ticket(session_factory, key)).agent_state == "waiting"


async def test_first_read_reporter_edit_gets_a_notice(session_factory):
    key = await seed(session_factory)
    jira, rooms = JiraMock(), ThreadRooms()
    jira.change("reporter-account", comment=True)
    jira.comments[0]["created"] = "2026-09-30T00:00:00+00:00"
    reader = activity(session_factory, jira, rooms)
    await reader.read_ticket(key, force_jira=True)
    assert [e.event_kind for e in await events(session_factory)] == ["ignored_input"]
    assert len(await commands(session_factory)) == 1
    assert (await ticket(session_factory, key)).agent_state == "waiting"


async def test_notice_sender_leaves_disabled_project_pending(session_factory):
    key = await seed(session_factory)
    async with session_factory() as session:
        row = await session.get(JiraWorkerTicket, key)
        row.status = "Waiting for approval"
        await session.commit()
    jira, rooms = JiraMock(), ThreadRooms()
    rooms.messages = [message(1)]
    reader = activity(session_factory, jira, rooms)
    await reader.read_once()
    sender = JiraOutboxSender(
        session_factory=session_factory,
        config=_config(),
        clients={"acme": client_for(jira)},
        clock=lambda: NOW,
        rooms=rooms,
    )
    request_count = len(jira.requests)
    await sender.run_once()
    assert (await commands(session_factory))[0].status == "pending"
    assert not rooms.notices and len(jira.requests) == request_count and not jira.writes


async def test_unmapped_ticket_room_still_notifies_admin_approval(session_factory):
    key = await seed(session_factory, mapped=False)
    async with session_factory() as session:
        row = await session.get(JiraWorkerTicket, key)
        row.status = "Waiting for approval"
        await session.commit()
    rooms, jira = ThreadRooms(), JiraMock()
    rooms.messages = [message(1, "admin-user")]
    reader = activity(session_factory, jira, rooms)
    await reader.read_once()
    assert (await ticket(session_factory, key)).wait_channel == "jira_comments"
    assert (await events(session_factory))[0].payload[
        "wrong_place_type"
    ] == "switch_approval"
    assert len(await commands(session_factory)) == 1
    assert not jira.writes


async def test_missing_room_link_fails_without_consuming_comment(session_factory):
    key = await seed(session_factory)
    jira = JiraMock()
    jira.change("reporter-account", comment=True)
    reader = activity(session_factory, jira, ThreadRooms(), frontend_base_url=None)
    with pytest.raises(ValueError, match="frontend_base_url"):
        await reader.read_ticket(key, force_jira=True)
    assert not await events(session_factory)
    assert not await commands(session_factory)
    assert (await ticket(session_factory, key)).jira_seen_comment_ids is None


async def test_human_sender_uses_client_type_not_message_text(session_factory):
    async with session_factory() as session:
        session.add_all(
            [
                Client(
                    matrix_user_id="@person:example.invalid",
                    display_name="Person",
                    type="user",
                ),
                Client(
                    matrix_user_id="@agent:example.invalid",
                    display_name="Agent",
                    type="agent",
                ),
            ]
        )
        await session.commit()
    rooms = SwitchTicketRooms(
        room_service=None,  # type: ignore[arg-type]
        protocol=None,  # type: ignore[arg-type]
        session_factory=session_factory,
        agent_store=None,  # type: ignore[arg-type]
        config=_config(),
    )
    assert await rooms.is_human_sender("@person:example.invalid")
    assert not await rooms.is_human_sender("@agent:example.invalid")
    assert not await rooms.is_human_sender("@unknown:example.invalid")
    with pytest.raises(ValueError, match="Jira credentials missing"):
        rooms.issue_url(instance="acme", issue_key="KAN-1")


def test_notice_window_default_and_validation():
    assert _config().jira_worker_notice_window_seconds == 86_400
    with pytest.raises(ValueError):
        _config(jira_worker_notice_window_seconds=0)
