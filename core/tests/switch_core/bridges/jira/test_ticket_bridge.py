"""Ticket reconciliation through real room services and PostgreSQL, without Mattermost."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from switch_core.bridges.jira.rooms import SwitchTicketRooms, reconcile_ticket_room
from switch_core.bridges.jira.worker import JiraWorkerScheduler, set_identity_mapping
from switch_core.db.models import (
    Agent,
    ApiKey,
    Client,
    CollaborationBridge,
    ExternalUser,
    ExternalUserClaim,
    JiraWorkerEvent,
    JiraWorkerIdentity,
    JiraWorkerOutbox,
    JiraWorkerTicket,
    Room,
    User,
)
from switch_core.db.stores.agent_store import AgentStore
from switch_core.db.stores.collaboration_bridge_store import CollaborationBridgeStore
from switch_core.db.stores.room_store import RoomStore
from switch_core.room_service import RoomService
from tests.switch_core.bridges.jira.test_activity import activity, message
from tests.switch_core.bridges.jira.test_outbox import JiraMock
from tests.switch_core.bridges.jira.test_rooms import _switch_config


@pytest.fixture
async def ticket_bridge(session_factory):
    async with session_factory() as session:
        session.add(
            User(
                id="reporter",
                name="Reporter",
                email="reporter@example.invalid",
                role="user",
            )
        )
        session.add_all(
            [
                Client(
                    id="bot",
                    matrix_user_id="@bot:test",
                    display_name="Bot",
                    type="bridge",
                ),
                Client(
                    id="jira",
                    matrix_user_id="@jira:test",
                    display_name="Jira",
                    type="agent",
                ),
                *[
                    Client(
                        id=name,
                        matrix_user_id=f"@{name}:test",
                        display_name=name,
                        type="user",
                    )
                    for name in ("first", "second", "other", "elsewhere")
                ],
            ]
        )
        await session.flush()
        session.add(
            ApiKey(
                id="key",
                user_id="reporter",
                key_hash="placeholder-hash",
                encrypted_key="placeholder",
                label="fixture",
                type="agent",
            )
        )
        session.add_all(
            [
                CollaborationBridge(
                    id="mm",
                    type="mattermost",
                    display_name="Mattermost",
                    client_id="bot",
                    status="active",
                    is_default=True,
                ),
                CollaborationBridge(
                    id="elsewhere",
                    type="mattermost",
                    display_name="Other",
                    client_id="bot",
                    status="active",
                ),
            ]
        )
        await session.flush()
        session.add(
            Agent(
                id="jira",
                name="jira",
                description="Jira worker",
                agent_type="system",
                connector_type="internal",
                integration_profile={},
                client_id="jira",
                api_key_id="key",
            )
        )
        session.add_all(
            [
                ExternalUser(
                    id=name,
                    bridge_id="elsewhere" if name == "elsewhere" else "mm",
                    external_user_id=f"ext-{name}",
                    external_username=name,
                    client_id=name,
                )
                for name in ("first", "second", "other", "elsewhere")
            ]
        )
        await session.flush()
        session.add_all(
            [
                ExternalUserClaim(external_user_id=name, user_id="reporter")
                for name in ("first", "second", "elsewhere")
            ]
        )
        session.add(
            JiraWorkerIdentity(
                instance="acme", jira_account_id="acct", switch_user_id="reporter"
            )
        )
        session.add(
            JiraWorkerTicket(
                instance="acme",
                issue_key="KAN-1",
                project_key="KAN",
                summary="Private details",
                status="To Do",
                reporter_account_id="acct",
                reporter_switch_user_id="reporter",
                wait_channel="switch",
            )
        )
        await session.commit()

    adapter = SimpleNamespace(
        create_channel=AsyncMock(return_value="private-channel"),
        ensure_channel_subscriptions=AsyncMock(),
        add_agents_to_channel=AsyncMock(),
        add_users_to_channel=AsyncMock(return_value=[]),
    )
    bridge = SimpleNamespace(
        adapter=adapter,
        _bridge_client_matrix_user_id="@bot:test",
        begin_provisioning=lambda channel: None,
        end_provisioning=lambda channel: None,
        add_room_mapping=lambda *args: None,
    )
    matrix = SimpleNamespace(
        create_room=AsyncMock(return_value="!ticket:test"), invite_to_room=AsyncMock()
    )
    service = RoomService(
        matrix_admin=matrix,
        room_store=RoomStore(),
        agent_store=AgentStore(),
        client_lifecycle=SimpleNamespace(
            get_by_agent_id=lambda _: SimpleNamespace(
                client_id="jira", matrix_user_id="@jira:test"
            ),
            get_by_type=lambda _: [],
        ),
        collab_lifecycle=SimpleNamespace(
            get=lambda bridge_id: bridge if bridge_id == "mm" else None
        ),
        collab_bridge_store=CollaborationBridgeStore(),
        resource_service=None,
        session_factory=session_factory,
    )
    protocol = SimpleNamespace(send_message=AsyncMock(return_value="$card"))
    rooms = SwitchTicketRooms(
        room_service=service,
        protocol=protocol,
        session_factory=session_factory,
        agent_store=AgentStore(),
        config=_switch_config(),
    )
    rooms.issue_url = lambda *, instance, issue_key: (
        f"https://jira.example.invalid/browse/{issue_key}"
    )
    return rooms, service, adapter, matrix


async def sync(factory, rooms):
    async with factory() as session:
        room_id = await reconcile_ticket_room(
            session,
            instance="acme",
            issue_key="KAN-1",
            rooms=rooms,
            jira_agent_name="jira",
        )
        await session.commit()
        return room_id


async def test_new_ticket_has_private_channel_and_only_reporter_accounts(
    session_factory, ticket_bridge
):
    rooms, _, adapter, matrix = ticket_bridge
    room_id = await sync(session_factory, rooms)
    async with session_factory() as session:
        room = await session.get(Room, room_id)
        assert room.bridge_id == "mm"
        assert room.external_channel_id == "private-channel"
        assert room.channel_type == "channel_private"
        assert room.read_visibility == room.write_visibility == "private"
        assert room.owner_id == "reporter"
    adapter.create_channel.assert_awaited_once_with(
        "KAN-1",
        "[KAN-1] Private details\nStatus: To Do\nReporter: Reporter\nJira: https://jira.example.invalid/browse/KAN-1",
        channel_type="channel_private",
        invite_default_member=False,
    )
    invited = adapter.add_users_to_channel.await_args.args
    assert invited[0] == "private-channel"
    assert set(invited[1]) == {"first", "second"}
    assert set(invited[2]) == {"ext-first", "ext-second"}
    assert any(
        call.args == ("!ticket:test", "@bot:test")
        for call in matrix.invite_to_room.await_args_list
    )


async def test_existing_room_repair_is_restart_and_double_claim_safe(
    session_factory, ticket_bridge
):
    rooms, _, adapter, matrix = ticket_bridge
    async with session_factory() as session:
        room = Room(
            matrix_room_id="!old:test",
            name="KAN-1",
            description="Private details",
            owner_id="reporter",
            read_visibility="private",
            write_visibility="private",
        )
        session.add(room)
        await session.flush()
        ticket = await session.scalar(select(JiraWorkerTicket))
        ticket.room_id = room.id
        ticket.card_event_id = "$old-card"
        ticket.room_claimed = True
        await session.commit()
        room_id = room.id
    scheduler = JiraWorkerScheduler(
        session_factory=session_factory,
        config=_switch_config(jira_worker_enabled_projects={"acme": ["KAN"]}),
        ticket_rooms=rooms,
    )
    assert await scheduler.sweep_ticket_rooms() == 1
    await asyncio.gather(sync(session_factory, rooms), sync(session_factory, rooms))
    assert await sync(session_factory, rooms) == room_id
    adapter.create_channel.assert_awaited_once()
    assert adapter.create_channel.await_args.kwargs["channel_type"] == "channel_private"
    assert adapter.create_channel.await_args.kwargs["invite_default_member"] is False
    async with session_factory() as session:
        repaired = await session.get(Room, room_id)
        assert repaired.external_channel_id == "private-channel"
        assert repaired.bridge_id == "mm"
    assert any(
        call.args == ("!old:test", "@bot:test")
        for call in matrix.invite_to_room.await_args_list
    )
    assert adapter.add_users_to_channel.await_count >= 1


async def test_no_default_keeps_ticket_internal(session_factory, ticket_bridge, caplog):
    rooms, _, adapter, _ = ticket_bridge
    async with session_factory() as session:
        bridge = await session.get(CollaborationBridge, "mm")
        bridge.is_default = False
        await session.commit()
    room_id = await sync(session_factory, rooms)
    async with session_factory() as session:
        room = await session.get(Room, room_id)
        assert room.bridge_id is None
        assert room.external_channel_id is None
        assert room.read_visibility == room.write_visibility == "private"
    adapter.create_channel.assert_not_awaited()
    adapter.add_users_to_channel.assert_not_awaited()
    assert "default bridge" in caplog.text


async def test_archived_room_is_only_bridged_after_reopen(
    session_factory, ticket_bridge
):
    rooms, _, adapter, _ = ticket_bridge
    async with session_factory() as session:
        room = Room(
            matrix_room_id="!old:test",
            name="KAN-1",
            description="Private details",
            archived_at=datetime.now(UTC),
            read_visibility="private",
            write_visibility="private",
        )
        session.add(room)
        await session.flush()
        ticket = await session.scalar(select(JiraWorkerTicket))
        ticket.room_id, ticket.card_event_id, ticket.status = (
            room.id,
            "$old-card",
            "Done",
        )
        await session.commit()
    await sync(session_factory, rooms)
    adapter.create_channel.assert_not_awaited()
    adapter.add_users_to_channel.assert_not_awaited()
    async with session_factory() as session:
        ticket = await session.scalar(select(JiraWorkerTicket))
        ticket.status = "To Do"
        await session.commit()
    await sync(session_factory, rooms)
    adapter.create_channel.assert_awaited_once()


async def test_reporter_sender_requires_claim_on_external_account(
    session_factory, ticket_bridge
):
    rooms, _, _, _ = ticket_bridge
    assert await rooms.is_reporter_sender("@first:test", "reporter") is True
    assert await rooms.is_reporter_sender("@second:test", "reporter") is True
    assert await rooms.is_reporter_sender("@other:test", "reporter") is False
    assert await rooms.is_reporter_sender("@first:test", "someone-else") is False


async def test_bridged_answers_record_reporter_input_only_for_claimed_sender(
    session_factory, ticket_bridge
):
    rooms, _, _, _ = ticket_bridge
    room_id = await sync(session_factory, rooms)
    rooms._protocol.read_context = AsyncMock(
        return_value={
            "next_seq": 2,
            "truncated": False,
            "threads": [
                {
                    "root": {"id": "$card"},
                    "replies": [message(1, "@first:test"), message(2, "@other:test")],
                }
            ],
        }
    )
    await activity(session_factory, JiraMock(), rooms).read_once()
    async with session_factory() as session:
        recorded = list(
            await session.scalars(
                select(JiraWorkerEvent)
                .where(JiraWorkerEvent.event_kind == "thread_message")
                .order_by(JiraWorkerEvent.idempotency_key)
            )
        )
        assert [event.payload["reporter_input"] for event in recorded] == [True, False]
        assert [event.payload["untrusted_data"]["sender"] for event in recorded] == [
            "@first:test",
            "@other:test",
        ]
        ticket = await session.scalar(select(JiraWorkerTicket))
        assert ticket.room_id == room_id
        assert ticket.thread_cursor == 2
        assert ticket.wait_channel == "switch"


async def test_mapping_follow_up_invites_reporter_to_existing_channel(
    session_factory, ticket_bridge
):
    rooms, _, adapter, _ = ticket_bridge
    async with session_factory() as session:
        identity = await session.scalar(select(JiraWorkerIdentity))
        await session.delete(identity)
        ticket = await session.scalar(select(JiraWorkerTicket))
        ticket.reporter_switch_user_id = None
        ticket.wait_channel = "jira_comments"
        await session.commit()
    room_id = await sync(session_factory, rooms)
    adapter.add_users_to_channel.assert_not_awaited()
    async with session_factory() as session:
        await set_identity_mapping(
            session,
            instance="acme",
            jira_account_id="acct",
            switch_user_id="reporter",
            enabled_projects=["KAN"],
        )
        await session.commit()
        pending = await session.scalar(
            select(JiraWorkerOutbox).where(
                JiraWorkerOutbox.command == "invite_ticket_reporter"
            )
        )
        assert pending.status == "pending"
    assert await sync(session_factory, rooms) == room_id
    adapter.create_channel.assert_awaited_once()
    adapter.add_users_to_channel.assert_awaited_once()
    async with session_factory() as session:
        invite = await session.get(JiraWorkerOutbox, pending.id)
        room = await session.get(Room, room_id)
        assert invite.status == "done"
        assert room.owner_id == "reporter"


async def test_invitation_failure_retries_without_another_channel(
    session_factory, ticket_bridge
):
    rooms, _, adapter, _ = ticket_bridge
    adapter.add_users_to_channel.return_value = ["ext-first"]
    with pytest.raises(ValueError, match="Could not invite claimed accounts"):
        await sync(session_factory, rooms)
    adapter.add_users_to_channel.return_value = []
    assert await sync(session_factory, rooms) is not None
    adapter.create_channel.assert_awaited_once()
    assert adapter.add_users_to_channel.await_count == 2
