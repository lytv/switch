"""Ticket reconciliation through real room services and PostgreSQL, without Mattermost."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from switch_core.bridges.collaboration.models import CHANNEL_GONE_METADATA_KEY
from switch_core.bridges.jira.admin import reconcile_admin_cards
from switch_core.bridges.jira.rooms import (
    CHANNEL_CLOSE_REASON,
    REPORTER_ACCESS_REASON,
    RESTORE_HISTORY_COMMAND,
    SwitchCardUpdater,
    SwitchTicketRooms,
    TerminalChannelCloseError,
    reconcile_ticket_room,
)
from switch_core.bridges.jira.worker import (
    JiraWorkerScheduler,
    record_polled_issue,
    set_identity_mapping,
)
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
    Message,
    Room,
    User,
)
from switch_core.db.stores.agent_store import AgentStore
from switch_core.db.stores.collaboration_bridge_store import CollaborationBridgeStore
from switch_core.db.stores.external_user_store import ExternalUserStore
from switch_core.db.stores.room_store import RoomStore
from switch_core.db.stores.user_store import UserStore
from switch_core.gateway.collaborations import (
    claim_bridge_identity,
    release_bridge_identity,
)
from switch_core.gateway.schemas import ClaimIdentityRequest
from switch_core.room_service import RoomService
from tests.switch_core.bridges.jira.test_activity import activity, message
from tests.switch_core.bridges.jira.test_admin import FakeAdminBoard, _worker_config
from tests.switch_core.bridges.jira.test_outbox import JiraMock, client_for, enqueue
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
        retain_channel_members=AsyncMock(),
        close_channel=AsyncMock(),
        reopen_channel=AsyncMock(),
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
    protocol = SimpleNamespace(
        send_message=AsyncMock(return_value="$card"), edit_message=AsyncMock()
    )
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
        ticket.intake_state = {
            "phase": "question",
            "history": [{"question": "Which customer?"}],
        }
        await RoomStore().add_agents(session, room.id, ["jira"])
        session.add(
            JiraWorkerOutbox(
                instance="acme",
                issue_key="KAN-1",
                channel="switch",
                command="intake_question",
                status="done",
                payload={
                    "room_id": room.id,
                    "thread_id": "$old-card",
                    "data": "Which customer?",
                },
            )
        )
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
    adapter.add_agents_to_channel.assert_awaited_once_with("private-channel", ["jira"])
    assert rooms._protocol.send_message.await_count == 2
    rooms._protocol.send_message.assert_any_await(
        "jira", room_id, "Which customer?", thread_id="$card"
    )
    async with session_factory() as session:
        ticket = await session.scalar(select(JiraWorkerTicket))
        assert ticket.card_event_id == "$card"
        restoration = await session.scalar(
            select(JiraWorkerOutbox).where(
                JiraWorkerOutbox.command == RESTORE_HISTORY_COMMAND
            )
        )
        assert restoration.status == "done"
    rooms._protocol.read_context = AsyncMock(
        return_value={
            "next_seq": 1,
            "truncated": False,
            "threads": [
                {"root": {"id": "$card"}, "replies": [message(1, "@first:test")]}
            ],
        }
    )
    await activity(session_factory, JiraMock(), rooms).read_once()
    async with session_factory() as session:
        event = await session.scalar(
            select(JiraWorkerEvent).where(
                JiraWorkerEvent.event_kind == "thread_message"
            )
        )
        assert event.payload["reporter_input"] is True
    rooms._protocol.edit_message.reset_mock()
    await SwitchCardUpdater(ticket_rooms=rooms).update_card(
        room_id, event_id="$card", body="Updated ticket"
    )
    rooms._protocol.edit_message.assert_awaited_once_with(
        "jira", room_id, "$card", "Updated ticket"
    )


async def test_deleted_channel_room_stays_internal(session_factory, ticket_bridge):
    rooms, _, adapter, _ = ticket_bridge
    room_id = await sync(session_factory, rooms)
    async with session_factory() as session:
        await RoomStore().clear_bridge(session, room_id)
        room = await session.get(Room, room_id)
        room.metadata_ = {CHANNEL_GONE_METADATA_KEY: True}
        await session.commit()
    adapter.create_channel.reset_mock()
    adapter.add_users_to_channel.reset_mock()

    assert await sync(session_factory, rooms) == room_id

    adapter.create_channel.assert_not_awaited()
    async with session_factory() as session:
        detached = await session.get(Room, room_id)
        assert detached.bridge_id is None
        assert detached.external_channel_id is None
    scheduler = JiraWorkerScheduler(
        session_factory=session_factory,
        config=_switch_config(jira_worker_enabled_projects={"acme": ["KAN"]}),
        ticket_rooms=rooms,
    )
    assert await scheduler.sweep_ticket_rooms() == 0


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
    adapter.ensure_channel_subscriptions.assert_awaited_once()
    adapter.add_agents_to_channel.assert_awaited_once()
    adapter.add_users_to_channel.assert_awaited_once()
    async with session_factory() as session:
        invite = await session.get(JiraWorkerOutbox, pending.id)
        room = await session.get(Room, room_id)
        assert invite.status == "done"
        assert room.owner_id == "reporter"


async def test_invitation_failure_completes_sync_and_clears_admin_reason_on_retry(
    session_factory, ticket_bridge, caplog
):
    rooms, _, adapter, _ = ticket_bridge
    added = set()

    async def invite(channel, names, ids):
        added.update(set(ids) - {"ext-first"})
        return ["ext-first"]

    adapter.add_users_to_channel.side_effect = invite
    async with session_factory() as session:
        ticket = await session.scalar(select(JiraWorkerTicket))
        session.add(
            JiraWorkerOutbox(
                instance="acme",
                issue_key="KAN-1",
                channel="switch",
                command="invite_ticket_reporter",
                payload={"ticket_id": ticket.id},
            )
        )
        await session.commit()
    room_id = await sync(session_factory, rooms)
    assert room_id is not None
    assert added == {"ext-second"}
    assert "reporter access failed" in caplog.text
    async with session_factory() as session:
        ticket = await session.scalar(select(JiraWorkerTicket))
        assert "ext-first" in ticket.admin_reasons[REPORTER_ACCESS_REASON]
        pending = await session.scalar(
            select(JiraWorkerOutbox).where(
                JiraWorkerOutbox.command == "invite_ticket_reporter"
            )
        )
        assert pending.status == "done"
    board = FakeAdminBoard()
    config = _worker_config()
    await reconcile_admin_cards(
        session_factory, engine=session_factory.kw["bind"], board=board, config=config
    )
    assert "ext-first" in next(iter(board.cards.values()))
    scheduler = JiraWorkerScheduler(
        session_factory=session_factory, config=config, ticket_rooms=rooms
    )
    assert await scheduler.sweep_ticket_rooms() == 0
    adapter.add_users_to_channel.side_effect = None
    adapter.add_users_to_channel.return_value = []
    assert await sync(session_factory, rooms) == room_id
    await reconcile_admin_cards(
        session_factory, engine=session_factory.kw["bind"], board=board, config=config
    )
    assert "ext-first" not in next(iter(board.cards.values()))
    async with session_factory() as session:
        ticket = await session.scalar(select(JiraWorkerTicket))
        assert REPORTER_ACCESS_REASON not in (ticket.admin_reasons or {})
    adapter.create_channel.assert_awaited_once()
    adapter.ensure_channel_subscriptions.assert_awaited_once()
    assert adapter.add_users_to_channel.await_count == 2


@pytest.mark.parametrize("failure", ["stopped", "disabled"])
@pytest.mark.parametrize("existing", [False, True])
async def test_unavailable_default_does_not_block_sync_or_starve_sweep(
    session_factory, ticket_bridge, failure, existing, caplog
):
    rooms, service, adapter, matrix = ticket_bridge
    if existing:
        async with session_factory() as session:
            bridge = await session.get(CollaborationBridge, "mm")
            bridge.is_default = False
            await session.commit()
        await sync(session_factory, rooms)
        async with session_factory() as session:
            bridge = await session.get(CollaborationBridge, "mm")
            bridge.is_default = True
            await session.commit()
    bridge_core = service._collab_lifecycle.get("mm")
    if failure == "stopped":
        service._collab_lifecycle.get = lambda _: None
    else:
        async with session_factory() as session:
            bridge = await session.get(CollaborationBridge, "mm")
            bridge.channel_creation_enabled = False
            await session.commit()
    room_id = await sync(session_factory, rooms)
    assert room_id is not None
    async with session_factory() as session:
        room = await session.get(Room, room_id)
        ticket = await session.scalar(select(JiraWorkerTicket))
        assert room.external_channel_id is None
        assert REPORTER_ACCESS_REASON in ticket.admin_reasons
        session.add(
            JiraWorkerTicket(
                instance="acme", issue_key="KAN-2", project_key="KAN", status="To Do"
            )
        )
        await session.commit()
    matrix.create_room.return_value = "!second:test"
    scheduler = JiraWorkerScheduler(
        session_factory=session_factory,
        config=_switch_config(jira_worker_enabled_projects={"acme": ["KAN"]}),
        ticket_rooms=rooms,
    )
    assert await scheduler.sweep_ticket_rooms(limit=1) == 1
    assert await scheduler.sweep_ticket_rooms(limit=1) == 0
    adapter.create_channel.assert_not_awaited()
    assert "reporter access failed" in caplog.text
    service._collab_lifecycle.get = lambda _: bridge_core
    async with session_factory() as session:
        bridge = await session.get(CollaborationBridge, "mm")
        bridge.channel_creation_enabled = True
        await session.commit()
    assert await sync(session_factory, rooms) == room_id
    adapter.create_channel.assert_awaited_once()
    adapter.add_users_to_channel.assert_awaited_once()
    async with session_factory() as session:
        ticket = await session.scalar(
            select(JiraWorkerTicket).where(JiraWorkerTicket.issue_key == "KAN-1")
        )
        assert REPORTER_ACCESS_REASON not in (ticket.admin_reasons or {})
        assert ticket.card_event_id == "$card"


async def test_sync_preserves_an_existing_channel_binding(
    session_factory, ticket_bridge
):
    rooms, service, adapter, matrix = ticket_bridge
    room_id = await sync(session_factory, rooms)
    async with session_factory() as session:
        room = await session.get(Room, room_id)
        room.channel_type = "channel_public"
        await session.commit()
    service.link_bridge_to_room = AsyncMock(
        side_effect=AssertionError("Must not relink")
    )
    matrix.invite_to_room.reset_mock()
    assert await sync(session_factory, rooms) == room_id
    service.link_bridge_to_room.assert_not_awaited()
    matrix.invite_to_room.assert_not_awaited()
    adapter.ensure_channel_subscriptions.assert_awaited_once()
    async with session_factory() as session:
        assert (await session.get(Room, room_id)).channel_type == "channel_public"


@pytest.mark.parametrize("status", ["pending", "claimed"])
async def test_history_restoration_retargets_queued_questions(
    session_factory, ticket_bridge, status
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
        ticket.intake_state = {
            "phase": "question",
            "history": [{"question": "Which customer?"}],
        }
        question = JiraWorkerOutbox(
            instance="acme",
            issue_key="KAN-1",
            channel="switch",
            command="intake_question",
            status=status,
            payload={
                "room_id": room_id,
                "thread_id": "$card",
                "data": "Which customer?",
            },
        )
        session.add(question)
        await session.commit()
        question_id = question.id
    rooms._protocol.send_message.reset_mock()
    rooms._protocol.send_message.return_value = "$new-card"
    assert await sync(session_factory, rooms) == room_id
    rooms._protocol.send_message.assert_awaited_once()
    async with session_factory() as session:
        question = await session.get(JiraWorkerOutbox, question_id)
        assert question.status == status
        assert question.payload["thread_id"] == "$new-card"
        assert (
            await session.scalar(select(JiraWorkerTicket))
        ).card_event_id == "$new-card"
    adapter.create_channel.assert_awaited_once()


@pytest.mark.parametrize("interrupted", ["card", "question"])
async def test_history_restoration_reuses_messages_after_interrupted_send(
    session_factory, ticket_bridge, interrupted
):
    rooms, service, _, _ = ticket_bridge
    async with session_factory() as session:
        bridge = await session.get(CollaborationBridge, "mm")
        bridge.is_default = False
        await session.commit()
    room_id = await sync(session_factory, rooms)
    async with session_factory() as session:
        bridge = await session.get(CollaborationBridge, "mm")
        bridge.is_default = True
        ticket = await session.scalar(select(JiraWorkerTicket))
        ticket.intake_state = {
            "phase": "question",
            "history": [{"question": "Which customer?"}],
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
                    "data": "Which customer?",
                },
            )
        )
        await session.commit()
    sent = []
    failed = False

    async def publish(agent_id, room_id, body, thread_id=None):
        nonlocal failed
        kind = "question" if thread_id else "card"
        event_id = f"$restored-{kind}"
        sent.append(kind)
        async with session_factory() as session:
            session.add(
                Message(
                    room_id=room_id,
                    seq=len(sent),
                    transport_event_id=event_id,
                    sender_id="@jira:test",
                    sender_client_id="jira",
                    event_type="m.room.message",
                    body=body,
                    thread_root_event_id=thread_id,
                    content={"body": body},
                )
            )
            await session.commit()
        if not failed and kind == interrupted:
            failed = True
            raise RuntimeError("Interrupted after send")
        return event_id

    rooms._protocol.send_message.side_effect = publish
    with pytest.raises(RuntimeError, match="Interrupted after send"):
        await sync(session_factory, rooms)
    restarted = SwitchTicketRooms(
        room_service=service,
        protocol=rooms._protocol,
        session_factory=session_factory,
        agent_store=AgentStore(),
        config=_switch_config(),
    )
    restarted.issue_url = rooms.issue_url
    assert await sync(session_factory, restarted) == room_id
    assert await sync(session_factory, restarted) == room_id
    assert sent == ["card", "question"]
    async with session_factory() as session:
        ticket = await session.scalar(select(JiraWorkerTicket))
        assert ticket.card_event_id == "$restored-card"
        history = await session.scalar(
            select(JiraWorkerOutbox).where(
                JiraWorkerOutbox.command == RESTORE_HISTORY_COMMAND
            )
        )
        assert history.status == "done"


async def test_claim_and_release_reconcile_channel_members(
    session_factory, ticket_bridge
):
    rooms, service, adapter, _ = ticket_bridge
    async with session_factory() as session:
        claim = await session.get(ExternalUserClaim, ("second", "reporter"))
        await session.delete(claim)
        await session.commit()
    adapter.add_users_to_channel.return_value = ["ext-first"]
    room_id = await sync(session_factory, rooms)
    config = _switch_config(jira_worker_enabled_projects={"acme": ["KAN"]})
    async with session_factory() as session:
        user = await session.get(User, "reporter")
        await claim_bridge_identity(
            bridge_id="mm",
            payload=ClaimIdentityRequest(
                external_user_id="ext-second", username="second"
            ),
            session=session,
            bridge_store=CollaborationBridgeStore(),
            external_user_store=ExternalUserStore(),
            user_store=UserStore(),
            collab_lifecycle=service._collab_lifecycle,
            user=user,
            config=config,
        )
        ticket = await session.scalar(select(JiraWorkerTicket))
        pending = await session.scalar(
            select(JiraWorkerOutbox).where(
                JiraWorkerOutbox.command == "sync_ticket_room",
                JiraWorkerOutbox.status == "pending",
            )
        )
        assert pending.payload["ticket_id"] == ticket.id
    adapter.add_users_to_channel.return_value = []
    scheduler = JiraWorkerScheduler(
        session_factory=session_factory, config=config, ticket_rooms=rooms
    )
    assert await scheduler.sweep_ticket_rooms() == 1
    assert set(adapter.add_users_to_channel.await_args.args[2]) == {
        "ext-first",
        "ext-second",
    }
    async with session_factory() as session:
        ticket = await session.scalar(select(JiraWorkerTicket))
        assert REPORTER_ACCESS_REASON not in (ticket.admin_reasons or {})
        user = await session.get(User, "reporter")
        await release_bridge_identity(
            bridge_id="mm",
            external_user_row_id="first",
            session=session,
            external_user_store=ExternalUserStore(),
            user_store=UserStore(),
            user=user,
            config=config,
        )
    assert await scheduler.sweep_ticket_rooms() == 1
    assert adapter.add_users_to_channel.await_args.args[2] == ["ext-second"]
    adapter.retain_channel_members.assert_awaited_with(
        "private-channel", {"ext-second"}
    )
    adapter.create_channel.assert_awaited_once()
    async with session_factory() as session:
        assert (
            await session.get(Room, room_id)
        ).external_channel_id == "private-channel"


async def test_terminal_sync_closes_bridged_channel(session_factory, ticket_bridge):
    rooms, _, adapter, _ = ticket_bridge
    room_id = await sync(session_factory, rooms)
    adapter.close_channel.reset_mock()
    async with session_factory() as session:
        ticket = await session.scalar(select(JiraWorkerTicket))
        ticket.status = "Done"
        await session.commit()
    assert await sync(session_factory, rooms) == room_id
    adapter.close_channel.assert_awaited_once_with("private-channel")
    async with session_factory() as session:
        room = await session.get(Room, room_id)
        assert room.archived_at is not None


async def test_human_reopen_restores_room_and_cards_without_resuming_intake(
    session_factory, ticket_bridge
):
    rooms, _, adapter, matrix = ticket_bridge
    room_id = await sync(session_factory, rooms)
    rooms._protocol.read_context = AsyncMock(
        return_value={"next_seq": 0, "truncated": False, "threads": []}
    )
    config = _worker_config()
    board = FakeAdminBoard()
    board.cards["admin-existing"] = "Existing card"
    jira = JiraMock()
    jira.fields["reporter"] = {"accountId": "acct"}
    client = client_for(jira)
    history = [
        {"question": "Which booking?"},
        {"reply": "Weekend booking"},
        {"complete": "Existing intake summary"},
    ]
    async with session_factory() as session:
        ticket = await session.scalar(select(JiraWorkerTicket))
        ticket.last_event_at = datetime.fromisoformat(jira.updated)
        ticket.jira_read_updated = jira.updated
        ticket.intake_state = {"phase": "approval", "questions": 1, "history": history}
        ticket.tokens_used = 1364
        ticket.admin_card_event_id = "admin-existing"
        await session.commit()
        ticket_id, card_id = ticket.id, ticket.card_event_id
        room = await session.get(Room, room_id)
        policy = (room.owner_id, room.read_visibility, room.write_visibility)

    scheduler = JiraWorkerScheduler(
        session_factory=session_factory,
        config=config,
        poll_clients={"acme": client},
        ticket_rooms=rooms,
        card_updater=SwitchCardUpdater(ticket_rooms=rooms),
        admin_board=board,
    )
    scheduler._orchestrator = AsyncMock(side_effect=AssertionError("Intake resumed"))
    assert await scheduler.activity.read_ticket(ticket_id, force_jira=True)

    async def transition(status):
        previous = jira.status
        jira.status = status
        jira.change("human-account")
        jira.histories[-1]["items"] = [
            {"field": "status", "fromString": previous, "toString": status}
        ]
        fields = (await client.request("GET", "/rest/api/3/issue/KAN-1")).json()[
            "fields"
        ]
        async with session_factory() as session:
            await record_polled_issue(
                session, {"key": "KAN-1", "fields": fields}, instance="acme"
            )
            await session.commit()

    await transition("Done")
    await scheduler.run_once()
    async with session_factory() as session:
        assert (await session.get(Room, room_id)).archived_at is not None
    assert "Done" in board.cards["admin-existing"]
    adapter.close_channel.assert_awaited_once_with("private-channel")

    await transition("To Do")
    stale_write = await enqueue(session_factory)
    await scheduler.run_once()
    await scheduler.run_once()
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, ticket_id)
        room = await session.get(Room, room_id)
        assert ticket.status == "To Do"
        assert ticket.worker_parked_reason == "Human Jira action"
        assert ticket.agent_state == "parked"
        assert ticket.room_id == room_id and ticket.card_event_id == card_id
        assert ticket.tokens_used == 1364
        assert ticket.intake_state == {
            "phase": "approval",
            "questions": 1,
            "history": history,
        }
        assert room.archived_at is None
        assert room.external_channel_id == "private-channel"
        assert (room.owner_id, room.read_visibility, room.write_visibility) == policy
        assert "Status: To Do" in room.description
        assert len(list(await session.scalars(select(Room)))) == 1
        assert (await session.get(JiraWorkerOutbox, stale_write)).status == "parked"
    adapter.reopen_channel.assert_awaited_once_with("private-channel")
    adapter.retain_channel_members.assert_awaited_with(
        "private-channel", {"ext-first", "ext-second"}
    )
    adapter.create_channel.assert_awaited_once()
    matrix.create_room.assert_awaited_once()
    rooms._protocol.send_message.assert_awaited_once()
    assert rooms._protocol.edit_message.await_args.args[2] == card_id
    assert "Status: To Do" in rooms._protocol.edit_message.await_args.args[3]
    assert "To Do" in board.cards["admin-existing"]
    assert "Human Jira action" in board.cards["admin-existing"]
    assert len(board.cards) == 1 and not board.posts
    scheduler._orchestrator.assert_not_awaited()
    assert not jira.writes
    await client._client.aclose()


async def test_terminal_channel_close_failure_surfaces_and_keeps_sync_pending(
    session_factory, ticket_bridge
):
    rooms, _, adapter, _ = ticket_bridge
    room_id = await sync(session_factory, rooms)
    adapter.close_channel.reset_mock()
    adapter.close_channel.side_effect = TerminalChannelCloseError(
        "mattermost unavailable"
    )
    async with session_factory() as session:
        ticket = await session.scalar(select(JiraWorkerTicket))
        ticket.status = "Done"
        session.add(
            JiraWorkerOutbox(
                channel="switch",
                command="sync_ticket_room",
                issue_key=ticket.issue_key,
                payload={
                    "instance": ticket.instance,
                    "project_key": ticket.project_key,
                    "ticket_id": ticket.id,
                    "switch_user_id": ticket.reporter_switch_user_id,
                },
                status="pending",
            )
        )
        await session.commit()
    assert await sync(session_factory, rooms) == room_id
    async with session_factory() as session:
        ticket = await session.scalar(select(JiraWorkerTicket))
        assert (ticket.admin_reasons or {}).get(CHANNEL_CLOSE_REASON)
        pending = list(
            await session.scalars(
                select(JiraWorkerOutbox).where(
                    JiraWorkerOutbox.command == "sync_ticket_room",
                    JiraWorkerOutbox.status == "pending",
                )
            )
        )
        assert pending
        room = await session.get(Room, room_id)
        assert room.archived_at is None


def _pending_room_sync(ticket: JiraWorkerTicket) -> JiraWorkerOutbox:
    return JiraWorkerOutbox(
        channel="switch",
        command="sync_ticket_room",
        issue_key=ticket.issue_key,
        payload={
            "instance": ticket.instance,
            "project_key": ticket.project_key,
            "ticket_id": ticket.id,
            "switch_user_id": ticket.reporter_switch_user_id,
        },
        status="pending",
    )


async def test_unsupported_channel_close_still_archives_and_completes_sync(
    session_factory, ticket_bridge
):
    rooms, _, adapter, _ = ticket_bridge
    room_id = await sync(session_factory, rooms)
    adapter.close_channel.reset_mock()
    adapter.close_channel.side_effect = NotImplementedError(
        "slack cannot close bridged channels from Switch"
    )
    async with session_factory() as session:
        ticket = await session.scalar(select(JiraWorkerTicket))
        ticket.status = "Done"
        session.add(_pending_room_sync(ticket))
        await session.commit()
        ticket_id = ticket.id
    assert await sync(session_factory, rooms) == room_id
    adapter.close_channel.assert_awaited_once_with("private-channel")
    async with session_factory() as session:
        ticket = await session.scalar(select(JiraWorkerTicket))
        assert CHANNEL_CLOSE_REASON not in (ticket.admin_reasons or {})
        room = await session.get(Room, room_id)
        assert room.archived_at is not None
        pending = await session.scalar(
            select(JiraWorkerOutbox).where(
                JiraWorkerOutbox.command == "sync_ticket_room",
                JiraWorkerOutbox.payload["ticket_id"].astext == ticket_id,
            )
        )
        assert pending.status == "done"


@pytest.mark.parametrize("failure", ["stopped", "disconnected"])
async def test_channel_reopen_failure_stays_pending_until_native_restore(
    session_factory, ticket_bridge, failure, caplog
):
    rooms, service, adapter, _ = ticket_bridge
    room_id = await sync(session_factory, rooms)
    async with session_factory() as session:
        ticket = await session.scalar(select(JiraWorkerTicket))
        ticket.status = "Done"
        await session.commit()
    assert await sync(session_factory, rooms) == room_id
    adapter.reopen_channel.reset_mock()
    bridge = service._collab_lifecycle.get("mm")
    if failure == "stopped":
        service._collab_lifecycle.get = lambda bridge_id: None
    else:
        adapter.reopen_channel.side_effect = RuntimeError(
            "Cannot reopen channel: Mattermost client not connected"
        )
    async with session_factory() as session:
        ticket = await session.scalar(select(JiraWorkerTicket))
        ticket.status = "To Do"
        session.add(_pending_room_sync(ticket))
        await session.commit()
        ticket_id = ticket.id
    scheduler = JiraWorkerScheduler(
        session_factory=session_factory,
        config=_worker_config(),
        ticket_rooms=rooms,
    )
    assert await scheduler.sweep_ticket_rooms() == 0
    if failure == "disconnected":
        adapter.reopen_channel.assert_awaited_once_with("private-channel")
    assert "ticket room sync failed" in caplog.text
    async with session_factory() as session:
        room = await session.get(Room, room_id)
        assert room.archived_at is not None
        pending = await session.scalar(
            select(JiraWorkerOutbox).where(
                JiraWorkerOutbox.command == "sync_ticket_room",
                JiraWorkerOutbox.payload["ticket_id"].astext == ticket_id,
            )
        )
        assert pending.status == "pending"
    service._collab_lifecycle.get = lambda _: bridge
    adapter.reopen_channel.side_effect = None
    assert await scheduler.sweep_ticket_rooms() == 1
    async with session_factory() as session:
        room = await session.get(Room, room_id)
        assert room.archived_at is None
        assert room.external_channel_id == "private-channel"
        assert (await session.get(JiraWorkerOutbox, pending.id)).status == "done"
    adapter.create_channel.assert_awaited_once()
