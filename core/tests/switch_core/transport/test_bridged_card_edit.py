from __future__ import annotations

import asyncio
from types import MethodType, SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import func, select
from sqlalchemy.engine import make_url
from test_postgres_transport import _make_room, _transport

from switch_core.bridges.agent.protocol.service import ProtocolService
from switch_core.bridges.collaboration.bridge_core import BridgeCore
from switch_core.bridges.collaboration.mattermost.adapter import (
    MattermostAdapter,
    MattermostConnectionConfig,
)
from switch_core.bridges.jira.rooms import (
    SwitchCardUpdater,
    SwitchTicketRooms,
    _apply_description_and_archive,
    render_ticket_card,
)
from switch_core.clients.bridge_client import BridgeClient
from switch_core.clients.client_base import ClientBase
from switch_core.db.models import BridgeMessageMap, CollaborationBridge, Message, Room
from switch_core.db.stores.bridge_message_map_store import BridgeMessageMapStore
from switch_core.db.stores.room_store import RoomStore
from switch_core.room_service import RoomService
from switch_core.transport import TransportHandlers


@pytest.mark.parametrize(
    "target", ["Waiting for input", "Waiting for approval", "Done"]
)
@pytest.mark.parametrize("fail_first", [False, True])
async def test_status_edit_reaches_original_native_card(
    session_factory, postgres_url, target, fail_first
):
    url = make_url(postgres_url)
    assert url.database == "test" and url.port not in (None, 5432)
    native = {}
    calls = []
    archived = False
    fail_patch = fail_first

    def patch(post_id, payload):
        calls.append(("patch", post_id))
        assert not archived
        if fail_patch:
            raise RuntimeError("native edit refused")
        native[post_id]["message"] = payload["message"]
        return {"id": post_id}

    def close(channel_id):
        nonlocal archived
        calls.append(("close", channel_id))
        archived = True

    adapter = MattermostAdapter(
        config=MattermostConnectionConfig(
            url="https://mattermost.example.invalid",
            admin_user="admin",
            admin_password="<unused>",
            team_name="test",
        )
    )
    adapter._main_loop = asyncio.get_running_loop()
    adapter._admin_driver = SimpleNamespace(
        posts=SimpleNamespace(patch_post=patch),
        channels=SimpleNamespace(delete_channel=close),
    )

    async def send(channel_id, sender_name, content, thread_root_id=None):
        post_id = f"post-{len(native)}"
        calls.append(("create", post_id))
        native[post_id] = {"message": content, "root_id": thread_root_id}
        return post_id

    adapter.send_message = send
    async with session_factory() as session:
        room_id, transport_room_id, client_id, user_id = await _make_room(session)
        session.add(
            CollaborationBridge(
                id="mm",
                type="mattermost",
                display_name="test",
                client_id=client_id,
                status="active",
            )
        )
        await session.flush()
        room = await session.get(Room, room_id)
        room.bridge_id, room.external_channel_id = "mm", "channel-test"
        await session.commit()

    bridge = BridgeCore(
        bridge_id="mm",
        bridge_type="mattermost",
        bridge_display_name="test",
        adapter=adapter,
        room_store=RoomStore(),
        external_user_store=None,
        bridge_message_map_store=BridgeMessageMapStore(),
        agent_store=None,
        client_store=None,
        room_service=None,
        client_lifecycle=None,
        matrix_admin=None,
        session_factory=session_factory,
        matrix_server_name="test",
        bridge_client_matrix_user_id="@bridge:test",
        max_attachment_bytes=1024,
    )
    bridge.add_room_mapping(room_id, transport_room_id, "channel-test")
    client = object.__new__(BridgeClient)
    client._bridge_core = bridge
    sender = _transport(session_factory, client_id=client_id, user_id=user_id)
    receiver = _transport(session_factory, client_id=client_id, user_id="@bridge:test")
    receiver.register_handlers(TransportHandlers(on_message=client.on_message))
    await receiver._watch(transport_room_id, from_seq=0)

    def body(status):
        return render_ticket_card(
            issue_key="TEST-1",
            summary="Synthetic intake",
            status=status,
            reporter_label="Reporter",
            issue_url="https://jira.example.invalid/browse/TEST-1",
        )

    try:
        initial = body("BACKLOG")
        sent = await sender.send_message(transport_room_id, initial, sender_name="jira")
        reply = await sender.send_message(
            transport_room_id,
            "Discussion",
            sender_name="jira",
            thread_root_id=sent.event_id,
        )
        await receiver._drain_room(room_id)
        assert native["post-0"] == {"message": initial, "root_id": None}
        assert native["post-1"] == {"message": "Discussion", "root_id": "post-0"}
        async with session_factory() as session:
            room = await session.get(Room, room_id)
            room.description = initial
            await session.commit()

        protocol = object.__new__(ProtocolService)
        author = SimpleNamespace(transport=sender)
        author.edit_message = MethodType(ClientBase.edit_message, author)
        protocol.client_lifecycle = SimpleNamespace(get_by_agent_id=lambda _: author)
        protocol.collab_lifecycle = SimpleNamespace(get=lambda _: bridge)
        protocol.require_room_member = AsyncMock(
            return_value=SimpleNamespace(
                matrix_room_id=transport_room_id, bridge_id="mm"
            )
        )
        service = object.__new__(RoomService)
        service._session_factory, service._room_store = session_factory, RoomStore()
        service._collab_lifecycle = protocol.collab_lifecycle
        rooms = object.__new__(SwitchTicketRooms)
        rooms._rooms, rooms._protocol, rooms._sessions = (
            service,
            protocol,
            session_factory,
        )
        rooms._jira_agent_id = AsyncMock(return_value="jira-agent")
        ticket = SimpleNamespace(status=target, card_event_id=sent.event_id)
        updater = SwitchCardUpdater(ticket_rooms=rooms)

        if fail_first:
            with pytest.raises(RuntimeError, match="native edit refused"):
                await _apply_description_and_archive(
                    rooms, updater, ticket, room_id, body(target)
                )
            async with session_factory() as session:
                edited = await session.scalar(
                    select(Message).where(Message.transport_event_id == sent.event_id)
                )
                room = await session.get(Room, room_id)
                assert edited.body == body(target)
                assert room.description == initial and room.archived_at is None
            assert native["post-0"]["message"] == initial and not archived
            fail_patch = False

        await _apply_description_and_archive(
            rooms, updater, ticket, room_id, body(target)
        )
        await receiver._drain_room(room_id)
        await _apply_description_and_archive(
            rooms, updater, ticket, room_id, body(target)
        )
        async with session_factory() as session:
            edited = await session.scalar(
                select(Message).where(Message.transport_event_id == sent.event_id)
            )
            discussion = await session.scalar(
                select(Message).where(Message.transport_event_id == reply.event_id)
            )
            mapping = await BridgeMessageMapStore().get_by_transport_event_id(
                session, "mm", sent.event_id
            )
            room = await session.get(Room, room_id)
            assert (
                edited.body
                == edited.content["body"]
                == room.description
                == body(target)
            )
            assert edited.seq == 1 and receiver._cursors[room_id] == 2
            assert discussion.thread_root_event_id == sent.event_id
            assert mapping.external_post_id == "post-0"
            assert await session.scalar(select(func.count()).select_from(Message)) == 2
            assert (
                await session.scalar(select(func.count()).select_from(BridgeMessageMap))
                == 2
            )
            assert (room.archived_at is not None) == (target == "Done")
        assert native["post-0"] == {"message": body(target), "root_id": None}
        assert native["post-1"] == {"message": "Discussion", "root_id": "post-0"}
        assert calls.count(("create", "post-0")) == 1 and len(native) == 2
        assert calls.count(("patch", "post-0")) == 1 + int(fail_first)
        assert archived == (target == "Done")
        if archived:
            assert calls[-1] == ("close", "channel-test")
    finally:
        receiver._unwatch_all()
