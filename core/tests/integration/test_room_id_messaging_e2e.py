"""End-to-end coverage for messaging an assigned room without moving sessions."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator

import pytest

from switch_core.bridges.agent.operations.callctx import (
    CallContext,
    reset_call_context,
    set_call_context,
)
from switch_core.bridges.agent.operations.context import init_operations_protocol
from switch_core.bridges.agent.operations.definitions import (
    connect_to_room,
    list_participants,
    list_rooms,
    post_message,
    send_targeted_message,
)
from switch_core.db.models import Message
from switch_core.room_service import RoomCreateConfig
from tests.integration.conftest import Harness, SessionEnv

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


@contextlib.asynccontextmanager
async def _acting_as(harness: Harness, agent_id: str) -> AsyncIterator[None]:
    init_operations_protocol(harness.protocol)
    token = set_call_context(
        CallContext(agent_id=agent_id, session_key=f"session-{agent_id}")
    )
    try:
        yield
    finally:
        reset_call_context(token)


async def _wait_for_bodies(
    harness: Harness,
    session_env: SessionEnv,
    room_id: str,
    expected: set[str],
    timeout: float = 20,
) -> list[Message]:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        async with harness.session_factory() as session:  # type: ignore[operator]
            rows = await session_env.message_store.list_for_room(
                session, room_id, after_seq=0, limit=100
            )
        bodies = {str(row.content.get("body")) for row in rows}
        if expected <= bodies:
            return rows
        await asyncio.sleep(0.25)
    raise AssertionError(f"messages not persisted in room {room_id}: {expected}")


async def test_room_id_messaging_keeps_the_callers_connected_room(
    harness: Harness, session_env: SessionEnv
) -> None:
    alice = await harness.register_agent("e2e-room-id-alice")
    bob = await harness.register_agent("e2e-room-id-bob")
    await harness.start_clients()

    home = await harness.room_service.create_room(
        RoomCreateConfig(
            name="e2e-room-id-home",
            description="The room that stays connected",
            agent_ids=[alice.agent_id],
        )
    )
    other = await harness.room_service.create_room(
        RoomCreateConfig(
            name="e2e-room-id-other",
            description="The room selected by an operation argument",
            agent_ids=[alice.agent_id, bob.agent_id],
        )
    )

    async with _acting_as(harness, alice.agent_id):
        await connect_to_room(home.room.id, include_general_instructions=False)

        participants = await list_participants(room_id=other.room.id)
        broadcast = await post_message("cross-room broadcast", room_id=other.room.id)
        targeted = await send_targeted_message(
            "cross-room targeted",
            target_names=["e2e-room-id-bob"],
            room_id=other.room.id,
        )
        rooms = await list_rooms()

    assert {person["id"] for person in participants} == {alice.agent_id, bob.agent_id}
    assert broadcast["event_id"]
    assert targeted["event_id"]
    assert next(room for room in rooms if room["room_id"] == home.room.id)["connected"]
    assert not next(room for room in rooms if room["room_id"] == other.room.id)[
        "connected"
    ]

    messages = await _wait_for_bodies(
        harness,
        session_env,
        other.room.id,
        {"cross-room broadcast", "@e2e-room-id-bob cross-room targeted"},
    )
    assert {str(message.content.get("body")) for message in messages} >= {
        "cross-room broadcast",
        "@e2e-room-id-bob cross-room targeted",
    }


async def test_room_id_messaging_refuses_an_unassigned_room(harness: Harness) -> None:
    alice = await harness.register_agent("e2e-room-id-member")
    stranger = await harness.register_agent("e2e-room-id-stranger")
    await harness.start_clients()
    private = await harness.room_service.create_room(
        RoomCreateConfig(
            name="e2e-room-id-private",
            description="The caller is not a member",
            agent_ids=[stranger.agent_id],
        )
    )

    async with _acting_as(harness, alice.agent_id):
        with pytest.raises(PermissionError):
            await list_participants(room_id=private.room.id)
        with pytest.raises(PermissionError):
            await post_message("must not send", room_id=private.room.id)
        with pytest.raises(PermissionError):
            await send_targeted_message(
                "must not send",
                target_names=["e2e-room-id-stranger"],
                room_id=private.room.id,
            )
