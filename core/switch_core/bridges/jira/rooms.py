"""Step 3: ticket rooms, ticket card, archive/reopen, invite follow-ups.

One Switch room per Jira ticket, created after the intake row exists and
archived at Done. The ticket_map row is the idempotency record: the worker
finds the existing room through ``ticket.room_id`` first and never by room
name alone (room names are not unique). A restart re-reads the same row, so
at most one live room exists per ticket.

Membership maps onto Switch authz (``switch_core.authz``): ticket rooms are
private, owned by the mapped reporter, so the reporter plus admins
(``User.role == "admin"` bypass) can read and write. An unmapped reporter
still gets a room for the record with no owner, which only admins can see.
When an admin later maps the reporter, the pending invite follow-up transfers
room ownership to them.

Card refresh (option C): the creation card message stays fixed as the
thread root, and the live ticket state is rewritten in place into the room
description (``SwitchCardUpdater``). True message edit stays a follow-up for
step 7's admin cards.
"""

from __future__ import annotations

import logging
from typing import Protocol

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from switch_core.bridges.agent.protocol.service import ProtocolService
from switch_core.bridges.jira.worker import (
    SYNC_TICKET_ROOM_COMMAND,
    is_terminal_ticket_status,
)
from switch_core.config import SwitchConfig
from switch_core.db.models import JiraWorkerIdentity, JiraWorkerOutbox, JiraWorkerTicket, Room, User
from switch_core.db.stores.agent_store import AgentStore
from switch_core.room_service import RoomCreateConfig, RoomService

TICKET_ROOM_INSTANCE_KEY = "jira_instance"
TICKET_ROOM_ISSUE_KEY = "jira_issue_key"

logger = logging.getLogger(__name__)

INVITE_REPORTER_COMMAND = "invite_ticket_reporter"
ADMIN_CARD_COMMAND = "upsert_ticket_admin_card"


class TicketRooms(Protocol):
    """Switch-side effects for ticket rooms. Production implementation lives
    in ``SwitchTicketRooms`` below; tests use fakes."""

    async def create_ticket_room(
        self,
        *,
        name: str,
        description: str,
        owner_id: str | None,
        instance: str,
        issue_key: str,
    ) -> str:
        """Create a private internal room marked for this ticket. Returns the room id."""
        ...

    async def ensure_agent_member(self, room_id: str, *, agent_name: str) -> None: ...

    async def post_card(self, room_id: str, *, body: str) -> str:
        """Post the ticket card. Returns the transport event id."""
        ...

    async def is_archived(self, room_id: str) -> bool | None:
        """Room archive state, or None when the room no longer exists."""
        ...

    async def set_archived(self, room_id: str, *, archived: bool) -> None: ...

    async def find_ticket_room(self, *, instance: str, issue_key: str) -> str | None:
        """Newest unreferenced room whose marker is this instance and issue key."""
        ...

    async def get_room_owner(self, room_id: str) -> str | None:
        """Current room owner, or None when ownerless or the room is gone."""
        ...

    async def get_room_description(self, room_id: str) -> str | None:
        """Current room description, or None when the room is gone."""
        ...

    async def set_room_owner(self, room_id: str, *, owner_id: str | None) -> bool:
        """Set room ownership. Returns False when the room is gone."""
        ...

    async def reporter_display_name(self, switch_user_id: str) -> str | None: ...

    def issue_url(self, *, instance: str, issue_key: str) -> str: ...


class CardUpdater(Protocol):
    """In-place card refresh. Switch has no message-edit path
    (``PostgresTransport`` only appends), so per the step-3 decision the
    creation card message stays fixed as the thread root and the live
    ticket state rides in the room description via ``update_room``."""

    async def update_card(self, room_id: str, *, event_id: str, body: str) -> None: ...


def render_ticket_card(
    *,
    issue_key: str,
    summary: str,
    status: str,
    reporter_label: str,
    issue_url: str,
) -> str:
    lines = [
        f"[{issue_key}] {summary or '(no summary)'}",
        f"Status: {status or '(unknown)'}",
        f"Reporter: {reporter_label}",
        f"Jira: {issue_url}",
    ]
    return "\n".join(lines)


async def _mapped_reporter_user_id(
    session: AsyncSession, ticket: JiraWorkerTicket
) -> str | None:
    """Switch user on the identity row for this ticket's reporter account.

    Done and cancelled tickets keep the reporter column from step 2, so the
    column is not the owner. No row, or a legacy row with no user, is unmapped.
    """
    return await session.scalar(
        select(JiraWorkerIdentity.switch_user_id).where(
            JiraWorkerIdentity.instance == ticket.instance,
            JiraWorkerIdentity.jira_account_id == ticket.reporter_account_id,
        )
    )


async def _reporter_label(
    session: AsyncSession, rooms: TicketRooms, switch_user_id: str | None
) -> str:
    if not switch_user_id:
        return "unmapped reporter (admins only)"
    name = await rooms.reporter_display_name(switch_user_id)
    if name:
        return name
    user = await session.get(User, switch_user_id)
    if user is not None:
        return user.name
    return "mapped reporter"


async def _render_card(
    session: AsyncSession,
    rooms: TicketRooms,
    ticket: JiraWorkerTicket,
    switch_user_id: str | None,
) -> str:
    return render_ticket_card(
        issue_key=ticket.issue_key,
        summary=ticket.summary,
        status=ticket.status,
        reporter_label=await _reporter_label(session, rooms, switch_user_id),
        issue_url=rooms.issue_url(
            instance=ticket.instance, issue_key=ticket.issue_key
        ),
    )


async def _relock_ticket(
    session: AsyncSession, *, instance: str, issue_key: str
) -> JiraWorkerTicket | None:
    return await session.scalar(
        select(JiraWorkerTicket)
        .where(
            JiraWorkerTicket.instance == instance,
            JiraWorkerTicket.issue_key == issue_key,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )


async def _mark_ticket_room_sync_done(session: AsyncSession, *, ticket_id: str) -> None:
    """Clear invite and room-sync rows only after the room state was applied."""
    stmt = (
        select(JiraWorkerOutbox)
        .where(
            JiraWorkerOutbox.channel == "switch",
            JiraWorkerOutbox.command.in_(
                (INVITE_REPORTER_COMMAND, SYNC_TICKET_ROOM_COMMAND)
            ),
            JiraWorkerOutbox.status == "pending",
            JiraWorkerOutbox.payload["ticket_id"].astext == ticket_id,
        )
        .with_for_update(skip_locked=True)
    )
    for row in (await session.execute(stmt)).scalars().all():
        row.status = "done"
    await session.flush()


def ticket_room_marker(instance: str, issue_key: str) -> dict[str, str]:
    return {
        TICKET_ROOM_INSTANCE_KEY: instance,
        TICKET_ROOM_ISSUE_KEY: issue_key,
    }


async def _ensure_owner(
    session: AsyncSession,
    rooms: TicketRooms,
    ticket: JiraWorkerTicket,
    room_id: str,
    owner_id: str | None,
) -> bool:
    """Point the room at ``owner_id``. False when the room is gone."""
    if await rooms.get_room_owner(room_id) == owner_id:
        return True
    if await rooms.set_room_owner(room_id, owner_id=owner_id):
        return True
    logger.warning(
        "Jira ticket room %s for %s is gone; converging a replacement",
        ticket.room_id,
        ticket.issue_key,
    )
    ticket.room_id = None
    ticket.card_event_id = None
    await session.flush()
    return False


async def _apply_description_and_archive(
    rooms: TicketRooms,
    cards: CardUpdater | None,
    ticket: JiraWorkerTicket,
    room_id: str,
    body: str,
) -> None:
    if (
        cards is not None
        and ticket.card_event_id is not None
        and body != await rooms.get_room_description(room_id)
    ):
        await cards.update_card(room_id, event_id=ticket.card_event_id, body=body)
    want_archived = is_terminal_ticket_status(ticket.status)
    if bool(await rooms.is_archived(room_id)) != want_archived:
        await rooms.set_archived(room_id, archived=want_archived)


async def reconcile_ticket_room(
    session: AsyncSession,
    *,
    instance: str,
    issue_key: str,
    rooms: TicketRooms,
    jira_agent_name: str,
    cards: CardUpdater | None = None,
) -> str | None:
    """Converge one ticket's room, owner, live card, thread root, and archive state.

    Idempotent desired-state reconciler: safe across webhook/poll duplicates,
    mapping changes, and restarts. Exactly one room exists per ticket: a stored
    pointer is verified, else an unreferenced room carrying this ticket's marker
    (Jira instance and issue key, written at create) is adopted, else a room is
    created and the pointer is committed before later side effects. Adoption
    never uses the room name. Owner and the reporter label follow the identity
    row for the reporter account, or nobody when that row is gone. The
    description is the freshly rendered card. The creation card is posted once.
    The room is archived when the ticket is Done or cancelled. Pending room-sync
    rows are cleared only after owner, description, and archive have been
    applied. The caller commits. Returns the room id, or None when the ticket
    row is missing.
    """
    ticket = await _relock_ticket(session, instance=instance, issue_key=issue_key)
    if ticket is None:
        return None

    for _ in range(2):
        room_id = ticket.room_id
        if room_id is not None and await rooms.is_archived(room_id) is None:
            logger.warning(
                "Jira ticket room %s for %s is gone; converging a replacement",
                ticket.room_id,
                ticket.issue_key,
            )
            ticket.room_id = None
            ticket.card_event_id = None
            await session.flush()
            room_id = None
        if room_id is None:
            room_id = await rooms.find_ticket_room(
                instance=ticket.instance, issue_key=ticket.issue_key
            )
            if room_id is not None and await rooms.is_archived(room_id) is None:
                room_id = None
            if room_id is not None:
                ticket.room_id = room_id
                await session.commit()
                ticket = await _relock_ticket(
                    session, instance=instance, issue_key=issue_key
                )
                if ticket is None:
                    return room_id
                await rooms.ensure_agent_member(room_id, agent_name=jira_agent_name)
            else:
                owner_id = await _mapped_reporter_user_id(session, ticket)
                body = await _render_card(session, rooms, ticket, owner_id)
                room_id = await rooms.create_ticket_room(
                    name=ticket.issue_key,
                    description=body,
                    owner_id=owner_id,
                    instance=ticket.instance,
                    issue_key=ticket.issue_key,
                )
                ticket.room_id = room_id
                await session.commit()
                ticket = await _relock_ticket(
                    session, instance=instance, issue_key=issue_key
                )
                if ticket is None:
                    return room_id
                await rooms.ensure_agent_member(room_id, agent_name=jira_agent_name)
        owner_id = await _mapped_reporter_user_id(session, ticket)
        if not await _ensure_owner(session, rooms, ticket, room_id, owner_id):
            continue
        body = await _render_card(session, rooms, ticket, owner_id)
        if ticket.card_event_id is None:
            ticket.card_event_id = await rooms.post_card(room_id, body=body)
            await session.commit()
            ticket = await _relock_ticket(
                session, instance=instance, issue_key=issue_key
            )
            if ticket is None:
                return room_id
            owner_id = await _mapped_reporter_user_id(session, ticket)
            if not await _ensure_owner(session, rooms, ticket, room_id, owner_id):
                continue
            body = await _render_card(session, rooms, ticket, owner_id)
        await _apply_description_and_archive(rooms, cards, ticket, room_id, body)
        await _mark_ticket_room_sync_done(session, ticket_id=ticket.id)
        await session.flush()
        return room_id
    await session.flush()
    return None


async def sync_ticket_room(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    instance: str,
    issue_key: str,
    rooms: TicketRooms | None,
    jira_agent_name: str,
    cards: CardUpdater | None = None,
) -> str | None:
    """Converge one ticket room. Shared by intake, poll, mapping, and the sweep.

    A failure rolls back this session and returns None. The caller continues
    with the next ticket. A pending room-sync row stays until reconcile applies
    owner, description, and archive.
    """
    if rooms is None:
        return None
    async with session_factory() as session:
        try:
            room_id = await reconcile_ticket_room(
                session,
                instance=instance,
                issue_key=issue_key,
                rooms=rooms,
                jira_agent_name=jira_agent_name,
                cards=cards,
            )
            await session.commit()
        except Exception:
            logger.exception("Jira ticket room sync failed for %s", issue_key)
            await session.rollback()
            return None
        return room_id


async def set_ticket_room_owner(
    session: AsyncSession, *, room_id: str, owner_id: str | None
) -> bool:
    """Transfer ownership of a worker-created room. Returns False when gone."""
    room = await session.get(Room, room_id)
    if room is None:
        return False
    room.owner_id = owner_id
    await session.flush()
    return True


class SwitchTicketRooms:
    """Production ``TicketRooms`` built on the server's internal code paths.

    Room create/archive go through ``RoomService`` and card posts through
    ``ProtocolService.send_message`` as the Jira system agent (never the
    public HTTP API). Rooms are private internal rooms owned by the mapped
    reporter, or ownerless (admins only) for unmapped reporters."""

    def __init__(
        self,
        *,
        room_service: RoomService,
        protocol: ProtocolService,
        session_factory: async_sessionmaker[AsyncSession],
        agent_store: AgentStore,
        config: SwitchConfig,
    ) -> None:
        self._rooms = room_service
        self._protocol = protocol
        self._sessions = session_factory
        self._agents = agent_store
        self._config = config

    async def _jira_agent_id(self) -> str:
        async with self._sessions() as session:
            agent = await self._agents.get_by_name(
                session, self._config.jira_agent_name
            )
        if agent is None:
            raise ValueError(
                f"Jira system agent {self._config.jira_agent_name!r} is not provisioned"
            )
        return agent.id

    async def create_ticket_room(
        self,
        *,
        name: str,
        description: str,
        owner_id: str | None,
        instance: str,
        issue_key: str,
    ) -> str:
        result = await self._rooms.create_room(
            RoomCreateConfig(
                name=name,
                description=description,
                agent_names=[self._config.jira_agent_name],
                internal_only=True,
                created_by=owner_id,
                owner_id=owner_id,
                read_visibility="private",
                write_visibility="private",
                metadata=ticket_room_marker(instance, issue_key),
            )
        )
        logger.info("Jira ticket room created for %s: %s", name, result.room.id)
        return result.room.id

    async def find_ticket_room(self, *, instance: str, issue_key: str) -> str | None:
        async with self._sessions() as session:
            referenced = select(JiraWorkerTicket.room_id).where(
                JiraWorkerTicket.room_id.is_not(None)
            )
            stmt = (
                select(Room.id)
                .where(
                    Room.metadata_[TICKET_ROOM_INSTANCE_KEY].astext == instance,
                    Room.metadata_[TICKET_ROOM_ISSUE_KEY].astext == issue_key,
                    Room.id.not_in(referenced),
                )
                .order_by(Room.created_at.desc())
                .limit(1)
            )
            return (await session.execute(stmt)).scalar_one_or_none()

    async def get_room_owner(self, room_id: str) -> str | None:
        async with self._sessions() as session:
            room = await session.get(Room, room_id)
            return room.owner_id if room is not None else None

    async def get_room_description(self, room_id: str) -> str | None:
        async with self._sessions() as session:
            room = await session.get(Room, room_id)
            return room.description if room is not None else None

    async def ensure_agent_member(self, room_id: str, *, agent_name: str) -> None:
        await self._rooms.add_agents_to_room(room_id, agent_names=[agent_name])
        await self._rooms.invite_missing_member_clients(room_id)

    async def post_card(self, room_id: str, *, body: str) -> str:
        return await self._protocol.send_message(
            await self._jira_agent_id(), room_id, body
        )

    async def is_archived(self, room_id: str) -> bool | None:
        async with self._sessions() as session:
            room = await session.get(Room, room_id)
            return None if room is None else room.archived_at is not None

    async def set_archived(self, room_id: str, *, archived: bool) -> None:
        await self._rooms.set_room_archived(room_id, archived)

    async def set_room_owner(self, room_id: str, *, owner_id: str | None) -> bool:
        async with self._sessions() as session:
            done = await set_ticket_room_owner(
                session, room_id=room_id, owner_id=owner_id
            )
            await session.commit()
        return done

    async def reporter_display_name(self, switch_user_id: str) -> str | None:
        async with self._sessions() as session:
            user = await session.get(User, switch_user_id)
            return user.name if user is not None else None

    def issue_url(self, *, instance: str, issue_key: str) -> str:
        credentials = self._config.jira_worker_credentials.get(instance)
        if credentials is None:
            return issue_key
        return f"{credentials.base_url.rstrip('/')}/browse/{issue_key}"


class SwitchCardUpdater:
    """Production ``CardUpdater``: the live card state is the room description.

    The creation card message stays fixed (it is the design's thread root),
    and each refresh rewrites the room description in place through the
    existing ``RoomService.update_room`` path."""

    def __init__(self, *, room_service: RoomService) -> None:
        self._rooms = room_service

    async def update_card(self, room_id: str, *, event_id: str, body: str) -> None:
        await self._rooms.update_room(room_id, description=body)
