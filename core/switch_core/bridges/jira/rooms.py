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

The card-update mechanism is one seam (``CardUpdater``) because Switch has no
message-edit path to reuse: see ``update_card`` below.
"""

from __future__ import annotations

import logging
from typing import Protocol

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from switch_core.bridges.agent.protocol.service import ProtocolService
from switch_core.bridges.jira.worker import is_terminal_ticket_status
from switch_core.config import SwitchConfig
from switch_core.db.models import JiraWorkerOutbox, JiraWorkerTicket, Room, User
from switch_core.db.stores.agent_store import AgentStore
from switch_core.room_service import RoomCreateConfig, RoomService

logger = logging.getLogger(__name__)

INVITE_REPORTER_COMMAND = "invite_ticket_reporter"
ADMIN_CARD_COMMAND = "upsert_ticket_admin_card"


class TicketRooms(Protocol):
    """Switch-side effects for ticket rooms. Production implementation lives
    in ``SwitchTicketRooms`` below; tests use fakes."""

    async def create_ticket_room(
        self, *, name: str, description: str, owner_id: str | None
    ) -> str:
        """Create a private internal room. Returns the room id."""
        ...

    async def ensure_agent_member(self, room_id: str, *, agent_name: str) -> None: ...

    async def post_card(self, room_id: str, *, body: str) -> str:
        """Post the ticket card. Returns the transport event id."""
        ...

    async def is_archived(self, room_id: str) -> bool | None:
        """Room archive state, or None when the room no longer exists."""
        ...

    async def set_archived(self, room_id: str, *, archived: bool) -> None: ...

    async def set_room_owner(self, room_id: str, *, owner_id: str) -> bool:
        """Transfer room ownership. Returns False when the room is gone."""
        ...

    async def reporter_display_name(self, switch_user_id: str) -> str | None: ...

    def issue_url(self, *, instance: str, issue_key: str) -> str: ...


class CardUpdater(Protocol):
    """In-place card refresh. Separate seam because Switch has no
    message-edit path today: ``PostgresTransport`` only appends, and
    ``read_context`` renders the ``messages`` table as-is, so there is no
    existing card or edited-message pattern to reuse."""

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


async def _reporter_label(
    session: AsyncSession, rooms: TicketRooms, ticket: JiraWorkerTicket
) -> str:
    if ticket.reporter_switch_user_id:
        name = await rooms.reporter_display_name(ticket.reporter_switch_user_id)
        if name:
            return name
        user = await session.get(User, ticket.reporter_switch_user_id)
        if user is not None:
            return user.name
        return "mapped reporter"
    return "unmapped reporter (admins only)"


async def sync_ticket_room(
    session: AsyncSession,
    *,
    instance: str,
    issue_key: str,
    rooms: TicketRooms,
    jira_agent_name: str,
    cards: CardUpdater | None = None,
) -> str | None:
    """Ensure the ticket room, card, and archive state for one ticket.

    Idempotent: safe across webhook/poll duplicates and restarts, because the
    room is found through ``ticket.room_id`` under a row lock. The caller
    commits. Returns the room id, or None when the ticket row is missing.
    """
    ticket = await session.scalar(
        select(JiraWorkerTicket)
        .where(
            JiraWorkerTicket.instance == instance,
            JiraWorkerTicket.issue_key == issue_key,
        )
        .with_for_update()
    )
    if ticket is None:
        return None

    archived: bool | None = None
    if ticket.room_id is not None:
        archived = await rooms.is_archived(ticket.room_id)
        if archived is None:
            logger.warning(
                "Jira ticket room %s for %s is gone; creating a replacement",
                ticket.room_id,
                ticket.issue_key,
            )
            ticket.room_id = None
            ticket.card_event_id = None

    if ticket.room_id is None:
        room_id = await rooms.create_ticket_room(
            name=ticket.issue_key,
            description=ticket.summary or ticket.issue_key,
            owner_id=ticket.reporter_switch_user_id,
        )
        ticket.room_id = room_id
        await session.flush()
        await rooms.ensure_agent_member(room_id, agent_name=jira_agent_name)
    else:
        room_id = ticket.room_id

    body = render_ticket_card(
        issue_key=ticket.issue_key,
        summary=ticket.summary,
        status=ticket.status,
        reporter_label=await _reporter_label(session, rooms, ticket),
        issue_url=rooms.issue_url(instance=ticket.instance, issue_key=ticket.issue_key),
    )
    if ticket.card_event_id is None:
        ticket.card_event_id = await rooms.post_card(room_id, body=body)
        await session.flush()
    elif cards is not None:
        await cards.update_card(room_id, event_id=ticket.card_event_id, body=body)

    if archived is None:
        archived = await rooms.is_archived(room_id)
    if is_terminal_ticket_status(ticket.status):
        if not archived:
            await rooms.set_archived(room_id, archived=True)
    elif archived:
        await rooms.set_archived(room_id, archived=False)
    await consume_reporter_invites(session, rooms=rooms, ticket_id=ticket.id)
    await session.flush()
    return room_id


async def consume_reporter_invites(
    session: AsyncSession,
    *,
    rooms: TicketRooms,
    ticket_id: str | None = None,
    switch_user_id: str | None = None,
    limit: int = 50,
) -> int:
    """Invite newly mapped reporters by transferring ticket-room ownership.

    Consumes pending ``invite_ticket_reporter`` rows (step 2 follow-ups) and
    marks each done. Admin-card rows are left for step 7. Rows whose room does
    not exist yet stay pending for a later pass. The caller commits.
    """
    stmt = (
        select(JiraWorkerOutbox)
        .where(
            JiraWorkerOutbox.channel == "switch",
            JiraWorkerOutbox.command == INVITE_REPORTER_COMMAND,
            JiraWorkerOutbox.status == "pending",
        )
        .order_by(JiraWorkerOutbox.created_at.asc())
        .limit(max(1, limit))
        .with_for_update(skip_locked=True)
    )
    rows = list((await session.execute(stmt)).scalars().all())
    done = 0
    for row in rows:
        payload = row.payload if isinstance(row.payload, dict) else {}
        row_ticket_id = payload.get("ticket_id")
        row_user_id = payload.get("switch_user_id")
        if ticket_id is not None and row_ticket_id != ticket_id:
            continue
        if switch_user_id is not None and row_user_id != switch_user_id:
            continue
        ticket = await session.get(JiraWorkerTicket, row_ticket_id)
        if ticket is None or not isinstance(row_user_id, str) or not row_user_id:
            row.status = "done"
            done += 1
            continue
        if ticket.room_id is None:
            continue
        if not await rooms.set_room_owner(ticket.room_id, owner_id=row_user_id):
            logger.warning(
                "Jira ticket room %s for %s is gone; dropping invite for %s",
                ticket.room_id,
                ticket.issue_key,
                row_user_id,
            )
        row.status = "done"
        done += 1
    await session.flush()
    return done


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
        self, *, name: str, description: str, owner_id: str | None
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
            )
        )
        logger.info("Jira ticket room created for %s: %s", name, result.room.id)
        return result.room.id

    async def ensure_agent_member(self, room_id: str, *, agent_name: str) -> None:
        await self._rooms.add_agents_to_room(room_id, agent_names=[agent_name])

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

    async def set_room_owner(self, room_id: str, *, owner_id: str) -> bool:
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
