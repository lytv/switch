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

Card refresh preserves the thread root and updates both its message and the
room description. Attaching a channel replaces the internal-only root once.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any, Protocol, cast

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from switch_core.bridges.agent.protocol.service import ProtocolService
from switch_core.bridges.collaboration.models import ChannelCreationUnsupported
from switch_core.bridges.jira.activity import (
    admit_room,
    capacity_lock,
    ticket_turn_lock,
)
from switch_core.bridges.jira.admin import enqueue_admin_card_follow_up
from switch_core.bridges.jira.worker import (
    SYNC_TICKET_ROOM_COMMAND,
    is_terminal_ticket_status,
)
from switch_core.config import JiraWorkerLimits, SwitchConfig
from switch_core.db.models import (
    Client,
    ExternalUser,
    ExternalUserClaim,
    JiraWorkerIdentity,
    JiraWorkerOutbox,
    JiraWorkerTicket,
    Message,
    Room,
    User,
)
from switch_core.db.stores.agent_store import AgentStore
from switch_core.db.stores.collaboration_bridge_store import CollaborationBridgeStore
from switch_core.room_service import RoomCreateConfig, RoomService

TICKET_ROOM_INSTANCE_KEY = "jira_instance"
TICKET_ROOM_ISSUE_KEY = "jira_issue_key"

logger = logging.getLogger(__name__)

INVITE_REPORTER_COMMAND = "invite_ticket_reporter"
ADMIN_CARD_COMMAND = "upsert_ticket_admin_card"
RESTORE_HISTORY_COMMAND = "restore_ticket_history"
REPORTER_ACCESS_REASON = "reporter_access"
CHANNEL_CLOSE_REASON = "channel_close"


class TerminalChannelCloseError(RuntimeError):
    """The linked bridge channel could not be closed for a terminal ticket."""


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
        """Create a private room on the default bridge, marked for this ticket."""
        ...

    async def ensure_agent_member(self, room_id: str, *, agent_name: str) -> None: ...

    async def is_bridged(self, room_id: str) -> bool: ...

    async def ensure_reporter_access(
        self, room_id: str, user_id: str | None
    ) -> None: ...

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

    async def read_context(self, room_id: str, *, after_seq: int) -> dict[str, Any]: ...

    async def is_reporter_sender(self, sender: str, user_id: str) -> bool: ...

    async def is_worker_sender(self, sender: str) -> bool: ...

    async def is_human_sender(self, sender: str) -> bool: ...

    async def post_notice(self, room_id: str, *, body: str) -> str: ...

    async def post_question(
        self, room_id: str, *, thread_id: str, body: str
    ) -> str: ...


class CardUpdater(Protocol):
    """Refresh the card and room description without changing the thread root."""

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
        issue_url=rooms.issue_url(instance=ticket.instance, issue_key=ticket.issue_key),
    )


async def _relock_ticket(
    session: AsyncSession, *, instance: str, issue_key: str
) -> JiraWorkerTicket | None:
    await capacity_lock(session)
    ticket = await session.scalar(
        select(JiraWorkerTicket)
        .where(
            JiraWorkerTicket.instance == instance,
            JiraWorkerTicket.issue_key == issue_key,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    return cast(JiraWorkerTicket | None, ticket)


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
    ticket.thread_cursor = 0
    await session.flush()
    return False


async def _apply_description_and_archive(
    rooms: TicketRooms,
    cards: CardUpdater | None,
    ticket: JiraWorkerTicket,
    room_id: str,
    body: str,
) -> None:
    if cards is not None and ticket.card_event_id is not None:
        stored = await rooms.get_room_description(room_id)
        if body != stored:
            await cards.update_card(room_id, event_id=ticket.card_event_id, body=body)
    want_archived = is_terminal_ticket_status(ticket.status)
    if bool(await rooms.is_archived(room_id)) != want_archived:
        await rooms.set_archived(room_id, archived=want_archived)


async def _find_history_message(
    session: AsyncSession,
    rooms: TicketRooms,
    room_id: str,
    after_seq: int,
    body: str,
    thread_id: str | None,
) -> str | None:
    messages = await session.scalars(
        select(Message)
        .where(
            Message.room_id == room_id,
            Message.seq > after_seq,
            Message.body == body,
            Message.thread_root_event_id == thread_id,
        )
        .order_by(Message.seq)
    )
    for message in messages:
        if await rooms.is_worker_sender(message.sender_id):
            return message.transport_event_id
    return None


async def _restore_ticket_history(
    session: AsyncSession,
    rooms: TicketRooms,
    ticket: JiraWorkerTicket,
    row: JiraWorkerOutbox,
) -> None:
    assert ticket.room_id is not None
    payload = dict(row.payload or {})
    if not payload.get("card_event_id"):
        ticket.card_event_id = await _find_history_message(
            session, rooms, ticket.room_id, payload["after_seq"], payload["body"], None
        ) or await rooms.post_card(ticket.room_id, body=payload["body"])
        payload["card_event_id"] = ticket.card_event_id
        row.payload = payload
        await session.commit()
        if (
            await _relock_ticket(
                session, instance=ticket.instance, issue_key=ticket.issue_key
            )
            is None
        ):
            raise ValueError("Ticket disappeared during history restoration")
    else:
        ticket.card_event_id = payload["card_event_id"]
    questions = list(
        await session.scalars(
            select(JiraWorkerOutbox)
            .where(
                JiraWorkerOutbox.instance == ticket.instance,
                JiraWorkerOutbox.issue_key == ticket.issue_key,
                JiraWorkerOutbox.channel == "switch",
                JiraWorkerOutbox.command == "intake_question",
                JiraWorkerOutbox.payload["room_id"].astext == ticket.room_id,
            )
            .order_by(JiraWorkerOutbox.created_at.desc(), JiraWorkerOutbox.id.desc())
        )
    )
    if ticket.wait_channel == "switch":
        for question in questions:
            if question.status in ("pending", "claimed"):
                question.payload = {
                    **(question.payload or {}),
                    "thread_id": ticket.card_event_id,
                }
    latest = questions[0] if questions else None
    if (
        ticket.wait_channel == "switch"
        and (ticket.intake_state or {}).get("phase") == "question"
        and latest is not None
        and latest.status == "done"
        and (latest.payload or {}).get("intake_generation", ticket.agent_generation)
        == ticket.agent_generation
        and not any(
            question.status in ("pending", "claimed", "sending")
            for question in questions
        )
    ):
        question_body = (latest.payload or {})["data"]
        if question_body:
            assert ticket.card_event_id is not None
            existing = await _find_history_message(
                session,
                rooms,
                ticket.room_id,
                payload["after_seq"],
                question_body,
                ticket.card_event_id,
            )
            if existing is None:
                await rooms.post_question(
                    ticket.room_id, thread_id=ticket.card_event_id, body=question_body
                )
    row.status = "done"
    await session.flush()


async def reconcile_ticket_room(
    session: AsyncSession,
    *,
    instance: str,
    issue_key: str,
    rooms: TicketRooms,
    jira_agent_name: str,
    cards: CardUpdater | None = None,
    limits: JiraWorkerLimits | None = None,
    now: datetime | None = None,
    enabled_projects: dict[str, list[str]] | None = None,
) -> str | None:
    async with ticket_turn_lock(
        cast(AsyncEngine, session.bind), f"room:{instance}:{issue_key}"
    ) as locked:
        if not locked:
            return None
        return await _reconcile_ticket_room(
            session,
            instance=instance,
            issue_key=issue_key,
            rooms=rooms,
            jira_agent_name=jira_agent_name,
            cards=cards,
            limits=limits or JiraWorkerLimits(),
            now=now or datetime.now(UTC),
            enabled_projects=enabled_projects,
        )


async def _reconcile_ticket_room(
    session: AsyncSession,
    *,
    instance: str,
    issue_key: str,
    rooms: TicketRooms,
    jira_agent_name: str,
    cards: CardUpdater | None,
    limits: JiraWorkerLimits,
    now: datetime,
    enabled_projects: dict[str, list[str]] | None,
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
    if enabled_projects is not None and ticket.project_key not in enabled_projects.get(
        ticket.instance, []
    ):
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
            ticket.thread_cursor = 0
            await session.flush()
            room_id = None
        if room_id is None:
            room_id = await rooms.find_ticket_room(
                instance=ticket.instance, issue_key=ticket.issue_key
            )
            if room_id is not None and await rooms.is_archived(room_id) is None:
                room_id = None
            if room_id is not None:
                if not await admit_room(
                    session,
                    ticket,
                    limits,
                    create=False,
                    now=now,
                    enabled_projects=enabled_projects,
                ):
                    return None
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
                if not await admit_room(
                    session,
                    ticket,
                    limits,
                    create=True,
                    now=now,
                    enabled_projects=enabled_projects,
                ):
                    return None
                await session.commit()
                ticket = await _relock_ticket(
                    session, instance=instance, issue_key=issue_key
                )
                if ticket is None:
                    return None
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
        if not is_terminal_ticket_status(ticket.status) and not ticket.room_claimed:
            if not await admit_room(
                session,
                ticket,
                limits,
                create=False,
                now=now,
                enabled_projects=enabled_projects,
            ):
                return None
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
        try:
            await _apply_description_and_archive(rooms, cards, ticket, room_id, body)
        except TerminalChannelCloseError as exc:
            reasons = dict(ticket.admin_reasons or {})
            reasons[CHANNEL_CLOSE_REASON] = str(exc)
            ticket.admin_reasons = reasons
            await enqueue_admin_card_follow_up(session, ticket)
            await session.flush()
            return room_id
        history = None
        if not is_terminal_ticket_status(ticket.status):
            history_key = (
                f"ticket-channel-history:{ticket.id}:{room_id}:{ticket.card_event_id}"
            )
            history = await session.scalar(
                select(JiraWorkerOutbox).where(
                    JiraWorkerOutbox.instance == ticket.instance,
                    JiraWorkerOutbox.command == RESTORE_HISTORY_COMMAND,
                    JiraWorkerOutbox.status == "pending",
                    JiraWorkerOutbox.payload["ticket_id"].astext == ticket.id,
                    JiraWorkerOutbox.payload["room_id"].astext == room_id,
                )
            )
            if history is None and not await rooms.is_bridged(room_id):
                history = JiraWorkerOutbox(
                    instance=ticket.instance,
                    issue_key=ticket.issue_key,
                    channel="switch",
                    command=RESTORE_HISTORY_COMMAND,
                    command_key=history_key,
                    payload={
                        "ticket_id": ticket.id,
                        "room_id": room_id,
                        "body": body,
                        "after_seq": await session.scalar(
                            select(func.coalesce(func.max(Message.seq), 0)).where(
                                Message.room_id == room_id
                            )
                        ),
                    },
                )
                session.add(history)
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
        reasons = dict(ticket.admin_reasons or {})
        if is_terminal_ticket_status(ticket.status):
            reasons.pop(REPORTER_ACCESS_REASON, None)
            reasons.pop(CHANNEL_CLOSE_REASON, None)
        else:
            try:
                await rooms.ensure_reporter_access(room_id, owner_id)
            except Exception as exc:
                logger.warning(
                    "Jira reporter access failed for %s: %s",
                    ticket.issue_key,
                    exc,
                    exc_info=True,
                )
                reasons[REPORTER_ACCESS_REASON] = str(exc)
            else:
                reasons.pop(REPORTER_ACCESS_REASON, None)
        if reasons != (ticket.admin_reasons or {}):
            ticket.admin_reasons = reasons or None
            await enqueue_admin_card_follow_up(session, ticket)
        if (
            history is not None
            and history.status == "pending"
            and await rooms.is_bridged(room_id)
        ):
            await _restore_ticket_history(session, rooms, ticket, history)
            if cards is not None and (history.payload or {})["body"] != body:
                assert ticket.card_event_id is not None
                await cards.update_card(
                    room_id, event_id=ticket.card_event_id, body=body
                )
        if is_terminal_ticket_status(ticket.status):
            ticket.room_claimed = False
            ticket.queue_reason = None
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
    config: SwitchConfig,
    now: datetime | None = None,
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
                limits=config.jira_worker_limits,
                enabled_projects=config.jira_worker_enabled_projects,
                now=now,
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
    public HTTP API). Rooms use private channels on the default bridge and
    belong to the mapped reporter, or only admins when the reporter is unmapped."""

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
        config = RoomCreateConfig(
            name=name,
            description=description,
            agent_names=[self._config.jira_agent_name],
            channel_type="channel_private",
            invite_default_member=False,
            allow_unbridged_on_channel_failure=True,
            created_by=owner_id,
            owner_id=owner_id,
            read_visibility="private",
            write_visibility="private",
            metadata=ticket_room_marker(instance, issue_key),
        )
        try:
            bridge_id = await self._rooms._resolve_bridge_id(config)
            if bridge_id is not None:
                await self._rooms._require_channel_creation(bridge_id)
        except (ValueError, ChannelCreationUnsupported):
            logger.warning(
                "Jira ticket room %s will remain internal-only", name, exc_info=True
            )
            config.internal_only = True
        result = await self._rooms.create_room(config)
        if not result.room.bridge_id:
            logger.warning(
                "Jira ticket room %s is internal-only; reporter access requires a bridge",
                result.room.id,
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

    async def is_bridged(self, room_id: str) -> bool:
        async with self._sessions() as session:
            room = await session.get(Room, room_id)
            return room is not None and room.external_channel_id is not None

    async def ensure_reporter_access(self, room_id: str, user_id: str | None) -> None:
        async with self._sessions() as session:
            room = await session.get(Room, room_id)
            if room is None:
                raise ValueError(f"Room not found: {room_id}")
            archived = room.archived_at is not None
            if archived and room.external_channel_id is None:
                return
            bridge_id = room.bridge_id
            if bridge_id is None:
                default = await CollaborationBridgeStore().get_default(session)
                bridge_id = default.id if default is not None else None
        if bridge_id is None:
            raise ValueError("No default bridge configured")
        if room.external_channel_id is None:
            await self._rooms.link_bridge_to_room(
                room_id, bridge_id, "channel_private", invite_default_member=False
            )
        async with self._sessions() as session:
            room = await session.get(Room, room_id)
            if (
                room is None
                or room.channel_type != "channel_private"
                or room.external_channel_id is None
            ):
                raise ValueError("Ticket access requires a private channel")
            allowed = set(
                await session.scalars(
                    select(ExternalUser.external_user_id)
                    .join(
                        ExternalUserClaim,
                        ExternalUserClaim.external_user_id == ExternalUser.id,
                    )
                    .join(User, User.id == ExternalUserClaim.user_id)
                    .where(
                        ExternalUser.bridge_id == bridge_id,
                        or_(User.id == user_id, User.role == "admin"),
                    )
                )
            )
        bridge = self._rooms._collab_lifecycle.get(bridge_id)
        if bridge is None:
            raise ValueError(f"Bridge not running: {bridge_id}")
        try:
            await bridge.adapter.retain_channel_members(
                room.external_channel_id, allowed
            )
        except NotImplementedError:
            if user_id is not None and not archived:
                await self._rooms.add_claimed_user_to_room(room_id, user_id)
            raise
        if user_id is not None and not archived:
            await self._rooms.add_claimed_user_to_room(room_id, user_id)

    async def read_context(self, room_id: str, *, after_seq: int) -> dict[str, Any]:
        return await self._protocol.read_context(
            await self._jira_agent_id(), room_id, limit=500, after_seq=after_seq
        )

    async def is_reporter_sender(self, sender: str, user_id: str) -> bool:
        async with self._sessions() as session:
            return (
                await session.scalar(
                    select(ExternalUserClaim.user_id)
                    .join(
                        ExternalUser,
                        ExternalUser.id == ExternalUserClaim.external_user_id,
                    )
                    .join(Client, Client.id == ExternalUser.client_id)
                    .where(
                        Client.matrix_user_id == sender,
                        ExternalUserClaim.user_id == user_id,
                    )
                    .limit(1)
                )
                is not None
            )

    async def is_worker_sender(self, sender: str) -> bool:
        matrix_id = getattr(self, "_worker_matrix_id", None)
        if matrix_id is None:
            async with self._sessions() as session:
                agent = await self._agents.get_by_name(
                    session, self._config.jira_agent_name
                )
                client = (
                    await session.get(Client, agent.client_id)
                    if agent is not None
                    else None
                )
            if client is None:
                return False
            matrix_id = client.matrix_user_id
            self._worker_matrix_id = matrix_id
        return bool(matrix_id) and sender == matrix_id

    async def is_human_sender(self, sender: str) -> bool:
        async with self._sessions() as session:
            return (
                await session.scalar(
                    select(Client.id).where(
                        Client.matrix_user_id == sender, Client.type == "user"
                    )
                )
                is not None
            )

    async def post_notice(self, room_id: str, *, body: str) -> str:
        return await self._protocol.send_message(
            await self._jira_agent_id(), room_id, body
        )

    async def post_question(self, room_id: str, *, thread_id: str, body: str) -> str:
        return await self._protocol.send_message(
            await self._jira_agent_id(), room_id, body, thread_id=thread_id
        )

    async def post_card(self, room_id: str, *, body: str) -> str:
        return await self._protocol.send_message(
            await self._jira_agent_id(), room_id, body
        )

    async def is_archived(self, room_id: str) -> bool | None:
        async with self._sessions() as session:
            room = await session.get(Room, room_id)
            return None if room is None else room.archived_at is not None

    async def set_archived(self, room_id: str, *, archived: bool) -> None:
        if archived:
            try:
                await self._rooms.close_bridged_channel(room_id)
            except Exception as exc:
                raise TerminalChannelCloseError(str(exc)) from exc
            await self._rooms.set_room_archived(room_id, True)
            return
        await self._rooms.set_room_archived(room_id, False)
        try:
            await self._rooms.reopen_bridged_channel(room_id)
        except Exception as exc:
            logger.warning(
                "Jira channel reopen failed for room %s: %s",
                room_id,
                exc,
                exc_info=True,
            )

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
            raise ValueError(f"Jira credentials missing for {instance}")
        return f"{credentials.base_url.rstrip('/')}/browse/{issue_key}"


class SwitchCardUpdater:
    """Refresh the card message and room description while preserving its id."""

    def __init__(self, *, ticket_rooms: SwitchTicketRooms) -> None:
        self._rooms = ticket_rooms

    async def update_card(self, room_id: str, *, event_id: str, body: str) -> None:
        await self._rooms._protocol.edit_message(
            await self._rooms._jira_agent_id(), room_id, event_id, body
        )
        await self._rooms._rooms.update_room(room_id, description=body)
