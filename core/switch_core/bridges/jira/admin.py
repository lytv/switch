"""Step 7: one admin room, one card per ticket, edited in place.

The card keeps its message id. Reasons are added and cleared on that same
message. A disabled project is not read or written.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Protocol, cast
from zoneinfo import ZoneInfo

from sqlalchemy import and_, or_, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from switch_core.bridges.agent.protocol.service import ProtocolService
from switch_core.bridges.jira.activity import (
    is_terminal_ticket_status,
    record_event,
    request_wake,
    ticket_turn_lock,
)
from switch_core.bridges.jira.outbox import enqueue_jira_command
from switch_core.config import JiraWorkerCalendar, SwitchConfig
from switch_core.db.models import (
    JiraWorkerEvent,
    JiraWorkerOutbox,
    JiraWorkerTicket,
    Message,
    Room,
    User,
)
from switch_core.db.stores.agent_store import AgentStore
from switch_core.room_service import RoomCreateConfig, RoomService

logger = logging.getLogger(__name__)

ADMIN_ROOM_MARKER = "jira_worker_admin_room"
ADMIN_ROOM_LOCK = "jira-worker-admin-room"
ADMIN_ROOM_NAME = "Jira admin"
ADMIN_CARD_COMMAND = "upsert_ticket_admin_card"
INTAKE_REASON = "intake_questions"
EXPLICIT_INPUT = "explicit_input"
_DERIVED = (
    "unmapped_reporter",
    "blocked_writes",
    "parked",
    "wait_admin",
    "wait_blocked",
)


class _WaitNotReady(Exception):
    """A wait milestone cannot be sent yet. Leave it unfired."""


class AdminBoard(Protocol):
    async def ensure_room(self) -> str: ...

    async def find_card(self, room_id: str, marker: str) -> str | None: ...

    async def post_card(self, room_id: str, *, body: str) -> str: ...

    async def edit_card(self, room_id: str, event_id: str, *, body: str) -> None: ...

    async def card_body(self, event_id: str) -> str | None: ...

    async def remind_reporter(self, *, room_id: str, body: str) -> None: ...


def card_marker(instance: str, issue_key: str) -> str:
    return f"jira-admin-card:{instance}:{issue_key}"


def _marker_like(marker: str) -> str:
    escaped = marker.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return escaped + "\n%"


def needs_person(reason: str | None) -> bool:
    if not reason:
        return False
    text_value = reason.casefold()
    return (
        "human jira" in text_value
        or "unattributed" in text_value
        or "token limit" in text_value
        or "unknown" in text_value
        or "uncertain" in text_value
    )


def add_work_time(
    start: datetime, hours: float, calendar: JiraWorkerCalendar
) -> datetime:
    """Return the instant `hours` of work time after `start`.

    Time outside the calendar window does not count. A start outside the
    window begins at the next open.
    """
    if start.tzinfo is None:
        raise ValueError("wait start requires a timezone")
    if hours < 0:
        raise ValueError("work hours must be nonnegative")
    zone = ZoneInfo(calendar.timezone)
    cursor = start.astimezone(zone)
    remaining = hours * 3600
    # ponytail: wall-clock windows; a DST spring-forward shortens that day.
    for _ in range(366 * 24):
        cursor = _at_or_after_open(cursor, calendar)
        end = _window_end(cursor, calendar)
        available = (end - cursor).total_seconds()
        if remaining <= available:
            done = cursor + timedelta(seconds=remaining)
            return done.astimezone(start.tzinfo)
        remaining -= available
        cursor = end
    raise ValueError("work calendar has no open time")


def _window_end(local: datetime, calendar: JiraWorkerCalendar) -> datetime:
    if calendar.end_hour == 24:
        return local.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(
            days=1
        )
    return local.replace(hour=calendar.end_hour, minute=0, second=0, microsecond=0)


def _is_open(local: datetime, calendar: JiraWorkerCalendar) -> bool:
    if local.weekday() not in set(calendar.weekdays):
        return False
    start = local.replace(hour=calendar.start_hour, minute=0, second=0, microsecond=0)
    return start <= local < _window_end(local, calendar)


def _at_or_after_open(local: datetime, calendar: JiraWorkerCalendar) -> datetime:
    if _is_open(local, calendar):
        return local
    day = local
    for _ in range(8):
        start = day.replace(hour=calendar.start_hour, minute=0, second=0, microsecond=0)
        if day.weekday() in set(calendar.weekdays) and start >= local:
            return start
        day = (day + timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
    raise ValueError("work calendar has no open day")


def render_admin_card(
    *,
    instance: str,
    issue_key: str,
    summary: str,
    status: str,
    reporter: str,
    issue_url: str,
    room_url: str,
    reasons: dict[str, str],
) -> str:
    lines = [
        card_marker(instance, issue_key),
        f"[{issue_key}] {summary or '(no summary)'}",
        f"Status: {status or '(unknown)'}",
        f"Reporter: {reporter}",
        f"Jira: {issue_url}",
        f"Room: {room_url}",
        "Reasons:",
    ]
    if not reasons:
        lines.append("- none")
    else:
        for key in sorted(reasons):
            lines.append(f"- {key}: {reasons[key]}")
    return "\n".join(lines)


def _issue_url(config: SwitchConfig, instance: str, issue_key: str) -> str:
    credentials = config.jira_worker_credentials.get(instance)
    if credentials is None:
        raise ValueError(f"Jira credentials missing for {instance}")
    return f"{credentials.base_url.rstrip('/')}/browse/{issue_key}"


def _room_url(config: SwitchConfig, room_id: str | None) -> str:
    if not room_id:
        return "(no ticket room)"
    if not config.frontend_base_url:
        return f"switch-room:{room_id}"
    return f"{config.frontend_base_url.rstrip('/')}/rooms/{room_id}"


def _reporter_label(ticket: JiraWorkerTicket) -> str:
    if ticket.reporter_switch_user_id:
        return f"mapped ({ticket.reporter_switch_user_id})"
    return "unmapped reporter"


def _enabled(config: SwitchConfig, ticket: JiraWorkerTicket) -> bool:
    return ticket.project_key in config.jira_worker_enabled_projects.get(
        ticket.instance, []
    )


def _scope(config: SwitchConfig):
    return or_(
        *(
            and_(
                JiraWorkerTicket.instance == instance,
                JiraWorkerTicket.project_key == project_key,
            )
            for instance, project_keys in config.jira_worker_enabled_projects.items()
            for project_key in project_keys
        )
    )


async def find_admin_room(session: AsyncSession) -> Room | None:
    return cast(
        Room | None,
        await session.scalar(
            select(Room)
            .where(Room.metadata_[ADMIN_ROOM_MARKER].astext == "1")
            .order_by(Room.created_at, Room.id)
            .limit(1)
        ),
    )


class SwitchAdminBoard:
    """Production board. The room is found by marker, never by name."""

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

    async def _agent_id(self) -> str:
        async with self._sessions() as session:
            agent = await self._agents.get_by_name(
                session, self._config.jira_agent_name
            )
        if agent is None:
            raise ValueError(
                f"Jira system agent {self._config.jira_agent_name!r} is not provisioned"
            )
        return agent.id

    async def ensure_room(self) -> str:
        async with self._sessions() as session:
            await session.execute(
                text("SELECT pg_advisory_lock(hashtextextended(:key, 0))"),
                {"key": ADMIN_ROOM_LOCK},
            )
            try:
                found = await find_admin_room(session)
                if found is not None:
                    return found.id
                result = await self._rooms.create_room(
                    RoomCreateConfig(
                        name=ADMIN_ROOM_NAME,
                        description="Jira worker alerts. One card per ticket.",
                        agent_names=[self._config.jira_agent_name],
                        internal_only=True,
                        owner_id=None,
                        read_visibility="private",
                        write_visibility="private",
                        metadata={ADMIN_ROOM_MARKER: "1"},
                    )
                )
                return result.room.id
            finally:
                await session.execute(
                    text("SELECT pg_advisory_unlock(hashtextextended(:key, 0))"),
                    {"key": ADMIN_ROOM_LOCK},
                )
                await session.commit()

    async def find_card(self, room_id: str, marker: str) -> str | None:
        async with self._sessions() as session:
            return cast(
                str | None,
                await session.scalar(
                    select(Message.transport_event_id)
                    .where(
                        Message.room_id == room_id,
                        Message.body.like(_marker_like(marker), escape="\\"),
                    )
                    .order_by(Message.seq)
                    .limit(1)
                ),
            )

    async def post_card(self, room_id: str, *, body: str) -> str:
        return await self._protocol.send_message(await self._agent_id(), room_id, body)

    async def edit_card(self, room_id: str, event_id: str, *, body: str) -> None:
        await self._protocol.edit_message(
            await self._agent_id(), room_id, event_id, body
        )

    async def card_body(self, event_id: str) -> str | None:
        async with self._sessions() as session:
            return await session.scalar(
                select(Message.body).where(Message.transport_event_id == event_id)
            )

    async def remind_reporter(self, *, room_id: str, body: str) -> None:
        await self._protocol.send_message(await self._agent_id(), room_id, body)


async def reconcile_admin_cards(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    engine: AsyncEngine,
    board: AdminBoard,
    config: SwitchConfig,
    now: datetime | None = None,
) -> int:
    """Post or edit one card per ticket that currently needs one."""
    if not any(config.jira_worker_enabled_projects.values()):
        return 0
    moment = now or datetime.now(UTC)
    async with session_factory() as session:
        ids = list(
            (
                await session.scalars(
                    select(JiraWorkerTicket.id)
                    .where(_scope(config))
                    .order_by(JiraWorkerTicket.first_seen_at, JiraWorkerTicket.id)
                )
            ).all()
        )
    done = 0
    for ticket_id in ids:
        if await sync_admin_card(
            session_factory,
            engine=engine,
            board=board,
            config=config,
            ticket_id=ticket_id,
            now=moment,
        ):
            done += 1
    return done


async def sync_admin_card(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    engine: AsyncEngine,
    board: AdminBoard,
    config: SwitchConfig,
    ticket_id: str,
    now: datetime | None = None,
) -> bool:
    async with ticket_turn_lock(engine, f"admin-card:{ticket_id}") as locked:
        if not locked:
            return False
        return await _sync_admin_card(
            session_factory,
            board=board,
            config=config,
            ticket_id=ticket_id,
            now=now or datetime.now(UTC),
        )


async def set_admin_card_reason(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    engine: AsyncEngine,
    board: AdminBoard,
    config: SwitchConfig,
    instance: str,
    issue_key: str,
    reason: str,
    detail: str | None,
    now: datetime | None = None,
) -> None:
    """Raise or clear one card reason. Step 8 calls this for intake questions.

    `detail` None clears the reason. The card stays.
    """
    if not reason.strip():
        raise ValueError("admin card reason is required")
    async with session_factory() as session:
        ticket = await session.scalar(
            select(JiraWorkerTicket)
            .where(
                JiraWorkerTicket.instance == instance,
                JiraWorkerTicket.issue_key == issue_key,
            )
            .with_for_update()
        )
        if ticket is None:
            raise ValueError(f"ticket not found: {issue_key}")
        if not _enabled(config, ticket):
            return
        reasons = dict(ticket.admin_reasons or {})
        if detail is None:
            reasons.pop(reason, None)
        else:
            reasons[reason] = detail
        ticket.admin_reasons = reasons or None
        ticket_id = ticket.id
        await session.commit()
    await sync_admin_card(
        session_factory,
        engine=engine,
        board=board,
        config=config,
        ticket_id=ticket_id,
        now=now,
    )


async def accept_ignored_input(
    session: AsyncSession,
    *,
    event_id: str,
    admin_user_id: str,
    enabled_projects: dict[str, list[str]],
) -> str:
    """Record a new explicit input event. The ignored_input row is not rewritten."""
    admin = await session.get(User, admin_user_id)
    if admin is None or admin.role != "admin":
        raise PermissionError("Only an admin may accept ignored input")
    event = await session.get(JiraWorkerEvent, event_id)
    if event is None or event.event_kind != "ignored_input":
        raise ValueError("ignored_input event not found")
    if event.project_key not in enabled_projects.get(event.instance, []):
        raise ValueError("project is not enabled")
    key = f"accept:{event.id}"
    payload = {
        "accepted_from": event.id,
        "accepted_by": admin_user_id,
        "untrusted_data": (event.payload or {}).get("untrusted_data"),
    }
    new_id = await record_event(
        session,
        instance=event.instance,
        idempotency_key=key,
        issue_key=event.issue_key,
        project_key=event.project_key,
        event_kind=EXPLICIT_INPUT,
        webhook_event="worker:explicit_input",
        payload=payload,
    )
    if new_id is None:
        new_id = await session.scalar(
            select(JiraWorkerEvent.id).where(
                JiraWorkerEvent.instance == event.instance,
                JiraWorkerEvent.idempotency_key == key,
            )
        )
    if new_id is None:
        raise RuntimeError("accepted input event was not stored")
    ticket = await session.scalar(
        select(JiraWorkerTicket)
        .where(
            JiraWorkerTicket.instance == event.instance,
            JiraWorkerTicket.issue_key == event.issue_key,
        )
        .with_for_update()
    )
    if ticket is not None:
        request_wake(ticket)
    return new_id


async def _sync_admin_card(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    board: AdminBoard,
    config: SwitchConfig,
    ticket_id: str,
    now: datetime,
) -> bool:
    async with session_factory() as session:
        ticket = await session.scalar(
            select(JiraWorkerTicket)
            .where(JiraWorkerTicket.id == ticket_id)
            .with_for_update()
        )
        if ticket is None or not _enabled(config, ticket):
            return False
        await _apply_state(session, ticket, config, now, board)
        reasons = {
            str(key): str(value) for key, value in (ticket.admin_reasons or {}).items()
        }
        if not reasons and ticket.admin_card_event_id is None:
            await _consume_follow_ups(session, ticket.id)
            await session.commit()
            return True
        room_id = await board.ensure_room()
        marker = card_marker(ticket.instance, ticket.issue_key)
        body = render_admin_card(
            instance=ticket.instance,
            issue_key=ticket.issue_key,
            summary=ticket.summary,
            status=ticket.status,
            reporter=_reporter_label(ticket),
            issue_url=_issue_url(config, ticket.instance, ticket.issue_key),
            room_url=_room_url(config, ticket.room_id),
            reasons=reasons,
        )
        event_id = ticket.admin_card_event_id or await board.find_card(room_id, marker)
        if event_id is None:
            event_id = await board.post_card(room_id, body=body)
        else:
            current = await board.card_body(event_id)
            if current != body:
                await board.edit_card(room_id, event_id, body=body)
        ticket.admin_card_event_id = event_id
        await _consume_follow_ups(session, ticket.id)
        await session.commit()
    return True


async def _apply_state(
    session: AsyncSession,
    ticket: JiraWorkerTicket,
    config: SwitchConfig,
    now: datetime,
    board: AdminBoard,
) -> None:
    reasons = {
        str(key): str(value)
        for key, value in (ticket.admin_reasons or {}).items()
        if key not in _DERIVED
    }
    if ticket.reporter_switch_user_id is None and not is_terminal_ticket_status(
        ticket.status
    ):
        reasons["unmapped_reporter"] = "Reporter has no Switch user"
    failed = await session.scalar(
        select(JiraWorkerOutbox.id)
        .where(
            JiraWorkerOutbox.instance == ticket.instance,
            JiraWorkerOutbox.issue_key == ticket.issue_key,
            JiraWorkerOutbox.channel == "jira",
            JiraWorkerOutbox.status == "failed",
            JiraWorkerOutbox.attempts >= 3,
            JiraWorkerOutbox.payload["failure_of"].astext.is_(None),
        )
        .limit(1)
    )
    if failed:
        reasons["blocked_writes"] = "Jira write failed 3 times"
    if needs_person(ticket.worker_parked_reason):
        reasons["parked"] = ticket.worker_parked_reason or "parked"
    await _apply_wait(session, ticket, config, now, board, reasons)
    ticket.admin_reasons = reasons or None


async def _apply_wait(
    session: AsyncSession,
    ticket: JiraWorkerTicket,
    config: SwitchConfig,
    now: datetime,
    board: AdminBoard,
    reasons: dict[str, str],
) -> None:
    waiting = ticket.status in (
        config.jira_worker_statuses.waiting_for_input,
        config.jira_worker_statuses.waiting_for_approval,
    )
    if not waiting or is_terminal_ticket_status(ticket.status):
        ticket.wait_anchor = None
        ticket.wait_fired = None
        return
    if ticket.wait_anchor is None:
        ticket.wait_anchor = now
        ticket.wait_fired = {}
    anchor = ticket.wait_anchor
    if anchor.tzinfo is None:
        anchor = anchor.replace(tzinfo=UTC)
    fired = dict(ticket.wait_fired or {})
    calendar = config.jira_worker_calendar
    if "reminder" not in fired and now >= add_work_time(
        anchor, config.jira_worker_wait_reminder_hours, calendar
    ):
        try:
            await _send_reminder(session, ticket, config, board, anchor)
        except _WaitNotReady:
            logger.warning("Jira wait reminder for %s is not ready", ticket.issue_key)
        else:
            fired["reminder"] = now.isoformat()
    if "admin" not in fired and now >= add_work_time(
        anchor, config.jira_worker_wait_admin_hours, calendar
    ):
        reasons["wait_admin"] = "No reply after the admin wait"
        fired["admin"] = now.isoformat()
    elif "admin" in fired:
        reasons["wait_admin"] = "No reply after the admin wait"
    if "blocked" not in fired and now >= add_work_time(
        anchor, config.jira_worker_wait_blocked_hours, calendar
    ):
        try:
            await _enqueue_blocked(session, ticket, config, anchor)
        except _WaitNotReady:
            logger.warning("Jira wait block for %s is not ready", ticket.issue_key)
        else:
            reasons["wait_blocked"] = "No reply after one work day"
            fired["blocked"] = now.isoformat()
    elif "blocked" in fired:
        reasons["wait_blocked"] = "No reply after one work day"
    ticket.wait_fired = fired


async def _send_reminder(
    session: AsyncSession,
    ticket: JiraWorkerTicket,
    config: SwitchConfig,
    board: AdminBoard,
    anchor: datetime,
) -> None:
    body = f"Reminder: the worker is still waiting for a reply on {ticket.issue_key}."
    if ticket.wait_channel == "jira_comments":
        if not ticket.jira_read_updated or not ticket.jira_read_changelog_id:
            raise _WaitNotReady()
        await enqueue_jira_command(
            session,
            config=config,
            ticket=ticket,
            command_key=f"wait-reminder:{ticket.id}:{anchor.isoformat()}",
            command="comment",
            data=body,
            based_updated=ticket.jira_read_updated,
            based_changelog_id=ticket.jira_read_changelog_id,
        )
        return
    if not ticket.room_id:
        raise _WaitNotReady()
    await board.remind_reporter(room_id=ticket.room_id, body=body)


async def _enqueue_blocked(
    session: AsyncSession,
    ticket: JiraWorkerTicket,
    config: SwitchConfig,
    anchor: datetime,
) -> None:
    if not ticket.jira_read_updated or not ticket.jira_read_changelog_id:
        raise _WaitNotReady()
    command_id = await enqueue_jira_command(
        session,
        config=config,
        ticket=ticket,
        command_key=f"wait-blocked:{ticket.id}:{anchor.isoformat()}",
        command="transition",
        data=config.jira_worker_statuses.blocked,
        based_updated=ticket.jira_read_updated,
        based_changelog_id=ticket.jira_read_changelog_id,
    )
    if command_id is None:
        raise _WaitNotReady()


async def _consume_follow_ups(session: AsyncSession, ticket_id: str) -> None:
    rows = (
        await session.scalars(
            select(JiraWorkerOutbox)
            .where(
                JiraWorkerOutbox.channel == "switch",
                JiraWorkerOutbox.command == ADMIN_CARD_COMMAND,
                JiraWorkerOutbox.status == "pending",
                JiraWorkerOutbox.payload["ticket_id"].astext == ticket_id,
            )
            .with_for_update(skip_locked=True)
        )
    ).all()
    for row in rows:
        row.status = "done"
