"""Step 7: one admin room, one card per ticket, edited in place.

The card keeps its message id. Reasons are added and cleared on that same
message. A disabled project is not read or written.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Protocol, cast
from zoneinfo import ZoneInfo

from sqlalchemy import JSON, and_, delete, func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from switch_core.bridges.agent.protocol.service import ProtocolService
from switch_core.bridges.jira.activity import (
    EXPLICIT_INPUT,
    is_terminal_ticket_status,
    record_event,
    request_wake,
    ticket_turn_lock,
)
from switch_core.bridges.jira.observe import _stamp, comment_actor
from switch_core.bridges.jira.outbox import (
    INTERRUPTED_TURN,
    STAMP_MISMATCH,
    TOKEN_LIMIT,
    TURN_FAILED,
    WAIT_COMMAND_PREFIXES,
    HumanChange,
    check_jira_version,
    enqueue_jira_command,
    needs_person,
    read_jira_version,
)
from switch_core.config import JiraWorkerCalendar, SwitchConfig
from switch_core.db.models import (
    JiraWorkerEvent,
    JiraWorkerJob,
    JiraWorkerOutbox,
    JiraWorkerTicket,
    Message,
    Room,
    User,
)
from switch_core.db.stores.agent_store import AgentStore
from switch_core.room_service import RoomCreateConfig, RoomService

if TYPE_CHECKING:
    from switch_core.bridges.jira.worker import JiraPollClient

logger = logging.getLogger(__name__)

WaitRefresh = Callable[[str], Awaitable[bool]]

ADMIN_ROOM_MARKER = "jira_worker_admin_room"
ADMIN_ROOM_LOCK = "jira-worker-admin-room"
ADMIN_ROOM_NAME = "Jira admin"
ADMIN_CARD_COMMAND = "upsert_ticket_admin_card"
ADMIN_ALERT_COMMAND = "admin_reason_alert"
INTAKE_REASON = "intake_questions"
INTAKE_LIMIT_DETAIL = (
    "Intake needs more than 5 questions; an admin must resolve the missing requirements"
)
WAIT_REMINDER_HOURS = 2
WAIT_ADMIN_HOURS = 4
_WAIT_INPUT_KIND = {"switch": "thread_message", "jira_comments": "jira_comment"}
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

    async def post_alert(self, room_id: str, *, body: str) -> None: ...


def card_marker(instance: str, issue_key: str) -> str:
    return f"jira-admin-card:{instance}:{issue_key}"


def _alert_key(ticket_id: str, reason: str) -> str:
    return f"admin-alert:{ticket_id}:{reason}"


def alert_line(issue_key: str, reason: str, outbox_id: str) -> str:
    return f"{issue_key}: {reason}. Card updated. {outbox_id}"


async def _admin_owner_id(session: AsyncSession, config: SwitchConfig) -> str:
    owner = await session.scalar(
        select(User).where(
            User.email == config.gateway_admin_email,
            User.role == "admin",
        )
    )
    if owner is None:
        raise ValueError(f"Admin user {config.gateway_admin_email!r} is missing")
    return owner.id


def _marker_like(marker: str) -> str:
    escaped = marker.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return escaped + "\n%"


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
                owner_id = await _admin_owner_id(session, self._config)
                found = await find_admin_room(session)
                if found is not None:
                    if found.owner_id != owner_id:
                        found.owner_id = owner_id
                    return found.id
                result = await self._rooms.create_room(
                    RoomCreateConfig(
                        name=ADMIN_ROOM_NAME,
                        description="Jira worker alerts. One card per ticket.",
                        agent_names=[self._config.jira_agent_name],
                        created_by=owner_id,
                        owner_id=owner_id,
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

    async def post_alert(self, room_id: str, *, body: str) -> None:
        await self._protocol.send_message(await self._agent_id(), room_id, body)


async def enqueue_admin_card_follow_up(
    session: AsyncSession, ticket: JiraWorkerTicket
) -> None:
    if ticket.admin_card_event_id is None:
        return
    pending = await session.scalar(
        select(JiraWorkerOutbox.id)
        .where(
            JiraWorkerOutbox.channel == "switch",
            JiraWorkerOutbox.command == ADMIN_CARD_COMMAND,
            JiraWorkerOutbox.status == "pending",
            JiraWorkerOutbox.payload["ticket_id"].astext == ticket.id,
        )
        .limit(1)
    )
    if pending is not None:
        return
    session.add(
        JiraWorkerOutbox(
            channel="switch",
            command=ADMIN_CARD_COMMAND,
            instance=ticket.instance,
            issue_key=ticket.issue_key,
            payload={"ticket_id": ticket.id},
        )
    )
    await session.flush()


async def reconcile_admin_cards(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    engine: AsyncEngine,
    board: AdminBoard,
    config: SwitchConfig,
    now: datetime | None = None,
    before_wait_send: WaitRefresh | None = None,
) -> int:
    """Post or edit one card per ticket that currently needs one."""
    if not any(config.jira_worker_enabled_projects.values()):
        return 0
    moment = now or datetime.now(UTC)
    async with session_factory() as session:
        pending_follow_ups = select(JiraWorkerOutbox.payload["ticket_id"].astext).where(
            JiraWorkerOutbox.channel == "switch",
            JiraWorkerOutbox.command == ADMIN_CARD_COMMAND,
            JiraWorkerOutbox.status == "pending",
        )
        ids = list(
            (
                await session.scalars(
                    select(JiraWorkerTicket.id)
                    .where(
                        _scope(config),
                        or_(
                            func.lower(func.trim(JiraWorkerTicket.status)).not_in(
                                ("done", "cancelled", "canceled")
                            ),
                            and_(
                                JiraWorkerTicket.admin_reasons.is_not(None),
                                JiraWorkerTicket.admin_reasons.not_in((JSON.NULL, {})),
                            ),
                            JiraWorkerTicket.id.in_(pending_follow_ups),
                        ),
                    )
                    .order_by(JiraWorkerTicket.first_seen_at, JiraWorkerTicket.id)
                )
            ).all()
        )
    done = 0
    for ticket_id in ids:
        try:
            changed = await sync_admin_card(
                session_factory,
                engine=engine,
                board=board,
                config=config,
                ticket_id=ticket_id,
                now=moment,
                before_wait_send=before_wait_send,
            )
        except Exception:
            logger.exception("Jira admin card sync failed for %s", ticket_id)
            continue
        if changed:
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
    before_wait_send: WaitRefresh | None = None,
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
            before_wait_send=before_wait_send,
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
        previous = set(ticket.admin_reasons or {})
        ticket.admin_reasons = reasons or None
        await _record_reason_alerts(session, ticket, previous)
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


def _release_retry_matches(
    row: JiraWorkerOutbox,
    version: dict,
    worker_account: str,
) -> bool:
    payload = row.payload or {}
    if (
        row.status not in ("parked", "pending", "claimed")
        or row.command == "fields"
        or payload.get("failure_of")
        or payload.get("based_status") != version["fields"]["status"]["name"]
        or payload.get("based_context")
        != {
            "summary": version["fields"]["summary"],
            "description": version["fields"].get("description"),
        }
        or not payload.get("based_updated")
        or "based_changelog_id" not in payload
        or not isinstance(payload.get("based_comment_ids"), list)
        or set(payload["based_comment_ids"])
        - {str(c["id"]) for c in version["comments"]}
    ):
        return False
    if any(
        comment_actor(comment) != worker_account
        and (
            str(comment["id"]) not in payload["based_comment_ids"]
            or _stamp(comment["updated"]) > _stamp(payload["based_updated"])
        )
        for comment in version["comments"]
    ):
        return False
    try:
        check_jira_version(version, payload, worker_account)
    except HumanChange as exc:
        return str(exc) == STAMP_MISMATCH
    return True


async def release_parked_ticket(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    config: SwitchConfig,
    client: JiraPollClient,
    board: AdminBoard,
    ticket_id: str,
    request_id: str,
    admin_user_id: str,
    now: datetime,
) -> JiraWorkerEvent:
    """Accept a fresh baseline once, without replaying unknown outcomes."""
    request_id = str(uuid.UUID(request_id))
    if now.tzinfo is None:
        raise ValueError("release time requires a timezone")
    engine = cast(AsyncEngine, session_factory.kw["bind"])
    async with ticket_turn_lock(engine, ticket_id) as locked:
        if not locked:
            raise BlockingIOError("Ticket turn is active; retry release later")
        async with session_factory() as session:
            admin = await session.get(User, admin_user_id)
            if admin is None or admin.role != "admin":
                raise PermissionError("Only an admin may release a parked ticket")
            ticket = await session.get(JiraWorkerTicket, ticket_id)
            if ticket is None:
                raise ValueError("ticket not found")
            if not _enabled(config, ticket):
                raise ValueError("project is not enabled")
            for lock_key, activity in (
                (f"jira-outbox:{ticket.instance}:{ticket.issue_key}", "send"),
                (f"jira-turn:admin-card:{ticket_id}", "admin card update"),
            ):
                locked = await session.scalar(
                    text("SELECT pg_try_advisory_xact_lock(hashtextextended(:key, 0))"),
                    {"key": lock_key},
                )
                if not locked:
                    raise BlockingIOError(
                        f"Ticket {activity} is active; retry release later"
                    )
            ticket = await session.get(
                JiraWorkerTicket,
                ticket_id,
                with_for_update=True,
                populate_existing=True,
            )
            assert ticket is not None
            key = f"admin-release:{ticket.id}:{request_id}"
            receipt = await session.scalar(
                select(JiraWorkerEvent).where(
                    JiraWorkerEvent.instance == ticket.instance,
                    JiraWorkerEvent.idempotency_key == key,
                )
            )
            if receipt is not None:
                if (
                    receipt.event_kind != "admin_release"
                    or receipt.issue_key != ticket.issue_key
                ):
                    raise ValueError("release request key belongs to another event")
                if (receipt.payload or {}).get("released_by") != admin_user_id:
                    raise ValueError("release request belongs to another admin")
            else:
                reason = ticket.worker_parked_reason
                retried: list[str] = []
                replanned = False
                if reason:
                    if reason in (TOKEN_LIMIT, "Jira ticket completed or cancelled"):
                        raise ValueError(
                            "Terminal or token-limit tickets cannot be released"
                        )
                    if (
                        ticket.tokens_used
                        >= config.jira_worker_limits.tokens_per_ticket
                    ):
                        raise ValueError("Ticket token limit still applies")
                    last_release = (
                        select(func.max(JiraWorkerEvent.received_at))
                        .where(
                            JiraWorkerEvent.instance == ticket.instance,
                            JiraWorkerEvent.issue_key == ticket.issue_key,
                            JiraWorkerEvent.event_kind == "admin_release",
                        )
                        .scalar_subquery()
                    )
                    unknown_turn = await session.scalar(
                        select(JiraWorkerJob.id)
                        .where(
                            JiraWorkerJob.payload["ticket_id"].astext == ticket.id,
                            JiraWorkerJob.status == "error",
                            JiraWorkerJob.error.in_((INTERRUPTED_TURN, TURN_FAILED)),
                            or_(
                                last_release.is_(None),
                                func.coalesce(
                                    JiraWorkerJob.completed_at,
                                    JiraWorkerJob.claimed_at,
                                    JiraWorkerJob.due_at,
                                )
                                > last_release,
                            ),
                        )
                        .limit(1)
                    )
                    if reason in (INTERRUPTED_TURN, TURN_FAILED) or unknown_turn:
                        raise ValueError(
                            "Unknown turn outcome and token usage require reconciliation"
                        )
                    state = ticket.intake_state or {}
                    cycle = state.get("cycle", 0)
                    prefix = f"intake:{ticket.id}:{cycle}:"
                    intake_keys = []
                    if state.get("phase") == "approval":
                        intake_keys = [prefix + "summary", prefix + "approval"]
                    elif state.get("phase") == "question":
                        count = state.get("questions", 0)
                        intake_keys = [
                            prefix + f"input:{count}",
                            prefix + f"question:{count}",
                        ]
                    rows = list(
                        (
                            await session.scalars(
                                select(JiraWorkerOutbox)
                                .where(
                                    JiraWorkerOutbox.instance == ticket.instance,
                                    JiraWorkerOutbox.issue_key == ticket.issue_key,
                                    or_(
                                        JiraWorkerOutbox.status.in_(
                                            (
                                                "pending",
                                                "claimed",
                                                "sending",
                                                "parked",
                                                "failed",
                                                "uncertain",
                                            )
                                        ),
                                        JiraWorkerOutbox.command_key.in_(intake_keys),
                                    ),
                                    or_(
                                        JiraWorkerOutbox.channel == "jira",
                                        JiraWorkerOutbox.command == "intake_question",
                                    ),
                                )
                                .with_for_update()
                            )
                        ).all()
                    )
                    if any(row.status in ("sending", "uncertain") for row in rows):
                        raise ValueError(
                            "Unknown prior send outcome requires reconciliation; no replay"
                        )
                    version = await read_jira_version(client, ticket.issue_key)
                    if ticket.jira_read_updated and _stamp(version["updated"]) < _stamp(
                        ticket.jira_read_updated
                    ):
                        raise ValueError(
                            "Jira version is older than the accepted baseline"
                        )
                    status = version["fields"]["status"]["name"]
                    if (
                        is_terminal_ticket_status(status)
                        or version["fields"]["status"]["statusCategory"]["key"]
                        == "done"
                    ):
                        raise ValueError("Jira ticket is completed or cancelled")
                    reporter = version["fields"].get("reporter") or {}
                    if (
                        version["fields"]["project"]["key"] != ticket.project_key
                        or reporter.get("accountId", "") != ticket.reporter_account_id
                    ):
                        raise ValueError(
                            "Jira project or reporter changed; refresh ticket before release"
                        )
                    worker_account = (
                        await client.request("GET", "/rest/api/3/myself")
                    ).json()["accountId"]
                    if not isinstance(worker_account, str) or not worker_account:
                        raise ValueError("Jira worker accountId is required")
                    planned = [
                        row
                        for row in rows
                        if row.status != "done"
                        and not (row.payload or {}).get("failure_of")
                        and not (row.command_key or "").startswith(
                            WAIT_COMMAND_PREFIXES
                        )
                    ]
                    retry = bool(planned) and all(
                        _release_retry_matches(row, version, worker_account)
                        for row in planned
                    )
                    for row in planned:
                        predecessor = (row.payload or {}).get("after_command")
                        if predecessor:
                            previous_command = await session.get(
                                JiraWorkerOutbox, predecessor
                            )
                            if previous_command is None or (
                                previous_command.status != "done"
                                and previous_command not in planned
                            ):
                                retry = False
                    base_stamp = (
                        _stamp(ticket.jira_read_updated)
                        if ticket.jira_read_updated
                        else ticket.last_event_at
                    )
                    for comment in version["comments"]:
                        new = (
                            str(comment["id"]) not in ticket.jira_seen_comment_ids
                            if ticket.jira_seen_comment_ids is not None
                            else _stamp(comment["created"]) > base_stamp
                        )
                        if (
                            ticket.reporter_account_id != worker_account
                            and comment_actor(comment) == ticket.reporter_account_id
                            and (new or _stamp(comment["updated"]) > base_stamp)
                        ):
                            await record_event(
                                session,
                                instance=ticket.instance,
                                issue_key=ticket.issue_key,
                                project_key=ticket.project_key,
                                idempotency_key=f"jira-comment:{ticket.issue_key}:{comment['id']}:{comment['updated']}",
                                event_kind="jira_comment"
                                if ticket.wait_channel == "jira_comments"
                                else "ignored_input",
                                webhook_event="worker:jira_comment",
                                payload={
                                    "untrusted_data": comment,
                                    "reporter_input": ticket.wait_channel
                                    == "jira_comments",
                                    **(
                                        {"wrong_place_type": "jira_answer"}
                                        if ticket.wait_channel != "jira_comments"
                                        else {}
                                    ),
                                },
                            )
                    ticket.agent_generation += 1
                    ticket.status = status
                    ticket.summary = version["fields"]["summary"]
                    ticket.description = version["fields"].get("description")
                    ticket.jira_worker_account_id = worker_account
                    ticket.jira_read_updated = version["updated"]
                    ticket.jira_read_changelog_id = version["changelog_id"]
                    ticket.jira_seen_comment_ids = sorted(
                        str(c["id"]) for c in version["comments"]
                    )
                    remaining = planned if retry else []
                    if not retry:
                        by_key = {row.command_key: row for row in rows}
                        predecessor = None
                        question = by_key.get(
                            prefix + f"question:{state.get('questions', 0)}"
                        )
                        if (
                            state.get("phase") == "question"
                            and question is not None
                            and question.status == "done"
                        ):
                            intake_keys = []
                        for command_key in intake_keys:
                            row = by_key.get(command_key)
                            if row is not None and row.status == "done":
                                if (
                                    row.command != "transition"
                                    or (row.payload or {}).get("data") == status
                                ):
                                    continue
                                row = JiraWorkerOutbox(
                                    channel=row.channel,
                                    instance=ticket.instance,
                                    issue_key=ticket.issue_key,
                                    command_key=f"{command_key}:release:{request_id}",
                                    command=row.command,
                                    payload={"data": (row.payload or {})["data"]},
                                )
                                session.add(row)
                                await session.flush()
                            if (
                                state.get("phase") == "approval"
                                and status
                                == config.jira_worker_statuses.waiting_for_approval
                            ):
                                continue
                            if row is None:
                                if state.get("phase") == "approval":
                                    raise ValueError(
                                        "Intake command is missing; reconcile before release"
                                    )
                                continue
                            if (
                                row.command == "transition"
                                and (row.payload or {}).get("data") == status
                                and predecessor is None
                            ):
                                continue
                            row.payload = {
                                **(row.payload or {}),
                                "intake_generation": ticket.agent_generation,
                                "after_command": predecessor.id
                                if predecessor
                                else None,
                            }
                            if predecessor is not None:
                                row.created_at = predecessor.created_at + timedelta(
                                    microseconds=1
                                )
                            predecessor = row
                            remaining.append(row)
                        replanned = True
                    for row in rows:
                        if row.status != "done" and row not in remaining:
                            row.status = "cancelled"
                            row.error = "Admin release cancelled obsolete or stale work"
                    for row in remaining:
                        row.payload = {
                            **(row.payload or {}),
                            "based_updated": version["updated"],
                            "based_status": status,
                            "based_context": {
                                "summary": ticket.summary,
                                "description": ticket.description,
                            },
                            "based_changelog_id": version["changelog_id"],
                            "based_comment_ids": ticket.jira_seen_comment_ids,
                            "reporter_account_id": ticket.reporter_account_id,
                            **(
                                {"intake_generation": ticket.agent_generation}
                                if "intake_generation" in (row.payload or {})
                                else {}
                            ),
                        }
                        row.status = "pending"
                        row.error = None
                        row.due_at = now
                        if retry:
                            retried.append(row.id)
                    ticket.worker_parked_reason = None
                    ticket.agent_state = "waiting"
                    ticket.wake_at = None
                    waiting = status in (
                        config.jira_worker_statuses.waiting_for_input,
                        config.jira_worker_statuses.waiting_for_approval,
                    )
                    ticket.wait_anchor = now if waiting else None
                    ticket.wait_fired = {} if waiting else None
                    request_wake(ticket)
                    previous = set(ticket.admin_reasons or {})
                    reasons = dict(ticket.admin_reasons or {})
                    reasons.pop("parked", None)
                    reasons.pop("blocked_writes", None)
                    ticket.admin_reasons = reasons or None
                    await _record_reason_alerts(session, ticket, previous)
                    await enqueue_admin_card_follow_up(session, ticket)
                receipt = JiraWorkerEvent(
                    instance=ticket.instance,
                    idempotency_key=key,
                    issue_key=ticket.issue_key,
                    project_key=ticket.project_key,
                    event_kind="admin_release",
                    webhook_event="worker:admin_release",
                    consumed=True,
                    payload={
                        "released_by": admin_user_id,
                        "released_at": now.isoformat(),
                        "previous_reason": reason,
                        "retried_command_ids": retried,
                        "replanned": replanned,
                    },
                )
                session.add(receipt)
                await session.commit()
    await sync_admin_card(
        session_factory,
        engine=engine,
        board=board,
        config=config,
        ticket_id=ticket_id,
        now=now,
    )
    return receipt


async def accept_ignored_input(
    session: AsyncSession,
    *,
    event_id: str,
    admin_user_id: str,
    config: SwitchConfig,
) -> str:
    """Record a new explicit input event. The ignored_input row is not rewritten."""
    admin = await session.get(User, admin_user_id)
    if admin is None or admin.role != "admin":
        raise PermissionError("Only an admin may accept ignored input")
    event = await session.get(JiraWorkerEvent, event_id)
    if event is None or event.event_kind != "ignored_input":
        raise ValueError("ignored_input event not found")
    if event.project_key not in config.jira_worker_enabled_projects.get(
        event.instance, []
    ):
        raise ValueError("project is not enabled")
    key = f"accept:{event.id}"
    payload = {
        "accepted_from": event.id,
        "accepted_by": admin_user_id,
        "untrusted_data": (event.payload or {}).get("untrusted_data"),
        "reporter_input": True,
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
        existing_id = await session.scalar(
            select(JiraWorkerEvent.id).where(
                JiraWorkerEvent.instance == event.instance,
                JiraWorkerEvent.idempotency_key == key,
            )
        )
        if existing_id is None:
            raise RuntimeError("accepted input event was not stored")
        return existing_id
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
        if ticket.status == config.jira_worker_statuses.waiting_for_input:
            await _cancel_wait_commands(session, ticket)
    return new_id


async def _record_reason_alerts(
    session: AsyncSession,
    ticket: JiraWorkerTicket,
    previous: set[str],
) -> None:
    current = set(ticket.admin_reasons or {})
    for reason in previous - current:
        await session.execute(
            delete(JiraWorkerOutbox).where(
                JiraWorkerOutbox.instance == ticket.instance,
                JiraWorkerOutbox.command_key == _alert_key(ticket.id, reason),
            )
        )
    for reason in sorted(current - previous):
        session.add(
            JiraWorkerOutbox(
                channel="switch",
                command=ADMIN_ALERT_COMMAND,
                instance=ticket.instance,
                issue_key=ticket.issue_key,
                command_key=_alert_key(ticket.id, reason),
                status="pending",
                payload={"reason": reason, "issue_key": ticket.issue_key},
            )
        )


async def _deliver_reason_alerts(
    session_factory: async_sessionmaker[AsyncSession],
    board: AdminBoard,
    instance: str,
    issue_key: str,
    room_id: str,
) -> None:
    async with session_factory() as session:
        rows = list(
            (
                await session.scalars(
                    select(JiraWorkerOutbox)
                    .where(
                        JiraWorkerOutbox.instance == instance,
                        JiraWorkerOutbox.issue_key == issue_key,
                        JiraWorkerOutbox.command == ADMIN_ALERT_COMMAND,
                        JiraWorkerOutbox.status == "pending",
                    )
                    .with_for_update()
                )
            ).all()
        )
        pending = [
            (
                row.id,
                alert_line(
                    issue_key, str((row.payload or {}).get("reason", "")), row.id
                ),
            )
            for row in rows
        ]
    for outbox_id, line in pending:
        async with session_factory() as session:
            posted = await session.scalar(
                select(Message.id).where(
                    Message.room_id == room_id, Message.body == line
                )
            )
        if posted is None:
            await board.post_alert(room_id, body=line)
        async with session_factory() as session:
            row = await session.get(JiraWorkerOutbox, outbox_id, with_for_update=True)
            if row is not None and row.status == "pending":
                row.status = "done"
                await session.commit()


async def _sync_admin_card(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    board: AdminBoard,
    config: SwitchConfig,
    ticket_id: str,
    now: datetime,
    before_wait_send: WaitRefresh | None,
) -> bool:
    reminders: list[tuple[str, str]] = []
    needs_card = False
    async with session_factory() as session:
        ticket = await session.scalar(
            select(JiraWorkerTicket)
            .where(JiraWorkerTicket.id == ticket_id)
            .with_for_update()
        )
        if ticket is None or not _enabled(config, ticket):
            return False
        previous = set(ticket.admin_reasons or {})
        instance = ticket.instance
        issue_key = ticket.issue_key
        await _apply_state(session, ticket, config, now, reminders)
        await _record_reason_alerts(session, ticket, previous)
        reasons = {
            str(key): str(value) for key, value in (ticket.admin_reasons or {}).items()
        }
        needs_card = bool(reasons) or ticket.admin_card_event_id is not None
        if not needs_card:
            await _consume_follow_ups(session, ticket.id)
            _mark_switch_reminder(ticket, now, reminders)
        await session.commit()
    if not needs_card:
        await _send_switch_reminders(
            session_factory, board, config, ticket_id, reminders, before_wait_send
        )
        return True
    async with session_factory() as session:
        ticket = await session.scalar(
            select(JiraWorkerTicket)
            .where(JiraWorkerTicket.id == ticket_id)
            .with_for_update()
        )
        if ticket is None or not _enabled(config, ticket):
            return True
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
        _mark_switch_reminder(ticket, now, reminders)
        await session.commit()
    try:
        await _deliver_reason_alerts(
            session_factory, board, instance, issue_key, room_id
        )
    finally:
        await _send_switch_reminders(
            session_factory, board, config, ticket_id, reminders, before_wait_send
        )
    return True


async def wait_send_cancelled(
    session: AsyncSession, ticket: JiraWorkerTicket, config: SwitchConfig
) -> bool:
    """True when a queued wait reminder or Blocked must not be sent."""
    statuses = config.jira_worker_statuses
    if ticket.status not in (statuses.waiting_for_input, statuses.waiting_for_approval):
        return True
    return (
        ticket.status == statuses.waiting_for_input
        and ticket.wait_anchor is not None
        and await _latest_wait_reply(session, ticket, ticket.wait_anchor) is not None
    )


async def _clear_reminder_fired(
    session_factory: async_sessionmaker[AsyncSession], ticket_id: str
) -> None:
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, ticket_id, with_for_update=True)
        if ticket is None:
            return
        fired = dict(ticket.wait_fired or {})
        fired.pop("reminder", None)
        ticket.wait_fired = fired
        await session.commit()


async def _send_switch_reminders(
    session_factory: async_sessionmaker[AsyncSession],
    board: AdminBoard,
    config: SwitchConfig,
    ticket_id: str,
    reminders: list[tuple[str, str]],
    before_wait_send: WaitRefresh | None,
) -> None:
    if not reminders:
        return
    try:
        if before_wait_send is not None and await before_wait_send(ticket_id):
            await _clear_reminder_fired(session_factory, ticket_id)
            return
        async with session_factory() as session:
            ticket = await session.get(JiraWorkerTicket, ticket_id)
            cancelled = ticket is None or await wait_send_cancelled(
                session, ticket, config
            )
        if cancelled:
            await _clear_reminder_fired(session_factory, ticket_id)
            return
        for room_id, body in reminders:
            await board.remind_reporter(room_id=room_id, body=body)
    except Exception:
        await _clear_reminder_fired(session_factory, ticket_id)
        raise


async def _apply_state(
    session: AsyncSession,
    ticket: JiraWorkerTicket,
    config: SwitchConfig,
    now: datetime,
    reminders: list[tuple[str, str]],
) -> None:
    reasons = {
        str(key): str(value)
        for key, value in (ticket.admin_reasons or {}).items()
        if key not in _DERIVED
    }
    if is_terminal_ticket_status(ticket.status):
        await enqueue_admin_card_follow_up(session, ticket)
        ticket.admin_reasons = None
        await _apply_wait(session, ticket, config, now, {}, reminders)
        return
    if ticket.intake_state is not None:
        if ticket.intake_state.get("phase") == "limit":
            reasons[INTAKE_REASON] = INTAKE_LIMIT_DETAIL
        else:
            reasons.pop(INTAKE_REASON, None)
    if ticket.reporter_switch_user_id is None:
        reasons["unmapped_reporter"] = "Reporter has no Switch user"
    failed = await session.scalar(
        select(JiraWorkerOutbox)
        .where(
            JiraWorkerOutbox.instance == ticket.instance,
            JiraWorkerOutbox.issue_key == ticket.issue_key,
            JiraWorkerOutbox.channel == "jira",
            JiraWorkerOutbox.status == "failed",
            or_(
                JiraWorkerOutbox.attempts >= 3,
                "Jira command failed: " + JiraWorkerOutbox.id
                == ticket.worker_parked_reason,
            ),
            JiraWorkerOutbox.payload["failure_of"].astext.is_(None),
        )
        .limit(1)
    )
    if failed:
        reasons["blocked_writes"] = (
            "Jira write failed 3 times"
            if failed.attempts >= 3
            else "Jira write failed; worker parked"
        )
    if needs_person(ticket.worker_parked_reason):
        reasons["parked"] = ticket.worker_parked_reason or "parked"
    await _apply_wait(session, ticket, config, now, reasons, reminders)
    ticket.admin_reasons = reasons or None


async def _latest_wait_reply(
    session: AsyncSession,
    ticket: JiraWorkerTicket,
    after: datetime,
) -> datetime | None:
    if after.tzinfo is None:
        after = after.replace(tzinfo=UTC)
    matched = [JiraWorkerEvent.event_kind == EXPLICIT_INPUT]
    channel_kind = _WAIT_INPUT_KIND.get(ticket.wait_channel)
    if channel_kind is not None:
        matched.append(
            and_(
                JiraWorkerEvent.event_kind == channel_kind,
                JiraWorkerEvent.payload["reporter_input"].as_boolean().is_(True),
            )
        )
    found = await session.scalar(
        select(JiraWorkerEvent.received_at)
        .where(
            JiraWorkerEvent.instance == ticket.instance,
            JiraWorkerEvent.issue_key == ticket.issue_key,
            JiraWorkerEvent.received_at > after,
            or_(*matched),
        )
        .order_by(JiraWorkerEvent.received_at.desc(), JiraWorkerEvent.id.desc())
        .limit(1)
    )
    if not isinstance(found, datetime):
        return None
    if found.tzinfo is None:
        return found.replace(tzinfo=UTC)
    return found


def _mark_switch_reminder(
    ticket: JiraWorkerTicket, now: datetime, reminders: list[tuple[str, str]]
) -> None:
    if not reminders:
        return
    fired = dict(ticket.wait_fired or {})
    fired["reminder"] = now.isoformat()
    ticket.wait_fired = fired


async def _cancel_wait_commands(
    session: AsyncSession, ticket: JiraWorkerTicket
) -> None:
    rows = (
        await session.scalars(
            select(JiraWorkerOutbox)
            .where(
                JiraWorkerOutbox.instance == ticket.instance,
                JiraWorkerOutbox.issue_key == ticket.issue_key,
                JiraWorkerOutbox.status.in_(("pending", "claimed")),
                or_(
                    *(
                        JiraWorkerOutbox.command_key.like(f"{prefix}%")
                        for prefix in WAIT_COMMAND_PREFIXES
                    )
                ),
            )
            .with_for_update()
        )
    ).all()
    for row in rows:
        row.status = "cancelled"
        row.error = "Wait ended before send"


async def _apply_wait(
    session: AsyncSession,
    ticket: JiraWorkerTicket,
    config: SwitchConfig,
    now: datetime,
    reasons: dict[str, str],
    reminders: list[tuple[str, str]],
) -> None:
    waiting_for_input = ticket.status == config.jira_worker_statuses.waiting_for_input
    waiting = waiting_for_input or (
        ticket.status == config.jira_worker_statuses.waiting_for_approval
    )
    if not waiting or is_terminal_ticket_status(ticket.status):
        ticket.wait_anchor = None
        ticket.wait_fired = None
        await _cancel_wait_commands(session, ticket)
        return
    if waiting_for_input and ticket.wait_anchor is not None:
        replied_at = await _latest_wait_reply(session, ticket, ticket.wait_anchor)
        if replied_at is not None:
            ticket.wait_anchor = replied_at
            ticket.wait_fired = {}
            await _cancel_wait_commands(session, ticket)
    if ticket.worker_parked_reason:
        if ticket.wait_anchor is None:
            ticket.wait_anchor = now
            ticket.wait_fired = {}
        fired = ticket.wait_fired or {}
        if "admin" in fired:
            reasons["wait_admin"] = "No reply after the admin wait"
        if "blocked" in fired:
            reasons["wait_blocked"] = "No reply after one work day"
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
        anchor, WAIT_REMINDER_HOURS, calendar
    ):
        try:
            await _send_reminder(session, ticket, config, anchor, reminders)
        except _WaitNotReady:
            logger.warning("Jira wait reminder for %s is not ready", ticket.issue_key)
        else:
            if ticket.wait_channel == "jira_comments":
                fired["reminder"] = now.isoformat()
    if "admin" not in fired and now >= add_work_time(
        anchor, WAIT_ADMIN_HOURS, calendar
    ):
        reasons["wait_admin"] = "No reply after the admin wait"
        fired["admin"] = now.isoformat()
    elif "admin" in fired:
        reasons["wait_admin"] = "No reply after the admin wait"
    if "blocked" not in fired and now >= add_work_time(
        anchor, calendar.end_hour - calendar.start_hour, calendar
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
    anchor: datetime,
    reminders: list[tuple[str, str]],
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
    reminders.append((ticket.room_id, body))


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
