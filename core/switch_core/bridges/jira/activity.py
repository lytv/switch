"""Ticket input, capacity claims, and the one-turn orchestrator boundary."""

from __future__ import annotations

import logging
import uuid
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from sqlalchemy import and_, func, or_, select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from switch_core.bridges.jira.outbox import (
    HumanChange,
    _stamp,
    comment_actor,
    fresh_comment_ids,
    park_ticket,
    read_jira_version,
    stored_comment_ids,
)
from switch_core.config import JiraWorkerLimits, SwitchConfig
from switch_core.db.models import JiraWorkerEvent, JiraWorkerJob, JiraWorkerTicket

if TYPE_CHECKING:
    from switch_core.bridges.jira.rooms import TicketRooms
    from switch_core.bridges.jira.worker import JiraPollClient

logger = logging.getLogger(__name__)
ROOM_CREATE = "ticket_room_create"
TURN = "orchestrator_turn"
INPUT_KINDS = ("thread_message", "jira_comment", "wake_timer")


def is_terminal_ticket_status(status: str) -> bool:
    return status.strip().casefold() in ("done", "cancelled", "canceled")


def request_wake(ticket: JiraWorkerTicket) -> None:
    if not ticket.worker_parked_reason and not is_terminal_ticket_status(ticket.status):
        if ticket.agent_state != "live":
            ticket.agent_state = "wake_requested"


async def record_event(
    session: AsyncSession,
    *,
    instance: str,
    idempotency_key: str,
    issue_key: str,
    project_key: str,
    event_kind: str,
    webhook_event: str,
    payload: dict[str, Any] | None,
) -> str | None:
    result = await session.execute(
        insert(JiraWorkerEvent)
        .values(
            id=str(uuid.uuid4()),
            instance=instance,
            idempotency_key=idempotency_key,
            issue_key=issue_key,
            project_key=project_key,
            event_kind=event_kind,
            webhook_event=webhook_event,
            payload=payload,
        )
        .on_conflict_do_nothing(index_elements=["instance", "idempotency_key"])
        .returning(JiraWorkerEvent.id)
    )
    await session.flush()
    return result.scalar_one_or_none()


async def capacity_lock(session: AsyncSession) -> None:
    # ponytail: one admission lock; split room and turn locks if admission throughput matters.
    await session.execute(text("SELECT pg_advisory_xact_lock(73429105)"))


async def admit_room(
    session: AsyncSession,
    ticket: JiraWorkerTicket,
    limits: JiraWorkerLimits,
    *,
    create: bool,
    now: datetime,
    enabled_projects: dict[str, list[str]] | None,
) -> bool:
    """Caller holds capacity_lock before the ticket row lock. Commit before create."""
    if not ticket.room_claimed:
        scope = (
            or_(
                *(
                    and_(
                        JiraWorkerTicket.instance == instance,
                        JiraWorkerTicket.project_key.in_(projects),
                    )
                    for instance, projects in enabled_projects.items()
                    if projects
                )
            )
            if enabled_projects is not None
            else text("true")
        )
        count = await session.scalar(
            select(func.count())
            .select_from(JiraWorkerTicket)
            .where(
                JiraWorkerTicket.room_claimed.is_(True),
                func.lower(func.trim(JiraWorkerTicket.status)).not_in(
                    ("done", "cancelled", "canceled")
                ),
                scope,
            )
        )
        older = await session.scalar(
            select(JiraWorkerTicket.id)
            .where(
                JiraWorkerTicket.room_claimed.is_(False),
                func.lower(func.trim(JiraWorkerTicket.status)).not_in(
                    ("done", "cancelled", "canceled")
                ),
                scope,
                or_(
                    JiraWorkerTicket.first_seen_at < ticket.first_seen_at,
                    and_(
                        JiraWorkerTicket.first_seen_at == ticket.first_seen_at,
                        JiraWorkerTicket.id < ticket.id,
                    ),
                ),
            )
            .limit(1)
        )
        assert count is not None
        if count >= limits.open_rooms or older:
            ticket.queue_reason = "To Do: open ticket room limit"
            ticket.agent_state = "parked"
            return False
    if create:
        count = await session.scalar(
            select(func.count())
            .select_from(JiraWorkerJob)
            .where(
                JiraWorkerJob.kind == ROOM_CREATE,
                JiraWorkerJob.due_at > now - timedelta(hours=1),
            )
        )
        assert count is not None
        if count >= limits.room_creates_per_hour:
            ticket.queue_reason = "To Do: room create rate limit"
            ticket.agent_state = "parked"
            return False
        session.add(
            JiraWorkerJob(
                kind=ROOM_CREATE,
                instance=ticket.instance,
                project_key=ticket.project_key,
                due_at=now,
                status="done",
                payload={"ticket_id": ticket.id},
            )
        )
    ticket.room_claimed = True
    ticket.queue_reason = None
    request_wake(ticket)
    await session.flush()
    return True


@asynccontextmanager
async def ticket_turn_lock(engine: AsyncEngine, ticket_id: str):
    """A connection-owned claim dies with its process, unlike a durable job row."""
    key = f"jira-turn:{ticket_id}"
    async with engine.connect() as connection:
        try:
            locked = await connection.scalar(
                text("SELECT pg_try_advisory_lock(hashtextextended(:key, 0))"),
                {"key": key},
            )
            await connection.commit()
            try:
                yield locked
            finally:
                if locked:
                    await connection.rollback()
                    await connection.execute(
                        text("SELECT pg_advisory_unlock(hashtextextended(:key, 0))"),
                        {"key": key},
                    )
                    await connection.commit()
        except BaseException:
            await connection.invalidate()
            raise


@dataclass(frozen=True)
class TicketTurn:
    ticket_id: str
    issue_key: str
    jira_status: str
    untrusted_data: dict[str, Any]
    token_budget: int


@dataclass(frozen=True)
class TurnResult:
    tokens_used: int
    wake_at: datetime | None


Orchestrator = Callable[[TicketTurn], Awaitable[TurnResult]]


class TicketActivity:
    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        config: SwitchConfig,
        clients: dict[str, JiraPollClient],
        rooms: TicketRooms | None,
        clock: Callable[[], datetime],
    ) -> None:
        self._factory = session_factory
        self._engine = session_factory.kw["bind"]
        self._config = config
        self._clients = clients
        self._rooms = rooms
        self._clock = clock
        self._accounts: dict[str, str] = {}
        self._comment_refresh_at: dict[str, datetime] = {}

    def _enabled(self, ticket: JiraWorkerTicket) -> bool:
        return ticket.project_key in self._config.jira_worker_enabled_projects.get(
            ticket.instance, []
        )

    def _scope(self):
        return or_(
            *(
                and_(
                    JiraWorkerTicket.instance == instance,
                    JiraWorkerTicket.project_key.in_(projects),
                )
                for instance, projects in self._config.jira_worker_enabled_projects.items()
                if projects
            )
        )

    async def read_once(self) -> None:
        if not any(self._config.jira_worker_enabled_projects.values()):
            return
        async with self._factory() as session:
            ids = list(
                (
                    await session.scalars(
                        select(JiraWorkerTicket.id)
                        .where(
                            self._scope(),
                            JiraWorkerTicket.room_id.is_not(None),
                            func.lower(func.trim(JiraWorkerTicket.status)).not_in(
                                ("done", "cancelled", "canceled")
                            ),
                        )
                        .order_by(JiraWorkerTicket.first_seen_at, JiraWorkerTicket.id)
                    )
                ).all()
            )
        for ticket_id in ids:
            try:
                await self.read_ticket(ticket_id)
            except Exception:
                logger.exception("Jira ticket input read failed for %s", ticket_id)
        await self.recover_turns()

    async def _input(
        self,
        session: AsyncSession,
        ticket: JiraWorkerTicket,
        kind: str,
        key: str,
        data: dict[str, Any],
        *,
        reporter: bool,
    ) -> None:
        event_id = await record_event(
            session,
            instance=ticket.instance,
            idempotency_key=key,
            issue_key=ticket.issue_key,
            project_key=ticket.project_key,
            event_kind=kind,
            webhook_event=f"worker:{kind}",
            payload={"untrusted_data": data, "reporter_input": reporter},
        )
        if event_id and reporter:
            request_wake(ticket)

    async def read_ticket(self, ticket_id: str, *, force_jira: bool = False) -> bool:
        async with self._factory() as session:
            ticket = await session.scalar(
                select(JiraWorkerTicket)
                .where(JiraWorkerTicket.id == ticket_id)
                .with_for_update(skip_locked=True)
            )
            if (
                ticket is None
                or not self._enabled(ticket)
                or is_terminal_ticket_status(ticket.status)
            ):
                return False
            if ticket.tokens_used >= self._config.jira_worker_limits.tokens_per_ticket:
                park_ticket(ticket, "Ticket token limit")
            known_comment_ids = (
                await stored_comment_ids(session, ticket)
                if ticket.jira_read_changelog_id is not None
                else None
            )
            version = await self._read_jira(
                ticket, force=force_jira, known_comment_ids=known_comment_ids
            )
            fell_back = False
            if (
                ticket.wait_channel == "switch"
                and self._rooms is not None
                and ticket.room_id
                and ticket.card_event_id
            ):
                try:
                    await self._read_thread(session, ticket)
                except Exception as exc:
                    fell_back = True
                    ticket.wait_channel = "jira_comments"
                    await record_event(
                        session,
                        instance=ticket.instance,
                        idempotency_key=f"thread-fallback:{ticket.id}:{ticket.thread_cursor}",
                        issue_key=ticket.issue_key,
                        project_key=ticket.project_key,
                        event_kind="read_fallback",
                        webhook_event="worker:read_fallback",
                        payload={
                            "reason": type(exc).__name__,
                            "wait_channel": "jira_comments",
                        },
                    )
                    logger.warning(
                        "Jira ticket %s uses Jira comments after thread read failure: %s",
                        ticket.issue_key,
                        type(exc).__name__,
                    )
            if ticket.wait_channel == "jira_comments":
                if version is None and fell_back:
                    version = await self._read_jira(
                        ticket, force=True, known_comment_ids=known_comment_ids
                    )
                for comment in version["comments"] if version is not None else []:
                    author = comment.get("author", {}).get("accountId", "")
                    await self._input(
                        session,
                        ticket,
                        "jira_comment",
                        f"jira-comment:{ticket.issue_key}:{comment['id']}:{comment['updated']}",
                        comment,
                        reporter=bool(author) and author == ticket.reporter_account_id,
                    )
            if ticket.wake_at and ticket.wake_at <= self._clock():
                due = ticket.wake_at.isoformat()
                await self._input(
                    session,
                    ticket,
                    "wake_timer",
                    f"timer:{ticket.id}:{due}",
                    {"due_at": due},
                    reporter=True,
                )
                ticket.wake_at = None
            if ticket.worker_parked_reason:
                ticket.agent_state = "parked"
            await session.commit()
            return True

    def _comment_refresh_due(self, ticket: JiraWorkerTicket) -> bool:
        if ticket.wait_channel != "jira_comments":
            return False
        due = self._comment_refresh_at.get(ticket.id)
        return due is None or due <= self._clock()

    def _schedule_comment_refresh(self, ticket: JiraWorkerTicket) -> None:
        if ticket.wait_channel != "jira_comments":
            return
        interval = max(1, self._config.jira_worker_poll_interval_seconds)
        self._comment_refresh_at[ticket.id] = self._clock() + timedelta(
            seconds=interval
        )

    async def _read_jira(
        self,
        ticket: JiraWorkerTicket,
        *,
        force: bool,
        known_comment_ids: set[str] | None,
    ) -> dict[str, Any] | None:
        if (
            not force
            and not self._comment_refresh_due(ticket)
            and ticket.jira_worker_account_id
            and ticket.jira_read_updated
            and ticket.last_event_at <= _stamp(ticket.jira_read_updated)
        ):
            return None
        self._schedule_comment_refresh(ticket)
        client = self._clients.get(ticket.instance)
        if client is None:
            raise ValueError(f"Jira read credentials missing for {ticket.instance}")
        account = self._accounts.get(ticket.instance)
        if account is None:
            account = (await client.request("GET", "/rest/api/3/myself")).json()[
                "accountId"
            ]
            if not isinstance(account, str) or not account:
                raise ValueError("Jira worker accountId is required")
            self._accounts[ticket.instance] = account
        ticket.jira_worker_account_id = account
        try:
            version = await read_jira_version(client, ticket.issue_key)
        except HumanChange as exc:
            park_ticket(ticket, str(exc))
            return None
        baseline = (
            _stamp(ticket.jira_read_updated)
            if ticket.jira_read_updated
            else ticket.last_event_at
        )
        changes = [
            entry
            for entry in version["histories"]
            if _stamp(entry["created"]) > baseline
        ]
        if ticket.jira_read_changelog_id == "":
            changes = version["histories"]
        elif ticket.jira_read_changelog_id:
            baseline_index = next(
                (
                    i
                    for i, entry in enumerate(version["histories"])
                    if str(entry["id"]) == ticket.jira_read_changelog_id
                ),
                None,
            )
            if baseline_index is None:
                park_ticket(ticket, "Jira changelog baseline is missing")
            else:
                changes = version["histories"][baseline_index + 1 :]
        if any(
            entry.get("author", {}).get("accountId") != account for entry in changes
        ):
            park_ticket(ticket, "Human Jira action")
        fresh = (
            fresh_comment_ids(version["comments"], known_comment_ids)
            if known_comment_ids is not None
            else set()
        )
        comment_changes = [
            comment
            for comment in version["comments"]
            if _stamp(comment["updated"]) > baseline
            or str(comment.get("id", "")) in fresh
        ]
        for comment in comment_changes:
            author = comment_actor(comment)
            if author not in (account, ticket.reporter_account_id) or not author:
                park_ticket(ticket, "Human Jira action")
        if (
            _stamp(version["updated"]) > baseline
            and not changes
            and not comment_changes
        ):
            park_ticket(ticket, "Unattributed Jira action")
        ticket.jira_read_updated = version["updated"]
        ticket.jira_read_changelog_id = version["changelog_id"]
        return version

    async def _read_thread(
        self, session: AsyncSession, ticket: JiraWorkerTicket
    ) -> None:
        assert self._rooms is not None and ticket.room_id
        while True:
            page = await self._rooms.read_context(
                ticket.room_id, after_seq=ticket.thread_cursor
            )
            cursor = page["next_seq"]
            if not isinstance(cursor, int) or cursor < ticket.thread_cursor:
                raise ValueError("Thread reader returned an invalid cursor")
            for group in page["threads"]:
                for message in (group["root"], *group["replies"]):
                    if message.get("kind") != "message" or message.get("elided"):
                        continue
                    if message.get("id") == ticket.card_event_id:
                        continue
                    sender = message.get("sender")
                    if not isinstance(sender, str) or await self._rooms.is_worker_sender(
                        sender
                    ):
                        continue
                    user_id = ticket.reporter_switch_user_id
                    reporter = (
                        user_id is not None
                        and await self._rooms.is_reporter_sender(sender, user_id)
                    )
                    await self._input(
                        session,
                        ticket,
                        "thread_message",
                        f"thread:{ticket.room_id}:{message['id']}",
                        message,
                        reporter=reporter,
                    )
            if page["truncated"] and cursor == ticket.thread_cursor:
                raise ValueError("Thread pagination made no progress")
            ticket.thread_cursor = cursor
            if not page["truncated"]:
                return

    async def recover_turns(self) -> None:
        if not any(self._config.jira_worker_enabled_projects.values()):
            return
        async with self._factory() as session:
            jobs = list(
                (
                    await session.scalars(
                        select(JiraWorkerJob)
                        .join(
                            JiraWorkerTicket,
                            JiraWorkerJob.payload["ticket_id"].astext
                            == JiraWorkerTicket.id,
                        )
                        .where(
                            JiraWorkerJob.kind == TURN,
                            JiraWorkerJob.status == "claimed",
                            self._scope(),
                        )
                    )
                ).all()
            )
        for job in jobs:
            assert job.payload is not None
            async with ticket_turn_lock(
                self._engine, job.payload["ticket_id"]
            ) as locked:
                if not locked:
                    continue
                async with self._factory() as session:
                    row = await session.get(JiraWorkerJob, job.id, with_for_update=True)
                    assert row is not None
                    if row.status != "claimed":
                        continue
                    ticket = await session.get(
                        JiraWorkerTicket, job.payload["ticket_id"], with_for_update=True
                    )
                    assert ticket is not None
                    row.status = "error"
                    row.error = (
                        "Interrupted orchestrator turn; outcome and token usage unknown"
                    )
                    park_ticket(ticket, row.error)
                    await session.commit()

    async def run_one_turn(self, orchestrator: Orchestrator) -> str | None:
        """Step 8 supplies the callback. It receives data, never Jira credentials.

        The callback must enforce token_budget. Returning never changes Jira status
        or clears a Jira wait; those decisions belong to the orchestrator/outbox.
        """
        if not any(self._config.jira_worker_enabled_projects.values()):
            return None
        await self.read_once()
        async with self._factory() as session:
            ids = list(
                (
                    await session.scalars(
                        select(JiraWorkerTicket.id)
                        .where(
                            self._scope(),
                            JiraWorkerTicket.worker_parked_reason.is_(None),
                            JiraWorkerTicket.room_claimed.is_(True),
                            JiraWorkerTicket.card_event_id.is_not(None),
                            JiraWorkerTicket.agent_state.in_(
                                ("wake_requested", "parked")
                            ),
                            func.lower(func.trim(JiraWorkerTicket.status)).not_in(
                                ("done", "cancelled", "canceled")
                            ),
                        )
                        .order_by(JiraWorkerTicket.first_seen_at, JiraWorkerTicket.id)
                    )
                ).all()
            )
        for ticket_id in ids:
            async with ticket_turn_lock(self._engine, ticket_id) as locked:
                if not locked:
                    continue
                try:
                    loaded = await self.read_ticket(ticket_id, force_jira=True)
                except Exception:
                    logger.exception(
                        "Jira ticket input read failed for %s", ticket_id
                    )
                    continue
                if not loaded:
                    continue
                claimed = await self._claim_turn(ticket_id)
                if claimed is None:
                    continue
                job_id, generation, turn, event_ids = claimed
                try:
                    result = await orchestrator(turn)
                    if (
                        not isinstance(result.tokens_used, int)
                        or isinstance(result.tokens_used, bool)
                        or result.tokens_used < 0
                    ):
                        raise ValueError(
                            "Orchestrator token usage must be a nonnegative integer"
                        )
                    if result.wake_at is not None and (
                        not isinstance(result.wake_at, datetime)
                        or result.wake_at.tzinfo is None
                    ):
                        raise ValueError(
                            "Orchestrator wake time must include a timezone"
                        )
                except BaseException:
                    async with self._factory() as session:
                        ticket = await session.get(
                            JiraWorkerTicket, ticket_id, with_for_update=True
                        )
                        job = await session.get(JiraWorkerJob, job_id)
                        assert ticket is not None and job is not None
                        job.status = "error"
                        job.error = (
                            "Orchestrator turn failed; outcome and token usage unknown"
                        )
                        park_ticket(ticket, job.error)
                        await session.commit()
                    raise
                await self._finish_turn(
                    ticket_id, job_id, generation, result, event_ids
                )
                return ticket_id
        return None

    async def _claim_turn(self, ticket_id: str):
        async with self._factory() as session:
            await capacity_lock(session)
            ticket = await session.get(
                JiraWorkerTicket, ticket_id, with_for_update=True
            )
            if ticket is None:
                return None
            if (
                not self._enabled(ticket)
                or ticket.worker_parked_reason
                or is_terminal_ticket_status(ticket.status)
                or ticket.agent_state not in ("wake_requested", "parked")
            ):
                return None
            if ticket.tokens_used >= self._config.jira_worker_limits.tokens_per_ticket:
                park_ticket(ticket, "Ticket token limit")
                await session.commit()
                return None
            live = await session.scalar(
                select(func.count())
                .select_from(JiraWorkerJob)
                .outerjoin(
                    JiraWorkerTicket,
                    JiraWorkerJob.payload["ticket_id"].astext == JiraWorkerTicket.id,
                )
                .where(
                    JiraWorkerJob.kind == TURN,
                    JiraWorkerJob.status == "claimed",
                    or_(JiraWorkerTicket.id.is_(None), self._scope()),
                )
            )
            assert live is not None
            if live >= self._config.jira_worker_limits.live_sessions:
                ticket.queue_reason = "To Do: live orchestrator session limit"
                ticket.agent_state = "parked"
                await session.commit()
                return None
            events = list(
                (
                    await session.scalars(
                        select(JiraWorkerEvent)
                        .where(
                            JiraWorkerEvent.instance == ticket.instance,
                            JiraWorkerEvent.issue_key == ticket.issue_key,
                            JiraWorkerEvent.consumed.is_(False),
                        )
                        .order_by(JiraWorkerEvent.received_at, JiraWorkerEvent.id)
                    )
                ).all()
            )
            ticket.agent_state = "live"
            ticket.queue_reason = None
            job = JiraWorkerJob(
                kind=TURN,
                instance=ticket.instance,
                project_key=ticket.project_key,
                due_at=self._clock(),
                status="claimed",
                payload={"ticket_id": ticket.id},
            )
            session.add(job)
            await session.flush()
            turn = TicketTurn(
                ticket.id,
                ticket.issue_key,
                ticket.status,
                {
                    "ticket_text": ticket.summary,
                    "events": [event.payload for event in events],
                },
                self._config.jira_worker_limits.tokens_per_ticket - ticket.tokens_used,
            )
            await session.commit()
            return job.id, ticket.agent_generation, turn, [event.id for event in events]

    async def _finish_turn(
        self,
        ticket_id: str,
        job_id: str,
        generation: int,
        result: TurnResult,
        event_ids: list[str],
    ) -> None:
        async with self._factory() as session:
            ticket = await session.get(
                JiraWorkerTicket, ticket_id, with_for_update=True
            )
            job = await session.get(JiraWorkerJob, job_id)
            assert ticket is not None and job is not None
            job.status = "done"
            ticket.tokens_used += result.tokens_used
            if ticket.tokens_used >= self._config.jira_worker_limits.tokens_per_ticket:
                park_ticket(ticket, "Ticket token limit")
            elif (
                ticket.agent_generation == generation
                and not ticket.worker_parked_reason
                and not is_terminal_ticket_status(ticket.status)
                and self._enabled(ticket)
            ):
                await session.execute(
                    update(JiraWorkerEvent)
                    .where(JiraWorkerEvent.id.in_(event_ids))
                    .values(consumed=True)
                )
                pending = await session.scalar(
                    select(JiraWorkerEvent.id)
                    .where(
                        JiraWorkerEvent.instance == ticket.instance,
                        JiraWorkerEvent.issue_key == ticket.issue_key,
                        JiraWorkerEvent.event_kind.in_(INPUT_KINDS),
                        JiraWorkerEvent.consumed.is_(False),
                        JiraWorkerEvent.payload["reporter_input"]
                        .as_boolean()
                        .is_(True),
                    )
                    .limit(1)
                )
                ticket.agent_state = "wake_requested" if pending else "waiting"
                ticket.wake_at = result.wake_at
            await session.commit()
