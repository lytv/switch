"""Admin cards, wait timers, and message-edit consumers. Jira is not called."""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy import null, select, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from switch_core.bridges.jira.admin import (
    ADMIN_CARD_COMMAND,
    ADMIN_ROOM_MARKER,
    INTAKE_REASON,
    SwitchAdminBoard,
    accept_ignored_input,
    add_work_time,
    card_marker,
    reconcile_admin_cards,
    set_admin_card_reason,
    sync_admin_card,
)
from switch_core.bridges.jira.outbox import JiraOutboxSender
from switch_core.bridges.jira.rooms import (
    ADMIN_CARD_COMMAND as ROOMS_ADMIN_CARD_COMMAND,
)
from switch_core.bridges.jira.worker import (
    JiraWorkerScheduler,
    record_polled_issue,
    record_webhook_event,
    set_identity_mapping,
)
from switch_core.config import (
    JiraWorkerCalendar,
    JiraWorkerCredentials,
    JiraWorkerStatuses,
)
from switch_core.db.models import (
    JiraWorkerEvent,
    JiraWorkerOutbox,
    JiraWorkerTicket,
    Message,
    Room,
    User,
)
from switch_core.room_service import RoomCreateConfig
from tests.switch_core.bridges.jira.test_activity import ThreadRooms, message
from tests.switch_core.bridges.jira.test_outbox import (
    JiraMock,
    client_for,
    enqueue,
    sender_for,
)
from tests.switch_core.bridges.jira.test_worker import (
    _config,
    _run_migrations,
    _webhook_event,
)

NOW = datetime(2026, 10, 2, 16, 0, tzinfo=UTC)


class FakeAdminBoard:
    def __init__(self) -> None:
        self.room_id = "admin-room"
        self.posts: list[str] = []
        self.edits: list[tuple[str, str]] = []
        self.cards: dict[str, str] = {}
        self.reminders: list[tuple[str, str]] = []
        self.alerts: list[tuple[str, str]] = []
        self._n = 0

    async def ensure_room(self) -> str:
        return self.room_id

    async def find_card(self, room_id: str, marker: str) -> str | None:
        prefix = marker + "\n"
        for event_id, body in self.cards.items():
            if body.startswith(prefix):
                return event_id
        return None

    async def post_card(self, room_id: str, *, body: str) -> str:
        self._n += 1
        event_id = f"admin-card-{self._n}"
        self.cards[event_id] = body
        self.posts.append(event_id)
        return event_id

    async def edit_card(self, room_id: str, event_id: str, *, body: str) -> None:
        self.cards[event_id] = body
        self.edits.append((event_id, body))

    async def card_body(self, event_id: str) -> str | None:
        return self.cards.get(event_id)

    async def remind_reporter(self, *, room_id: str, body: str) -> None:
        self.reminders.append((room_id, body))

    async def post_alert(self, room_id: str, *, body: str) -> None:
        self.alerts.append((room_id, body))


def _worker_config(**overrides: object):
    base: dict[str, object] = {
        "jira_worker_enabled_projects": {"acme": ["KAN"]},
        "jira_worker_credentials": {
            "acme": JiraWorkerCredentials(
                base_url="https://example.test",
                email="worker@example.test",
                api_token="token",
            )
        },
        "frontend_base_url": "https://switch.example",
        "jira_worker_calendar": JiraWorkerCalendar(
            weekdays=[0, 1, 2, 3, 4, 5, 6], start_hour=0, end_hour=24
        ),
    }
    base.update(overrides)
    return _config(**base)


async def _engine(factory: async_sessionmaker[AsyncSession]) -> AsyncEngine:
    async with factory() as session:
        return session.bind  # type: ignore[return-value]


async def _seed(factory: async_sessionmaker[AsyncSession], **overrides: object) -> str:
    values: dict[str, object] = {
        "instance": "acme",
        "project_key": "KAN",
        "issue_key": "KAN-1",
        "summary": "First ticket",
        "status": "In Progress",
        "reporter_account_id": "reporter-account",
        "reporter_switch_user_id": None,
        "wait_channel": "jira_comments",
        "first_seen_at": NOW,
        "last_event_at": NOW,
    }
    values.update(overrides)
    async with factory() as session:
        ticket = JiraWorkerTicket(**values)  # type: ignore[arg-type]
        session.add(ticket)
        await session.commit()
        return ticket.id


async def _ticket(
    factory: async_sessionmaker[AsyncSession], ticket_id: str
) -> JiraWorkerTicket:
    async with factory() as session:
        ticket = await session.get(JiraWorkerTicket, ticket_id)
        assert ticket is not None
        return ticket


def test_admin_card_command_matches_step_two() -> None:
    assert ADMIN_CARD_COMMAND == ROOMS_ADMIN_CARD_COMMAND


def test_work_hours_skip_the_weekend() -> None:
    friday = datetime(2026, 10, 2, 16, 0, tzinfo=UTC)
    assert friday.weekday() == 4
    calendar = JiraWorkerCalendar()
    assert add_work_time(friday, 2, calendar) == datetime(
        2026, 10, 5, 10, 0, tzinfo=UTC
    )
    assert add_work_time(friday, 4, calendar) == datetime(
        2026, 10, 5, 12, 0, tzinfo=UTC
    )
    assert add_work_time(friday, 8, calendar) == datetime(
        2026, 10, 5, 16, 0, tzinfo=UTC
    )
    before_open = datetime(2026, 10, 5, 8, 0, tzinfo=UTC)
    assert add_work_time(before_open, 2, calendar) == datetime(
        2026, 10, 5, 11, 0, tzinfo=UTC
    )


@pytest.mark.asyncio
async def test_card_posts_once_then_edits_across_restart_and_double_claim(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    ticket_id = await _seed(session_factory)
    board = FakeAdminBoard()
    config = _worker_config()
    engine = await _engine(session_factory)
    await sync_admin_card(
        session_factory,
        engine=engine,
        board=board,
        config=config,
        ticket_id=ticket_id,
        now=NOW,
    )
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, ticket_id)
        assert ticket is not None
        ticket.summary = "Updated summary"
        await session.commit()
    await sync_admin_card(
        session_factory,
        engine=engine,
        board=board,
        config=config,
        ticket_id=ticket_id,
        now=NOW,
    )
    assert board.posts == ["admin-card-1"]
    assert len(board.edits) == 1
    assert board.edits[0][0] == "admin-card-1"
    assert "Updated summary" in board.edits[0][1]
    assert card_marker("acme", "KAN-1") in board.cards["admin-card-1"]

    again = FakeAdminBoard()
    again.cards = dict(board.cards)
    again._n = 1
    await asyncio.gather(
        sync_admin_card(
            session_factory,
            engine=engine,
            board=again,
            config=config,
            ticket_id=ticket_id,
            now=NOW,
        ),
        sync_admin_card(
            session_factory,
            engine=engine,
            board=again,
            config=config,
            ticket_id=ticket_id,
            now=NOW,
        ),
    )
    assert again.posts == []
    stored = await _ticket(session_factory, ticket_id)
    assert stored.admin_card_event_id == "admin-card-1"


@pytest.mark.asyncio
async def test_step_two_rows_are_consumed_and_disabled_project_is_unchanged(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    enabled = await _seed(session_factory)
    disabled = await _seed(
        session_factory,
        project_key="OFF",
        issue_key="OFF-1",
        reporter_switch_user_id="user-1",
        wait_channel="switch",
    )
    async with session_factory() as session:
        session.add_all(
            [
                JiraWorkerOutbox(
                    channel="switch",
                    command=ADMIN_CARD_COMMAND,
                    instance="acme",
                    issue_key="KAN-1",
                    status="pending",
                    payload={"ticket_id": enabled, "switch_user_id": None},
                ),
                JiraWorkerOutbox(
                    channel="switch",
                    command=ADMIN_CARD_COMMAND,
                    instance="acme",
                    issue_key="OFF-1",
                    status="pending",
                    payload={"ticket_id": disabled, "switch_user_id": None},
                ),
            ]
        )
        await session.commit()
    board = FakeAdminBoard()
    await reconcile_admin_cards(
        session_factory,
        engine=await _engine(session_factory),
        board=board,
        config=_worker_config(),
        now=NOW,
    )
    async with session_factory() as session:
        rows = (
            await session.execute(
                select(JiraWorkerOutbox.issue_key, JiraWorkerOutbox.status)
                .where(JiraWorkerOutbox.command == ADMIN_CARD_COMMAND)
                .order_by(JiraWorkerOutbox.issue_key)
            )
        ).all()
    assert rows == [("KAN-1", "done"), ("OFF-1", "pending")]
    assert len(board.posts) == 1
    disabled_ticket = await _ticket(session_factory, disabled)
    assert disabled_ticket.admin_card_event_id is None
    assert disabled_ticket.admin_reasons is None


@pytest.mark.asyncio
async def test_each_reason_raises_and_clears(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    ticket_id = await _seed(session_factory, room_id="room-1")
    board = FakeAdminBoard()
    config = _worker_config()
    engine = await _engine(session_factory)

    async def sync() -> str:
        await sync_admin_card(
            session_factory,
            engine=engine,
            board=board,
            config=config,
            ticket_id=ticket_id,
            now=NOW,
        )
        body = board.cards[
            (await _ticket(session_factory, ticket_id)).admin_card_event_id
        ]
        return body

    body = await sync()
    assert "unmapped_reporter" in body
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, ticket_id)
        assert ticket is not None
        ticket.reporter_switch_user_id = "user-1"
        ticket.wait_channel = "switch"
        await session.commit()
    body = await sync()
    assert "unmapped_reporter" not in body
    assert "- none" in body

    async with session_factory() as session:
        session.add(
            JiraWorkerOutbox(
                channel="jira",
                command="comment",
                instance="acme",
                issue_key="KAN-1",
                command_key="failed-write",
                status="failed",
                attempts=3,
                payload={"data": "hello"},
            )
        )
        await session.commit()
    body = await sync()
    assert "blocked_writes" in body
    async with session_factory() as session:
        row = await session.scalar(
            select(JiraWorkerOutbox).where(
                JiraWorkerOutbox.command_key == "failed-write"
            )
        )
        assert row is not None
        row.status = "done"
        await session.commit()
    body = await sync()
    assert "blocked_writes" not in body

    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, ticket_id)
        assert ticket is not None
        ticket.worker_parked_reason = "Human Jira action"
        await session.commit()
    body = await sync()
    assert "parked" in body
    assert "Human Jira action" in body
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, ticket_id)
        assert ticket is not None
        ticket.worker_parked_reason = None
        await session.commit()
    body = await sync()
    assert "parked:" not in body

    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, ticket_id)
        assert ticket is not None
        ticket.worker_parked_reason = "Ticket token limit"
        await session.commit()
    assert "parked" in await sync()
    for reason in (
        "A non-worker Jira action superseded the command",
        "Issue changed while the version check read its history",
        "Jira updated stamp has no attributable worker change",
        "TimeoutError: Jira write outcome is unknown; automatic replay is unsafe",
        "Unattributed Jira action",
        "Interrupted orchestrator turn; outcome and token usage unknown",
        "The command's changelog baseline is missing",
        "Jira version is older than the command baseline",
    ):
        async with session_factory() as session:
            ticket = await session.get(JiraWorkerTicket, ticket_id)
            assert ticket is not None
            ticket.worker_parked_reason = reason
            await session.commit()
        body = await sync()
        assert "parked" in body
        assert reason in body
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, ticket_id)
        assert ticket is not None
        ticket.worker_parked_reason = "Jira ticket completed or cancelled"
        await session.commit()
    assert "parked:" not in await sync()
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, ticket_id)
        assert ticket is not None
        ticket.worker_parked_reason = "Jira changelog baseline is missing"
        await session.commit()
    assert "parked: Jira changelog baseline is missing" in await sync()
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, ticket_id)
        assert ticket is not None
        ticket.worker_parked_reason = None
        await session.commit()
    assert "parked:" not in await sync()

    await set_admin_card_reason(
        session_factory,
        engine=engine,
        board=board,
        config=config,
        instance="acme",
        issue_key="KAN-1",
        reason=INTAKE_REASON,
        detail="6 questions",
        now=NOW,
    )
    body = board.cards[(await _ticket(session_factory, ticket_id)).admin_card_event_id]
    assert "intake_questions: 6 questions" in body
    await set_admin_card_reason(
        session_factory,
        engine=engine,
        board=board,
        config=config,
        instance="acme",
        issue_key="KAN-1",
        reason=INTAKE_REASON,
        detail=None,
        now=NOW,
    )
    body = board.cards[(await _ticket(session_factory, ticket_id)).admin_card_event_id]
    assert "intake_questions" not in body
    assert (
        await _ticket(session_factory, ticket_id)
    ).admin_card_event_id == "admin-card-1"
    assert board.posts == ["admin-card-1"]


@pytest.mark.asyncio
async def test_wait_rules_use_work_hours(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    ticket_id = await _seed(
        session_factory,
        status="Waiting for input",
        reporter_switch_user_id="user-1",
        wait_channel="switch",
        room_id="room-1",
        jira_read_updated="2026-10-02T16:00:00.000+0000",
        jira_read_changelog_id="200",
    )
    board = FakeAdminBoard()
    config = _worker_config(jira_worker_calendar=JiraWorkerCalendar())
    engine = await _engine(session_factory)
    friday = NOW
    await sync_admin_card(
        session_factory,
        engine=engine,
        board=board,
        config=config,
        ticket_id=ticket_id,
        now=friday,
    )
    assert board.reminders == []
    monday_reminder = datetime(2026, 10, 5, 10, 0, tzinfo=UTC)
    await sync_admin_card(
        session_factory,
        engine=engine,
        board=board,
        config=config,
        ticket_id=ticket_id,
        now=monday_reminder,
    )
    assert board.reminders == []
    rows = await _switch_reminder_rows(session_factory)
    assert len(rows) == 1
    rooms = ReminderRooms()
    await _reminder_sender(session_factory, rooms).run_once()
    assert rooms.notices == [
        ("room-1", "Reminder: the worker is still waiting for a reply on KAN-1.")
    ]
    ticket = await _ticket(session_factory, ticket_id)
    assert "wait_admin" not in (ticket.admin_reasons or {})
    monday_admin = datetime(2026, 10, 5, 12, 0, tzinfo=UTC)
    await sync_admin_card(
        session_factory,
        engine=engine,
        board=board,
        config=config,
        ticket_id=ticket_id,
        now=monday_admin,
    )
    ticket = await _ticket(session_factory, ticket_id)
    assert "wait_admin" in (ticket.admin_reasons or {})
    assert "wait_blocked" not in (ticket.admin_reasons or {})
    monday_blocked = datetime(2026, 10, 5, 16, 0, tzinfo=UTC)
    await sync_admin_card(
        session_factory,
        engine=engine,
        board=board,
        config=config,
        ticket_id=ticket_id,
        now=monday_blocked,
    )
    async with session_factory() as session:
        command = await session.scalar(
            select(JiraWorkerOutbox).where(JiraWorkerOutbox.command == "transition")
        )
        assert command is not None
        assert command.payload["data"] == "BLOCKED"
    ticket = await _ticket(session_factory, ticket_id)
    assert "wait_blocked" in (ticket.admin_reasons or {})
    async with session_factory() as session:
        row = await session.get(JiraWorkerTicket, ticket_id)
        assert row is not None
        row.status = "In Progress"
        await session.commit()
    await sync_admin_card(
        session_factory,
        engine=engine,
        board=board,
        config=config,
        ticket_id=ticket_id,
        now=monday_blocked + timedelta(hours=1),
    )
    ticket = await _ticket(session_factory, ticket_id)
    assert ticket.admin_reasons in (None, {})
    assert ticket.admin_card_event_id == "admin-card-1"
    assert ticket.wait_anchor is None


@pytest.mark.asyncio
async def test_jira_comment_reminder_uses_the_wait_channel(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    ticket_id = await _seed(
        session_factory,
        status="Waiting for approval",
        jira_read_updated="2026-10-02T16:00:00.000+0000",
        jira_read_changelog_id="200",
        wait_anchor=NOW - timedelta(hours=3),
        wait_fired={},
    )
    board = FakeAdminBoard()
    await sync_admin_card(
        session_factory,
        engine=await _engine(session_factory),
        board=board,
        config=_worker_config(),
        ticket_id=ticket_id,
        now=NOW,
    )
    assert board.reminders == []
    async with session_factory() as session:
        row = await session.scalar(
            select(JiraWorkerOutbox).where(JiraWorkerOutbox.command == "comment")
        )
        assert row is not None
        assert row.channel == "jira"
        assert "KAN-1" in row.payload["data"]


async def _input_event(
    factory: async_sessionmaker[AsyncSession],
    *,
    kind: str,
    received_at: datetime,
    reporter_input: bool | None = None,
    issue_key: str = "KAN-1",
) -> None:
    payload: dict[str, object] = {"untrusted_data": {"body": "reply"}}
    if reporter_input is not None:
        payload["reporter_input"] = reporter_input
    async with factory() as session:
        session.add(
            JiraWorkerEvent(
                instance="acme",
                idempotency_key=f"{kind}:{issue_key}:{received_at.isoformat()}",
                issue_key=issue_key,
                project_key="KAN",
                event_kind=kind,
                payload=payload,
                received_at=received_at,
            )
        )
        await session.commit()


async def _sync_at(
    factory: async_sessionmaker[AsyncSession],
    ticket_id: str,
    board: FakeAdminBoard,
    moment: datetime,
) -> None:
    await sync_admin_card(
        factory,
        engine=await _engine(factory),
        board=board,
        config=_worker_config(),
        ticket_id=ticket_id,
        now=moment,
    )


class ReminderRooms:
    """Fake Switch endpoint for wait-reminder delivery."""

    def __init__(self) -> None:
        self.notices: list[tuple[str, str]] = []

    async def post_notice(self, room_id: str, *, body: str) -> str:
        self.notices.append((room_id, body))
        return "reminder-event-1"


def _reminder_sender(
    factory: async_sessionmaker[AsyncSession],
    rooms: ReminderRooms,
    hook: Any = None,
    **overrides: object,
) -> JiraOutboxSender:
    return JiraOutboxSender(
        session_factory=factory,
        config=_worker_config(**overrides),
        clients={},
        clock=lambda: datetime.now(UTC),
        rooms=rooms,  # type: ignore[arg-type]
        before_wait_send=hook,
    )


async def _switch_reminder_rows(
    factory: async_sessionmaker[AsyncSession],
) -> list[JiraWorkerOutbox]:
    async with factory() as session:
        return list(
            (
                await session.scalars(
                    select(JiraWorkerOutbox).where(
                        JiraWorkerOutbox.channel == "switch",
                        JiraWorkerOutbox.command == "wait_reminder",
                    )
                )
            ).all()
        )


@pytest.mark.asyncio
async def test_reporter_reply_on_the_wait_channel_resets_the_clock(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    board = FakeAdminBoard()
    switch_id = await _seed(
        session_factory,
        issue_key="KAN-1",
        status="Waiting for input",
        reporter_switch_user_id="user-1",
        wait_channel="switch",
        room_id="room-1",
    )
    await _sync_at(session_factory, switch_id, board, NOW)
    await _input_event(
        session_factory,
        kind="wake_timer",
        received_at=NOW + timedelta(hours=1),
        reporter_input=True,
    )
    await _input_event(
        session_factory,
        kind="jira_comment",
        received_at=NOW + timedelta(hours=1, minutes=1),
        reporter_input=True,
    )
    await _sync_at(session_factory, switch_id, board, NOW + timedelta(hours=2))
    assert len(await _switch_reminder_rows(session_factory)) == 1
    await _input_event(
        session_factory,
        kind="thread_message",
        received_at=NOW + timedelta(hours=2, minutes=5),
        reporter_input=True,
    )
    await _sync_at(session_factory, switch_id, board, NOW + timedelta(hours=5))
    ticket = await _ticket(session_factory, switch_id)
    assert ticket.wait_anchor == NOW + timedelta(hours=2, minutes=5)
    assert "wait_admin" not in (ticket.admin_reasons or {})

    comment_id = await _seed(
        session_factory,
        issue_key="KAN-2",
        status="Waiting for input",
        wait_channel="jira_comments",
        jira_read_updated="2026-10-02T16:00:00.000+0000",
        jira_read_changelog_id="200",
        first_seen_at=NOW + timedelta(seconds=1),
    )
    await _sync_at(session_factory, comment_id, board, NOW)
    await _input_event(
        session_factory,
        kind="jira_comment",
        received_at=NOW + timedelta(hours=1),
        reporter_input=True,
        issue_key="KAN-2",
    )
    await _sync_at(session_factory, comment_id, board, NOW + timedelta(hours=2))
    async with session_factory() as session:
        comment = await session.scalar(
            select(JiraWorkerOutbox).where(
                JiraWorkerOutbox.issue_key == "KAN-2",
                JiraWorkerOutbox.command == "comment",
            )
        )
    assert comment is None
    explicit_id = await _seed(
        session_factory,
        issue_key="KAN-3",
        status="Waiting for input",
        reporter_switch_user_id="user-1",
        wait_channel="switch",
        room_id="room-3",
        first_seen_at=NOW + timedelta(seconds=2),
    )
    explicit_board = FakeAdminBoard()
    await _sync_at(session_factory, explicit_id, explicit_board, NOW)
    await _input_event(
        session_factory,
        kind="explicit_input",
        received_at=NOW + timedelta(minutes=30),
        issue_key="KAN-3",
    )
    await _sync_at(
        session_factory, explicit_id, explicit_board, NOW + timedelta(hours=2)
    )
    assert explicit_board.reminders == []
    assert (await _ticket(session_factory, explicit_id)).wait_anchor == NOW + timedelta(
        minutes=30
    )


@pytest.mark.asyncio
async def test_reply_after_the_admin_alert_clears_wait_reasons(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    ticket_id = await _seed(
        session_factory,
        status="Waiting for input",
        reporter_switch_user_id="user-1",
        wait_channel="switch",
        room_id="room-1",
        jira_read_updated="2026-10-02T16:00:00.000+0000",
        jira_read_changelog_id="200",
    )
    board = FakeAdminBoard()
    await _sync_at(session_factory, ticket_id, board, NOW)
    await _sync_at(session_factory, ticket_id, board, NOW + timedelta(hours=5))
    assert "wait_admin" in (
        (await _ticket(session_factory, ticket_id)).admin_reasons or {}
    )
    await _input_event(
        session_factory,
        kind="thread_message",
        received_at=NOW + timedelta(hours=5, minutes=1),
        reporter_input=True,
    )
    await _sync_at(
        session_factory, ticket_id, board, NOW + timedelta(hours=5, minutes=2)
    )
    ticket = await _ticket(session_factory, ticket_id)
    assert "wait_admin" not in (ticket.admin_reasons or {})
    assert "wait_blocked" not in (ticket.admin_reasons or {})
    async with session_factory() as session:
        blocked = await session.scalar(
            select(JiraWorkerOutbox).where(JiraWorkerOutbox.command == "transition")
        )
    assert blocked is None


@pytest.mark.asyncio
async def test_approval_room_message_does_not_reset_the_wait(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    ticket_id = await _seed(
        session_factory,
        status="Waiting for approval",
        reporter_switch_user_id="user-1",
        wait_channel="switch",
        room_id="room-1",
    )
    board = FakeAdminBoard()
    await _sync_at(session_factory, ticket_id, board, NOW)
    await _input_event(
        session_factory,
        kind="thread_message",
        received_at=NOW + timedelta(hours=1),
        reporter_input=True,
    )
    await _sync_at(session_factory, ticket_id, board, NOW + timedelta(hours=3))
    assert len(await _switch_reminder_rows(session_factory)) == 1
    rooms = ReminderRooms()
    await _reminder_sender(session_factory, rooms).run_once()
    assert rooms.notices == [
        ("room-1", "Reminder: the worker is still waiting for a reply on KAN-1.")
    ]


@pytest.mark.asyncio
async def test_switch_reminder_survives_a_restart_between_commit_and_send(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Repro: the reminder must outlive a crash after the milestone commits."""
    ticket_id = await _seed(
        session_factory,
        status="Waiting for input",
        reporter_switch_user_id="user-1",
        wait_channel="switch",
        room_id="room-1",
        wait_anchor=NOW - timedelta(hours=3),
        wait_fired={},
    )
    board = FakeAdminBoard()
    await _sync_at(session_factory, ticket_id, board, NOW)
    # Reconciliation commits the milestone but sends nothing itself, so a
    # crash here loses nothing. The reminder waits in the outbox.
    assert board.reminders == []
    rows = await _switch_reminder_rows(session_factory)
    assert len(rows) == 1 and rows[0].status == "pending"
    # A fresh worker after restart delivers exactly one logical reminder.
    rooms = ReminderRooms()
    await _reminder_sender(session_factory, rooms).run_once()
    assert rooms.notices == [
        ("room-1", "Reminder: the worker is still waiting for a reply on KAN-1.")
    ]
    # A second fresh worker and repeated reconciliation send nothing more.
    await _reminder_sender(session_factory, rooms).run_once()
    await _sync_at(session_factory, ticket_id, board, NOW)
    assert len(rooms.notices) == 1
    assert len(await _switch_reminder_rows(session_factory)) == 1


async def _waiting_switch_ticket(
    factory: async_sessionmaker[AsyncSession], **overrides: object
) -> str:
    values: dict[str, object] = {
        "status": "Waiting for input",
        "reporter_switch_user_id": "user-1",
        "wait_channel": "switch",
        "room_id": "room-1",
        "wait_anchor": NOW - timedelta(hours=3),
        "wait_fired": {},
    }
    values.update(overrides)
    ticket_id = await _seed(factory, **values)
    await _sync_at(factory, ticket_id, FakeAdminBoard(), NOW)
    rows = await _switch_reminder_rows(factory)
    assert len(rows) == 1 and rows[0].status == "pending"
    return ticket_id


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["send", "reply", "park", "completion"])
async def test_switch_reminder_refreshes_through_the_scheduler(
    session_factory: async_sessionmaker[AsyncSession], outcome: str
) -> None:
    await _waiting_switch_ticket(
        session_factory,
        card_event_id="card-room-1",
        jira_read_updated=NOW.isoformat(),
        jira_read_changelog_id="",
    )
    jira = JiraMock()
    jira.updated = NOW.isoformat()
    jira.status = "Waiting for input"
    rooms = ThreadRooms()
    if outcome == "reply":
        rooms.messages = [message(1, sender="user-1")]
    elif outcome == "park":
        jira.change("human-account")
    elif outcome == "completion":
        jira.change("worker-account")
        jira.status = "Done"
        jira.histories[-1]["items"] = [{"field": "status", "toString": "Done"}]

    def scheduler() -> JiraWorkerScheduler:
        return JiraWorkerScheduler(
            session_factory=session_factory,
            config=_worker_config(),
            poll_clients={"acme": client_for(jira)},
            ticket_rooms=rooms,
            clock=lambda: datetime.now(UTC),
        )

    if outcome == "send":
        jira.issue_read_started = asyncio.Event()
        jira.issue_read_release = asyncio.Event()
        first = asyncio.create_task(scheduler()._outbox.run_once())
        try:
            await asyncio.wait_for(jira.issue_read_started.wait(), timeout=5)
            await asyncio.wait_for(scheduler()._outbox.run_once(), timeout=5)
            assert rooms.notices == []
        finally:
            jira.issue_read_release.set()
            await first
    else:
        await scheduler()._outbox.run_once()
    await scheduler()._outbox.run_once()
    rows = await _switch_reminder_rows(session_factory)
    assert len(rows) == 1
    if outcome == "send":
        assert rooms.notices == [
            ("room-1", "Reminder: the worker is still waiting for a reply on KAN-1.")
        ]
        assert rows[0].status == "done"
        assert rows[0].attempts == 1
    else:
        assert rooms.notices == []
        assert rows[0].status == "cancelled"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change", ["anchor", "reply", "park", "completion", "disable", "cancel"]
)
async def test_switch_reminder_rechecks_state_after_refresh(
    session_factory: async_sessionmaker[AsyncSession], change: str
) -> None:
    ticket_id = await _waiting_switch_ticket(session_factory)
    config = _worker_config()

    async def refresh(refreshed_id: str) -> bool:
        assert refreshed_id == ticket_id
        async with session_factory() as session:
            ticket = await session.get(
                JiraWorkerTicket, ticket_id, with_for_update=True
            )
            assert ticket is not None
            if change == "anchor":
                ticket.wait_anchor = NOW
            elif change == "reply":
                await _input_event(
                    session_factory,
                    kind="thread_message",
                    received_at=NOW - timedelta(hours=1),
                    reporter_input=True,
                )
            elif change == "park":
                ticket.worker_parked_reason = "Human Jira action"
            elif change == "completion":
                ticket.status = "Done"
            elif change == "disable":
                config.jira_worker_enabled_projects = {}
            else:
                row = await session.scalar(
                    select(JiraWorkerOutbox).where(
                        JiraWorkerOutbox.command == "wait_reminder"
                    )
                )
                assert row is not None
                row.status = "cancelled"
            await session.commit()
        return False

    rooms = ReminderRooms()
    sender = JiraOutboxSender(
        session_factory=session_factory,
        config=config,
        clients={},
        clock=lambda: datetime.now(UTC),
        rooms=rooms,  # type: ignore[arg-type]
        before_wait_send=refresh,
    )
    await asyncio.wait_for(sender.run_once(), timeout=5)
    assert rooms.notices == []
    rows = await _switch_reminder_rows(session_factory)
    assert len(rows) == 1 and rows[0].status == "cancelled"


@pytest.mark.asyncio
async def test_switch_reminder_reply_cancels_before_send(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await _waiting_switch_ticket(session_factory)
    await _input_event(
        session_factory,
        kind="thread_message",
        received_at=NOW - timedelta(hours=1),
        reporter_input=True,
    )
    rooms = ReminderRooms()
    await _reminder_sender(session_factory, rooms).run_once()
    assert rooms.notices == []
    rows = await _switch_reminder_rows(session_factory)
    assert len(rows) == 1 and rows[0].status == "cancelled"


@pytest.mark.asyncio
async def test_switch_reminder_anchor_change_cancels_before_send(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    ticket_id = await _waiting_switch_ticket(session_factory)
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, ticket_id)
        assert ticket is not None
        ticket.wait_anchor = NOW
        await session.commit()
    rooms = ReminderRooms()
    await _reminder_sender(session_factory, rooms).run_once()
    assert rooms.notices == []
    rows = await _switch_reminder_rows(session_factory)
    assert len(rows) == 1 and rows[0].status == "cancelled"


@pytest.mark.asyncio
async def test_switch_reminder_park_cancels_before_send(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    ticket_id = await _waiting_switch_ticket(session_factory)
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, ticket_id)
        assert ticket is not None
        ticket.worker_parked_reason = "Human Jira action"
        await session.commit()
    rooms = ReminderRooms()
    await _reminder_sender(session_factory, rooms).run_once()
    assert rooms.notices == []
    rows = await _switch_reminder_rows(session_factory)
    assert len(rows) == 1 and rows[0].status == "cancelled"


@pytest.mark.asyncio
async def test_switch_reminder_completion_cancels_before_send(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    ticket_id = await _waiting_switch_ticket(session_factory)
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, ticket_id)
        assert ticket is not None
        ticket.status = "Done"
        await session.commit()
    rooms = ReminderRooms()
    await _reminder_sender(session_factory, rooms).run_once()
    assert rooms.notices == []
    rows = await _switch_reminder_rows(session_factory)
    assert len(rows) == 1 and rows[0].status == "cancelled"


@pytest.mark.asyncio
async def test_switch_reminder_disabled_project_is_not_sent(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await _waiting_switch_ticket(session_factory)
    rooms = ReminderRooms()
    await _reminder_sender(
        session_factory, rooms, jira_worker_enabled_projects={}
    ).run_once()
    assert rooms.notices == []
    rows = await _switch_reminder_rows(session_factory)
    assert len(rows) == 1 and rows[0].status == "pending"


@pytest.mark.asyncio
async def test_switch_reminder_restart_after_send_start_is_uncertain_without_replay(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await _waiting_switch_ticket(session_factory)
    async with session_factory() as session:
        row = (await _switch_reminder_rows(session_factory))[0]
        stored = await session.get(JiraWorkerOutbox, row.id)
        assert stored is not None
        stored.status = "sending"
        await session.commit()
    rooms = ReminderRooms()
    await _reminder_sender(session_factory, rooms).run_once()
    assert rooms.notices == []
    rows = await _switch_reminder_rows(session_factory)
    assert len(rows) == 1 and rows[0].status == "uncertain"
    assert "unknown outcome" in (rows[0].error or "")
    await _reminder_sender(session_factory, rooms).run_once()
    assert rooms.notices == []
    assert (await _switch_reminder_rows(session_factory))[0].status == "uncertain"


@pytest.mark.asyncio
async def test_switch_reminder_failure_is_uncertain_without_replay(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await _waiting_switch_ticket(session_factory)

    class FailingRooms(ReminderRooms):
        def __init__(self) -> None:
            super().__init__()
            self.calls = 0

        async def post_notice(self, room_id: str, *, body: str) -> str:
            self.calls += 1
            raise RuntimeError("room unavailable")

    rooms = FailingRooms()
    await _reminder_sender(session_factory, rooms).run_once()
    rows = await _switch_reminder_rows(session_factory)
    assert len(rows) == 1 and rows[0].status == "uncertain"
    assert "unknown" in (rows[0].error or "")
    await _reminder_sender(session_factory, rooms).run_once()
    assert rooms.calls == 1
    assert (await _switch_reminder_rows(session_factory))[0].status == "uncertain"


@pytest.mark.asyncio
async def test_concurrent_switch_reminder_senders_post_once(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await _waiting_switch_ticket(session_factory)

    class BlockingRooms(ReminderRooms):
        def __init__(self) -> None:
            super().__init__()
            self.entered = asyncio.Event()
            self.release = asyncio.Event()

        async def post_notice(self, room_id: str, *, body: str) -> str:
            self.notices.append((room_id, body))
            self.entered.set()
            await self.release.wait()
            return "reminder-event-1"

    rooms = BlockingRooms()
    first = asyncio.create_task(_reminder_sender(session_factory, rooms).run_once())
    await asyncio.wait_for(rooms.entered.wait(), timeout=5)
    await _reminder_sender(session_factory, rooms).run_once()
    rooms.release.set()
    await first
    await _reminder_sender(session_factory, rooms).run_once()
    assert len(rooms.notices) == 1
    rows = await _switch_reminder_rows(session_factory)
    assert len(rows) == 1 and rows[0].status == "done"


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["Waiting for input", "Waiting for approval"])
@pytest.mark.parametrize(
    "fired",
    [
        {},
        {
            "reminder": NOW.isoformat(),
            "admin": NOW.isoformat(),
            "blocked": NOW.isoformat(),
        },
    ],
)
async def test_parked_ticket_does_not_fire_wait_milestones(
    session_factory: async_sessionmaker[AsyncSession],
    status: str,
    fired: dict[str, str],
) -> None:
    ticket_id = await _seed(
        session_factory,
        status=status,
        reporter_switch_user_id="user-1",
        wait_channel="switch",
        room_id="room-1",
        wait_anchor=NOW - timedelta(hours=10),
        wait_fired=fired,
        worker_parked_reason="Human Jira action",
        jira_read_updated="2026-10-02T16:00:00.000+0000",
        jira_read_changelog_id="200",
    )
    board = FakeAdminBoard()
    await _sync_at(session_factory, ticket_id, board, NOW)
    assert board.reminders == []
    assert await _switch_reminder_rows(session_factory) == []
    ticket = await _ticket(session_factory, ticket_id)
    assert ("wait_admin" in (ticket.admin_reasons or {})) == ("admin" in fired)
    assert ("wait_blocked" in (ticket.admin_reasons or {})) == ("blocked" in fired)
    assert ticket.wait_fired == fired
    async with session_factory() as session:
        blocked = await session.scalar(
            select(JiraWorkerOutbox).where(JiraWorkerOutbox.command == "transition")
        )
    assert blocked is None
    if status == "Waiting for input":
        await _input_event(
            session_factory,
            kind="explicit_input",
            received_at=NOW - timedelta(hours=1),
        )
        await _sync_at(session_factory, ticket_id, board, NOW)
        ticket = await _ticket(session_factory, ticket_id)
        assert "wait_admin" not in (ticket.admin_reasons or {})
        assert "wait_blocked" not in (ticket.admin_reasons or {})


async def _wait_command(
    factory: async_sessionmaker[AsyncSession],
    *,
    issue_key: str,
    command_key: str,
    command: str,
    status: str,
) -> None:
    async with factory() as session:
        session.add(
            JiraWorkerOutbox(
                channel="jira",
                command=command,
                instance="acme",
                issue_key=issue_key,
                command_key=command_key,
                status=status,
                payload={
                    "data": "BLOCKED" if command == "transition" else "reminder",
                    "based_updated": "2026-10-02T16:00:00.000+0000",
                    "based_changelog_id": "200",
                },
            )
        )
        await session.commit()


@pytest.mark.asyncio
async def test_reply_cancels_pending_wait_commands(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    ticket_id = await _seed(
        session_factory,
        status="Waiting for input",
        reporter_switch_user_id="user-1",
        wait_channel="switch",
        room_id="room-1",
        wait_anchor=NOW - timedelta(hours=10),
        wait_fired={"blocked": NOW.isoformat()},
        jira_read_updated="2026-10-02T16:00:00.000+0000",
        jira_read_changelog_id="200",
    )
    await _wait_command(
        session_factory,
        issue_key="KAN-1",
        command_key=f"wait-blocked:{ticket_id}:old",
        command="transition",
        status="pending",
    )
    await _wait_command(
        session_factory,
        issue_key="KAN-1",
        command_key=f"wait-reminder:{ticket_id}:old",
        command="comment",
        status="pending",
    )
    await _wait_command(
        session_factory,
        issue_key="KAN-1",
        command_key=f"wait-blocked:{ticket_id}:claimed",
        command="transition",
        status="claimed",
    )
    await _wait_command(
        session_factory,
        issue_key="KAN-1",
        command_key=f"wait-blocked:{ticket_id}:sending",
        command="transition",
        status="sending",
    )
    await _wait_command(
        session_factory,
        issue_key="KAN-1",
        command_key=f"failure:{ticket_id}:transition",
        command="transition",
        status="pending",
    )
    await _input_event(
        session_factory,
        kind="thread_message",
        received_at=NOW - timedelta(hours=1),
        reporter_input=True,
    )
    await _sync_at(session_factory, ticket_id, FakeAdminBoard(), NOW)
    async with session_factory() as session:
        rows = list(await session.scalars(select(JiraWorkerOutbox)))
    by_key = {row.command_key: row.status for row in rows}
    assert by_key[f"wait-blocked:{ticket_id}:old"] == "cancelled"
    assert by_key[f"wait-reminder:{ticket_id}:old"] == "cancelled"
    assert by_key[f"wait-blocked:{ticket_id}:claimed"] == "cancelled"
    assert by_key[f"wait-blocked:{ticket_id}:sending"] == "sending"
    assert by_key[f"failure:{ticket_id}:transition"] == "pending"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "expected"),
    [
        ("Waiting for input", "cancelled"),
        ("Waiting for approval", "claimed"),
        ("Need input", "cancelled"),
    ],
)
async def test_accept_only_cancels_input_wait_commands(
    session_factory: async_sessionmaker[AsyncSession], status: str, expected: str
) -> None:
    config = _worker_config(
        jira_worker_statuses=JiraWorkerStatuses(
            waiting_for_input="Need input"
            if status == "Need input"
            else "Waiting for input"
        )
    )
    await _seed(
        session_factory,
        status=status,
        reporter_switch_user_id="user-1",
        wait_channel="switch",
        agent_state="waiting",
    )
    async with session_factory() as session:
        admin = User(name="Admin", email="accept-admin@example.invalid", role="admin")
        event = JiraWorkerEvent(
            instance="acme",
            idempotency_key="ignored-accept-cancel",
            issue_key="KAN-1",
            project_key="KAN",
            event_kind="ignored_input",
            payload={"untrusted_data": {"body": "use this"}},
        )
        session.add_all([admin, event])
        await session.commit()
        await _wait_command(
            session_factory,
            issue_key="KAN-1",
            command_key="wait-blocked:accept:claimed",
            command="transition",
            status="claimed",
        )
        await _wait_command(
            session_factory,
            issue_key="KAN-1",
            command_key="wait-reminder:accept:sending",
            command="comment",
            status="sending",
        )
        await accept_ignored_input(
            session,
            event_id=event.id,
            admin_user_id=admin.id,
            config=config,
        )
        await session.commit()
    async with session_factory() as session:
        rows = list(await session.scalars(select(JiraWorkerOutbox)))
    by_key = {row.command_key: row.status for row in rows}
    assert by_key["wait-blocked:accept:claimed"] == expected
    assert by_key["wait-reminder:accept:sending"] == "sending"


@pytest.mark.asyncio
async def test_leaving_wait_cancels_pending_wait_commands(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    ticket_id = await _seed(
        session_factory,
        status="In Progress",
        reporter_switch_user_id="user-1",
        wait_channel="switch",
        room_id="room-1",
        wait_anchor=NOW - timedelta(hours=10),
        wait_fired={"blocked": NOW.isoformat()},
    )
    await _wait_command(
        session_factory,
        issue_key="KAN-1",
        command_key=f"wait-blocked:{ticket_id}:old",
        command="transition",
        status="pending",
    )
    await _sync_at(session_factory, ticket_id, FakeAdminBoard(), NOW)
    async with session_factory() as session:
        row = await session.scalar(select(JiraWorkerOutbox))
    assert row is not None
    assert row.status == "cancelled"
    assert (await _ticket(session_factory, ticket_id)).wait_anchor is None


@pytest.mark.asyncio
async def test_approval_room_message_does_not_cancel_wait_commands(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    ticket_id = await _seed(
        session_factory,
        status="Waiting for approval",
        reporter_switch_user_id="user-1",
        wait_channel="switch",
        room_id="room-1",
        wait_anchor=NOW,
        wait_fired={},
    )
    await _wait_command(
        session_factory,
        issue_key="KAN-1",
        command_key=f"wait-blocked:{ticket_id}:old",
        command="transition",
        status="pending",
    )
    await _input_event(
        session_factory,
        kind="thread_message",
        received_at=NOW + timedelta(minutes=5),
        reporter_input=True,
    )
    await _sync_at(
        session_factory, ticket_id, FakeAdminBoard(), NOW + timedelta(minutes=10)
    )
    async with session_factory() as session:
        row = await session.scalar(select(JiraWorkerOutbox))
    assert row is not None
    assert row.status == "pending"


@pytest.mark.asyncio
async def test_card_failure_does_not_repeat_a_switch_reminder(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    ticket_id = await _seed(
        session_factory,
        status="Waiting for input",
        reporter_switch_user_id="user-1",
        wait_channel="switch",
        room_id="room-1",
        admin_card_event_id="admin-card-1",
        wait_anchor=NOW - timedelta(hours=3),
        wait_fired={},
    )
    board = FakeAdminBoard()
    board.cards["admin-card-1"] = "old"
    missing = _worker_config(jira_worker_credentials={})
    with pytest.raises(ValueError):
        await sync_admin_card(
            session_factory,
            engine=await _engine(session_factory),
            board=board,
            config=missing,
            ticket_id=ticket_id,
            now=NOW,
        )
    # The milestone and its outbox row commit with the wait state even though
    # the card render failed; delivery happens through the sender, once.
    assert "reminder" in ((await _ticket(session_factory, ticket_id)).wait_fired or {})
    assert len(await _switch_reminder_rows(session_factory)) == 1
    await sync_admin_card(
        session_factory,
        engine=await _engine(session_factory),
        board=board,
        config=_worker_config(),
        ticket_id=ticket_id,
        now=NOW + timedelta(minutes=5),
    )
    assert "reminder" in ((await _ticket(session_factory, ticket_id)).wait_fired or {})
    assert len(await _switch_reminder_rows(session_factory)) == 1
    rooms = ReminderRooms()
    await _reminder_sender(session_factory, rooms).run_once()
    assert len(rooms.notices) == 1
    await sync_admin_card(
        session_factory,
        engine=await _engine(session_factory),
        board=board,
        config=_worker_config(),
        ticket_id=ticket_id,
        now=NOW + timedelta(minutes=10),
    )
    assert len(await _switch_reminder_rows(session_factory)) == 1
    await _reminder_sender(session_factory, rooms).run_once()
    assert len(rooms.notices) == 1


@pytest.mark.asyncio
async def test_one_card_failure_does_not_stop_the_others(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await _seed(session_factory, issue_key="KAN-1", first_seen_at=NOW)
    await _seed(
        session_factory,
        issue_key="KAN-2",
        first_seen_at=NOW + timedelta(seconds=1),
    )
    board = FakeAdminBoard()
    real_post = board.post_card

    async def post_card(room_id: str, *, body: str) -> str:
        if "KAN-2" in body:
            raise RuntimeError("jira agent down")
        return await real_post(room_id, body=body)

    board.post_card = post_card  # type: ignore[method-assign]
    done = await reconcile_admin_cards(
        session_factory,
        engine=await _engine(session_factory),
        board=board,
        config=_worker_config(),
        now=NOW,
    )
    assert done == 1
    assert board.posts == ["admin-card-1"]
    assert "KAN-1" in board.cards["admin-card-1"]


@pytest.mark.asyncio
async def test_admin_override_writes_a_new_event(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await _seed(
        session_factory,
        reporter_switch_user_id="user-1",
        wait_channel="switch",
        agent_state="waiting",
    )
    async with session_factory() as session:
        admin = User(name="Admin", email="admin@example.invalid", role="admin")
        stranger = User(name="Ada", email="ada@example.invalid", role="user")
        event = JiraWorkerEvent(
            instance="acme",
            idempotency_key="ignored-1",
            issue_key="KAN-1",
            project_key="KAN",
            event_kind="ignored_input",
            payload={"untrusted_data": {"body": "not an answer"}},
        )
        session.add_all([admin, stranger, event])
        await session.commit()
        admin_id, stranger_id, event_id = admin.id, stranger.id, event.id
        original = dict(event.payload)
    async with session_factory() as session:
        with pytest.raises(PermissionError):
            await accept_ignored_input(
                session,
                event_id=event_id,
                admin_user_id=stranger_id,
                config=_worker_config(),
            )
        new_id = await accept_ignored_input(
            session,
            event_id=event_id,
            admin_user_id=admin_id,
            config=_worker_config(),
        )
        await session.commit()
    async with session_factory() as session:
        old = await session.get(JiraWorkerEvent, event_id)
        new = await session.get(JiraWorkerEvent, new_id)
        assert old is not None and new is not None
        assert old.event_kind == "ignored_input"
        assert old.payload == original
        assert new.event_kind == "explicit_input"
        assert new.payload["accepted_from"] == event_id
        assert new.payload["reporter_input"] is True
        assert new.id != old.id
        ticket = await session.scalar(select(JiraWorkerTicket))
        assert ticket is not None
        assert ticket.agent_state == "wake_requested"


@pytest.mark.asyncio
async def test_admin_room_is_created_by_marker_not_name(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    created: list[RoomCreateConfig] = []

    class Rooms:
        def __init__(self, factory: async_sessionmaker[AsyncSession]) -> None:
            self.factory = factory

        async def create_room(self, config: RoomCreateConfig) -> Any:
            created.append(config)
            async with self.factory() as session:
                room = Room(
                    matrix_room_id=f"!admin-{uuid.uuid4().hex[:8]}:test",
                    name=config.name,
                    description=config.description,
                    owner_id=config.owner_id,
                    read_visibility=config.read_visibility,
                    write_visibility=config.write_visibility,
                    metadata_=config.metadata,
                    external_channel_id=None if config.internal_only else "channel-1",
                    channel_type=None if config.internal_only else "public",
                )
                session.add(room)
                await session.commit()
                return SimpleNamespace(room=room)

    async with session_factory() as session:
        admin = User(
            name="Admin",
            email="admin@example.com",
            role="admin",
        )
        session.add(admin)
        session.add(
            Room(
                matrix_room_id="!decoy:test",
                name="Jira admin",
                description="same name, no marker",
                metadata_={"other": "1"},
            )
        )
        await session.commit()
        admin_id = admin.id
    board = SwitchAdminBoard(
        room_service=Rooms(session_factory),  # type: ignore[arg-type]
        protocol=None,  # type: ignore[arg-type]
        session_factory=session_factory,
        agent_store=None,  # type: ignore[arg-type]
        config=_worker_config(),
    )
    first = await board.ensure_room()
    second = await board.ensure_room()
    assert first == second
    assert len(created) == 1
    assert created[0].internal_only is False
    assert created[0].owner_id == admin_id
    assert created[0].read_visibility == "private"
    assert created[0].metadata == {ADMIN_ROOM_MARKER: "1"}
    assert created[0].name == "Jira admin"
    async with session_factory() as session:
        room = await session.get(Room, first)
    assert room is not None
    assert room.external_channel_id == "channel-1"
    assert room.channel_type == "public"
    async with session_factory() as session:
        room = await session.get(Room, first)
        assert room is not None
        room.owner_id = None
        await session.commit()
    assert await board.ensure_room() == first
    assert len(created) == 1
    async with session_factory() as session:
        room = await session.get(Room, first)
    assert room is not None and room.owner_id == admin_id
    async with session_factory() as session:
        decoy = await session.scalar(
            select(Room).where(Room.matrix_room_id == "!decoy:test")
        )
        assert decoy is not None
        assert decoy.id != first


@pytest.mark.asyncio
class _RoomBoard(FakeAdminBoard):
    def __init__(self, factory: async_sessionmaker[AsyncSession], room_id: str) -> None:
        super().__init__()
        self.factory = factory
        self.room_id = room_id
        self._seq = 0

    async def ensure_room(self) -> str:
        return self.room_id

    async def post_card(self, room_id: str, *, body: str) -> str:
        return await self._insert(room_id, body, alert=False)

    async def edit_card(self, room_id: str, event_id: str, *, body: str) -> None:
        self.cards[event_id] = body
        self.edits.append((event_id, body))
        async with self.factory() as session:
            message = await session.scalar(
                select(Message).where(Message.transport_event_id == event_id)
            )
            if message is not None:
                message.body = body
                await session.commit()

    async def post_alert(self, room_id: str, *, body: str) -> None:
        await self._insert(room_id, body, alert=True)

    async def _insert(self, room_id: str, body: str, *, alert: bool) -> str:
        self._seq += 1
        event_id = f"room-msg-{self._seq}"
        async with self.factory() as session:
            session.add(
                Message(
                    seq=self._seq,
                    room_id=room_id,
                    transport_event_id=event_id,
                    sender_id="jira",
                    event_type="m.room.message",
                    body=body,
                    content={"body": body},
                )
            )
            await session.commit()
        self.cards[event_id] = body
        if alert:
            self.alerts.append((room_id, body))
        else:
            self.posts.append(event_id)
        return event_id


async def _alert_messages(
    factory: async_sessionmaker[AsyncSession], room_id: str
) -> list[str]:
    async with factory() as session:
        bodies = list(
            (
                await session.scalars(
                    select(Message.body)
                    .where(Message.room_id == room_id)
                    .order_by(Message.seq)
                )
            ).all()
        )
    return [body for body in bodies if body and "Card updated." in body]


@pytest.mark.asyncio
async def test_new_reason_posts_one_alert_and_clear_posts_none(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    ticket_id = await _seed(session_factory)
    async with session_factory() as session:
        room = Room(
            matrix_room_id="!alerts:test",
            name="Jira admin",
            description="alerts",
            external_channel_id="channel-1",
            channel_type="public",
        )
        session.add(room)
        await session.commit()
        room_id = room.id
    board = _RoomBoard(session_factory, room_id)
    missing = _worker_config(jira_worker_credentials={})
    with pytest.raises(ValueError):
        await sync_admin_card(
            session_factory,
            engine=await _engine(session_factory),
            board=board,
            config=missing,
            ticket_id=ticket_id,
            now=NOW,
        )
    assert await _alert_messages(session_factory, room_id) == []
    await sync_admin_card(
        session_factory,
        engine=await _engine(session_factory),
        board=board,
        config=_worker_config(),
        ticket_id=ticket_id,
        now=NOW,
    )
    alerts = await _alert_messages(session_factory, room_id)
    assert len(alerts) == 1
    assert alerts[0].startswith("KAN-1: unmapped_reporter. Card updated.")
    await sync_admin_card(
        session_factory,
        engine=await _engine(session_factory),
        board=board,
        config=_worker_config(),
        ticket_id=ticket_id,
        now=NOW,
    )
    assert len(await _alert_messages(session_factory, room_id)) == 1
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, ticket_id)
        assert ticket is not None
        ticket.reporter_switch_user_id = "user-1"
        await session.commit()
    await sync_admin_card(
        session_factory,
        engine=await _engine(session_factory),
        board=board,
        config=_worker_config(),
        ticket_id=ticket_id,
        now=NOW,
    )
    assert len(await _alert_messages(session_factory, room_id)) == 1


@pytest.mark.asyncio
async def test_set_admin_card_reason_posts_one_alert(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await _seed(
        session_factory,
        reporter_switch_user_id="user-1",
        wait_channel="switch",
        admin_card_event_id="card-1",
    )
    async with session_factory() as session:
        room = Room(
            matrix_room_id="!intake:test",
            name="Jira admin",
            description="alerts",
            external_channel_id="channel-1",
            channel_type="public",
        )
        session.add(room)
        await session.commit()
        room_id = room.id
    board = _RoomBoard(session_factory, room_id)
    board.cards["card-1"] = "old"
    await set_admin_card_reason(
        session_factory,
        engine=await _engine(session_factory),
        board=board,
        config=_worker_config(),
        instance="acme",
        issue_key="KAN-1",
        reason=INTAKE_REASON,
        detail="6 questions",
        now=NOW,
    )
    alerts = await _alert_messages(session_factory, room_id)
    assert len(alerts) == 1
    assert "intake_questions" in alerts[0]
    await set_admin_card_reason(
        session_factory,
        engine=await _engine(session_factory),
        board=board,
        config=_worker_config(),
        instance="acme",
        issue_key="KAN-1",
        reason=INTAKE_REASON,
        detail=None,
        now=NOW,
    )
    assert len(await _alert_messages(session_factory, room_id)) == 1
    await set_admin_card_reason(
        session_factory,
        engine=await _engine(session_factory),
        board=board,
        config=_worker_config(),
        instance="acme",
        issue_key="KAN-1",
        reason=INTAKE_REASON,
        detail="7 questions",
        now=NOW,
    )
    assert len(await _alert_messages(session_factory, room_id)) == 2


@pytest.mark.asyncio
async def test_alert_failure_still_sends_the_reminder_once(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    ticket_id = await _seed(
        session_factory,
        status="Waiting for input",
        reporter_switch_user_id="user-1",
        wait_channel="switch",
        room_id="room-1",
        admin_card_event_id="admin-card-1",
        wait_anchor=NOW - timedelta(hours=5),
        wait_fired={},
    )

    class FailingAlertBoard(FakeAdminBoard):
        fail = True

        async def post_alert(self, room_id: str, *, body: str) -> None:
            if self.fail:
                raise RuntimeError("admin room unavailable")
            await super().post_alert(room_id, body=body)

    board = FailingAlertBoard()
    board.cards["admin-card-1"] = "old"
    with pytest.raises(RuntimeError):
        await sync_admin_card(
            session_factory,
            engine=await _engine(session_factory),
            board=board,
            config=_worker_config(),
            ticket_id=ticket_id,
            now=NOW,
        )
    assert "reminder" in ((await _ticket(session_factory, ticket_id)).wait_fired or {})
    assert len(await _switch_reminder_rows(session_factory)) == 1
    rooms = ReminderRooms()
    await _reminder_sender(session_factory, rooms).run_once()
    assert len(rooms.notices) == 1
    board.fail = False
    await sync_admin_card(
        session_factory,
        engine=await _engine(session_factory),
        board=board,
        config=_worker_config(),
        ticket_id=ticket_id,
        now=NOW + timedelta(minutes=1),
    )
    assert len(await _switch_reminder_rows(session_factory)) == 1
    await _reminder_sender(session_factory, rooms).run_once()
    assert len(rooms.notices) == 1
    assert len(board.alerts) == 1


@pytest.mark.asyncio
async def test_wait_send_hook_cancels_the_room_reminder(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    ticket_id = await _seed(
        session_factory,
        status="Waiting for input",
        reporter_switch_user_id="user-1",
        wait_channel="switch",
        room_id="room-1",
        admin_card_event_id="admin-card-1",
        wait_anchor=NOW - timedelta(hours=3),
        wait_fired={},
    )
    calls: list[str] = []

    async def refresh(refreshed_id: str) -> bool:
        calls.append(refreshed_id)
        return True

    board = FakeAdminBoard()
    board.cards["admin-card-1"] = "old"
    await sync_admin_card(
        session_factory,
        engine=await _engine(session_factory),
        board=board,
        config=_worker_config(),
        ticket_id=ticket_id,
        now=NOW,
    )
    rows = await _switch_reminder_rows(session_factory)
    assert len(rows) == 1 and rows[0].status == "pending"
    rooms = ReminderRooms()
    await _reminder_sender(session_factory, rooms, hook=refresh).run_once()
    assert calls == [ticket_id]
    assert rooms.notices == []
    rows = await _switch_reminder_rows(session_factory)
    assert len(rows) == 1 and rows[0].status == "cancelled"


@pytest.mark.asyncio
async def test_reply_before_reminder_send_is_not_posted(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    ticket_id = await _seed(
        session_factory,
        status="Waiting for input",
        reporter_switch_user_id="user-1",
        wait_channel="switch",
        room_id="room-1",
        admin_card_event_id="admin-card-1",
        wait_anchor=NOW - timedelta(hours=3),
        wait_fired={},
    )

    class ReplyBoard(FakeAdminBoard):
        async def edit_card(self, room_id: str, event_id: str, *, body: str) -> None:
            await super().edit_card(room_id, event_id, body=body)
            await _input_event(
                session_factory,
                kind="explicit_input",
                received_at=NOW - timedelta(hours=1),
            )

    board = ReplyBoard()
    board.cards["admin-card-1"] = "old"
    await sync_admin_card(
        session_factory,
        engine=await _engine(session_factory),
        board=board,
        config=_worker_config(),
        ticket_id=ticket_id,
        now=NOW,
    )
    # The reply landed after the milestone was queued but before any send,
    # so the sender must cancel the reminder instead of posting it.
    rows = await _switch_reminder_rows(session_factory)
    assert len(rows) == 1
    rooms = ReminderRooms()
    await _reminder_sender(session_factory, rooms).run_once()
    assert rooms.notices == []
    rows = await _switch_reminder_rows(session_factory)
    assert len(rows) == 1 and rows[0].status == "cancelled"


@pytest.mark.asyncio
async def test_nontransient_failure_posts_and_clears_admin_reasons(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    ticket_id = await _seed(session_factory, reporter_switch_user_id="user-1")
    jira = JiraMock()
    command_id = await enqueue(
        session_factory, command="transition", data="In Progress"
    )
    sender = sender_for(session_factory, jira)
    jira.unavailable_transition = True
    await sender.run_once()
    async with session_factory() as session:
        failed = await session.get(JiraWorkerOutbox, command_id)
        assert failed is not None and failed.status == "failed"
        assert failed.attempts == 1
    jira.unavailable_transition = False
    await sender.run_once()
    await sender.run_once()
    assert jira.status == "BLOCKED"

    board = FakeAdminBoard()
    await _sync_at(session_factory, ticket_id, board, NOW)
    stored = await _ticket(session_factory, ticket_id)
    assert set(stored.admin_reasons or {}) == {"parked", "blocked_writes"}
    assert len(board.posts) == 1
    async with session_factory() as session:
        stored = await session.get(JiraWorkerTicket, ticket_id)
        assert stored is not None
        stored.worker_parked_reason = None
        await session.commit()
    await _sync_at(session_factory, ticket_id, board, NOW)
    assert (await _ticket(session_factory, ticket_id)).admin_reasons is None
    assert "- none" in board.cards["admin-card-1"]


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["Waiting for input", "Waiting for approval"])
async def test_accept_repeat_preserves_a_later_wait(
    session_factory: async_sessionmaker[AsyncSession], status: str
) -> None:
    ticket_id = await _seed(
        session_factory,
        reporter_switch_user_id="user-1",
        wait_channel="switch",
        room_id="room-1",
        agent_state="waiting",
    )
    config = _worker_config()
    async with session_factory() as session:
        admin = User(name="Admin", email="retry-admin@example.invalid", role="admin")
        event = JiraWorkerEvent(
            instance="acme",
            idempotency_key="ignored-retry",
            issue_key="KAN-1",
            project_key="KAN",
            event_kind="ignored_input",
        )
        session.add_all([admin, event])
        await session.commit()
        admin_id, event_id = admin.id, event.id
        accepted_id = await accept_ignored_input(
            session, event_id=event_id, admin_user_id=admin_id, config=config
        )
        await session.commit()

    anchor = datetime.now(UTC) + timedelta(hours=1)
    fired = {
        "reminder": anchor.isoformat(),
        "admin": anchor.isoformat(),
        "blocked": anchor.isoformat(),
    }
    async with session_factory() as session:
        stored = await session.get(JiraWorkerTicket, ticket_id)
        assert stored is not None
        stored.status = status
        stored.agent_state = "waiting"
        stored.wait_anchor = anchor
        stored.wait_fired = fired
        await session.commit()
    for prefix, command in (
        ("wait-reminder", "comment"),
        ("wait-blocked", "transition"),
    ):
        await _wait_command(
            session_factory,
            issue_key="KAN-1",
            command_key=f"{prefix}:{ticket_id}:later",
            command=command,
            status="pending",
        )
    async with session_factory() as session:
        assert (
            await accept_ignored_input(
                session, event_id=event_id, admin_user_id=admin_id, config=config
            )
            == accepted_id
        )
        await session.commit()
    await _sync_at(session_factory, ticket_id, FakeAdminBoard(), anchor)
    stored = await _ticket(session_factory, ticket_id)
    assert stored.agent_state == "waiting"
    assert stored.wait_anchor == anchor
    assert stored.wait_fired == fired
    async with session_factory() as session:
        commands = list(
            await session.scalars(
                select(JiraWorkerOutbox).where(JiraWorkerOutbox.channel == "jira")
            )
        )
        explicit = list(
            await session.scalars(
                select(JiraWorkerEvent).where(
                    JiraWorkerEvent.event_kind == "explicit_input"
                )
            )
        )
    assert len(explicit) == 1
    assert len(commands) == 2
    assert all(row.status == "pending" for row in commands)


@pytest.mark.asyncio
async def test_reconcile_stops_visiting_closed_tickets_after_final_update(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    active = await _seed(session_factory)
    closed: list[str] = []
    for number, (status, reasons) in enumerate(
        [("Done", None), (" cancelled ", {}), ("CANCELED", null())], start=2
    ):
        closed.append(
            await _seed(
                session_factory,
                issue_key=f"KAN-{number}",
                status=status,
                admin_reasons=reasons,
            )
        )
    board = FakeAdminBoard()
    config = _worker_config()
    engine = session_factory.kw["bind"]

    async def reconcile() -> int:
        return await reconcile_admin_cards(
            session_factory, engine=engine, board=board, config=config, now=NOW
        )

    assert await reconcile() == 1
    assert len(board.posts) == 1
    async with session_factory() as session:
        stored = await session.get(JiraWorkerTicket, active)
        assert stored is not None
        stored.status = " Done "
        stored.worker_parked_reason = "Human Jira action"
        session.add(
            JiraWorkerOutbox(
                channel="jira",
                command="comment",
                instance="acme",
                issue_key="KAN-1",
                status="failed",
                attempts=3,
                payload={},
            )
        )
        session.add(
            JiraWorkerOutbox(
                channel="switch",
                command=ADMIN_CARD_COMMAND,
                instance="acme",
                issue_key="KAN-2",
                status="pending",
                payload={"ticket_id": closed[0]},
            )
        )
        await session.commit()
    assert await reconcile() == 2
    assert (await _ticket(session_factory, active)).admin_reasons is None
    assert "- none" in board.cards["admin-card-1"]
    assert await reconcile() == 0
    async with session_factory() as session:
        follow_up = await session.scalar(
            select(JiraWorkerOutbox).where(
                JiraWorkerOutbox.command == ADMIN_CARD_COMMAND
            )
        )
        assert follow_up is not None and follow_up.status == "done"
        stored = await session.get(JiraWorkerTicket, closed[0])
        assert stored is not None
        stored.status = "In Progress"
        await session.commit()
    assert await reconcile() == 1
    assert len(board.posts) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["room", "credentials", "edit"])
async def test_closed_card_edit_remains_pending_after_failure(
    session_factory: async_sessionmaker[AsyncSession], failure: str
) -> None:
    class FailingBoard(FakeAdminBoard):
        fail = False

        async def ensure_room(self) -> str:
            if self.fail and failure == "room":
                raise RuntimeError("Admin room unavailable")
            return await super().ensure_room()

        async def edit_card(self, room_id: str, event_id: str, *, body: str) -> None:
            if self.fail and failure == "edit":
                raise RuntimeError("Agent client not running")
            await super().edit_card(room_id, event_id, body=body)

    ticket_id = await _seed(session_factory)
    board = FailingBoard()
    config = _worker_config()
    engine = session_factory.kw["bind"]
    assert (
        await reconcile_admin_cards(
            session_factory, engine=engine, board=board, config=config, now=NOW
        )
        == 1
    )
    original_body = board.cards["admin-card-1"]
    async with session_factory() as session:
        stored = await session.get(JiraWorkerTicket, ticket_id)
        assert stored is not None
        stored.status = "Done"
        await session.commit()
    board.fail = True
    failing_config = (
        _worker_config(jira_worker_credentials={})
        if failure == "credentials"
        else config
    )
    for _ in range(2):
        assert (
            await reconcile_admin_cards(
                session_factory,
                engine=engine,
                board=board,
                config=failing_config,
                now=NOW,
            )
            == 0
        )
        assert (await _ticket(session_factory, ticket_id)).admin_reasons is None
        async with session_factory() as session:
            pending = list(
                await session.scalars(
                    select(JiraWorkerOutbox).where(
                        JiraWorkerOutbox.command == ADMIN_CARD_COMMAND,
                        JiraWorkerOutbox.status == "pending",
                    )
                )
            )
        assert len(pending) == 1
        assert pending[0].payload["ticket_id"] == ticket_id
        assert board.cards["admin-card-1"] == original_body

    restarted = FakeAdminBoard()
    restarted.cards = dict(board.cards)
    assert (
        await reconcile_admin_cards(
            session_factory, engine=engine, board=restarted, config=config, now=NOW
        )
        == 1
    )
    assert restarted.posts == []
    assert len(restarted.edits) == 1
    assert "Status: Done" in restarted.cards["admin-card-1"]
    assert "Reasons:\n- none" in restarted.cards["admin-card-1"]
    assert (
        await _ticket(session_factory, ticket_id)
    ).admin_card_event_id == "admin-card-1"
    assert (
        await reconcile_admin_cards(
            session_factory, engine=engine, board=restarted, config=config, now=NOW
        )
        == 0
    )
    async with session_factory() as session:
        follow_up = await session.scalar(
            select(JiraWorkerOutbox).where(
                JiraWorkerOutbox.command == ADMIN_CARD_COMMAND
            )
        )
        assert follow_up is not None and follow_up.status == "done"


@pytest.mark.asyncio
@pytest.mark.parametrize("source", ["poll", "webhook"])
@pytest.mark.parametrize("status", ["Done", " cancelled ", "CANCELED"])
async def test_close_refreshes_a_card_without_reasons(
    session_factory: async_sessionmaker[AsyncSession], source: str, status: str
) -> None:
    ticket_id = await _seed(session_factory)
    board = FakeAdminBoard()
    config = _worker_config()
    engine = session_factory.kw["bind"]
    await _sync_at(session_factory, ticket_id, board, NOW)
    async with session_factory() as session:
        stored = await session.get(JiraWorkerTicket, ticket_id)
        assert stored is not None
        stored.reporter_switch_user_id = "user-1"
        await session.commit()
    await _sync_at(session_factory, ticket_id, board, NOW)
    assert (await _ticket(session_factory, ticket_id)).admin_reasons is None
    assert "Status: In Progress" in board.cards["admin-card-1"]

    issue = {
        "id": "10001",
        "key": "KAN-1",
        "fields": {
            "project": {"key": "KAN"},
            "status": {"name": status},
            "reporter": {"accountId": "reporter-account"},
            "summary": "Closed ticket",
            "updated": (NOW + timedelta(minutes=1)).isoformat(),
        },
    }
    async with session_factory() as session:
        if source == "poll":
            assert await record_polled_issue(session, issue, instance="acme")
        else:
            assert await record_webhook_event(
                session,
                replace(
                    _webhook_event(),
                    status=status,
                    summary="Closed ticket",
                    raw={"issue": issue},
                ),
                instance="acme",
                enabled_projects=["KAN"],
                webhook_identifier="closed-ticket",
            )
        await session.commit()
        pending = list(
            await session.scalars(
                select(JiraWorkerOutbox).where(
                    JiraWorkerOutbox.command == ADMIN_CARD_COMMAND,
                    JiraWorkerOutbox.status == "pending",
                )
            )
        )
        assert len(pending) == 1

    assert (
        await reconcile_admin_cards(
            session_factory, engine=engine, board=board, config=config, now=NOW
        )
        == 1
    )
    assert len(board.posts) == 1
    assert f"Status: {status}" in board.cards["admin-card-1"]
    assert "[KAN-1] Closed ticket" in board.cards["admin-card-1"]
    assert "Reasons:\n- none" in board.cards["admin-card-1"]
    async with session_factory() as session:
        reporter = User(name="Reporter", email="reporter@example.invalid", role="user")
        session.add(reporter)
        await session.flush()
        _, changed = await set_identity_mapping(
            session,
            instance="acme",
            jira_account_id="reporter-account",
            switch_user_id=reporter.id,
            enabled_projects=["KAN"],
        )
        assert changed == 0
        await session.commit()
    assert (
        await reconcile_admin_cards(
            session_factory, engine=engine, board=board, config=config, now=NOW
        )
        == 0
    )


@pytest.mark.asyncio
async def test_scheduler_reuses_engine_without_a_session_for_card_reconciliation(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    class CountingSessions(async_sessionmaker[AsyncSession]):
        calls = 0

        def __call__(self, **local_kw: Any) -> AsyncSession:
            self.calls += 1
            return super().__call__(**local_kw)

    sessions = CountingSessions(bind=session_factory.kw["bind"], expire_on_commit=False)
    scheduler = JiraWorkerScheduler(
        session_factory=sessions,
        config=_worker_config(jira_worker_enabled_projects={}),
        admin_board=FakeAdminBoard(),
    )
    await scheduler.run_once()
    assert sessions.calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("day_hours", [6, 8])
async def test_wait_thresholds_follow_the_configured_work_day(
    session_factory: async_sessionmaker[AsyncSession], day_hours: int
) -> None:
    anchor = datetime(2026, 10, 5, 9, tzinfo=UTC)
    ticket_id = await _seed(
        session_factory,
        status="Waiting for input",
        reporter_switch_user_id="user-1",
        wait_channel="switch",
        room_id="room-1",
        wait_anchor=anchor,
        wait_fired={},
        jira_read_updated="2026-10-02T16:00:00.000+0000",
        jira_read_changelog_id="200",
    )
    board = FakeAdminBoard()
    config = _worker_config(
        jira_worker_calendar=JiraWorkerCalendar(end_hour=9 + day_hours)
    )
    engine = session_factory.kw["bind"]

    async def sync(hours: float) -> JiraWorkerTicket:
        await sync_admin_card(
            session_factory,
            engine=engine,
            board=board,
            config=config,
            ticket_id=ticket_id,
            now=anchor + timedelta(hours=hours),
        )
        return await _ticket(session_factory, ticket_id)

    assert (await sync(2 - 1 / 60)).wait_fired == {}
    assert "reminder" in ((await sync(2)).wait_fired or {})
    rooms = ReminderRooms()
    await _reminder_sender(session_factory, rooms).run_once()
    assert len(rooms.notices) == 1
    assert "wait_admin" not in ((await sync(4 - 1 / 60)).admin_reasons or {})
    assert "wait_admin" in ((await sync(4)).admin_reasons or {})
    assert "wait_blocked" not in ((await sync(day_hours - 1 / 60)).admin_reasons or {})
    assert "wait_blocked" in ((await sync(day_hours)).admin_reasons or {})
    async with session_factory() as session:
        blocked = await session.scalar(
            select(JiraWorkerOutbox).where(JiraWorkerOutbox.command == "transition")
        )
        assert blocked is not None and blocked.payload["data"] == "BLOCKED"


@pytest.mark.asyncio
async def test_admin_card_columns_migrate(postgres_url: str) -> None:
    admin = create_async_engine(postgres_url, isolation_level="AUTOCOMMIT")
    async with admin.connect() as connection:
        await connection.execute(text('DROP DATABASE IF EXISTS "jira_admin_cards_mig"'))
        await connection.execute(text('CREATE DATABASE "jira_admin_cards_mig"'))
    await admin.dispose()
    base, _, _ = postgres_url.rpartition("/")
    url = f"{base}/jira_admin_cards_mig"
    engine = create_async_engine(url)
    try:
        async with engine.begin() as connection:
            await connection.run_sync(lambda c: _run_migrations(c, "head"))
            cols = set(
                (
                    await connection.execute(
                        text(
                            "SELECT column_name FROM information_schema.columns"
                            " WHERE table_name = 'jira_worker_ticket_map'"
                        )
                    )
                )
                .scalars()
                .all()
            )
            assert {
                "admin_card_event_id",
                "admin_reasons",
                "wait_anchor",
                "wait_fired",
            } <= cols
            await connection.run_sync(lambda c: _run_migrations(c, "c8d1e2f3a4b5"))
            cols = set(
                (
                    await connection.execute(
                        text(
                            "SELECT column_name FROM information_schema.columns"
                            " WHERE table_name = 'jira_worker_ticket_map'"
                        )
                    )
                )
                .scalars()
                .all()
            )
            assert "admin_card_event_id" not in cols
    finally:
        await engine.dispose()
        admin = create_async_engine(postgres_url, isolation_level="AUTOCOMMIT")
        async with admin.connect() as connection:
            await connection.execute(
                text('DROP DATABASE IF EXISTS "jira_admin_cards_mig"')
            )
        await admin.dispose()
