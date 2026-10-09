"""Admin cards, wait timers, and message-edit consumers. Jira is not called."""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy import select, text
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
from switch_core.bridges.jira.rooms import (
    ADMIN_CARD_COMMAND as ROOMS_ADMIN_CARD_COMMAND,
)
from switch_core.config import JiraWorkerCalendar, JiraWorkerCredentials
from switch_core.db.models import (
    Agent,
    ApiKey,
    Client,
    JiraWorkerEvent,
    JiraWorkerOutbox,
    JiraWorkerTicket,
    Message,
    Room,
    User,
)
from switch_core.db.stores.room_store import RoomStore
from switch_core.room_service import RoomCreateConfig
from tests.switch_core.bridges.jira.test_worker import _config, _run_migrations

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
    assert "parked:" not in await sync()
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, ticket_id)
        assert ticket is not None
        ticket.worker_parked_reason = None
        await session.commit()
    await sync()

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
    assert board.reminders == [
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
    assert board.reminders
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
    assert board.reminders == [
        ("room-1", "Reminder: the worker is still waiting for a reply on KAN-1.")
    ]


@pytest.mark.asyncio
async def test_parked_ticket_does_not_fire_wait_milestones(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    ticket_id = await _seed(
        session_factory,
        status="Waiting for input",
        reporter_switch_user_id="user-1",
        wait_channel="switch",
        room_id="room-1",
        wait_anchor=NOW - timedelta(hours=10),
        wait_fired={},
        worker_parked_reason="Human Jira action",
        jira_read_updated="2026-10-02T16:00:00.000+0000",
        jira_read_changelog_id="200",
    )
    board = FakeAdminBoard()
    await _sync_at(session_factory, ticket_id, board, NOW)
    assert board.reminders == []
    ticket = await _ticket(session_factory, ticket_id)
    assert "wait_admin" not in (ticket.admin_reasons or {})
    assert "wait_blocked" not in (ticket.admin_reasons or {})
    async with session_factory() as session:
        blocked = await session.scalar(
            select(JiraWorkerOutbox).where(JiraWorkerOutbox.command == "transition")
        )
    assert blocked is None


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
async def test_accept_cancels_claimed_wait_command(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await _seed(
        session_factory,
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
            enabled_projects={"acme": ["KAN"]},
        )
        await session.commit()
    async with session_factory() as session:
        rows = list(await session.scalars(select(JiraWorkerOutbox)))
    by_key = {row.command_key: row.status for row in rows}
    assert by_key["wait-blocked:accept:claimed"] == "cancelled"
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
    assert board.reminders == []
    assert "reminder" not in (
        (await _ticket(session_factory, ticket_id)).wait_fired or {}
    )
    await sync_admin_card(
        session_factory,
        engine=await _engine(session_factory),
        board=board,
        config=_worker_config(),
        ticket_id=ticket_id,
        now=NOW + timedelta(minutes=5),
    )
    assert len(board.reminders) == 1
    assert "reminder" in ((await _ticket(session_factory, ticket_id)).wait_fired or {})
    await sync_admin_card(
        session_factory,
        engine=await _engine(session_factory),
        board=board,
        config=_worker_config(),
        ticket_id=ticket_id,
        now=NOW + timedelta(minutes=10),
    )
    assert len(board.reminders) == 1


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
                enabled_projects={"acme": ["KAN"]},
            )
        new_id = await accept_ignored_input(
            session,
            event_id=event_id,
            admin_user_id=admin_id,
            enabled_projects={"acme": ["KAN"]},
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
        decoy = await session.scalar(
            select(Room).where(Room.matrix_room_id == "!decoy:test")
        )
        assert decoy is not None
        assert decoy.id != first


@pytest.mark.asyncio
async def test_admin_member_can_read_the_card(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    class Rooms:
        def __init__(self, factory: async_sessionmaker[AsyncSession]) -> None:
            self.factory = factory

        async def create_room(self, config: RoomCreateConfig) -> Any:
            async with self.factory() as session:
                room = Room(
                    matrix_room_id=f"!admin-{uuid.uuid4().hex[:8]}:test",
                    name=config.name,
                    description=config.description,
                    owner_id=config.owner_id,
                    external_channel_id="channel-1",
                    channel_type="public",
                    metadata_=config.metadata,
                )
                session.add(room)
                await session.flush()
                if config.agent_names:
                    agents = list(
                        (
                            await session.scalars(
                                select(Agent).where(Agent.name.in_(config.agent_names))
                            )
                        ).all()
                    )
                    await RoomStore().add_agents(
                        session, room.id, [agent.id for agent in agents]
                    )
                await session.commit()
                return SimpleNamespace(room=room)

    async with session_factory() as session:
        admin = User(name="Admin", email="admin@example.com", role="admin")
        stranger = User(name="Ada", email="ada@example.invalid", role="user")
        session.add_all([admin, stranger])
        await session.flush()
        admin_agent = await _owned_agent(session, "admin-reader", admin.id)
        other_agent = await _owned_agent(session, "other-reader", stranger.id)
        await session.commit()
        admin_agent_id = admin_agent.id
        other_agent_id = other_agent.id
    board = SwitchAdminBoard(
        room_service=Rooms(session_factory),  # type: ignore[arg-type]
        protocol=None,  # type: ignore[arg-type]
        session_factory=session_factory,
        agent_store=None,  # type: ignore[arg-type]
        config=_worker_config(),
    )
    room_id = await board.ensure_room()
    store = RoomStore()
    async with session_factory() as session:
        admin_room = await store.get_with_membership(session, room_id, admin_agent_id)
        other_room = await store.get_with_membership(session, room_id, other_agent_id)
        assert admin_room is not None and admin_room[1] is True
        assert other_room is not None and other_room[1] is False
        session.add(
            Message(
                seq=1,
                room_id=room_id,
                transport_event_id="card-1",
                sender_id="jira",
                event_type="m.room.message",
                body="jira-admin-card:acme:KAN-1\nStatus: In Progress",
                content={"body": "card"},
            )
        )
        await session.commit()
        body = await session.scalar(
            select(Message.body).where(Message.room_id == room_id)
        )
    assert body is not None
    assert "KAN-1" in body


async def _owned_agent(session: AsyncSession, name: str, owner_id: str) -> Agent:
    api_key = ApiKey(
        user_id=owner_id,
        key_hash=f"hash-{name}",
        encrypted_key="enc",
        label=name,
        type="agent",
    )
    client = Client(
        matrix_user_id=f"@{name}:test",
        display_name=name,
        type="agent",
    )
    session.add_all([api_key, client])
    await session.flush()
    agent = Agent(
        name=name,
        description=name,
        agent_type="session_addressable",
        connector_type="claude_code",
        integration_profile={},
        client_id=client.id,
        api_key_id=api_key.id,
        owner_id=owner_id,
    )
    session.add(agent)
    await session.flush()
    return agent


@pytest.mark.asyncio
async def test_new_reason_posts_one_alert_and_clear_posts_none(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    ticket_id = await _seed(session_factory)
    board = FakeAdminBoard()
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
    assert board.alerts == []
    await sync_admin_card(
        session_factory,
        engine=await _engine(session_factory),
        board=board,
        config=_worker_config(),
        ticket_id=ticket_id,
        now=NOW,
    )
    assert board.alerts == [("admin-room", "KAN-1: unmapped_reporter. Card updated.")]
    await sync_admin_card(
        session_factory,
        engine=await _engine(session_factory),
        board=board,
        config=_worker_config(),
        ticket_id=ticket_id,
        now=NOW,
    )
    assert len(board.alerts) == 1
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
    assert len(board.alerts) == 1


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
