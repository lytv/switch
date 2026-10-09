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
    JiraWorkerEvent,
    JiraWorkerOutbox,
    JiraWorkerTicket,
    Room,
    User,
)
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
                select(JiraWorkerOutbox.issue_key, JiraWorkerOutbox.status).order_by(
                    JiraWorkerOutbox.issue_key
                )
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
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, ticket_id)
        assert ticket is not None
        ticket.worker_parked_reason = "Jira write outcome is unknown"
        await session.commit()
    assert "unknown" in await sync()
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
                )
                session.add(room)
                await session.commit()
                return SimpleNamespace(room=room)

    async with session_factory() as session:
        session.add(
            Room(
                matrix_room_id="!decoy:test",
                name="Jira admin",
                description="same name, no marker",
                metadata_={"other": "1"},
            )
        )
        await session.commit()
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
    assert created[0].owner_id is None
    assert created[0].read_visibility == "private"
    assert created[0].metadata == {ADMIN_ROOM_MARKER: "1"}
    assert created[0].name == "Jira admin"
    async with session_factory() as session:
        decoy = await session.scalar(
            select(Room).where(Room.matrix_room_id == "!decoy:test")
        )
        assert decoy is not None
        assert decoy.id != first


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
