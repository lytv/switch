"""Step-3 worker tests: rooms, card, archive/reopen, invite follow-ups."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from alembic.config import Config
from alembic.runtime.environment import EnvironmentContext
from alembic.script import ScriptDirectory
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

import switch_core.db.models  # noqa: F401 — registers every table on Base.metadata
from switch_core.bridges.jira.rooms import (
    ADMIN_CARD_COMMAND,
    SwitchCardUpdater,
    SwitchTicketRooms,
    _relock_ticket,
    reconcile_ticket_room,
    render_ticket_card,
)
from switch_core.bridges.jira.worker import (
    JiraWorkerScheduler,
    record_webhook_event,
    remove_identity_mapping,
    set_identity_mapping,
)
from switch_core.bridges.trigger_source import NormalizedTriggerEvent
from switch_core.config import SwitchConfig
from switch_core.db.base import Base
from switch_core.db.models import JiraWorkerOutbox, JiraWorkerTicket, Room, User
from switch_core.gateway.jira_triggers import (
    delete_worker_identity_mapping,
    put_worker_identity_mapping,
)
from switch_core.gateway.schemas import JiraWorkerIdentityMappingRequest

_CORE = Path(__file__).resolve().parents[4]

INSTANCE = "acme"
PROJECT = "KAN"


class FakeRooms:
    """In-memory TicketRooms: Switch-side state survives worker restarts."""

    def __init__(self) -> None:
        self.rooms: dict[str, dict[str, Any]] = {}
        self.created: list[dict[str, Any]] = []
        self.cards: dict[str, str] = {}
        self.owner_changes: list[tuple[str, str | None]] = []
        self.archived_calls: list[tuple[str, bool]] = []
        self.fail_create_once = False
        self.fail_post_once = False
        self.fail_archive_once = False
        self.fail_owner_once = False
        self._next = 0

    async def create_ticket_room(
        self,
        *,
        name: str,
        description: str,
        owner_id: str | None,
        instance: str,
        issue_key: str,
    ) -> str:
        self._next += 1
        room_id = f"room-{self._next}"
        self.rooms[room_id] = {
            "name": name,
            "description": description,
            "owner_id": owner_id,
            "archived": False,
            "agents": [],
            "marker": (instance, issue_key),
        }
        self.created.append({"room_id": room_id, "name": name, "owner_id": owner_id})
        if self.fail_create_once:
            self.fail_create_once = False
            raise RuntimeError("create committed then raised")
        return room_id

    async def ensure_agent_member(self, room_id: str, *, agent_name: str) -> None:
        self.rooms[room_id]["agents"].append(agent_name)

    async def post_card(self, room_id: str, *, body: str) -> str:
        if self.fail_post_once:
            self.fail_post_once = False
            raise RuntimeError("post_card raised")
        event_id = f"card-{room_id}"
        self.cards[event_id] = body
        return event_id

    async def is_archived(self, room_id: str) -> bool | None:
        room = self.rooms.get(room_id)
        return None if room is None else room["archived"]

    async def set_archived(self, room_id: str, *, archived: bool) -> None:
        if self.fail_archive_once:
            self.fail_archive_once = False
            raise RuntimeError("archive failed")
        self.rooms[room_id]["archived"] = archived
        self.archived_calls.append((room_id, archived))

    async def find_ticket_room(self, *, instance: str, issue_key: str) -> str | None:
        marker = (instance, issue_key)
        for room_id in reversed(list(self.rooms)):
            if self.rooms[room_id].get("marker") == marker:
                return room_id
        return None

    async def get_room_owner(self, room_id: str) -> str | None:
        room = self.rooms.get(room_id)
        return None if room is None else room["owner_id"]

    async def get_room_description(self, room_id: str) -> str | None:
        room = self.rooms.get(room_id)
        return None if room is None else room["description"]

    async def set_room_owner(self, room_id: str, *, owner_id: str | None) -> bool:
        if self.fail_owner_once:
            self.fail_owner_once = False
            raise RuntimeError("owner write failed")
        room = self.rooms.get(room_id)
        if room is None:
            return False
        room["owner_id"] = owner_id
        self.owner_changes.append((room_id, owner_id))
        return True

    async def reporter_display_name(self, switch_user_id: str) -> str | None:
        return {"user-ada": "Ada", "user-bob": "Bob"}.get(switch_user_id)

    def issue_url(self, *, instance: str, issue_key: str) -> str:
        return f"https://jira.example/browse/{issue_key}"


class FakeCards:
    def __init__(self) -> None:
        self.updates: list[dict[str, str]] = []

    async def update_card(self, room_id: str, *, event_id: str, body: str) -> None:
        self.updates.append({"room_id": room_id, "event_id": event_id, "body": body})


def _webhook_event(
    issue_key: str = "KAN-1",
    status: str = "To Do",
    updated_ms: int = 1791443000000,
    event_id: str = "evt-1",
) -> NormalizedTriggerEvent:
    return NormalizedTriggerEvent(
        event_kind="updated",
        webhook_event="jira:issue_created",
        key=issue_key,
        summary="First ticket",
        issue_type="Task",
        project=PROJECT,
        status=status,
        assignee="",
        priority="",
        reporter="",
        url="",
        raw={
            "webhookEvent": "jira:issue_created",
            "timestamp": updated_ms,
            "issue": {"id": "10001", "key": issue_key},
            "changelog": {"id": event_id},
        },
    )


async def _intake(
    session_factory: async_sessionmaker[AsyncSession],
    event: NormalizedTriggerEvent,
    *,
    webhook_identifier: str | None = None,
) -> bool:
    async with session_factory() as session:
        new = await record_webhook_event(
            session,
            event,
            instance=INSTANCE,
            enabled_projects=[PROJECT],
            webhook_identifier=webhook_identifier,
        )
        await session.commit()
        return new


async def _sync(
    session_factory: async_sessionmaker[AsyncSession],
    issue_key: str,
    rooms: FakeRooms,
    cards: FakeCards | None = None,
) -> str | None:
    async with session_factory() as session:
        room_id = await reconcile_ticket_room(
            session,
            instance=INSTANCE,
            issue_key=issue_key,
            rooms=rooms,
            jira_agent_name="jira",
            cards=cards,
        )
        await session.commit()
        return room_id


async def _ticket(
    session_factory: async_sessionmaker[AsyncSession], issue_key: str
) -> JiraWorkerTicket:
    async with session_factory() as session:
        result = await session.execute(
            text(
                "SELECT id, room_id, card_event_id, status,"
                " reporter_switch_user_id FROM jira_worker_ticket_map"
                " WHERE instance = :instance AND issue_key = :key"
            ),
            {"instance": INSTANCE, "key": issue_key},
        )
        row = result.one()
        ticket = JiraWorkerTicket()
        ticket.id, ticket.room_id, ticket.card_event_id = row[0], row[1], row[2]
        ticket.status, ticket.reporter_switch_user_id = row[3], row[4]
        return ticket


def _script_directory() -> ScriptDirectory:
    config = Config(str(_CORE / "alembic.ini"))
    config.set_main_option("script_location", str(_CORE / "switch_core" / "migrations"))
    return ScriptDirectory.from_config(config)


def _run_migrations(connection: Any, target: str) -> None:
    config = Config(str(_CORE / "alembic.ini"))
    script = _script_directory()

    def do_migrate(revision: str, context: Any) -> Any:
        if target == "head":
            return script._upgrade_revs("head", revision)
        return script._downgrade_revs(target, revision)

    with EnvironmentContext(config, script, fn=do_migrate) as environment:
        environment.configure(connection=connection, target_metadata=Base.metadata)
        with environment.begin_transaction():
            environment.run_migrations()


@pytest.fixture
async def mig_url(postgres_url: str) -> Any:
    admin = create_async_engine(postgres_url, isolation_level="AUTOCOMMIT")
    async with admin.connect() as connection:
        await connection.execute(text('DROP DATABASE IF EXISTS "jira_worker_rooms"'))
        await connection.execute(text('CREATE DATABASE "jira_worker_rooms"'))
    await admin.dispose()
    base, _, _ = postgres_url.rpartition("/")
    try:
        yield f"{base}/jira_worker_rooms"
    finally:
        admin = create_async_engine(postgres_url, isolation_level="AUTOCOMMIT")
        async with admin.connect() as connection:
            await connection.execute(
                text('DROP DATABASE IF EXISTS "jira_worker_rooms"')
            )
        await admin.dispose()


async def test_card_column_migration_round_trip(mig_url: str) -> None:
    engine = create_async_engine(mig_url)
    try:
        async with engine.begin() as connection:
            await connection.run_sync(lambda c: _run_migrations(c, "head"))
            await connection.execute(
                text(
                    "INSERT INTO jira_worker_ticket_map (id, instance, issue_key,"
                    " issue_id, project_key, summary, status,"
                    " reporter_account_id, card_event_id)"
                    " VALUES ('t1', 'acme', 'KAN-1',"
                    " '10001', 'KAN', 'First ticket', 'To Do', '', 'evt-1')"
                )
            )
            await connection.run_sync(lambda c: _run_migrations(c, "f6e7d8c9b0a1"))
            cols = (
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
            assert "card_event_id" not in cols
            await connection.run_sync(lambda c: _run_migrations(c, "head"))
            card = (
                await connection.execute(
                    text(
                        "SELECT card_event_id FROM jira_worker_ticket_map"
                        " WHERE id = 't1'"
                    )
                )
            ).scalar_one_or_none()
            assert card is None
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_one_room_per_ticket_across_duplicates_and_restart(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    rooms, cards = FakeRooms(), FakeCards()
    assert await _intake(session_factory, _webhook_event()) is True
    assert await _intake(session_factory, _webhook_event()) is False
    first = await _sync(session_factory, "KAN-1", rooms, cards)
    assert first is not None
    # A duplicate sync and a "restart" (fresh provisioner, Switch state kept)
    # must reuse the ticket_map room, never create a second one.
    assert await _sync(session_factory, "KAN-1", rooms, cards) == first
    fresh = FakeRooms()
    fresh.rooms = {k: dict(v) for k, v in rooms.rooms.items()}
    assert await _sync(session_factory, "KAN-1", fresh, cards) == first
    assert len(rooms.created) == 1
    assert (await _ticket(session_factory, "KAN-1")).room_id == first


@pytest.mark.asyncio
async def test_mapped_reporter_owns_room_unmapped_is_admins_only(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    rooms = FakeRooms()
    async with session_factory() as session:
        await set_identity_mapping(
            session,
            instance=INSTANCE,
            jira_account_id="acct-ada",
            switch_user_id="user-ada",
            enabled_projects=[PROJECT],
        )
        await session.commit()
    raw: dict[str, Any] = {
        "webhookEvent": "jira:issue_created",
        "timestamp": 1791443000000,
        "issue": {
            "id": "10001",
            "key": "KAN-1",
            "fields": {"reporter": {"accountId": "acct-ada"}},
        },
    }
    event = NormalizedTriggerEvent(
        event_kind="created",
        webhook_event="jira:issue_created",
        key="KAN-1",
        summary="Mapped ticket",
        issue_type="Task",
        project=PROJECT,
        status="To Do",
        assignee="",
        priority="",
        reporter="",
        url="",
        raw=raw,
    )
    assert await _intake(session_factory, event, webhook_identifier="m-1") is True
    await _sync(session_factory, "KAN-1", rooms)
    assert rooms.created[0]["owner_id"] == "user-ada"


@pytest.mark.asyncio
async def test_reporter_changed_to_unmapped_clears_owner(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    rooms, updater = FakeRooms(), FakeCards()
    async with session_factory() as session:
        await set_identity_mapping(
            session,
            instance=INSTANCE,
            jira_account_id="acct-ada",
            switch_user_id="user-ada",
            enabled_projects=[PROJECT],
        )
        await session.commit()

    def _event(account_id: str, updated_ms: int, event_id: str) -> Any:
        return NormalizedTriggerEvent(
            event_kind="updated",
            webhook_event="jira:issue_updated",
            key="KAN-1",
            summary="Mapped ticket",
            issue_type="Task",
            project=PROJECT,
            status="To Do",
            assignee="",
            priority="",
            reporter="",
            url="",
            raw={
                "webhookEvent": "jira:issue_updated",
                "timestamp": updated_ms,
                "issue": {
                    "id": "10001",
                    "key": "KAN-1",
                    "fields": {"reporter": {"accountId": account_id}},
                },
                "changelog": {"id": event_id},
            },
        )

    assert await _intake(session_factory, _event("acct-ada", 1791443000000, "m-1")) is True
    room_id = await _sync(session_factory, "KAN-1", rooms, updater)
    assert rooms.rooms[room_id]["owner_id"] == "user-ada"

    assert await _intake(session_factory, _event("acct-bob", 1791443999000, "m-2")) is True
    assert await _sync(session_factory, "KAN-1", rooms, updater) == room_id
    assert rooms.rooms[room_id]["owner_id"] is None
    assert "unmapped reporter" in updater.updates[-1]["body"]

    assert (
        await _intake(
            session_factory, _webhook_event("KAN-2"), webhook_identifier="m-2"
        )
        is True
    )
    await _sync(session_factory, "KAN-2", rooms)
    assert rooms.created[1]["owner_id"] is None


@pytest.mark.asyncio
async def test_card_created_once_then_updated_in_place(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    provisioner, updater = FakeRooms(), FakeCards()
    assert await _intake(session_factory, _webhook_event()) is True
    await _sync(session_factory, "KAN-1", provisioner, updater)
    assert len(provisioner.cards) == 1
    card_id = (await _ticket(session_factory, "KAN-1")).card_event_id
    assert card_id is not None
    body = provisioner.cards[card_id]
    assert "KAN-1" in body and "First ticket" in body and "To Do" in body
    assert "unmapped reporter" in body
    assert "https://jira.example/browse/KAN-1" in body
    assert provisioner.rooms[(await _ticket(session_factory, "KAN-1")).room_id]["description"] == body
    assert len(updater.updates) == 0

    assert (
        await _intake(
            session_factory,
            _webhook_event(status="In Progress", updated_ms=1791443999000),
            webhook_identifier="evt-2",
        )
        is True
    )
    await _sync(session_factory, "KAN-1", provisioner, updater)
    assert len(provisioner.cards) == 1
    assert len(updater.updates) == 1
    assert updater.updates[-1]["event_id"] == card_id
    assert "In Progress" in updater.updates[-1]["body"]
    assert (await _ticket(session_factory, "KAN-1")).card_event_id == card_id


@pytest.mark.asyncio
async def test_create_orphan_adopted_on_retry(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    rooms, updater = FakeRooms(), FakeCards()
    assert await _intake(session_factory, _webhook_event()) is True
    rooms.fail_create_once = True
    with pytest.raises(RuntimeError, match="create committed then raised"):
        await _sync(session_factory, "KAN-1", rooms, updater)
    assert (await _ticket(session_factory, "KAN-1")).room_id is None
    room_id = await _sync(session_factory, "KAN-1", rooms, updater)
    assert room_id == "room-1"
    assert len(rooms.created) == 1
    assert (await _ticket(session_factory, "KAN-1")).room_id == room_id
    assert len(rooms.cards) == 1


@pytest.mark.asyncio
async def test_post_card_failure_retries_without_duplicate(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    rooms, updater = FakeRooms(), FakeCards()
    assert await _intake(session_factory, _webhook_event()) is True
    rooms.fail_post_once = True
    with pytest.raises(RuntimeError, match="post_card raised"):
        await _sync(session_factory, "KAN-1", rooms, updater)
    room_id = (await _ticket(session_factory, "KAN-1")).room_id
    assert room_id is not None
    assert await _sync(session_factory, "KAN-1", rooms, updater) == room_id
    assert len(rooms.created) == 1
    assert len(rooms.cards) == 1


@pytest.mark.asyncio
async def test_redelivered_event_reconcile_is_noop(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    rooms, updater = FakeRooms(), FakeCards()
    assert await _intake(session_factory, _webhook_event()) is True
    first = await _sync(session_factory, "KAN-1", rooms, updater)
    assert await _intake(session_factory, _webhook_event()) is False
    assert await _sync(session_factory, "KAN-1", rooms, updater) == first
    assert len(rooms.created) == 1
    assert len(rooms.cards) == 1
    assert updater.updates == []
    assert rooms.owner_changes == []


@pytest.mark.asyncio
async def test_mapping_added_later_invites_user_and_leaves_admin_cards(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    rooms = FakeRooms()
    assert (
        await _intake(session_factory, _webhook_event(), webhook_identifier="evt-1")
        is True
    )
    room_id = await _sync(session_factory, "KAN-1", rooms)
    assert rooms.rooms[room_id]["owner_id"] is None

    async with session_factory() as session:
        await set_identity_mapping(
            session,
            instance=INSTANCE,
            jira_account_id="",
            switch_user_id="user-ada",
            enabled_projects=[PROJECT],
        )
        await session.commit()
        invites = (
            await session.execute(
                text(
                    "SELECT count(*) FROM jira_worker_outbox"
                    " WHERE command = 'invite_ticket_reporter' AND status = 'pending'"
                )
            )
        ).scalar_one()
        assert invites == 1
        assert await _sync(session_factory, "KAN-1", rooms) == room_id

    assert rooms.rooms[room_id]["owner_id"] == "user-ada"
    assert rooms.owner_changes == [(room_id, "user-ada")]
    async with session_factory() as session:
        pending_invites = (
            await session.execute(
                text(
                    "SELECT count(*) FROM jira_worker_outbox"
                    " WHERE command = 'invite_ticket_reporter' AND status = 'pending'"
                )
            )
        ).scalar_one()
        admin_cards = (
            await session.execute(
                text(
                    f"SELECT count(*) FROM jira_worker_outbox"
                    f" WHERE command = '{ADMIN_CARD_COMMAND}' AND status = 'pending'"
                )
            )
        ).scalar_one()
        assert pending_invites == 0
        # Creation enqueued one admin card and the later mapping another;
        # the invite consumer leaves both for step 7.
        assert admin_cards == 2


@pytest.mark.asyncio
async def test_archive_on_done_and_reopen_reuses_room(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    rooms, updater = FakeRooms(), FakeCards()
    assert (
        await _intake(session_factory, _webhook_event(), webhook_identifier="evt-1")
        is True
    )
    room_id = await _sync(session_factory, "KAN-1", rooms, updater)
    assert rooms.rooms[room_id]["archived"] is False

    assert (
        await _intake(
            session_factory,
            _webhook_event(status="Done", updated_ms=1791443999000),
            webhook_identifier="evt-2",
        )
        is True
    )
    assert await _sync(session_factory, "KAN-1", rooms, updater) == room_id
    assert rooms.rooms[room_id]["archived"] is True
    assert rooms.archived_calls[-1] == (room_id, True)

    assert (
        await _intake(
            session_factory,
            _webhook_event(status="In Progress", updated_ms=1791444999000),
            webhook_identifier="evt-3",
        )
        is True
    )
    assert await _sync(session_factory, "KAN-1", rooms, updater) == room_id
    assert rooms.rooms[room_id]["archived"] is False
    assert len(rooms.created) == 1


@pytest.mark.asyncio
async def test_disabled_project_unchanged(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    rooms = FakeRooms()
    async with session_factory() as session:
        new = await record_webhook_event(
            session,
            _webhook_event(),
            instance=INSTANCE,
            enabled_projects=[],
            webhook_identifier="evt-1",
        )
        await session.commit()
        assert new is False
        assert (
            await reconcile_ticket_room(
                session,
                instance=INSTANCE,
                issue_key="KAN-1",
                rooms=rooms,
                jira_agent_name="jira",
            )
            is None
        )
        await session.commit()
    assert rooms.created == []
    assert rooms.cards == {}


def test_render_ticket_card_fields() -> None:
    body = render_ticket_card(
        issue_key="KAN-9",
        summary="Fix login",
        status="Waiting for approval",
        reporter_label="Ada",
        issue_url="https://jira.example/browse/KAN-9",
    )
    assert "KAN-9" in body
    assert "Fix login" in body
    assert "Waiting for approval" in body
    assert "Ada" in body
    assert "https://jira.example/browse/KAN-9" in body


class StubRoomService:
    def __init__(self) -> None:
        self.descriptions: dict[str, str] = {}

    async def update_room(self, room_id: str, **fields: Any) -> None:
        self.descriptions[room_id] = str(fields["description"])


@pytest.mark.asyncio
async def test_production_card_updater_rewrites_room_description() -> None:
    service = StubRoomService()
    updater = SwitchCardUpdater(room_service=service)  # type: ignore[arg-type]
    await updater.update_card("room-1", event_id="card-room-1", body="Status: Done")
    assert service.descriptions == {"room-1": "Status: Done"}


@pytest.mark.asyncio
async def test_missing_ticket_sync_returns_none(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        assert (
            await reconcile_ticket_room(
                session,
                instance=INSTANCE,
                issue_key="KAN-404",
                rooms=FakeRooms(),
                jira_agent_name="jira",
            )
            is None
        )


def _switch_config(**overrides: object) -> SwitchConfig:
    base: dict[str, Any] = {
        "db_host": "localhost",
        "db_port": "5432",
        "db_user": "u",
        "db_password": "p",
        "db_name": "db",
        "matrix_server_name": "localhost",
        "agent_registration_token": "t",
        "jwt_secret_key": "j" * 32,
        "gateway_admin_email": "admin@example.com",
        "gateway_admin_password": "pw",
    }
    base.update(overrides)
    return SwitchConfig(**base)


def _bare_room(**fields: Any) -> dict[str, Any]:
    room = {
        "name": "KAN-1",
        "description": "personal notes",
        "owner_id": "user-ada",
        "archived": False,
        "agents": [],
        "marker": None,
    }
    room.update(fields)
    return room


@pytest.mark.asyncio
async def test_adopt_requires_marker_for_this_instance_and_issue(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    rooms, updater = FakeRooms(), FakeCards()
    rooms.rooms["person"] = _bare_room()
    rooms.rooms["beta"] = _bare_room(description="beta ticket", marker=("beta", "KAN-1"))
    rooms._next = 2
    assert await _intake(session_factory, _webhook_event()) is True
    room_id = await _sync(session_factory, "KAN-1", rooms, updater)
    assert room_id == "room-3"
    assert rooms.rooms["person"]["description"] == "personal notes"
    assert rooms.rooms["person"]["owner_id"] == "user-ada"
    assert rooms.rooms["beta"]["description"] == "beta ticket"
    assert len(rooms.created) == 1
    assert rooms.rooms[room_id]["marker"] == (INSTANCE, "KAN-1")


@pytest.mark.asyncio
async def test_relock_sees_a_concurrent_ticket_update(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    assert await _intake(session_factory, _webhook_event()) is True
    async with session_factory() as session:
        first = await _relock_ticket(session, instance=INSTANCE, issue_key="KAN-1")
        assert first is not None
        assert first.summary == "First ticket"
        await session.commit()
        async with session_factory() as other:
            await other.execute(
                text(
                    "UPDATE jira_worker_ticket_map SET summary = :summary,"
                    " status = :status WHERE instance = :instance AND issue_key = :key"
                ),
                {
                    "summary": "Updated while open",
                    "status": "Done",
                    "instance": INSTANCE,
                    "key": "KAN-1",
                },
            )
            await other.commit()
        again = await _relock_ticket(session, instance=INSTANCE, issue_key="KAN-1")
        assert again is not None
        assert again.summary == "Updated while open"
        assert again.status == "Done"


def _reporter_event(
    account_id: str, updated_ms: int, event_id: str
) -> NormalizedTriggerEvent:
    return NormalizedTriggerEvent(
        event_kind="updated",
        webhook_event="jira:issue_updated",
        key="KAN-1",
        summary="Mapped ticket",
        issue_type="Task",
        project=PROJECT,
        status="To Do",
        assignee="",
        priority="",
        reporter="",
        url="",
        raw={
            "webhookEvent": "jira:issue_updated",
            "timestamp": updated_ms,
            "issue": {
                "id": "10001",
                "key": "KAN-1",
                "fields": {"reporter": {"accountId": account_id}},
            },
            "changelog": {"id": event_id},
        },
    )


@pytest.mark.asyncio
async def test_done_ticket_owner_follows_current_mapping(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    rooms, updater = FakeRooms(), FakeCards()
    async with session_factory() as session:
        await set_identity_mapping(
            session,
            instance=INSTANCE,
            jira_account_id="acct-ada",
            switch_user_id="user-ada",
            enabled_projects=[PROJECT],
        )
        await session.commit()
    assert (
        await _intake(
            session_factory, _reporter_event("acct-ada", 1791443000000, "d-1")
        )
        is True
    )
    room_id = await _sync(session_factory, "KAN-1", rooms, updater)
    assert rooms.rooms[room_id]["owner_id"] == "user-ada"

    assert (
        await _intake(
            session_factory,
            _webhook_event(status="Done", updated_ms=1791443999000),
            webhook_identifier="d-2",
        )
        is True
    )
    await _sync(session_factory, "KAN-1", rooms, updater)
    assert rooms.rooms[room_id]["archived"] is True

    async with session_factory() as session:
        await set_identity_mapping(
            session,
            instance=INSTANCE,
            jira_account_id="acct-ada",
            switch_user_id="user-bob",
            enabled_projects=[PROJECT],
        )
        await session.commit()
    assert (await _ticket(session_factory, "KAN-1")).reporter_switch_user_id == "user-ada"
    await _sync(session_factory, "KAN-1", rooms, updater)
    assert rooms.rooms[room_id]["owner_id"] == "user-bob"
    assert "Bob" in updater.updates[-1]["body"]

    async with session_factory() as session:
        await remove_identity_mapping(
            session,
            instance=INSTANCE,
            jira_account_id="acct-ada",
            enabled_projects=[PROJECT],
        )
        await session.commit()
    await _sync(session_factory, "KAN-1", rooms, updater)
    assert rooms.rooms[room_id]["owner_id"] is None
    assert "unmapped reporter (admins only)" in updater.updates[-1]["body"]
    assert (await _ticket(session_factory, "KAN-1")).reporter_switch_user_id == "user-ada"


async def _pending_sync_count(session_factory: async_sessionmaker[AsyncSession]) -> int:
    async with session_factory() as session:
        return (
            await session.execute(
                text(
                    "SELECT count(*) FROM jira_worker_outbox"
                    " WHERE command = 'sync_ticket_room' AND status = 'pending'"
                )
            )
        ).scalar_one()


@pytest.mark.asyncio
async def test_sweep_retries_until_archive_and_owner_are_applied(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    rooms, updater = FakeRooms(), FakeCards()
    async with session_factory() as session:
        await set_identity_mapping(
            session,
            instance=INSTANCE,
            jira_account_id="acct-ada",
            switch_user_id="user-ada",
            enabled_projects=[PROJECT],
        )
        await session.commit()
    assert (
        await _intake(
            session_factory, _reporter_event("acct-ada", 1791443000000, "s-1")
        )
        is True
    )
    room_id = await _sync(session_factory, "KAN-1", rooms, updater)
    assert await _pending_sync_count(session_factory) == 0

    assert (
        await _intake(
            session_factory,
            _webhook_event(status="Done", updated_ms=1791443999000),
            webhook_identifier="s-2",
        )
        is True
    )
    assert await _pending_sync_count(session_factory) == 1
    rooms.fail_archive_once = True
    scheduler = JiraWorkerScheduler(
        session_factory=session_factory,
        config=_switch_config(jira_worker_enabled_projects={INSTANCE: [PROJECT]}),
        ticket_rooms=rooms,
        card_updater=updater,
    )
    assert await scheduler.sweep_ticket_rooms() == 0
    assert rooms.rooms[room_id]["archived"] is False
    assert await _pending_sync_count(session_factory) == 1
    assert await scheduler.sweep_ticket_rooms() == 1
    assert rooms.rooms[room_id]["archived"] is True
    assert await _pending_sync_count(session_factory) == 0

    async with session_factory() as session:
        await remove_identity_mapping(
            session,
            instance=INSTANCE,
            jira_account_id="acct-ada",
            enabled_projects=[PROJECT],
        )
        await session.commit()
    assert await _pending_sync_count(session_factory) == 1
    rooms.fail_owner_once = True
    assert await scheduler.sweep_ticket_rooms() == 0
    assert rooms.rooms[room_id]["owner_id"] == "user-ada"
    assert await _pending_sync_count(session_factory) == 1
    assert await scheduler.sweep_ticket_rooms() == 1
    assert rooms.rooms[room_id]["owner_id"] is None
    assert await _pending_sync_count(session_factory) == 0


@pytest.mark.asyncio
async def test_sweep_leaves_a_disabled_project_unchanged(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    rooms = FakeRooms()
    rooms.rooms["off-room"] = _bare_room(owner_id="user-ada")
    async with session_factory() as session:
        ticket = JiraWorkerTicket(
            instance=INSTANCE,
            issue_key="OFF-1",
            project_key="OFF",
            summary="Left alone",
            status="To Do",
            reporter_account_id="acct-ada",
            reporter_switch_user_id="user-ada",
            room_id="off-room",
            card_event_id="card-off",
        )
        session.add(ticket)
        await session.flush()
        session.add(
            JiraWorkerOutbox(
                channel="switch",
                command="sync_ticket_room",
                issue_key="OFF-1",
                payload={"ticket_id": ticket.id, "instance": INSTANCE},
            )
        )
        await session.commit()
    scheduler = JiraWorkerScheduler(
        session_factory=session_factory,
        config=_switch_config(jira_worker_enabled_projects={INSTANCE: [PROJECT]}),
        ticket_rooms=rooms,
        card_updater=FakeCards(),
    )
    assert await scheduler.sweep_ticket_rooms() == 0
    assert rooms.rooms["off-room"]["owner_id"] == "user-ada"
    assert rooms.rooms["off-room"]["description"] == "personal notes"
    assert await _pending_sync_count(session_factory) == 1


@pytest.mark.asyncio
async def test_mapping_routes_reconcile_only_enabled_projects(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    rooms, cards = FakeRooms(), FakeCards()
    async with session_factory() as session:
        user = User(name="Ada", email="ada@example.invalid", role="user")
        admin = User(name="Admin", email="admin@example.invalid", role="admin")
        session.add_all([user, admin])
        await session.flush()
        session.add_all(
            [
                JiraWorkerTicket(
                    instance=INSTANCE,
                    issue_key="KAN-1",
                    project_key=PROJECT,
                    summary="Enabled",
                    status="To Do",
                    reporter_account_id="acct-ada",
                ),
                JiraWorkerTicket(
                    instance=INSTANCE,
                    issue_key="OFF-1",
                    project_key="OFF",
                    summary="Disabled",
                    status="To Do",
                    reporter_account_id="acct-ada",
                ),
            ]
        )
        await session.commit()
        user_id = user.id
        config = _switch_config(
            jira_worker_enabled_projects={INSTANCE: [PROJECT]}
        )
        await put_worker_identity_mapping(
            "acct-ada",
            JiraWorkerIdentityMappingRequest(switch_user_id=user_id),
            INSTANCE,
            session,
            config,
            admin,
            rooms,  # type: ignore[arg-type]
            cards,  # type: ignore[arg-type]
            session_factory,
        )
        assert [item["name"] for item in rooms.created] == ["KAN-1"]
        kan_room = rooms.created[0]["room_id"]
        assert rooms.rooms[kan_room]["owner_id"] == user_id

        rooms.rooms["off-room"] = _bare_room(owner_id=user_id)
        off = (
            await session.execute(
                text(
                    "SELECT id FROM jira_worker_ticket_map WHERE issue_key = 'OFF-1'"
                )
            )
        ).scalar_one()
        await session.execute(
            text(
                "UPDATE jira_worker_ticket_map SET room_id = 'off-room'"
                " WHERE id = :id"
            ),
            {"id": off},
        )
        await session.commit()
        await delete_worker_identity_mapping(
            "acct-ada",
            INSTANCE,
            session,
            config,
            admin,
            rooms,  # type: ignore[arg-type]
            cards,  # type: ignore[arg-type]
            session_factory,
        )
    assert rooms.rooms[kan_room]["owner_id"] is None
    assert rooms.rooms["off-room"]["owner_id"] == user_id
    assert rooms.rooms["off-room"]["description"] == "personal notes"


@pytest.mark.asyncio
async def test_find_ticket_room_matches_marker_not_name(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        person = Room(
            matrix_room_id="!person:test",
            name="KAN-1",
            description="personal notes",
        )
        beta = Room(
            matrix_room_id="!beta:test",
            name="KAN-1",
            description="beta",
            metadata_={"jira_instance": "beta", "jira_issue_key": "KAN-1"},
        )
        acme = Room(
            matrix_room_id="!acme:test",
            name="other-name",
            description="acme ticket",
            metadata_={"jira_instance": INSTANCE, "jira_issue_key": "KAN-1"},
        )
        session.add_all([person, beta, acme])
        await session.commit()
        acme_id = acme.id
    finder = SwitchTicketRooms(
        room_service=None,  # type: ignore[arg-type]
        protocol=None,  # type: ignore[arg-type]
        session_factory=session_factory,
        agent_store=None,  # type: ignore[arg-type]
        config=_switch_config(),
    )
    assert await finder.find_ticket_room(instance=INSTANCE, issue_key="KAN-1") == acme_id
    async with session_factory() as session:
        session.add(
            JiraWorkerTicket(
                instance=INSTANCE,
                issue_key="KAN-1",
                project_key=PROJECT,
                room_id=acme_id,
            )
        )
        await session.commit()
    assert await finder.find_ticket_room(instance=INSTANCE, issue_key="KAN-1") is None


@pytest.mark.asyncio
async def test_create_ticket_room_writes_marker_into_room_metadata() -> None:
    captured: dict[str, Any] = {}

    class _Recording:
        async def create_room(self, config: Any) -> Any:
            captured["metadata"] = config.metadata
            captured["name"] = config.name

            class _Result:
                room = type("Room", (), {"id": "room-9"})()

            return _Result()

    rooms = SwitchTicketRooms(
        room_service=_Recording(),  # type: ignore[arg-type]
        protocol=None,  # type: ignore[arg-type]
        session_factory=None,  # type: ignore[arg-type]
        agent_store=None,  # type: ignore[arg-type]
        config=_switch_config(),
    )
    assert (
        await rooms.create_ticket_room(
            name="KAN-1",
            description="card",
            owner_id=None,
            instance=INSTANCE,
            issue_key="KAN-1",
        )
        == "room-9"
    )
    assert captured["name"] == "KAN-1"
    assert captured["metadata"] == {
        "jira_instance": INSTANCE,
        "jira_issue_key": "KAN-1",
    }


@pytest.mark.asyncio
async def test_ensure_agent_member_invites_clients_missing_membership() -> None:
    calls: list[tuple[Any, ...]] = []

    class _Rooms:
        async def add_agents_to_room(
            self, room_id: str, agent_names: list[str] | None = None
        ) -> None:
            calls.append(("add", room_id, tuple(agent_names or [])))

        async def invite_missing_member_clients(self, room_id: str) -> None:
            calls.append(("invite", room_id))

    rooms = SwitchTicketRooms(
        room_service=_Rooms(),  # type: ignore[arg-type]
        protocol=None,  # type: ignore[arg-type]
        session_factory=None,  # type: ignore[arg-type]
        agent_store=None,  # type: ignore[arg-type]
        config=_switch_config(),
    )
    await rooms.ensure_agent_member("room-1", agent_name="jira")
    assert calls == [("add", "room-1", ("jira",)), ("invite", "room-1")]
