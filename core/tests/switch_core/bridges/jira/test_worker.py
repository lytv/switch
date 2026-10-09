"""Step-1 worker tests: migration, intake, claims, scheduler, poll."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
from alembic.config import Config
from alembic.runtime.environment import EnvironmentContext
from alembic.script import ScriptDirectory
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

import switch_core.db.models  # noqa: F401 — registers every table on Base.metadata
from switch_core.bridges.jira.worker import (
    JiraPollClient,
    JiraWorkerScheduler,
    build_poll_clients,
    claim_due_jobs,
    complete_job,
    ensure_poll_job,
    is_worker_enabled,
    lookup_identity,
    record_event,
    record_polled_issue,
    record_webhook_event,
    remove_identity_mapping,
    set_identity_mapping,
    webhook_idempotency_key,
)
from switch_core.bridges.trigger_source import NormalizedTriggerEvent
from switch_core.config import SwitchConfig
from switch_core.db.base import Base
from switch_core.db.models import JiraWorkerJob, JiraWorkerTicket

_CORE = Path(__file__).resolve().parents[4]
_MIG_DB = "jira_worker_mig"

WORKER_TABLES = (
    "jira_worker_identity_map",
    "jira_worker_ticket_map",
    "jira_worker_job_record",
    "jira_worker_event_log",
    "jira_worker_outbox",
)


def _config(**overrides: object) -> SwitchConfig:
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
        await connection.execute(text(f'DROP DATABASE IF EXISTS "{_MIG_DB}"'))
        await connection.execute(text(f'CREATE DATABASE "{_MIG_DB}"'))
    await admin.dispose()
    base, _, _ = postgres_url.rpartition("/")
    try:
        yield f"{base}/{_MIG_DB}"
    finally:
        admin = create_async_engine(postgres_url, isolation_level="AUTOCOMMIT")
        async with admin.connect() as connection:
            await connection.execute(text(f'DROP DATABASE IF EXISTS "{_MIG_DB}"'))
        await admin.dispose()


async def test_worker_migration_creates_five_tables(mig_url: str) -> None:
    engine = create_async_engine(mig_url)
    try:
        async with engine.begin() as connection:
            await connection.run_sync(lambda c: _run_migrations(c, "head"))
        async with engine.connect() as connection:
            for table in WORKER_TABLES:
                count = (
                    await connection.execute(text(f"SELECT count(*) FROM {table}"))
                ).scalar_one()
                assert count == 0
        async with engine.begin() as connection:
            await connection.run_sync(lambda c: _run_migrations(c, "f5a6b7c8d9e0"))
        async with engine.connect() as connection:
            for table in WORKER_TABLES:
                exists = (
                    await connection.execute(
                        text(
                            "SELECT count(*) FROM information_schema.tables "
                            f"WHERE table_name = '{table}'"
                        )
                    )
                ).scalar_one()
                assert exists == 0, table
    finally:
        await engine.dispose()


async def test_identity_migration_retains_legacy_rows_as_unmapped(
    mig_url: str,
) -> None:
    engine = create_async_engine(mig_url)
    try:
        async with engine.begin() as connection:
            await connection.run_sync(lambda c: _run_migrations(c, "head"))
            await connection.run_sync(lambda c: _run_migrations(c, "ac187a6d14e08d9f"))
            await connection.execute(
                text(
                    "INSERT INTO jira_worker_identity_map "
                    "(id, jira_account_id, switch_agent_name) "
                    "VALUES ('legacy-mapped', 'jira-mapped', 'old-agent'), "
                    "('legacy-unmapped', 'jira-unmapped', NULL)"
                )
            )
            await connection.run_sync(lambda c: _run_migrations(c, "head"))
            rows = (
                await connection.execute(
                    text(
                        "SELECT instance, jira_account_id, switch_user_id "
                        "FROM jira_worker_identity_map ORDER BY jira_account_id"
                    )
                )
            ).all()
            assert rows == [("", "jira-mapped", None), ("", "jira-unmapped", None)]
            await connection.run_sync(lambda c: _run_migrations(c, "ac187a6d14e08d9f"))
            rows = (
                await connection.execute(
                    text(
                        "SELECT jira_account_id, switch_agent_name "
                        "FROM jira_worker_identity_map ORDER BY jira_account_id"
                    )
                )
            ).all()
            assert rows == [("jira-mapped", None), ("jira-unmapped", None)]
    finally:
        await engine.dispose()


def _webhook_event(
    project: str = "KAN", issue_key: str = "KAN-1"
) -> NormalizedTriggerEvent:
    raw: dict[str, Any] = {
        "webhookEvent": "jira:issue_updated",
        "timestamp": 1791443000000,
        "issue": {"id": "10001", "key": issue_key},
        "changelog": {"id": "20001"},
    }
    return NormalizedTriggerEvent(
        event_kind="updated",
        webhook_event="jira:issue_updated",
        key=issue_key,
        summary="First ticket",
        issue_type="Task",
        project=project,
        status="In Progress",
        assignee="",
        priority="",
        reporter="",
        url="",
        raw=raw,
    )


def test_webhook_idempotency_key_uses_raw_parts() -> None:
    event = _webhook_event()
    assert (
        webhook_idempotency_key(event.raw, event.key)
        == "jira:issue_updated|10001|KAN-1|1791443000000|20001"
    )


@pytest.mark.asyncio
async def test_disabled_by_default_records_nothing(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    assert _config().jira_worker_enabled_projects == {}
    assert is_worker_enabled(_config(), "acme", "KAN") is False
    async with session_factory() as session:
        assert (
            await record_webhook_event(
                session,
                _webhook_event(),
                instance="acme",
                enabled_projects=[],
                webhook_identifier=None,
            )
            is False
        )
        await session.commit()
    async with session_factory() as session:
        events = (
            await session.execute(text("SELECT count(*) FROM jira_worker_event_log"))
        ).scalar_one()
        tickets = (
            await session.execute(text("SELECT count(*) FROM jira_worker_ticket_map"))
        ).scalar_one()
        assert events == 0
        assert tickets == 0


@pytest.mark.asyncio
async def test_idempotent_intake_and_ticket_upsert(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        assert (
            await record_webhook_event(
                session,
                _webhook_event(),
                instance="acme",
                enabled_projects=["KAN"],
                webhook_identifier=None,
            )
            is True
        )
        assert (
            await record_webhook_event(
                session,
                _webhook_event(),
                instance="acme",
                enabled_projects=["KAN"],
                webhook_identifier=None,
            )
            is False
        )
        await session.commit()
    async with session_factory() as session:
        events = list(
            (await session.execute(text("SELECT * FROM jira_worker_event_log"))).all()
        )
        assert len(events) == 1
        ticket = await session.get(
            JiraWorkerTicket,
            (
                await session.execute(
                    text(
                        "SELECT id FROM jira_worker_ticket_map WHERE issue_key='KAN-1'"
                    )
                )
            ).scalar_one(),
        )
        assert ticket is not None
        assert ticket.summary == "First ticket"
        assert ticket.status == "In Progress"


@pytest.mark.asyncio
async def test_webhook_identifier_is_primary_idempotency_key(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    first, second = _webhook_event(), _webhook_event()
    second.raw["changelog"] = {"id": "20002"}
    async with session_factory() as session:
        assert await record_webhook_event(
            session,
            first,
            instance="acme",
            enabled_projects=["KAN"],
            webhook_identifier="atlassian-event-1",
        )
        assert not await record_webhook_event(
            session,
            second,
            instance="acme",
            enabled_projects=["KAN"],
            webhook_identifier="atlassian-event-1",
        )
        await session.commit()
    async with session_factory() as session:
        key = (
            await session.execute(
                text("SELECT idempotency_key FROM jira_worker_event_log")
            )
        ).scalar_one()
        assert key == "atlassian-event-1"


@pytest.mark.asyncio
async def test_fallback_webhook_idempotency_key_records_distinct_changelog_ids(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    first, second = _webhook_event(), _webhook_event()
    second.raw["changelog"] = {"id": "20002"}
    async with session_factory() as session:
        assert await record_webhook_event(
            session,
            first,
            instance="acme",
            enabled_projects=["KAN"],
            webhook_identifier=None,
        )
        assert await record_webhook_event(
            session,
            second,
            instance="acme",
            enabled_projects=["KAN"],
            webhook_identifier=None,
        )
        await session.commit()
    async with session_factory() as session:
        count = (
            await session.execute(text("SELECT count(*) FROM jira_worker_event_log"))
        ).scalar_one()
        assert count == 2


@pytest.mark.asyncio
async def test_distinct_changelog_ids_are_distinct_events(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    first, second = _webhook_event(), _webhook_event()
    second.raw["changelog"] = {"id": "20002"}
    async with session_factory() as session:
        assert (
            await record_webhook_event(
                session,
                first,
                instance="acme",
                enabled_projects=["KAN"],
                webhook_identifier=None,
            )
            is True
        )
        assert (
            await record_webhook_event(
                session,
                second,
                instance="acme",
                enabled_projects=["KAN"],
                webhook_identifier=None,
            )
            is True
        )
        await session.commit()
    async with session_factory() as session:
        count = (
            await session.execute(text("SELECT count(*) FROM jira_worker_event_log"))
        ).scalar_one()
        assert count == 2


@pytest.mark.asyncio
async def test_instances_with_same_project_and_issue_key_stay_separate(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        assert await record_webhook_event(
            session,
            _webhook_event(),
            instance="acme",
            enabled_projects=["KAN"],
            webhook_identifier=None,
        )
        assert await record_webhook_event(
            session,
            _webhook_event(),
            instance="other",
            enabled_projects=["KAN"],
            webhook_identifier=None,
        )
        await session.commit()
    async with session_factory() as session:
        rows = (
            await session.execute(
                text(
                    "SELECT instance, issue_key FROM jira_worker_ticket_map ORDER BY instance"
                )
            )
        ).all()
        assert rows == [("acme", "KAN-1"), ("other", "KAN-1")]


@pytest.mark.asyncio
async def test_identity_mapping_is_exact_and_scoped_per_instance(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        first, _ = await set_identity_mapping(
            session,
            instance="acme",
            jira_account_id="jira-account-1",
            switch_user_id="switch-user-1",
            enabled_projects=["KAN"],
        )
        second, _ = await set_identity_mapping(
            session,
            instance="other",
            jira_account_id="jira-account-1",
            switch_user_id="switch-user-2",
            enabled_projects=["KAN"],
        )
        await session.commit()
        assert first.instance == "acme"
        assert second.instance == "other"

    async with session_factory() as session:
        assert (
            await lookup_identity(
                session, instance="acme", jira_account_id="jira-account-1"
            )
            == "switch-user-1"
        )
        assert (
            await lookup_identity(
                session, instance="other", jira_account_id="jira-account-1"
            )
            == "switch-user-2"
        )
        assert (
            await lookup_identity(session, instance="acme", jira_account_id="missing")
            is None
        )


@pytest.mark.asyncio
async def test_identity_mapping_never_guesses_reporter_name_or_email(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    event = _webhook_event()
    event.raw["issue"]["fields"] = {
        "reporter": {
            "accountId": "jira-account-unknown",
            "displayName": "Switch User",
            "emailAddress": "switch-user@example.invalid",
        }
    }
    async with session_factory() as session:
        await set_identity_mapping(
            session,
            instance="acme",
            jira_account_id="jira-account-known",
            switch_user_id="switch-user-1",
            enabled_projects=["KAN"],
        )
        assert await record_webhook_event(
            session,
            event,
            instance="acme",
            enabled_projects=["KAN"],
            webhook_identifier=None,
        )
        await session.commit()

    async with session_factory() as session:
        row = (
            await session.execute(
                text(
                    "SELECT reporter_switch_user_id, wait_channel "
                    "FROM jira_worker_ticket_map WHERE instance = 'acme'"
                )
            )
        ).one()
        assert row == (None, "jira_comments")


@pytest.mark.asyncio
async def test_unmapped_reporter_uses_jira_comments_then_mapping_wakes_ticket(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    event = _webhook_event()
    event.raw["issue"]["fields"] = {
        "reporter": {"accountId": "jira-account-1", "displayName": "Ignored"}
    }
    async with session_factory() as session:
        assert await record_webhook_event(
            session,
            event,
            instance="acme",
            enabled_projects=["KAN"],
            webhook_identifier=None,
        )
        await session.commit()

    async with session_factory() as session:
        initial = (
            await session.execute(
                text(
                    "SELECT reporter_account_id, reporter_switch_user_id, wait_channel "
                    "FROM jira_worker_ticket_map"
                )
            )
        ).one()
        assert initial == ("jira-account-1", None, "jira_comments")
        mapping, updated_ticket_count = await set_identity_mapping(
            session,
            instance="acme",
            jira_account_id="jira-account-1",
            switch_user_id="switch-user-1",
            enabled_projects=["KAN"],
        )
        assert mapping.switch_user_id == "switch-user-1"
        assert updated_ticket_count == 1
        await session.commit()

    async with session_factory() as session:
        resolved = (
            await session.execute(
                text(
                    "SELECT reporter_switch_user_id, wait_channel "
                    "FROM jira_worker_ticket_map"
                )
            )
        ).one()
        assert resolved == ("switch-user-1", "switch")
        outbox = (
            await session.execute(
                text(
                    "SELECT command, payload->>'switch_user_id' FROM jira_worker_outbox"
                )
            )
        ).all()
        # created_at ties within one transaction, so row order is not a contract.
        key = lambda row: (row[0], row[1] or "")  # noqa: E731
        assert sorted(outbox, key=key) == sorted(
            [
                ("sync_ticket_room", None),
                ("upsert_ticket_admin_card", None),
                ("invite_ticket_reporter", "switch-user-1"),
                ("upsert_ticket_admin_card", "switch-user-1"),
                ("sync_ticket_room", "switch-user-1"),
            ],
            key=key,
        )


@pytest.mark.asyncio
async def test_later_webhook_mapping_transition_invites_reporter(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    first = _webhook_event()
    first.raw["issue"]["fields"] = {"reporter": {"accountId": "jira-account-1"}}
    async with session_factory() as session:
        assert await record_webhook_event(
            session,
            first,
            instance="acme",
            enabled_projects=["KAN"],
            webhook_identifier=None,
        )
        await session.commit()

    async with session_factory() as session:
        await set_identity_mapping(
            session,
            instance="acme",
            jira_account_id="jira-account-1",
            switch_user_id="switch-user-1",
            enabled_projects=[],
        )
        await session.commit()

    later = _webhook_event()
    later.raw["changelog"] = {"id": "20002"}
    later.raw["issue"]["fields"] = {
        "reporter": {"accountId": "jira-account-1"},
        "updated": "2026-10-08T08:00:00+00:00",
    }
    async with session_factory() as session:
        assert await record_webhook_event(
            session,
            later,
            instance="acme",
            enabled_projects=["KAN"],
            webhook_identifier=None,
        )
        await session.commit()

    async with session_factory() as session:
        outbox = (
            await session.execute(
                text(
                    "SELECT command, payload->>'switch_user_id' "
                    "FROM jira_worker_outbox ORDER BY created_at, id"
                )
            )
        ).all()
        # created_at ties within one transaction (server now()), so the
        # order of same-instant rows is UUID luck; the set is the assertion.
        key = lambda row: (row[0], row[1] or "")  # noqa: E731
        assert sorted(outbox, key=key) == sorted(
            [
                ("sync_ticket_room", None),
                ("upsert_ticket_admin_card", None),
                ("sync_ticket_room", "switch-user-1"),
                ("invite_ticket_reporter", "switch-user-1"),
                ("upsert_ticket_admin_card", "switch-user-1"),
            ],
            key=key,
        )


@pytest.mark.asyncio
async def test_concurrent_new_webhooks_enqueue_one_follow_up(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        await set_identity_mapping(
            session,
            instance="acme",
            jira_account_id="jira-account-1",
            switch_user_id="switch-user-1",
            enabled_projects=["KAN"],
        )
        await session.commit()

    first, second = _webhook_event(), _webhook_event()
    first.raw["issue"]["fields"] = {"reporter": {"accountId": "jira-account-1"}}
    second.raw["issue"]["fields"] = {"reporter": {"accountId": "jira-account-1"}}
    second.raw["changelog"] = {"id": "20002"}

    async def record(event: NormalizedTriggerEvent) -> None:
        async with session_factory() as session:
            assert await record_webhook_event(
                session,
                event,
                instance="acme",
                enabled_projects=["KAN"],
                webhook_identifier=None,
            )
            await session.commit()

    await asyncio.gather(record(first), record(second))

    async with session_factory() as session:
        commands = (
            (
                await session.execute(
                    text(
                        "SELECT command FROM jira_worker_outbox ORDER BY created_at, id"
                    )
                )
            )
            .scalars()
            .all()
        )
        assert commands.count("invite_ticket_reporter") == 1
        assert commands.count("sync_ticket_room") == 2


@pytest.mark.asyncio
async def test_polled_issue_enqueues_mapped_reporter_invitation(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        await set_identity_mapping(
            session,
            instance="acme",
            jira_account_id="jira-account-1",
            switch_user_id="switch-user-1",
            enabled_projects=["KAN"],
        )
        assert await record_polled_issue(
            session,
            {
                "id": "10001",
                "key": "KAN-1",
                "fields": {
                    "project": {"key": "KAN"},
                    "reporter": {"accountId": "jira-account-1"},
                    "status": {"name": "In Progress"},
                    "updated": "2026-10-08T02:00:00+00:00",
                },
            },
            instance="acme",
        )
        await session.commit()

    async with session_factory() as session:
        commands = (
            (await session.execute(text("SELECT command FROM jira_worker_outbox")))
            .scalars()
            .all()
        )
        assert set(commands) == {"sync_ticket_room", "invite_ticket_reporter"}


@pytest.mark.asyncio
async def test_mapping_change_leaves_disabled_project_ticket_unchanged(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    event = _webhook_event()
    event.raw["issue"]["fields"] = {"reporter": {"accountId": "jira-account-1"}}
    async with session_factory() as session:
        assert await record_webhook_event(
            session,
            event,
            instance="acme",
            enabled_projects=["KAN"],
            webhook_identifier=None,
        )
        await session.commit()

    async with session_factory() as session:
        _, updated_ticket_count = await set_identity_mapping(
            session,
            instance="acme",
            jira_account_id="jira-account-1",
            switch_user_id="switch-user-1",
            enabled_projects=[],
        )
        assert updated_ticket_count == 0
        await session.commit()

    async with session_factory() as session:
        row = (
            await session.execute(
                text(
                    "SELECT reporter_switch_user_id, wait_channel "
                    "FROM jira_worker_ticket_map"
                )
            )
        ).one()
        assert row == (None, "jira_comments")


@pytest.mark.asyncio
async def test_identity_mapping_update_and_remove(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        await set_identity_mapping(
            session,
            instance="acme",
            jira_account_id="jira-account-1",
            switch_user_id="switch-user-1",
            enabled_projects=["KAN"],
        )
        updated, _ = await set_identity_mapping(
            session,
            instance="acme",
            jira_account_id="jira-account-1",
            switch_user_id="switch-user-2",
            enabled_projects=["KAN"],
        )
        assert updated.switch_user_id == "switch-user-2"
        await remove_identity_mapping(
            session,
            instance="acme",
            jira_account_id="jira-account-1",
            enabled_projects=["KAN"],
        )
        await session.commit()

    async with session_factory() as session:
        assert (
            await lookup_identity(
                session, instance="acme", jira_account_id="jira-account-1"
            )
            is None
        )


@pytest.mark.asyncio
async def test_ticket_keeps_latest_jira_updated_timestamp(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    newer, older = _webhook_event(), _webhook_event()
    newer.raw["issue"]["fields"] = {"updated": "2026-10-08T02:00:00+00:00"}
    older.raw["issue"]["fields"] = {"updated": "2026-10-08T01:00:00+00:00"}
    newer = replace(newer, summary="New summary")
    older = replace(older, summary="Old summary")
    older.raw["changelog"] = {"id": "20002"}
    async with session_factory() as session:
        assert await record_webhook_event(
            session,
            newer,
            instance="acme",
            enabled_projects=["KAN"],
            webhook_identifier=None,
        )
        assert await record_webhook_event(
            session,
            older,
            instance="acme",
            enabled_projects=["KAN"],
            webhook_identifier=None,
        )
        await session.commit()
    async with session_factory() as session:
        ticket = (
            await session.execute(
                text(
                    "SELECT summary FROM jira_worker_ticket_map WHERE instance = 'acme'"
                )
            )
        ).scalar_one()
        assert ticket == "New summary"


@pytest.mark.asyncio
async def test_unique_key_enforced_at_db_level(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        assert await record_event(
            session,
            instance="acme",
            idempotency_key="k|1",
            issue_key="KAN-1",
            project_key="KAN",
            event_kind="updated",
            webhook_event="jira:issue_updated",
            payload=None,
        )
        assert (
            await record_event(
                session,
                instance="acme",
                idempotency_key="k|1",
                issue_key="KAN-1",
                project_key="KAN",
                event_kind="updated",
                webhook_event="jira:issue_updated",
                payload=None,
            )
            is None
        )
        await session.commit()


async def _insert_due_job(
    session: AsyncSession,
    *,
    instance: str = "acme",
    project: str = "KAN",
    due: datetime,
) -> str:
    job = JiraWorkerJob(
        kind="watermark_poll",
        instance=instance,
        project_key=project,
        due_at=due,
        status="pending",
    )
    session.add(job)
    await session.flush()
    return job.id


@pytest.mark.asyncio
async def test_two_claimers_one_wins(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    now = datetime.now(UTC)
    async with session_factory() as session:
        await _insert_due_job(session, due=now - timedelta(seconds=1))
        await session.commit()

    async def claim(owner: str) -> int:
        async with session_factory() as session:
            jobs = await claim_due_jobs(
                session, now=now, owner=owner, interval_seconds=300
            )
            await session.commit()
            return len(jobs)

    import asyncio

    first, second = await asyncio.gather(claim("a"), claim("b"))
    assert sorted((first, second)) == [0, 1]


@pytest.mark.asyncio
async def test_restart_rereads_overdue_rows(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """An overdue pending job survives a 'restart': a fresh scheduler claims it."""
    async with session_factory() as session:
        await _insert_due_job(
            session, instance="acme", due=datetime.now(UTC) - timedelta(hours=1)
        )
        await session.commit()
    scheduler = JiraWorkerScheduler(
        session_factory=session_factory,
        config=_config(jira_worker_enabled_projects={"acme": ["KAN"]}),
        poll_clients={"acme": _FakePollClient([])},  # type: ignore[dict-item]
    )
    await scheduler.run_once()
    async with session_factory() as session:
        job = (
            await session.execute(
                text(
                    "SELECT status FROM jira_worker_job_record WHERE project_key='KAN' ORDER BY due_at LIMIT 1"
                )
            )
        ).scalar_one()
        assert job == "done"


@pytest.mark.asyncio
async def test_claim_commits_successor_before_poll_runs(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    now = datetime.now(UTC)
    async with session_factory() as session:
        await _insert_due_job(session, due=now - timedelta(seconds=1))
        jobs = await claim_due_jobs(
            session, now=now, owner="first", interval_seconds=300
        )
        assert len(jobs) == 1
        await session.commit()
    async with session_factory() as session:
        rows = (
            await session.execute(
                text(
                    "SELECT status, payload FROM jira_worker_job_record ORDER BY due_at"
                )
            )
        ).all()
        assert rows[0][0] == "claimed"
        assert rows[1][0] == "pending"
        assert rows[1][1]["predecessor_id"] == jobs[0].id
    async with session_factory() as session:
        await session.execute(
            text(
                "UPDATE jira_worker_job_record SET due_at = now() - interval '1 second' "
                "WHERE status = 'pending'"
            )
        )
        await session.commit()
    client = _FakePollClient([])
    scheduler = JiraWorkerScheduler(
        session_factory=session_factory,
        config=_config(jira_worker_enabled_projects={"acme": ["KAN"]}),
        poll_clients={"acme": client},  # type: ignore[dict-item]
    )
    await scheduler.run_once()
    assert client.calls == [{"project_key": "KAN", "updated_since": None}]


class _FakePollClient:
    def __init__(self, issues: list[dict[str, Any]]) -> None:
        self._issues = issues
        self.calls: list[dict[str, Any]] = []

    async def search_updated(
        self, *, project_key: str, updated_since: str | None
    ) -> list[dict[str, Any]]:
        self.calls.append({"project_key": project_key, "updated_since": updated_since})
        return self._issues


@pytest.mark.asyncio
async def test_poll_reads_all_jira_pages_before_recording_watermark(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    issues = [
        _issue(f"KAN-{number}", "2026-10-08T01:00:00.000+0000")
        for number in range(1, 52)
    ]
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.params.get("nextPageToken") == "page-2":
            return httpx.Response(200, json={"issues": issues[50:], "isLast": True})
        return httpx.Response(
            200,
            json={"issues": issues[:50], "nextPageToken": "page-2", "isLast": False},
        )

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = JiraPollClient(
        base_url="https://jira.example",
        email="worker@example.com",
        api_token="token",
        client=http_client,
    )
    scheduler = JiraWorkerScheduler(
        session_factory=session_factory,
        config=_config(jira_worker_enabled_projects={"acme": ["KAN"]}),
        poll_clients={"acme": client},
    )
    await scheduler.run_once()
    async with session_factory() as session:
        count = (
            await session.execute(text("SELECT count(*) FROM jira_worker_event_log"))
        ).scalar_one()
        watermark = (
            await session.execute(
                text(
                    "SELECT payload->>'watermark' FROM jira_worker_job_record "
                    "WHERE status = 'pending'"
                )
            )
        ).scalar_one()
    await http_client.aclose()
    assert len(requests) == 2
    assert count == 51
    assert watermark == "2026-10-08T01:00:00.000+0000"


def _issue(key: str, updated: str) -> dict[str, Any]:
    return {
        "id": f"id-{key}",
        "key": key,
        "fields": {
            "summary": f"Summary {key}",
            "status": {"name": "To Do"},
            "updated": updated,
            "project": {"key": "KAN"},
        },
    }


@pytest.mark.asyncio
async def test_poll_advances_watermark(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    client = _FakePollClient(
        [
            _issue("KAN-1", "2026-10-08T01:00:00.000+0000"),
            _issue("KAN-2", "2026-10-08T02:00:00.000+0000"),
        ]
    )
    scheduler = JiraWorkerScheduler(
        session_factory=session_factory,
        config=_config(jira_worker_enabled_projects={"acme": ["KAN"]}),
        poll_clients={"acme": client},  # type: ignore[dict-item]
    )
    await scheduler.run_once()
    assert client.calls and client.calls[0]["updated_since"] is None
    async with session_factory() as session:
        count = (
            await session.execute(text("SELECT count(*) FROM jira_worker_event_log"))
        ).scalar_one()
        assert count == 2
        row = (
            await session.execute(
                text(
                    "SELECT status, payload FROM jira_worker_job_record ORDER BY due_at"
                )
            )
        ).all()
        assert row[0][0] == "done"
        assert row[1][0] == "pending"
        assert row[1][1]["watermark"] == "2026-10-08T02:00:00.000+0000"
    # Second pass sends the watermark and records nothing new: make the
    # scheduled job due again, as if the interval elapsed.
    async with session_factory() as session:
        await session.execute(
            text(
                "UPDATE jira_worker_job_record SET due_at = now() - interval '1 second' WHERE status='pending'"
            )
        )
        await session.commit()
    await scheduler.run_once()
    assert client.calls[-1]["updated_since"] == "2026-10-08T02:00:00.000+0000"
    async with session_factory() as session:
        count = (
            await session.execute(text("SELECT count(*) FROM jira_worker_event_log"))
        ).scalar_one()
        assert count == 2


@pytest.mark.asyncio
async def test_poll_skipped_without_credentials(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    assert build_poll_clients(_config()) == {}
    scheduler = JiraWorkerScheduler(
        session_factory=session_factory,
        config=_config(jira_worker_enabled_projects={"acme": ["KAN"]}),
        poll_clients={},
    )
    await scheduler.run_once()
    async with session_factory() as session:
        count = (
            await session.execute(text("SELECT count(*) FROM jira_worker_event_log"))
        ).scalar_one()
        assert count == 0
        pending = (
            await session.execute(
                text(
                    "SELECT count(*) FROM jira_worker_job_record WHERE status='pending'"
                )
            )
        ).scalar_one()
        assert pending == 0


@pytest.mark.asyncio
async def test_ensure_poll_job_is_idempotent(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        await ensure_poll_job(
            session, instance="acme", project_key="KAN", interval_seconds=300
        )
        await ensure_poll_job(
            session, instance="acme", project_key="KAN", interval_seconds=300
        )
        await session.commit()
        count = (
            await session.execute(text("SELECT count(*) FROM jira_worker_job_record"))
        ).scalar_one()
        assert count == 1


@pytest.mark.asyncio
async def test_complete_job_marks_error(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        job_id = await _insert_due_job(session, due=datetime.now(UTC))
        await session.commit()
    async with session_factory() as session:
        jobs = await claim_due_jobs(session, owner="t", interval_seconds=300)
        assert len(jobs) == 1
        await complete_job(session, jobs[0].id, status="error", error="boom")
        await session.commit()
    async with session_factory() as session:
        job = await session.get(JiraWorkerJob, job_id)
        assert job is not None and job.status == "error" and job.error == "boom"


@pytest.mark.asyncio
async def test_scheduler_start_stop(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    scheduler = JiraWorkerScheduler(
        session_factory=session_factory,
        config=_config(),
        poll_clients={},
    )
    assert scheduler.running is False
    scheduler.start()
    assert scheduler.running is True
    await scheduler.stop()
    assert scheduler.running is False
