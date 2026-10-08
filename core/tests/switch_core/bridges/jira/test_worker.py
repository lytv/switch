"""Step-1 worker tests: migration, intake, claims, scheduler, poll."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from alembic.config import Config
from alembic.runtime.environment import EnvironmentContext
from alembic.script import ScriptDirectory
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

import switch_core.db.models  # noqa: F401 — registers every table on Base.metadata
from switch_core.bridges.jira.worker import (
    JiraWorkerScheduler,
    build_poll_client,
    claim_due_jobs,
    complete_job,
    ensure_poll_job,
    is_worker_enabled,
    record_event,
    record_webhook_event,
    webhook_idempotency_key,
)
from switch_core.bridges.trigger_source import NormalizedTriggerEvent
from switch_core.config import SwitchConfig
from switch_core.db.base import Base
from switch_core.db.models import JiraWorkerEvent, JiraWorkerJob, JiraWorkerTicket

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


def _webhook_event(project: str = "KAN") -> NormalizedTriggerEvent:
    raw: dict[str, Any] = {
        "webhookEvent": "jira:issue_updated",
        "timestamp": 1791443000000,
        "issue": {"id": "10001", "key": "KAN-1"},
        "changelog": {"id": "20001"},
    }
    return NormalizedTriggerEvent(
        event_kind="updated",
        webhook_event="jira:issue_updated",
        key="KAN-1",
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
    assert _config().jira_worker_enabled_projects == []
    assert is_worker_enabled(_config(), "KAN") is False
    async with session_factory() as session:
        assert (
            await record_webhook_event(session, _webhook_event(), enabled_projects=[])
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
                session, _webhook_event(), enabled_projects=["KAN"]
            )
            is True
        )
        assert (
            await record_webhook_event(
                session, _webhook_event(), enabled_projects=["KAN"]
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
async def test_distinct_changelog_ids_are_distinct_events(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    first, second = _webhook_event(), _webhook_event()
    second.raw["changelog"] = {"id": "20002"}
    async with session_factory() as session:
        assert (
            await record_webhook_event(session, first, enabled_projects=["KAN"]) is True
        )
        assert (
            await record_webhook_event(session, second, enabled_projects=["KAN"])
            is True
        )
        await session.commit()
    async with session_factory() as session:
        count = (
            await session.execute(text("SELECT count(*) FROM jira_worker_event_log"))
        ).scalar_one()
        assert count == 2


@pytest.mark.asyncio
async def test_unique_key_enforced_at_db_level(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        assert await record_event(
            session,
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
    session: AsyncSession, *, project: str = "KAN", due: datetime
) -> str:
    job = JiraWorkerJob(
        kind="watermark_poll", project_key=project, due_at=due, status="pending"
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
            jobs = await claim_due_jobs(session, now=now, owner=owner)
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
        await _insert_due_job(session, due=datetime.now(UTC) - timedelta(hours=1))
        await session.commit()
    scheduler = JiraWorkerScheduler(
        session_factory=session_factory,
        config=_config(jira_worker_enabled_projects=["KAN"]),
        poll_client=None,
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


class _FakePollClient:
    def __init__(self, issues: list[dict[str, Any]]) -> None:
        self._issues = issues
        self.calls: list[dict[str, Any]] = []

    async def search_updated(
        self, *, project_key: str, updated_since: str | None
    ) -> list[dict[str, Any]]:
        self.calls.append({"project_key": project_key, "updated_since": updated_since})
        return self._issues


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
        config=_config(jira_worker_enabled_projects=["KAN"]),
        poll_client=client,  # type: ignore[arg-type]
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
            text("UPDATE jira_worker_job_record SET due_at = now() - interval '1 second' WHERE status='pending'")
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
    caplog: pytest.LogCaptureFixture,
) -> None:
    assert build_poll_client(_config()) is None
    scheduler = JiraWorkerScheduler(
        session_factory=session_factory,
        config=_config(jira_worker_enabled_projects=["KAN"]),
        poll_client=None,
    )
    with caplog.at_level(logging.WARNING, logger="switch_core.bridges.jira.worker"):
        await scheduler.run_once()
    assert "no Jira credentials" in caplog.text
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
        assert pending == 1


@pytest.mark.asyncio
async def test_ensure_poll_job_is_idempotent(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        await ensure_poll_job(session, project_key="KAN", interval_seconds=300)
        await ensure_poll_job(session, project_key="KAN", interval_seconds=300)
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
        jobs = await claim_due_jobs(session, owner="t")
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
        poll_client=None,
    )
    assert scheduler.running is False
    scheduler.start()
    assert scheduler.running is True
    await scheduler.stop()
    assert scheduler.running is False


def test_event_model_registered() -> None:
    assert JiraWorkerEvent.__tablename__ == "jira_worker_event_log"
