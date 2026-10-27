"""Step 8 hardening: fault matrix probes with real PostgreSQL.

Each test pins one cell of the cross-component fault matrix documented in
``core/switch_core/bridges/jira/README.md`` ("Hardening and recovery").
Cells proven elsewhere are referenced there, not duplicated here.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi import FastAPI
from httpx import ASGITransport
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from switch_core.bridges.jira.auth import SECRET_HEADER
from switch_core.bridges.jira.routes import init_jira_routes, router
from switch_core.bridges.jira.service import JiraBridgeService
from switch_core.bridges.jira.worker import JiraWorkerScheduler
from switch_core.config import SwitchConfig
from switch_core.db.models import JiraWorkerEvent, JiraWorkerJob

INSTANCE = "acme"
PROJECT = "PROJ"
SECRET = "hardening-webhook-secret"


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
        "jira_worker_enabled_projects": {INSTANCE: [PROJECT]},
    }
    base.update(overrides)
    return SwitchConfig(**base)


def _payload() -> dict[str, Any]:
    return {
        "webhookEvent": "jira:issue_updated",
        "issue": {
            "key": f"{PROJECT}-7",
            "id": "10007",
            "self": "https://example.atlassian.net/rest/api/2/issue/10007",
            "fields": {
                "summary": "Hardening probe ticket",
                "issuetype": {"name": "Bug"},
                "project": {"key": PROJECT},
                "status": {"name": "To Do"},
                "updated": "2026-10-27T03:00:00.000+0000",
            },
        },
    }


def _service(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    config: SwitchConfig | None = None,
) -> JiraBridgeService:
    trigger_store = AsyncMock()
    trigger_store.list = AsyncMock(return_value=[])
    return JiraBridgeService(
        session_factory=session_factory,  # type: ignore[arg-type]
        trigger_store=trigger_store,
        agent_store=AsyncMock(),
        room_store=AsyncMock(),
        protocol=AsyncMock(),
        config=config or _config(),
    )


def _app(service: JiraBridgeService) -> FastAPI:
    init_jira_routes(service=service, secrets_by_instance={INSTANCE: SECRET})
    app = FastAPI()
    app.include_router(router)
    return app


async def _post(app: FastAPI, payload: dict[str, Any]) -> httpx.Response:
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        return await client.post(
            f"/integrations/jira/{INSTANCE}",
            json=payload,
            headers={
                SECRET_HEADER: SECRET,
                "X-Atlassian-Webhook-Identifier": "hardening-event-1",
            },
        )


async def _event_rows(
    session_factory: async_sessionmaker[AsyncSession],
) -> list[JiraWorkerEvent]:
    async with session_factory() as session:
        result = await session.execute(
            select(JiraWorkerEvent).where(JiraWorkerEvent.instance == INSTANCE)
        )
        return list(result.scalars().all())


async def test_webhook_persists_comment_event_before_202(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """[WH-1] A 202 means the acknowledged event row is already committed."""
    app = _app(_service(session_factory))

    response = await _post(app, _payload())

    assert response.status_code == 202
    rows = await _event_rows(session_factory)
    assert len(rows) == 1
    assert rows[0].issue_key == f"{PROJECT}-7"


async def test_webhook_duplicate_delivery_is_idempotent(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """[WH-2] Redelivery of an acknowledged event stays 202 with one row."""
    app = _app(_service(session_factory))

    first = await _post(app, _payload())
    second = await _post(app, _payload())

    assert first.status_code == 202
    assert second.status_code == 202
    assert len(await _event_rows(session_factory)) == 1


async def test_webhook_storage_failure_returns_503_without_fan_out(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """[WH-3] Nothing is acknowledged when the event cannot persist."""
    service = _service(session_factory)

    def _failing_factory():  # type: ignore[no-untyped-def]
        raise RuntimeError("db is down")

    service._session_factory = _failing_factory  # type: ignore[assignment]
    service._protocol = AsyncMock()
    app = _app(service)

    response = await _post(app, _payload())

    assert response.status_code == 503
    service._protocol.send_targeted_message.assert_not_called()


async def test_rejected_webhook_never_logs_or_returns_secret(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """[WH-4] The provided secret never appears in logs or responses."""
    from fastapi.testclient import TestClient

    from switch_core.bridges.agent.api_key_cache import ApiKeyCache
    from switch_core.bridges.agent.auth import BearerAuthMiddleware

    provided = "WRONG-S3CR3T-PROVIDED-VALUE"
    init_jira_routes(service=AsyncMock(), secrets_by_instance={INSTANCE: "real-secret"})
    app = FastAPI()
    app.include_router(router)
    app.add_middleware(
        BearerAuthMiddleware,
        agent_store=None,  # type: ignore[arg-type]
        api_key_store=None,  # type: ignore[arg-type]
        api_key_cache=ApiKeyCache(ttl_seconds=0, max_entries=1),
        session_factory=None,  # type: ignore[arg-type]
    )
    client = TestClient(app)
    with caplog.at_level(logging.WARNING, logger="switch_core.bridges.jira.auth"):
        response = client.post(
            f"/integrations/jira/{INSTANCE}",
            json=_payload(),
            headers={SECRET_HEADER: provided},
        )
    assert response.status_code == 401
    assert provided not in response.text
    assert provided not in caplog.text


async def test_unknown_job_handler_leaves_row_claimed(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """[CG-1] The fallback never resolves work it does not understand."""
    async with session_factory() as session:
        session.add(
            JiraWorkerJob(
                kind="future_job",
                instance=INSTANCE,
                project_key=PROJECT,
                due_at=datetime.now(UTC),
                status="claimed",
                attempts=1,
                payload={"preserve": "input"},
            )
        )
        await session.commit()
    scheduler = JiraWorkerScheduler(session_factory=session_factory, config=_config())
    async with session_factory() as session:
        job = (
            await session.execute(
                select(JiraWorkerJob).where(JiraWorkerJob.kind == "future_job")
            )
        ).scalar_one()
        await scheduler._handle_job(job)
    async with session_factory() as session:
        persisted = (
            await session.execute(
                select(JiraWorkerJob).where(JiraWorkerJob.kind == "future_job")
            )
        ).scalar_one()
        assert persisted.status == "claimed"
        assert persisted.attempts == 1
        assert persisted.payload == {"preserve": "input"}
