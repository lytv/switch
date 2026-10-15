"""The gateway release surface requires admin authorization and a request ID."""

from __future__ import annotations

import uuid
from datetime import timedelta

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import select

from switch_core.bridges.jira.activity import TURN
from switch_core.bridges.jira.outbox import INTERRUPTED_TURN, TURN_FAILED, park_ticket
from switch_core.db.models import JiraWorkerEvent, JiraWorkerJob, JiraWorkerTicket, User
from switch_core.gateway import jira_triggers
from switch_core.gateway.auth import get_current_user
from switch_core.gateway.dependencies import (
    get_config,
    get_session,
    get_session_factory,
)
from tests.switch_core.bridges.jira.test_admin import NOW
from tests.switch_core.bridges.jira.test_outbox import client_for
from tests.switch_core.bridges.jira.test_release import setup_release


def app_for(factory, user, config):
    app = FastAPI()
    app.include_router(jira_triggers.router, prefix="/jira-triggers")

    async def session():
        async with factory() as value:
            yield value

    app.dependency_overrides[get_session] = session
    app.dependency_overrides[get_config] = lambda: config
    app.dependency_overrides[get_current_user] = lambda: user
    app.dependency_overrides[get_session_factory] = lambda: factory
    return app


async def test_release_api_authentication_validation_and_duplicate_request(
    session_factory, monkeypatch
):
    setup = await setup_release(session_factory)
    _, admin_id, member_id, _, jira, config = setup
    monkeypatch.setattr(
        jira_triggers, "build_poll_clients", lambda _: {"acme": client_for(jira)}
    )
    async with session_factory() as session:
        admin = await session.get(User, admin_id)
        member = await session.get(User, member_id)
    path = "/jira-triggers/worker/tickets/KAN-1/release?instance=acme"
    request = {"request_id": str(uuid.uuid4())}
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app_for(session_factory, member, config)),
        base_url="http://test",
    ) as client:
        assert (await client.post(path, json=request)).status_code == 403
    assert not jira.requests
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app_for(session_factory, admin, config)),
        base_url="http://test",
    ) as client:
        assert (await client.post(path, json={})).status_code == 422
        assert (
            await client.post(path, json={"request_id": "not-a-uuid"})
        ).status_code == 422
        assert (
            await client.post(
                "/jira-triggers/worker/tickets/MISSING/release?instance=acme",
                json=request,
            )
        ).status_code == 404
        first = await client.post(path, json=request)
        repeated = await client.post(path, json=request)
        new_request = await client.post(path, json={"request_id": str(uuid.uuid4())})
    assert new_request.status_code == 400
    assert new_request.json()["detail"] == "ticket is not parked"
    assert first.status_code == repeated.status_code == 200
    assert first.json() == repeated.json()
    assert first.json()["payload"]["released_by"] == admin_id
    assert not jira.writes


@pytest.mark.parametrize("park_reason", [None, ""])
@pytest.mark.parametrize("error", [INTERRUPTED_TURN, TURN_FAILED])
async def test_release_api_rejects_unparked_ticket_without_reconciling_unknown_turn(
    session_factory, monkeypatch, park_reason, error
):
    setup = await setup_release(session_factory)
    key, admin_id, _, _, jira, config = setup
    monkeypatch.setattr(
        jira_triggers, "build_poll_clients", lambda _: {"acme": client_for(jira)}
    )
    async with session_factory() as session:
        admin = await session.get(User, admin_id)
        ticket = await session.get(JiraWorkerTicket, key)
        ticket.worker_parked_reason = park_reason
        ticket.agent_state = "waiting"
        ticket.admin_reasons = None
        session.add(
            JiraWorkerJob(
                kind=TURN,
                instance="acme",
                project_key="KAN",
                status="error",
                due_at=NOW - timedelta(days=1),
                claimed_at=NOW - timedelta(days=1),
                payload={"ticket_id": key},
                error=error,
            )
        )
        await session.commit()
    path = "/jira-triggers/worker/tickets/KAN-1/release?instance=acme"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app_for(session_factory, admin, config)),
        base_url="http://test",
    ) as client:
        response = await client.post(path, json={"request_id": str(uuid.uuid4())})
        assert response.status_code == 400
        assert response.json()["detail"] == "ticket is not parked"
        async with session_factory() as session:
            assert not await session.scalar(
                select(JiraWorkerEvent.id).where(
                    JiraWorkerEvent.event_kind == "admin_release"
                )
            )
            ticket = await session.get(JiraWorkerTicket, key)
            assert ticket.worker_parked_reason == park_reason
            assert ticket.agent_state == "waiting"
            assert ticket.tokens_used == 123
            park_ticket(ticket, "Human Jira action")
            await session.commit()
        response = await client.post(path, json={"request_id": str(uuid.uuid4())})
    assert response.status_code == 400
    assert "Unknown turn outcome" in response.json()["detail"]
    assert not jira.requests
    async with session_factory() as session:
        assert not await session.scalar(
            select(JiraWorkerEvent.id).where(
                JiraWorkerEvent.event_kind == "admin_release"
            )
        )
        ticket = await session.get(JiraWorkerTicket, key)
        assert ticket.worker_parked_reason == "Human Jira action"
        assert ticket.tokens_used == 123


async def test_release_api_reports_unstable_jira_read(session_factory, monkeypatch):
    setup = await setup_release(session_factory)
    _, admin_id, _, _, jira, config = setup
    jira.read_change = True
    monkeypatch.setattr(
        jira_triggers, "build_poll_clients", lambda _: {"acme": client_for(jira)}
    )
    async with session_factory() as session:
        admin = await session.get(User, admin_id)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app_for(session_factory, admin, config)),
        base_url="http://test",
    ) as client:
        response = await client.post(
            "/jira-triggers/worker/tickets/KAN-1/release?instance=acme",
            json={"request_id": str(uuid.uuid4())},
        )
    assert response.status_code == 409
    assert "changed while" in response.json()["detail"]
    assert not jira.writes
