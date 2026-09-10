"""End-to-end Jira webhook → rule match → addressed room post → dedupe (KAN-103)."""

from __future__ import annotations

import asyncio
from typing import Any

import httpx
import pytest
from fastapi import FastAPI
from httpx import ASGITransport

from switch_core.bridges.agent.api_key_cache import ApiKeyCache
from switch_core.bridges.agent.auth import BearerAuthMiddleware
from switch_core.bridges.jira.auth import SECRET_HEADER
from switch_core.bridges.jira.identity import ensure_jira_system_agent
from switch_core.bridges.jira.routes import init_jira_routes, router
from switch_core.bridges.jira.service import JiraBridgeService
from switch_core.db.models import JiraTrigger
from switch_core.db.stores.jira_trigger_store import JiraTriggerStore
from switch_core.room_service import RoomCreateConfig
from tests.integration.conftest import Harness, SessionEnv

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

INSTANCE = "acme"
SECRET = "e2e-jira-webhook-secret"


def _payload() -> dict[str, Any]:
    return {
        "webhookEvent": "jira:issue_updated",
        "issue": {
            "key": "E2E-7",
            "self": "https://example.atlassian.net/rest/api/2/issue/7",
            "fields": {
                "summary": "Wire Jira e2e",
                "issuetype": {"name": "Story"},
                "project": {"key": "E2E"},
                "status": {"name": "In Progress"},
                "assignee": {"displayName": "Ada"},
                "priority": {"name": "High"},
                "reporter": {"displayName": "Bob"},
                "labels": ["agentic"],
            },
        },
        "changelog": {
            "items": [
                {
                    "field": "status",
                    "fieldId": "status",
                    "fromString": "To Do",
                    "toString": "In Progress",
                }
            ]
        },
    }


async def _timeline_bodies(
    harness: Harness, session_env: SessionEnv, room_id: str
) -> list[str]:
    async with harness.session_factory() as session:  # type: ignore[operator]
        rows = await session_env.message_store.list_for_room(
            session, room_id, after_seq=0, limit=100
        )
    out: list[str] = []
    for row in rows:
        if row.event_type != "m.room.message":
            continue
        body = row.content.get("body") if isinstance(row.content, dict) else None
        if isinstance(body, str):
            out.append(body)
    return out


async def test_jira_webhook_addresses_agent_and_dedupes(
    harness: Harness, session_env: SessionEnv
) -> None:
    # Point the shared config at a webhook secret so the system agent provisions.
    session_env.config.jira_webhook_secrets = {INSTANCE: SECRET}
    session_env.config.jira_agent_name = "jira"
    session_env.config.jira_rule_cooldown_seconds = 0
    session_env.config.jira_retry_max_attempts = 1

    jira_agent_id = await ensure_jira_system_agent(
        protocol=harness.protocol,
        session_factory=harness.session_factory,  # type: ignore[arg-type]
        agent_store=session_env.agent_store,
        user_store=session_env.user_store,
        config=session_env.config,
    )
    assert jira_agent_id is not None

    coder = await harness.register_agent("e2e-coder")
    await harness.start_clients()
    # ensure_jira_system_agent is outside harness._registered; wait for it too.
    deadline = asyncio.get_event_loop().time() + 20
    jira_client = None
    while asyncio.get_event_loop().time() < deadline:
        jira_client = harness.client_lifecycle.get_by_agent_id(jira_agent_id)
        if jira_client is not None:
            break
        await asyncio.sleep(0.1)
    assert jira_client is not None, "Jira system agent client never started"
    await jira_client.wait_ready()

    room_result = await harness.room_service.create_room(
        RoomCreateConfig(
            name="e2e-jira-room",
            description="jira webhook e2e",
            agent_ids=[jira_agent_id, coder.agent_id],
        )
    )
    room = room_result.room

    trigger_store = JiraTriggerStore()
    async with harness.session_factory() as session:  # type: ignore[operator]
        rule = JiraTrigger(
            name="e2e-start",
            enabled=True,
            instance=INSTANCE,
            project_key="E2E",
            issue_type="Story",
            fire_on="transition",
            target_status="In Progress",
            target_kind="room",
            target_room_id=room.id,
            agent_name="e2e-coder",
            message_template="{{issue.key}} → {{issue.status}}",
            thread_by="new",
        )
        await trigger_store.create(session, rule)
        await session.commit()
        rule_id = rule.id

    service = JiraBridgeService(
        session_factory=harness.session_factory,  # type: ignore[arg-type]
        trigger_store=trigger_store,
        agent_store=session_env.agent_store,
        room_store=session_env.room_store,
        protocol=harness.protocol,
        config=session_env.config,
    )
    init_jira_routes(service=service, secrets_by_instance={INSTANCE: SECRET})
    app = FastAPI()
    app.include_router(router)
    app.add_middleware(
        BearerAuthMiddleware,
        agent_store=session_env.agent_store,
        api_key_store=session_env.api_key_store,
        api_key_cache=ApiKeyCache(ttl_seconds=0, max_entries=8),
        session_factory=harness.session_factory,
    )

    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            f"/integrations/jira/{INSTANCE}",
            json=_payload(),
            headers={SECRET_HEADER: SECRET},
        )
        assert resp.status_code == 202, resp.text

        bodies = await _timeline_bodies(harness, session_env, room.id)
        assert any(
            "@e2e-coder" in b and "E2E-7" in b and "In Progress" in b for b in bodies
        ), bodies

        async with harness.session_factory() as session:  # type: ignore[operator]
            deliveries = await trigger_store.list_deliveries(
                session, instance=INSTANCE, rule_id=rule_id, limit=20
            )
        delivered = [d for d in deliveries if d.status == "delivered"]
        assert len(delivered) == 1
        assert delivered[0].matched_rule_ids == [rule_id]
        assert delivered[0].room_results
        assert delivered[0].room_results[0]["status"] == "ok"

        before_jira = sum(1 for b in bodies if "E2E-7" in b and "@e2e-coder" in b)
        assert before_jira == 1

        resp2 = await client.post(
            f"/integrations/jira/{INSTANCE}",
            json=_payload(),
            headers={SECRET_HEADER: SECRET},
        )
        assert resp2.status_code == 202

        bodies2 = await _timeline_bodies(harness, session_env, room.id)
        after_jira = sum(1 for b in bodies2 if "E2E-7" in b and "@e2e-coder" in b)
        assert after_jira == 1

        async with harness.session_factory() as session:  # type: ignore[operator]
            deliveries2 = await trigger_store.list_deliveries(
                session, instance=INSTANCE, rule_id=rule_id, limit=20
            )
        assert any(d.status == "suppressed_dedupe" for d in deliveries2)
