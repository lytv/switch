from __future__ import annotations

from typing import Any
from unittest.mock import ANY, AsyncMock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from switch_core.bridges.agent.api_key_cache import ApiKeyCache
from switch_core.bridges.agent.auth import BearerAuthMiddleware, _is_public_path
from switch_core.bridges.jira.auth import SECRET_HEADER
from switch_core.bridges.jira.routes import init_jira_routes, router


def _payload() -> dict[str, Any]:
    return {
        "webhookEvent": "jira:issue_created",
        "issue": {
            "key": "PROJ-42",
            "self": "https://example.atlassian.net/rest/api/2/issue/42",
            "fields": {
                "summary": "Ship it",
                "issuetype": {"name": "Story"},
                "project": {"key": "PROJ"},
                "status": {"name": "To Do"},
            },
        },
    }


def _client(secrets: dict[str, str], service: Any) -> TestClient:
    init_jira_routes(service=service, secrets_by_instance=secrets)
    app = FastAPI()
    app.include_router(router)
    app.add_middleware(
        BearerAuthMiddleware,
        agent_store=None,  # type: ignore[arg-type]
        api_key_store=None,  # type: ignore[arg-type]
        api_key_cache=ApiKeyCache(ttl_seconds=0, max_entries=1),
        session_factory=None,  # type: ignore[arg-type]
    )
    return TestClient(app)


class TestJiraWebhookRoute:
    def test_integrations_prefix_is_public(self) -> None:
        assert _is_public_path("/integrations/jira/acme") is True

    def test_missing_secret_401(self) -> None:
        service = AsyncMock()
        client = _client({"acme": "s3cret"}, service)
        resp = client.post("/integrations/jira/acme", json=_payload())
        assert resp.status_code == 401
        service.process_event.assert_not_called()

    def test_wrong_secret_401(self) -> None:
        service = AsyncMock()
        client = _client({"acme": "s3cret"}, service)
        resp = client.post(
            "/integrations/jira/acme",
            json=_payload(),
            headers={SECRET_HEADER: "nope"},
        )
        assert resp.status_code == 401

    def test_valid_secret_returns_202_and_processes(self) -> None:
        service = AsyncMock()
        client = _client({"acme": "s3cret"}, service)
        resp = client.post(
            "/integrations/jira/acme",
            json=_payload(),
            headers={SECRET_HEADER: "s3cret"},
        )
        assert resp.status_code == 202
        assert resp.json()["status"] == "accepted"
        service.process_event.assert_awaited()

    def test_worker_intake_uses_authenticated_instance_and_header_key(self) -> None:
        service = AsyncMock()
        client = _client({"acme": "s3cret"}, service)
        response = client.post(
            "/integrations/jira/acme",
            json=_payload(),
            headers={
                SECRET_HEADER: "s3cret",
                "X-Atlassian-Webhook-Identifier": "atlassian-event-1",
            },
        )
        assert response.status_code == 202
        service.record_worker_intake.assert_awaited_once_with(
            instance="acme", event=ANY, webhook_identifier="atlassian-event-1"
        )

    def test_malformed_payload_400(self) -> None:
        service = AsyncMock()
        client = _client({"acme": "s3cret"}, service)
        resp = client.post(
            "/integrations/jira/acme",
            json={"webhookEvent": "jira:issue_created"},
            headers={SECRET_HEADER: "s3cret"},
        )
        assert resp.status_code == 400
        service.process_event.assert_not_called()
