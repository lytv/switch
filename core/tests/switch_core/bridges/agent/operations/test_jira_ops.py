"""Jira agent operations — Option B (CHOO agent-ops for Jira).

Proves each new operation works over the agent-ops HTTP channel
(`POST /agents/{id}/ops/...`, the door switch-axi uses), enforces the same
validation as the admin-cookie routes, and that secret reveal/rotate remain
unreachable through agent-ops while their admin-cookie gate stays intact.

The channel is exercised with httpx's ASGI transport (same event loop as the
test, so the real Postgres fixture works) rather than TestClient, whose
worker-thread portal cannot share asyncpg connections.
"""

from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from switch_core.bridges.agent.api.operations import router
from switch_core.bridges.agent.auth import get_agent_from_scope
from switch_core.bridges.agent.dependencies import get_protocol as get_api_protocol
from switch_core.bridges.agent.operations import all_operations
from switch_core.bridges.agent.operations import context as op_context
from switch_core.bridges.jira import routes as jira_webhook_routes
from switch_core.config import SwitchConfig
from switch_core.db.models import Agent, ApiKey, Client, Room, User
from switch_core.db.stores.agent_store import AgentStore
from switch_core.db.stores.jira_trigger_store import JiraTriggerStore
from switch_core.db.stores.room_group_store import RoomGroupStore
from switch_core.db.stores.room_store import RoomStore
from switch_core.gateway import jira_triggers as admin_routes
from switch_core.gateway.auth import require_admin

AGENT = "agent-1"
SECRET = "super-secret-value"

_TRIGGER_STORE = JiraTriggerStore()
_ROOM_STORE = RoomStore()
_ROOM_GROUP_STORE = RoomGroupStore()
_AGENT_STORE = AgentStore()


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
        "jira_webhook_secrets": {"acme": SECRET},
        "jira_agent_name": "jira",
        "gateway_public_url": "https://switch.example",
    }
    base.update(overrides)
    return SwitchConfig(**base)


def _ops_client(
    monkeypatch: pytest.MonkeyPatch,
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncClient:
    protocol = SimpleNamespace(session_factory=session_factory, config=_config())
    monkeypatch.setattr(op_context, "_protocol", protocol)
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_agent_from_scope] = lambda: SimpleNamespace(id=AGENT)
    app.dependency_overrides[get_api_protocol] = lambda: SimpleNamespace(
        connections=SimpleNamespace(require=lambda *a: None)
    )
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


async def _make_room(session: AsyncSession, name: str) -> Room:
    room = Room(matrix_room_id=f"!{name}:test", name=name, description=f"{name} desc")
    session.add(room)
    await session.flush()
    return room


async def _make_agent(session: AsyncSession, name: str) -> Agent:
    user = User(name=f"{name}-owner", email=f"{name}-owner@test", role="user")
    session.add(user)
    await session.flush()
    api_key = ApiKey(
        user_id=user.id,
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
        description=f"{name} desc",
        agent_type="session_addressable",
        connector_type="claude_code",
        integration_profile={},
        client_id=client.id,
        api_key_id=api_key.id,
    )
    session.add(agent)
    await session.flush()
    return agent


async def _room_with_member(
    session: AsyncSession, room_name: str, agent_name: str
) -> tuple[Room, Agent]:
    room = await _make_room(session, room_name)
    agent = await _make_agent(session, agent_name)
    await _ROOM_STORE.add_agents(session, room.id, [agent.id])
    await session.commit()
    return room, agent


def _create_body(room_id: str, **overrides: object) -> dict[str, object]:
    body: dict[str, object] = {
        "name": "KAN to In Progress",
        "instance": "acme",
        "fire_on": "transition",
        "message_template": "{{issue.key}} is {{issue.status}}",
        "target_kind": "room",
        "agent_name": "coder",
        "target_room_id": room_id,
        "project_key": "KAN",
        "target_status": "In Progress",
    }
    body.update(overrides)
    return body


async def test_list_jira_instances_masks_the_secret(
    monkeypatch: pytest.MonkeyPatch,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    jira_webhook_routes.init_jira_routes(
        service=object(),  # type: ignore[arg-type]
        secrets_by_instance={"acme": SECRET},
    )
    client = _ops_client(monkeypatch, session_factory)

    response = await client.post(f"/agents/{AGENT}/ops/list_jira_instances", json={})

    assert response.status_code == 200
    result = response.json()["result"]
    assert result["jira_agent_name"] == "jira"
    assert len(result["instances"]) == 1
    assert result["instances"][0]["instance"] == "acme"
    assert "/integrations/jira/acme" in result["instances"][0]["webhook_url"]
    assert SECRET not in response.text
    assert SECRET not in result["instances"][0]["secret_masked"]


async def test_create_list_and_show_trigger_over_ops(
    monkeypatch: pytest.MonkeyPatch,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        room, _ = await _room_with_member(session, "dev-room", "coder")
        room_id = room.id
    client = _ops_client(monkeypatch, session_factory)

    created = await client.post(
        f"/agents/{AGENT}/ops/create_jira_trigger", json=_create_body(room_id)
    )
    assert created.status_code == 200
    trigger_id = created.json()["result"]["id"]
    assert created.json()["result"]["target_room_name"] == "dev-room"

    listed = await client.post(
        f"/agents/{AGENT}/ops/list_jira_triggers", json={"instance": "acme"}
    )
    assert listed.status_code == 200
    assert [t["id"] for t in listed.json()["result"]] == [trigger_id]

    shown = await client.post(
        f"/agents/{AGENT}/ops/get_jira_trigger", json={"trigger_id": trigger_id}
    )
    assert shown.status_code == 200
    assert shown.json()["result"]["name"] == "KAN to In Progress"


async def test_show_missing_trigger_is_404(
    monkeypatch: pytest.MonkeyPatch,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    client = _ops_client(monkeypatch, session_factory)

    response = await client.post(
        f"/agents/{AGENT}/ops/get_jira_trigger", json={"trigger_id": "nope"}
    )

    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


async def test_create_refuses_agent_outside_the_room(
    monkeypatch: pytest.MonkeyPatch,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        room = await _make_room(session, "lonely-room")
        await _make_agent(session, "coder")
        await session.commit()
        room_id = room.id
    client = _ops_client(monkeypatch, session_factory)

    response = await client.post(
        f"/agents/{AGENT}/ops/create_jira_trigger", json=_create_body(room_id)
    )

    assert response.status_code == 400
    assert "not a member" in response.json()["detail"]


async def test_create_refuses_empty_template(
    monkeypatch: pytest.MonkeyPatch,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        room, _ = await _room_with_member(session, "tpl-room", "coder")
        room_id = room.id
    client = _ops_client(monkeypatch, session_factory)

    response = await client.post(
        f"/agents/{AGENT}/ops/create_jira_trigger",
        json=_create_body(room_id, message_template="   "),
    )

    assert response.status_code == 400
    assert "message_template" in response.json()["detail"]


async def test_update_and_delete_trigger_over_ops(
    monkeypatch: pytest.MonkeyPatch,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        room, _ = await _room_with_member(session, "ops-room", "coder")
        room_id = room.id
    client = _ops_client(monkeypatch, session_factory)
    created = await client.post(
        f"/agents/{AGENT}/ops/create_jira_trigger", json=_create_body(room_id)
    )
    trigger_id = created.json()["result"]["id"]

    patched = await client.post(
        f"/agents/{AGENT}/ops/update_jira_trigger",
        json={"trigger_id": trigger_id, "enabled": False, "name": "Paused"},
    )
    assert patched.status_code == 200
    assert patched.json()["result"]["enabled"] is False
    assert patched.json()["result"]["name"] == "Paused"

    deleted = await client.post(
        f"/agents/{AGENT}/ops/delete_jira_trigger",
        json={"trigger_id": trigger_id},
    )
    assert deleted.status_code == 200
    assert deleted.json()["result"] == {
        "trigger_id": trigger_id,
        "status": "deleted",
    }

    gone = await client.post(
        f"/agents/{AGENT}/ops/delete_jira_trigger",
        json={"trigger_id": trigger_id},
    )
    assert gone.status_code == 404


async def test_dry_run_matches_and_never_posts(
    monkeypatch: pytest.MonkeyPatch,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        room, _ = await _room_with_member(session, "dry-room", "coder")
        room_id = room.id
    client = _ops_client(monkeypatch, session_factory)
    created = await client.post(
        f"/agents/{AGENT}/ops/create_jira_trigger", json=_create_body(room_id)
    )
    trigger_id = created.json()["result"]["id"]

    hit = await client.post(
        f"/agents/{AGENT}/ops/dry_run_jira_trigger",
        json={
            "trigger_id": trigger_id,
            "sample_overrides": {"project": "KAN", "key": "KAN-7"},
        },
    )
    assert hit.status_code == 200
    result = hit.json()["result"]
    assert result["matched"] is True
    assert result["would_post"] is False
    assert "KAN-7" in result["rendered_message"]
    assert result["targets"][0]["room_name"] == "dry-room"

    miss = await client.post(
        f"/agents/{AGENT}/ops/dry_run_jira_trigger",
        json={"trigger_id": trigger_id, "sample_overrides": {"project": "OTHER"}},
    )
    assert miss.status_code == 200
    assert miss.json()["result"]["matched"] is False
    assert miss.json()["result"]["rendered_message"] is None


async def test_list_deliveries_over_ops(
    monkeypatch: pytest.MonkeyPatch,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        room, _ = await _room_with_member(session, "del-room", "coder")
        room_id = room.id
    client = _ops_client(monkeypatch, session_factory)
    created = await client.post(
        f"/agents/{AGENT}/ops/create_jira_trigger",
        json=_create_body(room_id, fire_on="created", target_status=""),
    )
    rule_id = created.json()["result"]["id"]
    rule_name = created.json()["result"]["name"]
    async with session_factory() as session:
        await _TRIGGER_STORE.try_record_firing(
            session,
            issue_key="DEL-1",
            rule_id=rule_id,
            transition_key="created",
            dedupe_window=timedelta(seconds=60),
            instance="acme",
            rule_name=rule_name,
            matched_rule_ids=[rule_id],
        )
        await session.commit()

    response = await client.post(
        f"/agents/{AGENT}/ops/list_jira_deliveries",
        json={"instance": "acme", "limit": 20},
    )

    assert response.status_code == 200
    result = response.json()["result"]
    assert result["retain_seconds"] > 0
    assert any(d["issue_key"] == "DEL-1" for d in result["deliveries"])


async def test_message_tokens_over_ops(
    monkeypatch: pytest.MonkeyPatch,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    client = _ops_client(monkeypatch, session_factory)

    response = await client.post(
        f"/agents/{AGENT}/ops/list_jira_message_tokens", json={}
    )

    assert response.status_code == 200
    tokens = response.json()["result"]
    assert "issue.key" in tokens
    assert "transition.to" in tokens


async def test_agent_options_needs_exactly_one_scope(
    monkeypatch: pytest.MonkeyPatch,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        room, _ = await _room_with_member(session, "opt-room", "coder")
        room_id = room.id
    client = _ops_client(monkeypatch, session_factory)

    by_room = await client.post(
        f"/agents/{AGENT}/ops/list_jira_agent_options", json={"room_id": room_id}
    )
    assert by_room.status_code == 200
    assert [a["name"] for a in by_room.json()["result"]] == ["coder"]

    neither = await client.post(f"/agents/{AGENT}/ops/list_jira_agent_options", json={})
    assert neither.status_code == 400

    both = await client.post(
        f"/agents/{AGENT}/ops/list_jira_agent_options",
        json={"room_id": room_id, "group_id": "g"},
    )
    assert both.status_code == 400


@pytest.mark.parametrize("operation", ["reveal_jira_secret", "rotate_jira_secret"])
async def test_secret_reveal_and_rotate_are_not_agent_operations(
    monkeypatch: pytest.MonkeyPatch,
    session_factory: async_sessionmaker[AsyncSession],
    operation: str,
) -> None:
    client = _ops_client(monkeypatch, session_factory)

    response = await client.post(f"/agents/{AGENT}/ops/{operation}", json={})

    assert response.status_code == 404


def test_no_registered_operation_touches_secrets() -> None:
    names = set(all_operations())
    assert not {n for n in names if "reveal" in n or "rotate" in n or "secret" in n}


def test_reveal_and_rotate_routes_keep_the_admin_gate() -> None:
    """Option B must not weaken the admin-cookie gate on secrets."""

    def _calls(dependant: object) -> list[object]:
        out = [dependant.call]  # type: ignore[attr-defined]
        for sub in dependant.dependencies:  # type: ignore[attr-defined]
            out.extend(_calls(sub))
        return out

    guarded = {
        f"{sorted(route.methods)} {route.path}": (  # type: ignore[attr-defined]
            require_admin in _calls(route.dependant)  # type: ignore[attr-defined]
        )
        for route in admin_routes.router.routes  # type: ignore[attr-defined]
        if "reveal" in route.path or "rotate" in route.path  # type: ignore[attr-defined]
    }
    assert guarded, "reveal/rotate routes are gone from the admin router"
    assert all(guarded.values()), f"admin gate weakened: {guarded}"
