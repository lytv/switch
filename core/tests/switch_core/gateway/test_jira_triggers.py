"""Gateway /jira-triggers CRUD, dry-run, and setup routes (KAN-92, KAN-96, KAN-97)."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest
from fastapi import HTTPException, Response
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from switch_core.bridges.jira import routes as jira_webhook_routes
from switch_core.config import SwitchConfig
from switch_core.db.models import Agent, ApiKey, Client, Room, RoomGroup, User
from switch_core.db.stores.agent_store import AgentStore
from switch_core.db.stores.jira_trigger_store import JiraTriggerStore
from switch_core.db.stores.room_group_store import RoomGroupStore
from switch_core.db.stores.room_store import RoomStore
from switch_core.gateway.auth import require_admin
from switch_core.gateway.jira_triggers import (
    create_trigger,
    delete_trigger,
    dry_run_trigger,
    get_setup,
    list_deliveries,
    list_triggers,
    patch_trigger,
    reveal_instance_secret,
    rotate_instance_secret,
    router,
)
from switch_core.gateway.schemas import (
    JiraDryRunRequest,
    JiraTriggerCreateRequest,
    JiraTriggerUpdateRequest,
)

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
        "jira_webhook_secrets": {"acme": "super-secret-value"},
        "jira_agent_name": "jira",
        "gateway_public_url": "https://switch.example",
    }
    base.update(overrides)
    return SwitchConfig(**base)


async def _make_user(session: AsyncSession, name: str, role: str = "admin") -> User:
    user = User(name=name, email=f"{name}@example.invalid", role=role)
    session.add(user)
    await session.flush()
    return user


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


def test_write_routes_require_admin() -> None:
    write_methods = {"POST", "PATCH", "PUT", "DELETE"}

    def _calls(dependant: object) -> list[object]:
        out = [dependant.call]  # type: ignore[attr-defined]
        for sub in dependant.dependencies:  # type: ignore[attr-defined]
            out.extend(_calls(sub))
        return out

    unguarded = [
        f"{sorted(route.methods)} {route.path}"  # type: ignore[attr-defined]
        for route in router.routes  # type: ignore[attr-defined]
        if route.methods & write_methods  # type: ignore[attr-defined]
        and require_admin not in _calls(route.dependant)  # type: ignore[attr-defined]
    ]
    assert unguarded == [], f"jira-trigger writes missing require_admin: {unguarded}"


@pytest.mark.asyncio
async def test_crud_validation_and_list(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        admin = await _make_user(session, "jira-admin")
        room = await _make_room(session, "feature-x")
        agent = await _make_agent(session, "coder")
        await _ROOM_STORE.add_agents(session, room.id, [agent.id])
        await session.commit()

        with pytest.raises(HTTPException) as empty_template:
            await create_trigger(
                JiraTriggerCreateRequest(
                    name="bad",
                    instance="acme",
                    fire_on="transition",
                    target_kind="room",
                    target_room_id=room.id,
                    agent_name="coder",
                    message_template="   ",
                ),
                session,
                _TRIGGER_STORE,
                _ROOM_STORE,
                _ROOM_GROUP_STORE,
                _AGENT_STORE,
                admin,
            )
        assert empty_template.value.status_code == 400
        assert "message_template" in str(empty_template.value.detail)

        with pytest.raises(HTTPException) as bad_room:
            await create_trigger(
                JiraTriggerCreateRequest(
                    name="bad room",
                    instance="acme",
                    fire_on="created",
                    target_kind="room",
                    target_room_id="missing-room",
                    agent_name="coder",
                    message_template="{{issue.key}}",
                ),
                session,
                _TRIGGER_STORE,
                _ROOM_STORE,
                _ROOM_GROUP_STORE,
                _AGENT_STORE,
                admin,
            )
        assert bad_room.value.status_code == 400
        assert "Unknown room" in str(bad_room.value.detail)

        created = await create_trigger(
            JiraTriggerCreateRequest(
                name="Start coder",
                instance="acme",
                project_key="PROJ",
                fire_on="transition",
                target_status="In Progress",
                target_kind="room",
                target_room_id=room.id,
                agent_name="coder",
                message_template="{{issue.key}} → {{issue.status}}",
                thread_by="issue_key",
            ),
            session,
            _TRIGGER_STORE,
            _ROOM_STORE,
            _ROOM_GROUP_STORE,
            _AGENT_STORE,
            admin,
        )
        assert created.enabled is True
        assert created.target_room_name == "feature-x"
        assert created.thread_by == "issue_key"

        listed = await list_triggers(
            session, _TRIGGER_STORE, _ROOM_STORE, _ROOM_GROUP_STORE, admin, None
        )
        assert len(listed) == 1

        patched = await patch_trigger(
            created.id,
            JiraTriggerUpdateRequest(enabled=False, name="Paused"),
            session,
            _TRIGGER_STORE,
            _ROOM_STORE,
            _ROOM_GROUP_STORE,
            _AGENT_STORE,
            admin,
        )
        assert patched.enabled is False
        assert patched.name == "Paused"

        got = await _TRIGGER_STORE.get(session, created.id)
        assert got is not None
        assert got.enabled is False

        deleted = await delete_trigger(created.id, session, _TRIGGER_STORE, admin)
        assert isinstance(deleted, Response)
        assert deleted.status_code == 204
        assert (
            await list_triggers(
                session, _TRIGGER_STORE, _ROOM_STORE, _ROOM_GROUP_STORE, admin, None
            )
            == []
        )


@pytest.mark.asyncio
async def test_dry_run_match_and_miss(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        admin = await _make_user(session, "dry-admin")
        room = await _make_room(session, "dry-room")
        agent = await _make_agent(session, "dry-coder")
        await _ROOM_STORE.add_agents(session, room.id, [agent.id])
        created = await create_trigger(
            JiraTriggerCreateRequest(
                name="dry",
                instance="acme",
                project_key="PROJ",
                fire_on="transition",
                target_status="In Progress",
                target_kind="room",
                target_room_id=room.id,
                agent_name="dry-coder",
                message_template="{{issue.key}} is {{issue.status}}",
            ),
            session,
            _TRIGGER_STORE,
            _ROOM_STORE,
            _ROOM_GROUP_STORE,
            _AGENT_STORE,
            admin,
        )

        hit = await dry_run_trigger(
            created.id,
            JiraDryRunRequest(),
            session,
            _TRIGGER_STORE,
            _ROOM_STORE,
            _ROOM_GROUP_STORE,
            _config(),
            admin,
        )
        assert hit.matched is True
        assert hit.would_post is False
        assert hit.rendered_message is not None
        assert "PROJ-123" in hit.rendered_message
        assert hit.targets[0].room_name == "dry-room"
        assert hit.targets[0].agent_name == "dry-coder"

        miss = await dry_run_trigger(
            created.id,
            JiraDryRunRequest(sample_overrides={"project": "OTHER"}),
            session,
            _TRIGGER_STORE,
            _ROOM_STORE,
            _ROOM_GROUP_STORE,
            _config(),
            admin,
        )
        assert miss.matched is False
        assert miss.rendered_message is None
        assert any("Project filter" in r for r in miss.reasons)


@pytest.mark.asyncio
async def test_setup_reveal_and_rotate(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    jira_webhook_routes.init_jira_routes(
        service=object(),  # type: ignore[arg-type]
        secrets_by_instance={"acme": "super-secret-value"},
    )
    cfg = _config()
    async with session_factory() as session:
        admin = await _make_user(session, "setup-admin")
        await session.commit()

        setup = await get_setup(admin, cfg)
        assert setup.jira_agent_name == "jira"
        assert setup.instances[0].instance == "acme"
        assert "super-secret-value" not in setup.instances[0].secret_masked
        assert "/integrations/jira/acme" in setup.instances[0].webhook_url
        assert "classic_webhook" in setup.guidance

        revealed = await reveal_instance_secret("acme", admin)
        assert revealed.key == "super-secret-value"

        rotated = await rotate_instance_secret("acme", admin, cfg)
        assert rotated.secret != "super-secret-value"
        assert jira_webhook_routes.get_jira_secrets()["acme"] == rotated.secret
        assert cfg.jira_webhook_secrets["acme"] == rotated.secret


@pytest.mark.asyncio
async def test_group_target_create(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        admin = await _make_user(session, "group-admin")
        group = RoomGroup(name="fleet", description="", color=None)
        session.add(group)
        await session.flush()
        await _make_agent(session, "group-coder")
        await session.commit()

        created = await create_trigger(
            JiraTriggerCreateRequest(
                name="Fan out",
                instance="acme",
                fire_on="updated",
                target_kind="group",
                target_group_id=group.id,
                agent_name="group-coder",
                message_template="{{issue.key}}",
            ),
            session,
            _TRIGGER_STORE,
            _ROOM_STORE,
            _ROOM_GROUP_STORE,
            _AGENT_STORE,
            admin,
        )
        assert created.target_kind == "group"
        assert created.target_group_name == "fleet"
        assert created.target_room_id is None


@pytest.mark.asyncio
async def test_list_deliveries(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        admin = await _make_user(session, "del-admin")
        room = await _make_room(session, "del-room")
        agent = await _make_agent(session, "del-coder")
        await _ROOM_STORE.add_agents(session, room.id, [agent.id])
        await session.commit()

        trigger = await create_trigger(
            JiraTriggerCreateRequest(
                name="Deliver",
                instance="acme",
                fire_on="created",
                target_kind="room",
                target_room_id=room.id,
                agent_name="del-coder",
                message_template="{{issue.key}}",
            ),
            session,
            _TRIGGER_STORE,
            _ROOM_STORE,
            _ROOM_GROUP_STORE,
            _AGENT_STORE,
            admin,
        )
        firing_id = await _TRIGGER_STORE.try_record_firing(
            session,
            issue_key="DEL-1",
            rule_id=trigger.id,
            transition_key="created",
            dedupe_window=timedelta(seconds=60),
            instance="acme",
            rule_name=trigger.name,
            matched_rule_ids=[trigger.id],
        )
        assert firing_id is not None
        await _TRIGGER_STORE.finalize_firing(
            session,
            firing_id,
            status="delivered",
            room_results=[
                {
                    "room_id": room.id,
                    "room_name": room.name,
                    "status": "ok",
                    "event_id": "$x",
                    "attempts": 1,
                }
            ],
            error=None,
            attempt_count=1,
        )
        await session.commit()

        listed = await list_deliveries(
            admin,
            session,
            _TRIGGER_STORE,
            _config(),
            instance="acme",
            rule_id=None,
            limit=20,
            offset=0,
        )
        assert listed.retain_seconds > 0
        assert listed.max_rows > 0
        assert any(
            d.issue_key == "DEL-1" and d.status == "delivered"
            for d in listed.deliveries
        )
        row = next(d for d in listed.deliveries if d.issue_key == "DEL-1")
        assert row.matched_rule_ids == [trigger.id]
        assert row.room_results[0].event_id == "$x"
