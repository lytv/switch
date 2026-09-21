"""The per-agent "may create agents" permission.

`create_agent_as` lets one flagged agent register new known-type agents with
its own token: the flag is re-read fresh (revocation bites immediately), the
new agent lands on the caller's owner, and creation is create-only. The
register-known REST wrapper keeps translating the same domain errors to the
same HTTP codes after the extraction into `ProtocolService`.
"""

from __future__ import annotations

import pytest
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from switch_core.bridges.agent.api import operations as ops_api
from switch_core.bridges.agent.api.handlers import _register_known
from switch_core.bridges.agent.operations import context as op_context
from switch_core.bridges.agent.protocol.service import (
    AgentExistsError,
    ProtocolService,
)
from switch_core.db.models import User
from switch_core.db.stores.agent_store import AgentStore
from tests.switch_core.bridges.agent.protocol.registration_harness import (
    make_owner,
    make_service,
    register,
)

_AGENT_STORE = AgentStore()


async def _flagged_caller(
    svc: ProtocolService,
    session_factory: async_sessionmaker[AsyncSession],
    name: str,
    owner_id: str,
) -> str:
    caller_id = await register(svc, name, owner_id)
    async with session_factory() as session:
        await _AGENT_STORE.update(session, caller_id, can_create_agents=True)
        await session.commit()
    return caller_id


class TestCreateAgentAs:
    async def test_unflagged_caller_is_denied(
        self, session_factory: async_sessionmaker[AsyncSession]
    ) -> None:
        svc = make_service(session_factory)
        owner_id = await make_owner(session_factory)
        caller_id = await register(svc, "caller", owner_id)

        with pytest.raises(PermissionError):
            await svc.create_agent_as(
                caller_id,
                agent_type="claude-code",
                name="child",
                description="child desc",
            )

    async def test_flagged_caller_creates_an_agent_on_its_own_owner(
        self, session_factory: async_sessionmaker[AsyncSession]
    ) -> None:
        svc = make_service(session_factory)
        owner_id = await make_owner(session_factory)
        async with session_factory() as session:
            other = User(
                name="other", email="other@test", role="user", password_hash="x"
            )
            session.add(other)
            await session.commit()
            other_id = other.id
        caller_id = await _flagged_caller(svc, session_factory, "caller", owner_id)
        # A second flagged agent owned by someone else proves the new agent
        # lands on the *caller's* owner, not a fixed one.
        await _flagged_caller(svc, session_factory, "sibling", other_id)

        result = await svc.create_agent_as(
            caller_id,
            agent_type="claude-code",
            name="child",
            description="child desc",
        )

        assert result.agent_id
        assert result.api_key
        async with session_factory() as session:
            stored = await _AGENT_STORE.get(session, result.agent_id)
            assert stored is not None
            assert stored.owner_id == owner_id
            assert stored.name == "child"
            # Owner-only start: a scoped policy is stored, not open access.
            assert stored.addressing_policy is not None
            md = stored.metadata_
            assert isinstance(md, dict)
            assert md["known_agent_type"] == "claude-code"

    async def test_revoking_the_flag_denies_the_next_call(
        self, session_factory: async_sessionmaker[AsyncSession]
    ) -> None:
        svc = make_service(session_factory)
        owner_id = await make_owner(session_factory)
        caller_id = await _flagged_caller(svc, session_factory, "caller", owner_id)
        async with session_factory() as session:
            await _AGENT_STORE.update(session, caller_id, can_create_agents=False)
            await session.commit()

        with pytest.raises(PermissionError):
            await svc.create_agent_as(
                caller_id,
                agent_type="claude-code",
                name="child",
                description="child desc",
            )

    async def test_unknown_caller_is_a_value_error(
        self, session_factory: async_sessionmaker[AsyncSession]
    ) -> None:
        svc = make_service(session_factory)

        with pytest.raises(ValueError, match="Agent not found"):
            await svc.create_agent_as(
                "no-such-agent",
                agent_type="claude-code",
                name="child",
                description="child desc",
            )

    async def test_unknown_agent_type_is_a_value_error(
        self, session_factory: async_sessionmaker[AsyncSession]
    ) -> None:
        svc = make_service(session_factory)
        owner_id = await make_owner(session_factory)
        caller_id = await _flagged_caller(svc, session_factory, "caller", owner_id)

        with pytest.raises(ValueError, match="Unknown agent type"):
            await svc.create_agent_as(
                caller_id,
                agent_type="not-a-type",
                name="child",
                description="child desc",
            )

    async def test_name_clash_fails_create_only(
        self, session_factory: async_sessionmaker[AsyncSession]
    ) -> None:
        svc = make_service(session_factory)
        owner_id = await make_owner(session_factory)
        caller_id = await _flagged_caller(svc, session_factory, "caller", owner_id)
        await svc.create_agent_as(
            caller_id,
            agent_type="claude-code",
            name="child",
            description="child desc",
        )

        with pytest.raises(AgentExistsError):
            await svc.create_agent_as(
                caller_id,
                agent_type="claude-code",
                name="child",
                description="child desc",
            )


class TestRegisterKnownWrapper:
    """The REST wrapper translates the extracted method's errors unchanged."""

    async def test_success_returns_id_and_key(
        self, session_factory: async_sessionmaker[AsyncSession]
    ) -> None:
        svc = make_service(session_factory)
        owner_id = await make_owner(session_factory)

        agent_id, api_key = await _register_known(
            agent_type="claude-code",
            name="child",
            description="child desc",
            icon_url=None,
            display_name=None,
            options_raw={},
            parent_agent_id=None,
            overwrite=False,
            owner_id=owner_id,
            protocol=svc,
        )

        assert agent_id
        assert api_key

    async def test_unknown_type_is_400(
        self, session_factory: async_sessionmaker[AsyncSession]
    ) -> None:
        svc = make_service(session_factory)
        owner_id = await make_owner(session_factory)

        with pytest.raises(HTTPException) as exc:
            await _register_known(
                agent_type="not-a-type",
                name="child",
                description="child desc",
                icon_url=None,
                display_name=None,
                options_raw={},
                parent_agent_id=None,
                overwrite=False,
                owner_id=owner_id,
                protocol=svc,
            )

        assert exc.value.status_code == 400
        assert "Unknown agent type" in str(exc.value.detail)

    async def test_invalid_options_are_400(
        self, session_factory: async_sessionmaker[AsyncSession]
    ) -> None:
        svc = make_service(session_factory)
        owner_id = await make_owner(session_factory)

        with pytest.raises(HTTPException) as exc:
            await _register_known(
                agent_type="claude-code",
                name="child",
                description="child desc",
                icon_url=None,
                display_name=None,
                options_raw={"repo_dir": 123},
                parent_agent_id=None,
                overwrite=False,
                owner_id=owner_id,
                protocol=svc,
            )

        assert exc.value.status_code == 400

    async def test_name_clash_is_409(
        self, session_factory: async_sessionmaker[AsyncSession]
    ) -> None:
        svc = make_service(session_factory)
        owner_id = await make_owner(session_factory)
        await _register_known(
            agent_type="claude-code",
            name="child",
            description="child desc",
            icon_url=None,
            display_name=None,
            options_raw={},
            parent_agent_id=None,
            overwrite=False,
            owner_id=owner_id,
            protocol=svc,
        )

        with pytest.raises(HTTPException) as exc:
            await _register_known(
                agent_type="claude-code",
                name="child",
                description="child desc",
                icon_url=None,
                display_name=None,
                options_raw={},
                parent_agent_id=None,
                overwrite=False,
                owner_id=owner_id,
                protocol=svc,
            )

        assert exc.value.status_code == 409


class TestCreateAgentOperation:
    """The `create_agent` operation wires the caller through and returns id+key."""

    async def test_denied_without_the_flag(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        svc = make_service(session_factory)
        owner_id = await make_owner(session_factory)
        caller_id = await register(svc, "caller", owner_id)
        monkeypatch.setattr(op_context, "_protocol", svc)

        with pytest.raises(PermissionError):
            await ops_api.call_operation(
                operation="create_agent",
                arguments={
                    "agent_type": "claude-code",
                    "name": "child",
                    "description": "child desc",
                },
                agent_id=caller_id,
                connection_id=None,
            )

    async def test_granted_returns_id_and_single_use_key(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        svc = make_service(session_factory)
        owner_id = await make_owner(session_factory)
        caller_id = await _flagged_caller(svc, session_factory, "caller", owner_id)
        monkeypatch.setattr(op_context, "_protocol", svc)

        out = await ops_api.call_operation(
            operation="create_agent",
            arguments={
                "agent_type": "claude-code",
                "name": "child",
                "description": "child desc",
            },
            agent_id=caller_id,
            connection_id=None,
        )

        assert set(out) == {"id", "api_key"}
        assert out["api_key"]
        async with session_factory() as session:
            stored = await _AGENT_STORE.get(session, out["id"])
            assert stored is not None
            assert stored.owner_id == owner_id
