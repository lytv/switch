"""The gateway route that grants and revokes an agent's create permission.

Exercised against real Postgres with real stores, like the display-name route
test: only the agent's owner or an admin may flip the flag, and the stored
value is what the operation's fresh-read permission check sees.
"""

from __future__ import annotations

import pytest
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from switch_core.db.stores.agent_store import AgentStore
from switch_core.gateway.agents import update_agent_create_permission
from switch_core.gateway.schemas import UpdateAgentCreatePermissionRequest
from tests.switch_core.gateway.agent_route_harness import add_agent, add_user

_AGENT_STORE = AgentStore()


class TestUpdateAgentCreatePermission:
    async def test_owner_can_grant_it(
        self, session_factory: async_sessionmaker[AsyncSession]
    ) -> None:
        async with session_factory() as session:
            owner = await add_user(session, name="owner")
            agent = await add_agent(session, name="switchdev", owner_id=owner.id)

            summary = await update_agent_create_permission(
                agent.id,
                UpdateAgentCreatePermissionRequest(can_create_agents=True),
                session,
                _AGENT_STORE,
                owner,
            )

            assert summary.can_create_agents is True
            stored = await _AGENT_STORE.get(session, agent.id)
            assert stored is not None
            assert stored.can_create_agents is True

    async def test_owner_can_revoke_it(
        self, session_factory: async_sessionmaker[AsyncSession]
    ) -> None:
        async with session_factory() as session:
            owner = await add_user(session, name="owner")
            agent = await add_agent(session, name="switchdev", owner_id=owner.id)
            await _AGENT_STORE.update(session, agent.id, can_create_agents=True)

            summary = await update_agent_create_permission(
                agent.id,
                UpdateAgentCreatePermissionRequest(can_create_agents=False),
                session,
                _AGENT_STORE,
                owner,
            )

            assert summary.can_create_agents is False
            stored = await _AGENT_STORE.get(session, agent.id)
            assert stored is not None
            assert stored.can_create_agents is False

    async def test_new_agents_start_without_it(
        self, session_factory: async_sessionmaker[AsyncSession]
    ) -> None:
        async with session_factory() as session:
            owner = await add_user(session, name="owner")
            agent = await add_agent(session, name="switchdev", owner_id=owner.id)

            stored = await _AGENT_STORE.get(session, agent.id)
            assert stored is not None
            assert stored.can_create_agents is False

    async def test_non_owner_is_refused(
        self, session_factory: async_sessionmaker[AsyncSession]
    ) -> None:
        async with session_factory() as session:
            owner = await add_user(session, name="owner")
            other = await add_user(session, name="other")
            agent = await add_agent(session, name="switchdev", owner_id=owner.id)

            with pytest.raises(HTTPException) as exc:
                await update_agent_create_permission(
                    agent.id,
                    UpdateAgentCreatePermissionRequest(can_create_agents=True),
                    session,
                    _AGENT_STORE,
                    other,
                )

            assert exc.value.status_code == 403
            stored = await _AGENT_STORE.get(session, agent.id)
            assert stored is not None
            assert stored.can_create_agents is False

    async def test_admin_can_grant_it_on_an_agent_they_do_not_own(
        self, session_factory: async_sessionmaker[AsyncSession]
    ) -> None:
        async with session_factory() as session:
            owner = await add_user(session, name="owner")
            admin = await add_user(session, name="admin", role="admin")
            agent = await add_agent(session, name="switchdev", owner_id=owner.id)

            summary = await update_agent_create_permission(
                agent.id,
                UpdateAgentCreatePermissionRequest(can_create_agents=True),
                session,
                _AGENT_STORE,
                admin,
            )

            assert summary.can_create_agents is True

    async def test_unknown_agent_is_404(
        self, session_factory: async_sessionmaker[AsyncSession]
    ) -> None:
        async with session_factory() as session:
            owner = await add_user(session, name="owner")

            with pytest.raises(HTTPException) as exc:
                await update_agent_create_permission(
                    "no-such-agent",
                    UpdateAgentCreatePermissionRequest(can_create_agents=True),
                    session,
                    _AGENT_STORE,
                    owner,
                )

            assert exc.value.status_code == 404
