from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from switch_core.bridges.jira.identity import ensure_jira_system_agent
from switch_core.config import SwitchConfig


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
        "jira_webhook_secrets": {"acme": "s"},
        "jira_agent_name": "jira",
    }
    base.update(overrides)
    return SwitchConfig(**base)


class _SessionCM:
    def __init__(self, session: Any) -> None:
        self._session = session

    async def __aenter__(self) -> Any:
        return self._session

    async def __aexit__(self, *args: object) -> None:
        return None


@pytest.mark.asyncio
async def test_ensure_skips_without_secrets() -> None:
    result = await ensure_jira_system_agent(
        protocol=AsyncMock(),
        session_factory=MagicMock(),
        agent_store=AsyncMock(),
        user_store=AsyncMock(),
        config=_config(jira_webhook_secrets={}),
    )
    assert result is None


@pytest.mark.asyncio
async def test_ensure_returns_existing() -> None:
    session = AsyncMock()
    session_factory = MagicMock(return_value=_SessionCM(session))
    agent_store = AsyncMock()
    agent_store.get_by_name = AsyncMock(
        return_value=SimpleNamespace(id="existing", name="jira")
    )
    protocol = AsyncMock()
    result = await ensure_jira_system_agent(
        protocol=protocol,
        session_factory=session_factory,
        agent_store=agent_store,
        user_store=AsyncMock(),
        config=_config(),
    )
    assert result == "existing"
    protocol.register_agent.assert_not_called()


@pytest.mark.asyncio
async def test_ensure_registers_shared_agent() -> None:
    session = AsyncMock()
    session_factory = MagicMock(return_value=_SessionCM(session))
    agent_store = AsyncMock()
    agent_store.get_by_name = AsyncMock(return_value=None)
    user_store = AsyncMock()
    user_store.get_by_email = AsyncMock(return_value=SimpleNamespace(id="admin-id"))
    protocol = AsyncMock()
    protocol.register_agent = AsyncMock(
        return_value=SimpleNamespace(agent_id="new-id", api_key="secret-key")
    )
    result = await ensure_jira_system_agent(
        protocol=protocol,
        session_factory=session_factory,
        agent_store=agent_store,
        user_store=user_store,
        config=_config(),
    )
    assert result == "new-id"
    kwargs = protocol.register_agent.await_args.kwargs
    assert kwargs["name"] == "jira"
    assert kwargs["connector_type"] == "external"
    assert kwargs["owner_only"] is False
    assert kwargs["integration_profile"].connection_model == "always_on"
    # API key must not appear in the registration call's logged kwargs beyond
    # the return value — register_agent returns it; we never log it here.
    assert "api_key" not in kwargs
