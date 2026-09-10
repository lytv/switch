from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from switch_core.bridges.agent.protocol.types import AgentStatus, SendTargetedResult
from switch_core.bridges.jira.parse import ParsedJiraEvent, StatusTransition
from switch_core.bridges.jira.service import JiraBridgeService
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
        "jira_message_max_chars": 4000,
        "jira_dedupe_window_seconds": 300,
        "jira_rate_limit_per_rule": 10,
        "jira_rate_limit_window_seconds": 60,
    }
    base.update(overrides)
    return SwitchConfig(**base)


def _event() -> ParsedJiraEvent:
    return ParsedJiraEvent(
        event_kind="updated",
        webhook_event="jira:issue_updated",
        key="PROJ-1",
        summary="Add export",
        issue_type="Story",
        project="PROJ",
        status="In Progress",
        assignee="Ada",
        priority="High",
        reporter="Bob",
        url="https://example/browse/PROJ-1",
        labels=("agentic",),
        transition=StatusTransition("To Do", "In Progress"),
    )


class _SessionCM:
    def __init__(self, session: Any) -> None:
        self._session = session

    async def __aenter__(self) -> Any:
        return self._session

    async def __aexit__(self, *args: object) -> None:
        return None


@pytest.mark.asyncio
async def test_process_event_posts_addressed_message() -> None:
    rule = SimpleNamespace(
        id="rule-1",
        name="start",
        enabled=True,
        project_key="PROJ",
        issue_type="Story",
        fire_on="transition",
        target_status="In Progress",
        jql="",
        target_kind="room",
        target_room_id="!room:test",
        agent_name="coder",
        message_template="{{issue.key}} is {{issue.status}}",
    )
    session = AsyncMock()
    session.commit = AsyncMock()
    session_factory = MagicMock(return_value=_SessionCM(session))

    trigger_store = AsyncMock()
    trigger_store.list = AsyncMock(return_value=[rule])
    trigger_store.count_firings_in_window = AsyncMock(return_value=0)
    trigger_store.try_record_firing = AsyncMock(return_value=True)

    agent_store = AsyncMock()
    agent_store.get_by_name = AsyncMock(
        return_value=SimpleNamespace(id="jira-agent-id", name="jira")
    )

    protocol = AsyncMock()
    protocol.send_targeted_message = AsyncMock(
        return_value=SendTargetedResult(
            event_id="$evt",
            target_statuses={"coder": AgentStatus.NO_SESSION},
        )
    )

    service = JiraBridgeService(
        session_factory=session_factory,
        trigger_store=trigger_store,
        agent_store=agent_store,
        protocol=protocol,
        config=_config(),
    )
    await service.process_event(instance="acme", event=_event())

    protocol.send_targeted_message.assert_awaited_once()
    kwargs = protocol.send_targeted_message.await_args.kwargs
    assert kwargs["agent_id"] == "jira-agent-id"
    assert kwargs["room_id"] == "!room:test"
    assert kwargs["target_names"] == ["coder"]
    assert "PROJ-1" in kwargs["content"]
    assert "In Progress" in kwargs["content"]


@pytest.mark.asyncio
async def test_process_event_dedupes() -> None:
    rule = SimpleNamespace(
        id="rule-1",
        name="start",
        enabled=True,
        project_key="",
        issue_type="",
        fire_on="transition",
        target_status="In Progress",
        jql="",
        target_kind="room",
        target_room_id="!room:test",
        agent_name="coder",
        message_template="x",
    )
    session = AsyncMock()
    session.commit = AsyncMock()
    session_factory = MagicMock(return_value=_SessionCM(session))
    trigger_store = AsyncMock()
    trigger_store.list = AsyncMock(return_value=[rule])
    trigger_store.count_firings_in_window = AsyncMock(return_value=0)
    trigger_store.try_record_firing = AsyncMock(return_value=False)
    agent_store = AsyncMock()
    agent_store.get_by_name = AsyncMock(
        return_value=SimpleNamespace(id="jira-agent-id")
    )
    protocol = AsyncMock()

    service = JiraBridgeService(
        session_factory=session_factory,
        trigger_store=trigger_store,
        agent_store=agent_store,
        protocol=protocol,
        config=_config(),
    )
    await service.process_event(instance="acme", event=_event())
    protocol.send_targeted_message.assert_not_called()


@pytest.mark.asyncio
async def test_process_event_rate_limits() -> None:
    rule = SimpleNamespace(
        id="rule-1",
        name="start",
        enabled=True,
        project_key="",
        issue_type="",
        fire_on="transition",
        target_status="In Progress",
        jql="",
        target_kind="room",
        target_room_id="!room:test",
        agent_name="coder",
        message_template="x",
    )
    session = AsyncMock()
    session.commit = AsyncMock()
    session_factory = MagicMock(return_value=_SessionCM(session))
    trigger_store = AsyncMock()
    trigger_store.list = AsyncMock(return_value=[rule])
    trigger_store.count_firings_in_window = AsyncMock(return_value=10)
    agent_store = AsyncMock()
    agent_store.get_by_name = AsyncMock(
        return_value=SimpleNamespace(id="jira-agent-id")
    )
    protocol = AsyncMock()

    service = JiraBridgeService(
        session_factory=session_factory,
        trigger_store=trigger_store,
        agent_store=agent_store,
        protocol=protocol,
        config=_config(jira_rate_limit_per_rule=10),
    )
    await service.process_event(instance="acme", event=_event())
    protocol.send_targeted_message.assert_not_called()
    trigger_store.try_record_firing.assert_not_called()
