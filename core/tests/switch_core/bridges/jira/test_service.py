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


def _room_rule(**overrides: object) -> SimpleNamespace:
    base: dict[str, object] = {
        "id": "rule-1",
        "name": "start",
        "enabled": True,
        "project_key": "PROJ",
        "issue_type": "Story",
        "fire_on": "transition",
        "target_status": "In Progress",
        "jql": "",
        "target_kind": "room",
        "target_room_id": "room-1",
        "target_group_id": None,
        "agent_name": "coder",
        "message_template": "{{issue.key}} is {{issue.status}}",
        "thread_by": "new",
    }
    base.update(overrides)
    return SimpleNamespace(**base)


class _SessionCM:
    def __init__(self, session: Any) -> None:
        self._session = session

    async def __aenter__(self) -> Any:
        return self._session

    async def __aexit__(self, *args: object) -> None:
        return None


def _service(
    *,
    trigger_store: Any,
    agent_store: Any,
    protocol: Any,
    room_store: Any,
    config: SwitchConfig | None = None,
) -> JiraBridgeService:
    session = AsyncMock()
    session.commit = AsyncMock()
    session_factory = MagicMock(return_value=_SessionCM(session))
    return JiraBridgeService(
        session_factory=session_factory,
        trigger_store=trigger_store,
        agent_store=agent_store,
        room_store=room_store,
        protocol=protocol,
        config=config or _config(),
    )


@pytest.mark.asyncio
async def test_process_event_posts_addressed_message() -> None:
    rule = _room_rule()
    trigger_store = AsyncMock()
    trigger_store.list = AsyncMock(return_value=[rule])
    trigger_store.count_firings_in_window = AsyncMock(return_value=0)
    trigger_store.try_record_firing = AsyncMock(return_value=True)

    agent_store = AsyncMock()
    agent_store.get_by_name = AsyncMock(
        return_value=SimpleNamespace(id="jira-agent-id", name="jira")
    )
    room_store = AsyncMock()
    room_store.get = AsyncMock(
        return_value=SimpleNamespace(id="room-1", name="Feature")
    )
    protocol = AsyncMock()
    protocol.send_targeted_message = AsyncMock(
        return_value=SendTargetedResult(
            event_id="$evt",
            target_statuses={"coder": AgentStatus.NO_SESSION},
        )
    )

    service = _service(
        trigger_store=trigger_store,
        agent_store=agent_store,
        protocol=protocol,
        room_store=room_store,
    )
    await service.process_event(instance="acme", event=_event())

    protocol.send_targeted_message.assert_awaited_once()
    kwargs = protocol.send_targeted_message.await_args.kwargs
    assert kwargs["agent_id"] == "jira-agent-id"
    assert kwargs["room_id"] == "room-1"
    assert kwargs["target_names"] == ["coder"]
    assert kwargs["thread_id"] is None
    assert "PROJ-1" in kwargs["content"]
    assert "In Progress" in kwargs["content"]


@pytest.mark.asyncio
async def test_process_event_dedupes() -> None:
    rule = _room_rule(message_template="x", project_key="", issue_type="")
    trigger_store = AsyncMock()
    trigger_store.list = AsyncMock(return_value=[rule])
    trigger_store.count_firings_in_window = AsyncMock(return_value=0)
    trigger_store.try_record_firing = AsyncMock(return_value=False)
    agent_store = AsyncMock()
    agent_store.get_by_name = AsyncMock(
        return_value=SimpleNamespace(id="jira-agent-id")
    )
    room_store = AsyncMock()
    room_store.get = AsyncMock(
        return_value=SimpleNamespace(id="room-1", name="Feature")
    )
    protocol = AsyncMock()

    service = _service(
        trigger_store=trigger_store,
        agent_store=agent_store,
        protocol=protocol,
        room_store=room_store,
    )
    await service.process_event(instance="acme", event=_event())
    protocol.send_targeted_message.assert_not_called()


@pytest.mark.asyncio
async def test_process_event_rate_limits() -> None:
    rule = _room_rule(message_template="x", project_key="", issue_type="")
    trigger_store = AsyncMock()
    trigger_store.list = AsyncMock(return_value=[rule])
    trigger_store.count_firings_in_window = AsyncMock(return_value=10)
    agent_store = AsyncMock()
    agent_store.get_by_name = AsyncMock(
        return_value=SimpleNamespace(id="jira-agent-id")
    )
    room_store = AsyncMock()
    room_store.get = AsyncMock(
        return_value=SimpleNamespace(id="room-1", name="Feature")
    )
    protocol = AsyncMock()

    service = _service(
        trigger_store=trigger_store,
        agent_store=agent_store,
        protocol=protocol,
        room_store=room_store,
        config=_config(jira_rate_limit_per_rule=10),
    )
    await service.process_event(instance="acme", event=_event())
    protocol.send_targeted_message.assert_not_called()
    trigger_store.try_record_firing.assert_not_called()


@pytest.mark.asyncio
async def test_group_fan_out_posts_to_each_room() -> None:
    rule = _room_rule(
        target_kind="group",
        target_room_id=None,
        target_group_id="group-1",
        project_key="",
        issue_type="",
        message_template="hi {{issue.key}}",
    )
    trigger_store = AsyncMock()
    trigger_store.list = AsyncMock(return_value=[rule])
    trigger_store.count_firings_in_window = AsyncMock(return_value=0)
    trigger_store.try_record_firing = AsyncMock(return_value=True)
    agent_store = AsyncMock()
    agent_store.get_by_name = AsyncMock(
        return_value=SimpleNamespace(id="jira-agent-id")
    )

    rooms = [
        SimpleNamespace(id="r1", name="One"),
        SimpleNamespace(id="r2", name="Two"),
    ]
    session = AsyncMock()
    session.commit = AsyncMock()
    execute_result = MagicMock()
    execute_result.scalars.return_value.all.return_value = rooms
    session.execute = AsyncMock(return_value=execute_result)
    session_factory = MagicMock(return_value=_SessionCM(session))

    protocol = AsyncMock()
    protocol.send_targeted_message = AsyncMock(
        return_value=SendTargetedResult(
            event_id="$evt",
            target_statuses={"coder": AgentStatus.LIVE},
        )
    )

    service = JiraBridgeService(
        session_factory=session_factory,
        trigger_store=trigger_store,
        agent_store=agent_store,
        room_store=AsyncMock(),
        protocol=protocol,
        config=_config(),
    )
    await service.process_event(instance="acme", event=_event())
    assert protocol.send_targeted_message.await_count == 2
    room_ids = {
        c.kwargs["room_id"] for c in protocol.send_targeted_message.await_args_list
    }
    assert room_ids == {"r1", "r2"}


@pytest.mark.asyncio
async def test_group_empty_is_noop() -> None:
    rule = _room_rule(
        target_kind="group",
        target_room_id=None,
        target_group_id="group-1",
        project_key="",
        issue_type="",
        message_template="x",
    )
    trigger_store = AsyncMock()
    trigger_store.list = AsyncMock(return_value=[rule])
    agent_store = AsyncMock()
    agent_store.get_by_name = AsyncMock(
        return_value=SimpleNamespace(id="jira-agent-id")
    )
    session = AsyncMock()
    session.commit = AsyncMock()
    execute_result = MagicMock()
    execute_result.scalars.return_value.all.return_value = []
    session.execute = AsyncMock(return_value=execute_result)
    session_factory = MagicMock(return_value=_SessionCM(session))
    protocol = AsyncMock()

    service = JiraBridgeService(
        session_factory=session_factory,
        trigger_store=trigger_store,
        agent_store=agent_store,
        room_store=AsyncMock(),
        protocol=protocol,
        config=_config(),
    )
    await service.process_event(instance="acme", event=_event())
    protocol.send_targeted_message.assert_not_called()
    trigger_store.try_record_firing.assert_not_called()


@pytest.mark.asyncio
async def test_thread_by_issue_reuses_root() -> None:
    rule = _room_rule(
        project_key="",
        issue_type="",
        message_template="x",
        thread_by="issue_key",
    )
    trigger_store = AsyncMock()
    trigger_store.list = AsyncMock(return_value=[rule])
    trigger_store.count_firings_in_window = AsyncMock(return_value=0)
    trigger_store.try_record_firing = AsyncMock(return_value=True)
    trigger_store.get_issue_thread_root = AsyncMock(return_value="$root")
    agent_store = AsyncMock()
    agent_store.get_by_name = AsyncMock(
        return_value=SimpleNamespace(id="jira-agent-id")
    )
    room_store = AsyncMock()
    room_store.get = AsyncMock(
        return_value=SimpleNamespace(id="room-1", name="Feature")
    )
    protocol = AsyncMock()
    protocol.send_targeted_message = AsyncMock(
        return_value=SendTargetedResult(
            event_id="$reply",
            target_statuses={"coder": AgentStatus.LIVE},
        )
    )

    service = _service(
        trigger_store=trigger_store,
        agent_store=agent_store,
        protocol=protocol,
        room_store=room_store,
    )
    await service.process_event(instance="acme", event=_event())
    kwargs = protocol.send_targeted_message.await_args.kwargs
    assert kwargs["thread_id"] == "$root"
    trigger_store.upsert_issue_thread.assert_not_called()


@pytest.mark.asyncio
async def test_thread_by_issue_opens_new_thread() -> None:
    rule = _room_rule(
        project_key="",
        issue_type="",
        message_template="x",
        thread_by="issue_key",
    )
    trigger_store = AsyncMock()
    trigger_store.list = AsyncMock(return_value=[rule])
    trigger_store.count_firings_in_window = AsyncMock(return_value=0)
    trigger_store.try_record_firing = AsyncMock(return_value=True)
    trigger_store.get_issue_thread_root = AsyncMock(return_value=None)
    trigger_store.upsert_issue_thread = AsyncMock()
    agent_store = AsyncMock()
    agent_store.get_by_name = AsyncMock(
        return_value=SimpleNamespace(id="jira-agent-id")
    )
    room_store = AsyncMock()
    room_store.get = AsyncMock(
        return_value=SimpleNamespace(id="room-1", name="Feature")
    )
    protocol = AsyncMock()
    protocol.send_targeted_message = AsyncMock(
        return_value=SendTargetedResult(
            event_id="$new-root",
            target_statuses={"coder": AgentStatus.LIVE},
        )
    )

    service = _service(
        trigger_store=trigger_store,
        agent_store=agent_store,
        protocol=protocol,
        room_store=room_store,
    )
    await service.process_event(instance="acme", event=_event())
    kwargs = protocol.send_targeted_message.await_args.kwargs
    assert kwargs["thread_id"] is None
    trigger_store.upsert_issue_thread.assert_awaited_once()
    upsert_kwargs = trigger_store.upsert_issue_thread.await_args.kwargs
    assert upsert_kwargs["thread_root_event_id"] == "$new-root"
    assert upsert_kwargs["issue_key"] == "PROJ-1"
