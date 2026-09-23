"""Jira trigger agent operations — Option B (CHOO agent-ops for Jira).

The agent bearer-token channel (`POST /agents/{id}/ops/...`, and MCP tools
from the same registry) reaches these; the admin-cookie `/jira-triggers`
routes cannot be reached with agent credentials. Each operation delegates to
`bridges/jira/management.py`, the same logic the admin routes use — same
rules, different door.

Deliberately absent: secret reveal and secret rotate. Those stay
admin-cookie-only in `gateway/jira_triggers.py`; no operation here prints or
mints a real secret (setup-read carries the masked secret only).
"""

from __future__ import annotations

from typing import Any

from switch_core.bridges.agent.operations.context import get_protocol
from switch_core.bridges.agent.operations.registry import operation
from switch_core.bridges.jira import management as jira_management
from switch_core.bridges.jira.template import MESSAGE_TOKENS
from switch_core.db.stores.agent_store import AgentStore
from switch_core.db.stores.jira_trigger_store import JiraTriggerStore
from switch_core.db.stores.room_group_store import RoomGroupStore
from switch_core.db.stores.room_store import RoomStore
from switch_core.gateway.schemas import (
    JiraDryRunRequest,
    JiraTriggerCreateRequest,
    JiraTriggerUpdateRequest,
)

_TRIGGER_STORE = JiraTriggerStore()
_ROOM_STORE = RoomStore()
_ROOM_GROUP_STORE = RoomGroupStore()
_AGENT_STORE = AgentStore()


@operation
async def list_jira_instances() -> dict[str, Any]:
    """List configured Jira webhook instances (setup-read).

    Returns:
        {instances, jira_agent_name, gateway_public_url, guidance} — each
        instance carries its webhook URL and the secret MASKED. The real
        secret is never exposed here; revealing or rotating one stays an
        admin-UI action.
    """
    protocol = get_protocol()
    setup = await jira_management.get_setup(protocol.config)
    return setup.model_dump()


@operation
async def list_jira_triggers(instance: str | None = None) -> list[dict[str, Any]]:
    """List Jira trigger rules, optionally filtered to one instance.

    Args:
        instance: Only rules for this Jira instance. Omit for all rules.

    Returns:
        List of trigger details with resolved room/group names.
    """
    protocol = get_protocol()
    async with protocol.session_factory() as session:
        triggers = await jira_management.list_triggers(
            session,
            _TRIGGER_STORE,
            _ROOM_STORE,
            _ROOM_GROUP_STORE,
            instance=instance,
        )
    return [t.model_dump() for t in triggers]


@operation
async def get_jira_trigger(trigger_id: str) -> dict[str, Any]:
    """Show one Jira trigger rule in detail.

    Args:
        trigger_id: The rule id (from `list_jira_triggers`).

    Returns:
        The trigger detail with resolved room/group names.
    """
    protocol = get_protocol()
    async with protocol.session_factory() as session:
        trigger = await jira_management.get_trigger(
            session,
            _TRIGGER_STORE,
            _ROOM_STORE,
            _ROOM_GROUP_STORE,
            trigger_id,
        )
    return trigger.model_dump()


@operation
async def create_jira_trigger(
    name: str,
    instance: str,
    fire_on: str,
    message_template: str,
    target_kind: str,
    agent_name: str,
    target_room_id: str | None = None,
    target_group_id: str | None = None,
    project_key: str = "",
    issue_type: str = "",
    target_status: str = "",
    jql: str = "",
    thread_by: str = "new",
    enabled: bool = True,
) -> dict[str, Any]:
    """Create a Jira trigger rule: when a Jira event matches, mention an agent.

    Args:
        name: Display name for the rule.
        instance: Jira instance (from `list_jira_instances`).
        fire_on: One of "created", "updated", "transition".
        message_template: Message text with tokens from
            `list_jira_message_tokens` (e.g. "{{issue.key}} is now
            {{issue.status}}").
        target_kind: "room" or "group".
        agent_name: Agent to address when the rule fires. Must be a member
            of the target room (server-enforced, same rule as the admin UI).
        target_room_id: Target room id (required when target_kind is room).
        target_group_id: Target group id (required when target_kind is group).
        project_key: Only match this Jira project (empty = all).
        issue_type: Only match this issue type (empty = all).
        target_status: For "transition" rules, the status moved to.
        jql: Extra mini-JQL filter (empty = none).
        thread_by: "new" (a fresh thread per fire) or "issue_key" (one
            thread per issue).
        enabled: Whether the rule fires. Defaults to true.

    Returns:
        The created trigger detail.
    """
    protocol = get_protocol()
    req = JiraTriggerCreateRequest(
        name=name,
        enabled=enabled,
        instance=instance,
        project_key=project_key,
        issue_type=issue_type,
        fire_on=fire_on,
        target_status=target_status,
        jql=jql,
        target_kind=target_kind,
        target_room_id=target_room_id,
        target_group_id=target_group_id,
        agent_name=agent_name,
        message_template=message_template,
        thread_by=thread_by,
    )
    async with protocol.session_factory() as session:
        trigger = await jira_management.create_trigger(
            session,
            _TRIGGER_STORE,
            _ROOM_STORE,
            _ROOM_GROUP_STORE,
            _AGENT_STORE,
            req,
        )
    return trigger.model_dump()


@operation
async def update_jira_trigger(
    trigger_id: str,
    name: str | None = None,
    enabled: bool | None = None,
    instance: str | None = None,
    project_key: str | None = None,
    issue_type: str | None = None,
    fire_on: str | None = None,
    target_status: str | None = None,
    jql: str | None = None,
    target_kind: str | None = None,
    target_room_id: str | None = None,
    target_group_id: str | None = None,
    agent_name: str | None = None,
    message_template: str | None = None,
    thread_by: str | None = None,
) -> dict[str, Any]:
    """Update a Jira trigger rule. Pass only the fields to change.

    Args:
        trigger_id: The rule id (from `list_jira_triggers`).
        name, enabled, instance, project_key, issue_type, fire_on,
            target_status, jql, target_kind, target_room_id, target_group_id,
            agent_name, message_template, thread_by: New values; omitted
            fields keep what the rule already has. Same validation as
            `create_jira_trigger`.

    Returns:
        The updated trigger detail.
    """
    protocol = get_protocol()
    fields = {
        key: value
        for key, value in {
            "name": name,
            "enabled": enabled,
            "instance": instance,
            "project_key": project_key,
            "issue_type": issue_type,
            "fire_on": fire_on,
            "target_status": target_status,
            "jql": jql,
            "target_kind": target_kind,
            "target_room_id": target_room_id,
            "target_group_id": target_group_id,
            "agent_name": agent_name,
            "message_template": message_template,
            "thread_by": thread_by,
        }.items()
        if value is not None
    }
    req = JiraTriggerUpdateRequest(**fields)
    async with protocol.session_factory() as session:
        trigger = await jira_management.update_trigger(
            session,
            _TRIGGER_STORE,
            _ROOM_STORE,
            _ROOM_GROUP_STORE,
            _AGENT_STORE,
            trigger_id,
            req,
        )
    return trigger.model_dump()


@operation
async def delete_jira_trigger(trigger_id: str) -> dict[str, str]:
    """Delete a Jira trigger rule.

    Args:
        trigger_id: The rule id (from `list_jira_triggers`).

    Returns:
        {"trigger_id": "...", "status": "deleted"}.
    """
    protocol = get_protocol()
    async with protocol.session_factory() as session:
        await jira_management.delete_trigger(session, _TRIGGER_STORE, trigger_id)
    return {"trigger_id": trigger_id, "status": "deleted"}


@operation
async def dry_run_jira_trigger(
    trigger_id: str,
    sample_overrides: dict[str, Any] | None = None,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Dry-run a Jira trigger rule against a sample event. Never posts.

    Args:
        trigger_id: The rule id (from `list_jira_triggers`).
        sample_overrides: Field overrides on the default PROJ-123 sample
            event (e.g. {"project": "KAN", "status": "In Progress"}).
        payload: A raw Jira webhook payload to test instead of the sample.

    Returns:
        {matched, reasons, rendered_message, targets, would_post (always
        false), issue_key, event_kind}.
    """
    protocol = get_protocol()
    req = JiraDryRunRequest(payload=payload, sample_overrides=sample_overrides)
    async with protocol.session_factory() as session:
        result = await jira_management.dry_run_trigger(
            session,
            _TRIGGER_STORE,
            _ROOM_STORE,
            _ROOM_GROUP_STORE,
            protocol.config,
            trigger_id,
            req,
        )
    return result.model_dump()


@operation
async def list_jira_deliveries(
    instance: str | None = None,
    rule_id: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    """List recent Jira delivery attempts — the diagnosis path for "why
    didn't the rule fire".

    Args:
        instance: Only deliveries for this Jira instance.
        rule_id: Only deliveries for this rule.
        limit: Maximum rows (1-200, default 50).
        offset: Rows to skip for paging.

    Returns:
        {deliveries, retain_seconds, max_rows}.
    """
    protocol = get_protocol()
    async with protocol.session_factory() as session:
        result = await jira_management.list_deliveries(
            session,
            _TRIGGER_STORE,
            protocol.config,
            instance=instance,
            rule_id=rule_id,
            limit=limit,
            offset=offset,
        )
    return result.model_dump()


@operation
async def list_jira_message_tokens() -> list[str]:
    """List the {{placeholders}} a trigger message template may use.

    Returns:
        Token names such as "issue.key" through "transition.to".
    """
    return list(MESSAGE_TOKENS)


@operation
async def list_jira_agent_options(
    room_id: str | None = None,
    group_id: str | None = None,
) -> list[dict[str, str]]:
    """List agents eligible for a trigger target — members of a room, or of
    every room in a group.

    Args:
        room_id: Room to list members of. Exactly one of room_id / group_id
            is required.
        group_id: Group whose rooms' members to list.

    Returns:
        List of {id, name} dicts, sorted by name.
    """
    protocol = get_protocol()
    async with protocol.session_factory() as session:
        return await jira_management.list_agent_options(
            session,
            _ROOM_STORE,
            _AGENT_STORE,
            room_id=room_id,
            group_id=group_id,
        )
