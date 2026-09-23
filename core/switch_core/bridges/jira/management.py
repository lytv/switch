"""Shared Jira trigger management — one set of rules, two doors.

Both the admin-cookie gateway routes (`gateway/jira_triggers.py`) and the
agent-operations channel (`bridges/agent/operations/jira.py`, reachable as
`POST /agents/{id}/ops/...` and as MCP tools) are thin wrappers over the
functions here. Validation, name resolution, dry-run semantics and delivery
shaping live in exactly one place, so the two auth paths cannot diverge in
behavior — same rules, different door.

Like `bridges/agent/protocol/agent_detail.py`, this imports `gateway.schemas`
(pure pydantic leaves) and `bridges/jira/*` — neither pulls in `gateway`
handlers, so there is no import cycle.

Secret reveal/rotate deliberately do NOT live here. They stay
admin-cookie-only in `gateway/jira_triggers.py` (Option B); the agent-ops
door exposes setup-read (masked secret only) and nothing that prints or
mints a real secret.
"""

from __future__ import annotations

import logging
from typing import Any
from urllib.parse import urljoin

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from switch_core.bridges.jira import routes as jira_webhook_routes
from switch_core.bridges.jira.matching import explain_match
from switch_core.bridges.jira.parse import (
    JiraPayloadError,
    ParsedJiraEvent,
    StatusTransition,
    parse_jira_payload,
)
from switch_core.bridges.jira.template import render_template
from switch_core.config import SwitchConfig
from switch_core.db.models import JiraTrigger, Room
from switch_core.db.stores.agent_store import AgentStore
from switch_core.db.stores.jira_trigger_store import JiraTriggerStore
from switch_core.db.stores.room_group_store import RoomGroupStore
from switch_core.db.stores.room_store import RoomStore
from switch_core.gateway.schemas import (
    JiraDeliveryDetail,
    JiraDeliveryListResponse,
    JiraDryRunRequest,
    JiraDryRunResponse,
    JiraDryRunTarget,
    JiraInstanceSetup,
    JiraRoomDeliveryResult,
    JiraSetupResponse,
    JiraTriggerCreateRequest,
    JiraTriggerDetail,
    JiraTriggerUpdateRequest,
)

logger = logging.getLogger(__name__)

_FIRE_ON = frozenset({"created", "updated", "transition"})
_TARGET_KINDS = frozenset({"room", "group"})
_THREAD_BY = frozenset({"new", "issue_key"})


def _to_detail(
    trigger: JiraTrigger,
    *,
    target_room_name: str | None = None,
    target_group_name: str | None = None,
) -> JiraTriggerDetail:
    return JiraTriggerDetail(
        id=trigger.id,
        name=trigger.name,
        enabled=trigger.enabled,
        instance=trigger.instance,
        project_key=trigger.project_key,
        issue_type=trigger.issue_type,
        fire_on=trigger.fire_on,
        target_status=trigger.target_status,
        jql=trigger.jql,
        target_kind=trigger.target_kind,
        target_room_id=trigger.target_room_id,
        target_room_name=target_room_name,
        target_group_id=trigger.target_group_id,
        target_group_name=target_group_name,
        agent_name=trigger.agent_name,
        message_template=trigger.message_template,
        thread_by=trigger.thread_by,
        created_at=str(trigger.created_at),
        updated_at=str(trigger.updated_at),
    )


async def _resolve_names(
    session: AsyncSession,
    room_store: RoomStore,
    room_group_store: RoomGroupStore,
    trigger: JiraTrigger,
) -> JiraTriggerDetail:
    room_name: str | None = None
    group_name: str | None = None
    if trigger.target_room_id:
        room = await room_store.get(session, trigger.target_room_id)
        room_name = room.name if room is not None else None
    if trigger.target_group_id:
        group = await room_group_store.get(session, trigger.target_group_id)
        group_name = group.name if group is not None else None
    return _to_detail(trigger, target_room_name=room_name, target_group_name=group_name)


async def _validate_target_and_agent(
    session: AsyncSession,
    *,
    room_store: RoomStore,
    room_group_store: RoomGroupStore,
    agent_store: AgentStore,
    target_kind: str,
    target_room_id: str | None,
    target_group_id: str | None,
    agent_name: str,
) -> tuple[str | None, str | None]:
    """Return normalised (target_room_id, target_group_id) or raise HTTPException."""
    kind = target_kind.strip().casefold()
    if kind not in _TARGET_KINDS:
        raise HTTPException(
            status_code=400,
            detail=f"target_kind must be one of: {', '.join(sorted(_TARGET_KINDS))}",
        )

    name = agent_name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="agent_name is required")
    agent = await agent_store.get_by_name(session, name)
    if agent is None:
        raise HTTPException(status_code=400, detail=f"Unknown agent: {name}")

    if kind == "room":
        if not target_room_id:
            raise HTTPException(
                status_code=400,
                detail="target_room_id is required when target_kind is room",
            )
        room = await room_store.get(session, target_room_id)
        if room is None:
            raise HTTPException(
                status_code=400, detail=f"Unknown room: {target_room_id}"
            )
        member_ids = await room_store.get_agent_ids(session, target_room_id)
        if agent.id not in member_ids:
            raise HTTPException(
                status_code=400,
                detail=f"Agent {name!r} is not a member of room {room.name!r}",
            )
        return target_room_id, None

    if not target_group_id:
        raise HTTPException(
            status_code=400,
            detail="target_group_id is required when target_kind is group",
        )
    group = await room_group_store.get(session, target_group_id)
    if group is None:
        raise HTTPException(
            status_code=400, detail=f"Unknown room group: {target_group_id}"
        )
    return None, target_group_id


def _validate_common(
    *,
    name: str,
    instance: str,
    fire_on: str,
    message_template: str,
    thread_by: str,
) -> None:
    if not name.strip():
        raise HTTPException(status_code=400, detail="name is required")
    if not instance.strip():
        raise HTTPException(status_code=400, detail="instance is required")
    if fire_on.strip().casefold() not in _FIRE_ON:
        raise HTTPException(
            status_code=400,
            detail=f"fire_on must be one of: {', '.join(sorted(_FIRE_ON))}",
        )
    if not message_template.strip():
        raise HTTPException(
            status_code=400, detail="message_template must not be empty"
        )
    if thread_by.strip().casefold() not in _THREAD_BY:
        raise HTTPException(
            status_code=400,
            detail=f"thread_by must be one of: {', '.join(sorted(_THREAD_BY))}",
        )


def _mask_secret(secret: str) -> str:
    if len(secret) <= 8:
        return "••••••••"
    return f"{secret[:4]}…{secret[-4:]}"


def webhook_url(config: SwitchConfig, instance: str) -> str:
    """Public webhook URL for an instance (rotate needs it too)."""
    path = f"/integrations/jira/{instance}"
    base = (config.gateway_public_url or "").rstrip("/")
    if not base:
        return path
    return urljoin(base + "/", path.lstrip("/"))


def _sample_event() -> ParsedJiraEvent:
    return ParsedJiraEvent(
        event_kind="updated",
        webhook_event="jira:issue_updated",
        key="PROJ-123",
        summary="Sample issue for dry-run",
        issue_type="Story",
        project="PROJ",
        status="In Progress",
        assignee="Ada",
        priority="High",
        reporter="Bob",
        url="https://example.invalid/browse/PROJ-123",
        labels=("agentic",),
        transition=StatusTransition("To Do", "In Progress"),
    )


async def _rooms_for_group(session: AsyncSession, group_id: str) -> list[Room]:
    result = await session.execute(
        select(Room).where(
            Room.group_id == group_id,
            Room.archived_at.is_(None),
        )
    )
    return list(result.scalars().all())


def _delivery_to_detail(
    row: object, visible_rule_ids: set[str] | None = None
) -> JiraDeliveryDetail:
    room_results_raw = getattr(row, "room_results", None) or []
    room_results = [
        JiraRoomDeliveryResult(
            room_id=str(item.get("room_id", "")),
            room_name=item.get("room_name"),
            status=str(item.get("status", "")),
            event_id=item.get("event_id"),
            error=item.get("error"),
            attempts=item.get("attempts"),
        )
        for item in room_results_raw
        if isinstance(item, dict)
    ]
    matched = getattr(row, "matched_rule_ids", None) or []
    return JiraDeliveryDetail(
        id=row.id,  # type: ignore[attr-defined]
        issue_key=row.issue_key,  # type: ignore[attr-defined]
        rule_id=row.rule_id,  # type: ignore[attr-defined]
        rule_name=row.rule_name or "",  # type: ignore[attr-defined]
        instance=row.instance or "",  # type: ignore[attr-defined]
        transition_key=row.transition_key,  # type: ignore[attr-defined]
        status=row.status,  # type: ignore[attr-defined]
        matched_rule_ids=[
            str(rule_id)
            for rule_id in matched
            if visible_rule_ids is None or str(rule_id) in visible_rule_ids
        ],
        room_results=room_results,
        error=row.error,  # type: ignore[attr-defined]
        attempt_count=int(row.attempt_count or 0),  # type: ignore[attr-defined]
        created_at=str(row.created_at),  # type: ignore[attr-defined]
    )


def _apply_sample_overrides(
    event: ParsedJiraEvent, overrides: dict[str, Any]
) -> ParsedJiraEvent:
    labels = event.labels
    transition = event.transition
    raw = event.raw
    fields: dict[str, str] = {
        "event_kind": event.event_kind,
        "webhook_event": event.webhook_event,
        "key": event.key,
        "summary": event.summary,
        "issue_type": event.issue_type,
        "project": event.project,
        "status": event.status,
        "assignee": event.assignee,
        "priority": event.priority,
        "reporter": event.reporter,
        "url": event.url,
    }
    for key, value in overrides.items():
        if key == "labels" and isinstance(value, list):
            labels = tuple(str(v) for v in value)
        elif key == "transition" and isinstance(value, dict):
            transition = StatusTransition(
                from_status=str(value.get("from_status") or value.get("from") or ""),
                to_status=str(value.get("to_status") or value.get("to") or ""),
            )
        elif key in fields:
            fields[key] = str(value)
    return ParsedJiraEvent(
        event_kind=fields["event_kind"],
        webhook_event=fields["webhook_event"],
        key=fields["key"],
        summary=fields["summary"],
        issue_type=fields["issue_type"],
        project=fields["project"],
        status=fields["status"],
        assignee=fields["assignee"],
        priority=fields["priority"],
        reporter=fields["reporter"],
        url=fields["url"],
        labels=labels,
        transition=transition,
        raw=raw,
    )


async def get_setup(config: SwitchConfig) -> JiraSetupResponse:
    """Configured instances with masked secrets only — never the real secret."""
    live = jira_webhook_routes.get_jira_secrets()
    instances = [
        JiraInstanceSetup(
            instance=name,
            webhook_url=webhook_url(config, name),
            secret_masked=_mask_secret(secret),
            secret_configured=bool(secret),
        )
        for name, secret in sorted(live.items())
    ]
    return JiraSetupResponse(
        instances=instances,
        jira_agent_name=config.jira_agent_name,
        gateway_public_url=config.gateway_public_url,
        guidance={
            "classic_webhook": (
                "In Jira → System → WebHooks, create a webhook pointed at the "
                "URL below. Set a custom header X-Switch-Secret to the instance "
                "secret. Subscribe to issue created and updated events."
            ),
            "automation": (
                "In a Jira Automation rule, add a Send web request action. "
                "Method POST, URL as below, header X-Switch-Secret with the "
                "secret, and a JSON body that includes issue fields (or the "
                "issue object). Include changelog items when matching transitions."
            ),
            "room_membership": (
                f"Add the Switch system agent {config.jira_agent_name!r} as a "
                "member of every target room (and every room in a target group) "
                "so it can post addressed messages."
            ),
            "docs": (
                "Full setup guide: docs/old/bridges/JIRA_SETUP.md — Cloud and "
                "Server/Data Center webhooks, secrets, rule fields, dry-run, "
                "and troubleshooting."
            ),
        },
    )


async def list_agent_options(
    session: AsyncSession,
    room_store: RoomStore,
    agent_store: AgentStore,
    *,
    room_id: str | None = None,
    group_id: str | None = None,
) -> list[dict[str, str]]:
    if bool(room_id) == bool(group_id):
        raise HTTPException(
            status_code=400, detail="Provide exactly one of room_id or group_id"
        )
    agent_ids: set[str] = set()
    if room_id:
        room = await room_store.get(session, room_id)
        if room is None:
            raise HTTPException(status_code=404, detail="Room not found")
        agent_ids.update(await room_store.get_agent_ids(session, room_id))
    else:
        assert group_id is not None
        rooms = await _rooms_for_group(session, group_id)
        for room in rooms:
            agent_ids.update(await room_store.get_agent_ids(session, room.id))

    agents = []
    for agent_id in agent_ids:
        agent = await agent_store.get(session, agent_id)
        if agent is not None:
            agents.append(agent)
    agents.sort(key=lambda a: a.name.lower())
    return [{"id": a.id, "name": a.name} for a in agents]


async def list_triggers(
    session: AsyncSession,
    trigger_store: JiraTriggerStore,
    room_store: RoomStore,
    room_group_store: RoomGroupStore,
    *,
    instance: str | None = None,
    for_update: bool = False,
) -> list[JiraTriggerDetail]:
    triggers = await trigger_store.list(
        session, instance=instance, for_update=for_update
    )
    return [
        await _resolve_names(session, room_store, room_group_store, t) for t in triggers
    ]


async def create_trigger(
    session: AsyncSession,
    trigger_store: JiraTriggerStore,
    room_store: RoomStore,
    room_group_store: RoomGroupStore,
    agent_store: AgentStore,
    req: JiraTriggerCreateRequest,
) -> JiraTriggerDetail:
    _validate_common(
        name=req.name,
        instance=req.instance,
        fire_on=req.fire_on,
        message_template=req.message_template,
        thread_by=req.thread_by,
    )
    room_id, group_id = await _validate_target_and_agent(
        session,
        room_store=room_store,
        room_group_store=room_group_store,
        agent_store=agent_store,
        target_kind=req.target_kind,
        target_room_id=req.target_room_id,
        target_group_id=req.target_group_id,
        agent_name=req.agent_name,
    )
    trigger = JiraTrigger(
        name=req.name.strip(),
        enabled=req.enabled,
        instance=req.instance.strip(),
        project_key=req.project_key.strip(),
        issue_type=req.issue_type.strip(),
        fire_on=req.fire_on.strip().casefold(),
        target_status=req.target_status.strip(),
        jql=req.jql.strip(),
        target_kind=req.target_kind.strip().casefold(),
        target_room_id=room_id,
        target_group_id=group_id,
        agent_name=req.agent_name.strip(),
        message_template=req.message_template,
        thread_by=req.thread_by.strip().casefold(),
    )
    await trigger_store.create(session, trigger)
    await session.refresh(trigger)
    detail = await _resolve_names(session, room_store, room_group_store, trigger)
    await session.commit()
    return detail


async def get_trigger(
    session: AsyncSession,
    trigger_store: JiraTriggerStore,
    room_store: RoomStore,
    room_group_store: RoomGroupStore,
    trigger_id: str,
) -> JiraTriggerDetail:
    trigger = await trigger_store.get(session, trigger_id)
    if trigger is None:
        raise HTTPException(status_code=404, detail="Jira trigger not found")
    return await _resolve_names(session, room_store, room_group_store, trigger)


async def update_trigger(
    session: AsyncSession,
    trigger_store: JiraTriggerStore,
    room_store: RoomStore,
    room_group_store: RoomGroupStore,
    agent_store: AgentStore,
    trigger_id: str,
    req: JiraTriggerUpdateRequest,
) -> JiraTriggerDetail:
    trigger = await trigger_store.get(session, trigger_id)
    if trigger is None:
        raise HTTPException(status_code=404, detail="Jira trigger not found")

    fields = req.model_dump(exclude_unset=True)
    if not fields:
        return await _resolve_names(session, room_store, room_group_store, trigger)

    name = fields.get("name", trigger.name)
    instance = fields.get("instance", trigger.instance)
    fire_on = fields.get("fire_on", trigger.fire_on)
    message_template = fields.get("message_template", trigger.message_template)
    thread_by = fields.get("thread_by", trigger.thread_by)
    _validate_common(
        name=name,
        instance=instance,
        fire_on=fire_on,
        message_template=message_template,
        thread_by=thread_by,
    )

    target_kind = fields.get("target_kind", trigger.target_kind)
    target_room_id = fields.get("target_room_id", trigger.target_room_id)
    target_group_id = fields.get("target_group_id", trigger.target_group_id)
    agent_name = fields.get("agent_name", trigger.agent_name)
    room_id, group_id = await _validate_target_and_agent(
        session,
        room_store=room_store,
        room_group_store=room_group_store,
        agent_store=agent_store,
        target_kind=target_kind,
        target_room_id=target_room_id,
        target_group_id=target_group_id,
        agent_name=agent_name,
    )
    fields["target_room_id"] = room_id
    fields["target_group_id"] = group_id
    fields["target_kind"] = target_kind.strip().casefold()
    fields["agent_name"] = agent_name.strip()

    for key in (
        "name",
        "instance",
        "project_key",
        "issue_type",
        "target_status",
        "jql",
    ):
        if key in fields and isinstance(fields[key], str):
            fields[key] = fields[key].strip()
    if "fire_on" in fields:
        fields["fire_on"] = str(fields["fire_on"]).strip().casefold()
    if "thread_by" in fields:
        fields["thread_by"] = str(fields["thread_by"]).strip().casefold()

    updated = await trigger_store.update(session, trigger_id, **fields)
    assert updated is not None
    await session.refresh(updated)
    detail = await _resolve_names(session, room_store, room_group_store, updated)
    await session.commit()
    return detail


async def delete_trigger(
    session: AsyncSession,
    trigger_store: JiraTriggerStore,
    trigger_id: str,
) -> bool:
    removed = await trigger_store.delete(session, trigger_id)
    if not removed:
        raise HTTPException(status_code=404, detail="Jira trigger not found")
    await session.commit()
    return True


async def dry_run_trigger(
    session: AsyncSession,
    trigger_store: JiraTriggerStore,
    room_store: RoomStore,
    room_group_store: RoomGroupStore,
    config: SwitchConfig,
    trigger_id: str,
    req: JiraDryRunRequest,
) -> JiraDryRunResponse:
    trigger = await trigger_store.get(session, trigger_id)
    if trigger is None:
        raise HTTPException(status_code=404, detail="Jira trigger not found")

    if req.payload is not None:
        try:
            event = parse_jira_payload(req.payload)
        except JiraPayloadError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
    else:
        event = _sample_event()
        if req.sample_overrides:
            event = _apply_sample_overrides(event, req.sample_overrides)

    matched, reasons = explain_match(trigger, event)
    rendered = (
        render_template(
            trigger.message_template,
            event,
            max_chars=config.jira_message_max_chars,
        )
        if matched
        else None
    )

    targets: list[JiraDryRunTarget] = []
    if matched:
        if trigger.target_kind == "room" and trigger.target_room_id:
            room = await room_store.get(session, trigger.target_room_id)
            targets.append(
                JiraDryRunTarget(
                    room_id=trigger.target_room_id,
                    room_name=room.name if room else None,
                    agent_name=trigger.agent_name,
                )
            )
        elif trigger.target_kind == "group" and trigger.target_group_id:
            group = await room_group_store.get(session, trigger.target_group_id)
            rooms = await _rooms_for_group(session, trigger.target_group_id)
            if not rooms:
                reasons.append(
                    f"Group {group.name if group else trigger.target_group_id!r} "
                    "has no member rooms; nothing would be posted."
                )
            for room in rooms:
                targets.append(
                    JiraDryRunTarget(
                        room_id=room.id,
                        room_name=room.name,
                        agent_name=trigger.agent_name,
                        group_id=trigger.target_group_id,
                        group_name=group.name if group else None,
                    )
                )

    return JiraDryRunResponse(
        matched=matched,
        reasons=reasons,
        rendered_message=rendered,
        targets=targets,
        would_post=False,
        issue_key=event.key,
        event_kind=event.event_kind,
    )


async def list_deliveries(
    session: AsyncSession,
    store: JiraTriggerStore,
    config: SwitchConfig,
    *,
    instance: str | None = None,
    rule_id: str | None = None,
    rule_ids: list[str] | None = None,
    limit: int = 50,
    offset: int = 0,
) -> JiraDeliveryListResponse:
    rows = await store.list_deliveries(
        session,
        instance=instance,
        rule_id=rule_id,
        rule_ids=rule_ids,
        limit=limit,
        offset=offset,
    )
    visible_rule_ids = set(rule_ids) if rule_ids is not None else None
    return JiraDeliveryListResponse(
        deliveries=[
            _delivery_to_detail(row, visible_rule_ids)
            for row in rows
        ],
        retain_seconds=config.jira_delivery_log_retain_seconds,
        max_rows=config.jira_delivery_log_max_rows,
    )
