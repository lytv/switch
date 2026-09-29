from __future__ import annotations

import logging
import secrets
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from sqlalchemy.ext.asyncio import AsyncSession

from switch_core.bridges.jira import management as jira_management
from switch_core.bridges.jira import routes as jira_webhook_routes
from switch_core.bridges.jira.template import MESSAGE_TOKENS
from switch_core.config import SwitchConfig
from switch_core.db.models import User
from switch_core.db.stores.agent_store import AgentStore
from switch_core.db.stores.jira_trigger_store import JiraTriggerStore
from switch_core.db.stores.room_group_store import RoomGroupStore
from switch_core.db.stores.room_store import RoomStore
from switch_core.gateway.auth import require_admin
from switch_core.gateway.dependencies import (
    get_agent_store,
    get_config,
    get_jira_trigger_store,
    get_room_group_store,
    get_room_store,
    get_session,
)
from switch_core.gateway.schemas import (
    JiraDeliveryListResponse,
    JiraDryRunRequest,
    JiraDryRunResponse,
    JiraRotateSecretResponse,
    JiraSetupResponse,
    JiraTriggerCreateRequest,
    JiraTriggerDetail,
    JiraTriggerUpdateRequest,
    RevealKeyResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/message-tokens")
async def list_message_tokens(
    _admin: Annotated[User, Depends(require_admin)],
) -> list[str]:
    return list(MESSAGE_TOKENS)


@router.get("/deliveries")
async def list_deliveries(
    _admin: Annotated[User, Depends(require_admin)],
    session: Annotated[AsyncSession, Depends(get_session)],
    store: Annotated[JiraTriggerStore, Depends(get_jira_trigger_store)],
    config: Annotated[SwitchConfig, Depends(get_config)],
    instance: Annotated[str | None, Query()] = None,
    rule_id: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> JiraDeliveryListResponse:
    return await jira_management.list_deliveries(
        session,
        store,
        config,
        instance=instance,
        rule_id=rule_id,
        limit=limit,
        offset=offset,
    )


@router.get("/setup")
async def get_setup(
    _admin: Annotated[User, Depends(require_admin)],
    config: Annotated[SwitchConfig, Depends(get_config)],
) -> JiraSetupResponse:
    return await jira_management.get_setup(config)


@router.get("/setup/{instance}/reveal")
async def reveal_instance_secret(
    instance: str,
    _admin: Annotated[User, Depends(require_admin)],
) -> RevealKeyResponse:
    secret = jira_webhook_routes.get_jira_secrets().get(instance)
    if secret is None:
        raise HTTPException(
            status_code=404, detail=f"Unknown Jira instance: {instance}"
        )
    return RevealKeyResponse(key=secret)


@router.post("/setup/{instance}/rotate")
async def rotate_instance_secret(
    instance: str,
    _admin: Annotated[User, Depends(require_admin)],
    config: Annotated[SwitchConfig, Depends(get_config)],
) -> JiraRotateSecretResponse:
    live = jira_webhook_routes.get_jira_secrets()
    if instance not in live and instance not in config.jira_webhook_secrets:
        raise HTTPException(
            status_code=404, detail=f"Unknown Jira instance: {instance}"
        )
    new_secret = secrets.token_urlsafe(32)
    jira_webhook_routes.set_jira_instance_secret(instance, new_secret)
    config.jira_webhook_secrets[instance] = new_secret
    logger.info("Rotated Jira webhook secret for instance %r", instance)
    return JiraRotateSecretResponse(
        instance=instance,
        secret=new_secret,
        webhook_url=jira_management.webhook_url(config, instance),
        note=(
            "The new secret is live in this process immediately. Update "
            "JIRA_WEBHOOK_SECRETS (and Jira's X-Switch-Secret header) so the "
            "value survives a restart."
        ),
    )


@router.get("/agent-options")
async def list_agent_options(
    session: Annotated[AsyncSession, Depends(get_session)],
    room_store: Annotated[RoomStore, Depends(get_room_store)],
    agent_store: Annotated[AgentStore, Depends(get_agent_store)],
    _admin: Annotated[User, Depends(require_admin)],
    room_id: Annotated[str | None, Query()] = None,
    group_id: Annotated[str | None, Query()] = None,
) -> list[dict[str, str]]:
    return await jira_management.list_agent_options(
        session,
        room_store,
        agent_store,
        room_id=room_id,
        group_id=group_id,
    )


@router.get("")
async def list_triggers(
    session: Annotated[AsyncSession, Depends(get_session)],
    trigger_store: Annotated[JiraTriggerStore, Depends(get_jira_trigger_store)],
    room_store: Annotated[RoomStore, Depends(get_room_store)],
    room_group_store: Annotated[RoomGroupStore, Depends(get_room_group_store)],
    _admin: Annotated[User, Depends(require_admin)],
    instance: Annotated[str | None, Query()] = None,
) -> list[JiraTriggerDetail]:
    return await jira_management.list_triggers(
        session,
        trigger_store,
        room_store,
        room_group_store,
        instance=instance,
    )


@router.post("", status_code=201)
async def create_trigger(
    req: JiraTriggerCreateRequest,
    session: Annotated[AsyncSession, Depends(get_session)],
    trigger_store: Annotated[JiraTriggerStore, Depends(get_jira_trigger_store)],
    room_store: Annotated[RoomStore, Depends(get_room_store)],
    room_group_store: Annotated[RoomGroupStore, Depends(get_room_group_store)],
    agent_store: Annotated[AgentStore, Depends(get_agent_store)],
    _admin: Annotated[User, Depends(require_admin)],
) -> JiraTriggerDetail:
    return await jira_management.create_trigger(
        session,
        trigger_store,
        room_store,
        room_group_store,
        agent_store,
        req,
    )


@router.get("/{trigger_id}")
async def get_trigger(
    trigger_id: str,
    session: Annotated[AsyncSession, Depends(get_session)],
    trigger_store: Annotated[JiraTriggerStore, Depends(get_jira_trigger_store)],
    room_store: Annotated[RoomStore, Depends(get_room_store)],
    room_group_store: Annotated[RoomGroupStore, Depends(get_room_group_store)],
    _admin: Annotated[User, Depends(require_admin)],
) -> JiraTriggerDetail:
    return await jira_management.get_trigger(
        session,
        trigger_store,
        room_store,
        room_group_store,
        trigger_id,
    )


@router.patch("/{trigger_id}")
async def patch_trigger(
    trigger_id: str,
    req: JiraTriggerUpdateRequest,
    session: Annotated[AsyncSession, Depends(get_session)],
    trigger_store: Annotated[JiraTriggerStore, Depends(get_jira_trigger_store)],
    room_store: Annotated[RoomStore, Depends(get_room_store)],
    room_group_store: Annotated[RoomGroupStore, Depends(get_room_group_store)],
    agent_store: Annotated[AgentStore, Depends(get_agent_store)],
    _admin: Annotated[User, Depends(require_admin)],
) -> JiraTriggerDetail:
    return await jira_management.update_trigger(
        session,
        trigger_store,
        room_store,
        room_group_store,
        agent_store,
        trigger_id,
        req,
    )


@router.delete("/{trigger_id}", status_code=204)
async def delete_trigger(
    trigger_id: str,
    session: Annotated[AsyncSession, Depends(get_session)],
    trigger_store: Annotated[JiraTriggerStore, Depends(get_jira_trigger_store)],
    _admin: Annotated[User, Depends(require_admin)],
) -> Response:
    await jira_management.delete_trigger(session, trigger_store, trigger_id)
    return Response(status_code=204)


@router.post("/{trigger_id}/dry-run")
async def dry_run_trigger(
    trigger_id: str,
    req: JiraDryRunRequest,
    session: Annotated[AsyncSession, Depends(get_session)],
    trigger_store: Annotated[JiraTriggerStore, Depends(get_jira_trigger_store)],
    room_store: Annotated[RoomStore, Depends(get_room_store)],
    room_group_store: Annotated[RoomGroupStore, Depends(get_room_group_store)],
    config: Annotated[SwitchConfig, Depends(get_config)],
    _admin: Annotated[User, Depends(require_admin)],
) -> JiraDryRunResponse:
    return await jira_management.dry_run_trigger(
        session,
        trigger_store,
        room_store,
        room_group_store,
        config,
        trigger_id,
        req,
    )
