from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, BackgroundTasks, Header, HTTPException, Request
from fastapi.responses import JSONResponse

from switch_core.bridges.jira.auth import SECRET_HEADER, verify_instance_secret
from switch_core.bridges.jira.parse import JiraPayloadError, parse_jira_payload
from switch_core.bridges.jira.service import JiraBridgeService

logger = logging.getLogger(__name__)

router = APIRouter(tags=["jira"])

_service: JiraBridgeService | None = None
_secrets: dict[str, str] = {}


def init_jira_routes(
    *,
    service: JiraBridgeService,
    secrets_by_instance: dict[str, str],
) -> None:
    global _service, _secrets
    _service = service
    # Keep a mutable copy so Gateway rotate can update live auth without restart.
    _secrets = dict(secrets_by_instance)


def get_jira_secrets() -> dict[str, str]:
    return _secrets


def set_jira_instance_secret(instance: str, secret: str) -> None:
    _secrets[instance] = secret


def _require_service() -> JiraBridgeService:
    if _service is None:
        raise RuntimeError("Jira bridge routes were not initialised")
    return _service


@router.post("/integrations/jira/{instance}")
async def jira_webhook(
    instance: str,
    request: Request,
    background_tasks: BackgroundTasks,
    x_switch_secret: str | None = Header(default=None, alias=SECRET_HEADER),
) -> JSONResponse:
    """Accept a signed Jira webhook and process matching rules asynchronously."""
    if not verify_instance_secret(
        instance=instance,
        provided=x_switch_secret,
        secrets_by_instance=_secrets,
    ):
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        payload: Any = await request.json()
    except Exception as exc:
        logger.warning(
            "Jira webhook for instance %r rejected: body is not JSON (%s)",
            instance,
            type(exc).__name__,
        )
        raise HTTPException(status_code=400, detail="Invalid JSON body") from exc

    try:
        event = parse_jira_payload(payload)
    except JiraPayloadError as exc:
        logger.warning(
            "Jira webhook for instance %r rejected: malformed payload: %s",
            instance,
            exc,
        )
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    logger.info(
        "Jira webhook received instance=%s event=%s issue=%s",
        instance,
        event.webhook_event,
        event.key,
    )

    service = _require_service()
    background_tasks.add_task(service.process_event, instance=instance, event=event)
    return JSONResponse(status_code=202, content={"status": "accepted"})
