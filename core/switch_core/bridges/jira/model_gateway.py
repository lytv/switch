"""Execution-scoped model access in the existing server, with durable spend holds."""

from __future__ import annotations

import hashlib
import hmac
import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from switch_core.bridges.jira.contracts import Job
from switch_core.bridges.jira.firstmate_jobs import Execution
from switch_core.bridges.jira.model_upstream import model_upstream
from switch_core.bridges.jira.publisher import delivery_lock
from switch_core.bridges.jira.target import delivery_state
from switch_core.config import SwitchConfig
from switch_core.db.models import JiraWorkerJob, JiraWorkerTicket


def model_identity(job: Job) -> str:
    return hashlib.sha256(
        f"{job.run.instance}/{job.run.execution_key}".encode()
    ).hexdigest()


def model_capability(config: SwitchConfig, identity: str) -> str:
    return hmac.new(
        config.jwt_secret_key.encode(),
        ("jira-model:" + identity).encode(),
        hashlib.sha256,
    ).hexdigest()


def _execution(row: JiraWorkerJob) -> Execution:
    return Execution.model_validate_json(json.dumps(row.payload))


def _save(row: JiraWorkerJob, execution: Execution, **changes: Any) -> None:
    assert execution.meter is not None
    row.payload = execution.model_copy(
        update={"meter": execution.meter.model_copy(update=changes)}
    ).model_dump(mode="json")


def _counts(body: dict[str, Any]) -> tuple[int, int, int, int]:
    usage = body["usage"]
    prompt, output, total = (
        usage[key] for key in ("prompt_tokens", "completion_tokens", "total_tokens")
    )
    details = usage.get("prompt_tokens_details") or {}
    cached = details.get("cached_tokens", usage.get("prompt_cache_hit_tokens", 0))
    cache_counts = [
        fields[key]
        for fields, key in (
            (details, "cached_tokens"),
            (usage, "prompt_cache_hit_tokens"),
        )
        if key in fields
    ]
    if (
        any(type(value) is not int or value != cached for value in cache_counts)
        or any(
            type(value) is not int or value < 0
            for value in (prompt, output, total, cached)
        )
        or cached > prompt
        or total != prompt + output
    ):
        raise ValueError("Provider usage is missing or invalid")
    return prompt - cached, output, cached, 0


def _text_only(body: dict[str, Any]) -> bool:
    messages = body.get("messages")
    if not isinstance(messages, list) or not messages:
        return False
    for item in messages:
        if not isinstance(item, dict) or set(item) - {
            "role",
            "content",
            "name",
            "tool_calls",
            "tool_call_id",
        }:
            return False
        content = item.get("content")
        if isinstance(content, list):
            if any(
                not isinstance(block, dict)
                or set(block) != {"type", "text"}
                or block["type"] != "text"
                or not isinstance(block["text"], str)
                for block in content
            ):
                return False
        elif not isinstance(content, (str, type(None))):
            return False
    tools = body.get("tools", [])
    return isinstance(tools, list) and all(
        isinstance(tool, dict) and tool.get("type") == "function" for tool in tools
    )


def _response(body: dict[str, Any], stream: bool) -> Response:
    if not stream:
        return JSONResponse(body)
    choice = body["choices"][0]
    delta = dict(choice["message"])
    if "tool_calls" in delta:
        delta["tool_calls"] = [
            {**tool, "index": index} for index, tool in enumerate(delta["tool_calls"])
        ]
    chunk = {key: body[key] for key in ("id", "created", "model")}
    chunk.update(
        object="chat.completion.chunk",
        choices=[
            {"index": 0, "delta": delta, "finish_reason": choice["finish_reason"]}
        ],
        usage=body["usage"],
    )
    return Response(
        "data: " + json.dumps(chunk) + "\n\ndata: [DONE]\n\n",
        media_type="text/event-stream",
    )


class DeliveryModelGateway:
    def __init__(
        self, sessions: async_sessionmaker[AsyncSession], config: SwitchConfig
    ) -> None:
        self._sessions, self._config = sessions, config
        self.router = APIRouter()
        self.router.add_api_route(
            "/integrations/jira/delivery/model/{identity}/v1/chat/completions",
            self.complete,
            methods=["POST"],
        )

    async def _row(self, session: AsyncSession, identity: str) -> JiraWorkerJob:
        row = await session.scalar(
            select(JiraWorkerJob)
            .where(
                JiraWorkerJob.kind == "role_job",
                JiraWorkerJob.payload["model_identity"].astext == identity,
            )
            .with_for_update()
        )
        if row is None:
            raise HTTPException(403, "Unknown model capability")
        return row

    async def complete(self, identity: str, request: Request) -> Response:
        expected = "Bearer " + model_capability(self._config, identity)
        if not hmac.compare_digest(request.headers.get("authorization", ""), expected):
            raise HTTPException(403, "Invalid model capability")
        data = bytearray()
        async for chunk in request.stream():
            data.extend(chunk)
            if len(data) > 2_000_000:
                raise HTTPException(413, "Model request exceeds its limit")
        try:
            body = json.loads(data)
            if not isinstance(body, dict) or not _text_only(body):
                raise ValueError("This pack supports text and function tools only")
        except (ValueError, TypeError) as exc:
            raise HTTPException(400, "Invalid text-only model request") from exc
        async with delivery_lock(self._sessions, identity, "model") as acquired:
            if not acquired:
                raise HTTPException(409, "A model request is already active")
            return await self._complete(identity, body)

    async def _complete(self, identity: str, body: dict[str, Any]) -> Response:
        async with self._sessions() as session:
            row = await self._row(session, identity)
            execution = _execution(row)
            meter = execution.meter
            ticket = await session.get(JiraWorkerTicket, row.ticket_id)
            config = self._config
            state = delivery_state(ticket) if ticket is not None else None
            if (
                meter is None
                or meter.pending
                or meter.uncertain
                or meter.stopped
                or row.status not in {"claimed", "running"}
                or row.cancel_requested_at
                or ticket is None
                or ticket.worker_parked_reason
                or ticket.agent_generation != row.generation
                or state is None
                or state.step != execution.contract.run.step
                or not state.approvals
                or not config.jira_worker_delivery_execution_enabled
                or ticket.project_key
                not in config.jira_worker_enabled_projects.get(ticket.instance, [])
                or ticket.project_key
                not in config.jira_worker_delivery_projects.get(ticket.instance, {})
                or body.get("model") != execution.pack.model
            ):
                raise HTTPException(
                    403, "Model execution is fenced or its usage is uncertain"
                )
            usage = meter.usage(execution.pack)
            used = sum(
                (
                    meter.input_tokens,
                    meter.output_tokens,
                    meter.cache_read_tokens,
                    meter.cache_write_tokens,
                )
            )
            remaining = min(
                execution.reservation.tokens - used,
                int(
                    (
                        execution.reservation.estimated_usd
                        - (usage.estimated_usd or Decimal("0"))
                    )
                    / meter.prices.maximum
                ),
            )
            allowed = {
                key: body[key]
                for key in ("model", "messages", "tools", "tool_choice", "temperature")
                if key in body and (key != "tools" or body[key])
            }
            prompt_bound = len(json.dumps(allowed, ensure_ascii=False).encode()) + 256
            requested = body.get("max_tokens", 4096)
            if type(requested) is not int or requested <= 0:
                raise HTTPException(400, "A positive max_tokens is required")
            output_limit = min(requested, 16384, remaining - prompt_bound)
            if output_limit <= 0:
                raise HTTPException(
                    429, "The execution reservation cannot cover this request"
                )
            allowed.update(stream=False, max_tokens=output_limit, n=1)
            _save(
                row,
                execution,
                pending=True,
                pending_since=datetime.now(UTC).isoformat(),
            )
            await session.commit()
        try:
            upstream = model_upstream(config, execution.pack)
            async with httpx.AsyncClient(timeout=120, trust_env=False) as client:
                response = await client.post(
                    upstream.base_url.rstrip("/") + "/chat/completions",
                    headers={
                        "Authorization": "Bearer " + upstream.api_key.get_secret_value()
                    },
                    json=allowed,
                    follow_redirects=False,
                )
                if response.is_redirect:
                    raise ValueError("Model redirects are forbidden")
                response.raise_for_status()
                result = response.json()
            counts = _counts(result)
            if result["model"] != execution.pack.model or not result["choices"]:
                raise ValueError(
                    "Provider changed the pinned model or omitted its result"
                )
            rendered = _response(result, body.get("stream") is True)
            async with self._sessions() as session:
                row = await self._row(session, identity)
                execution = _execution(row)
                assert execution.meter is not None
                current = execution.meter
                _save(
                    row,
                    execution,
                    pending=False,
                    pending_since=None,
                    input_tokens=current.input_tokens + counts[0],
                    output_tokens=current.output_tokens + counts[1],
                    cache_read_tokens=current.cache_read_tokens + counts[2],
                    cache_write_tokens=current.cache_write_tokens + counts[3],
                    uncertain=sum(counts) > prompt_bound + output_limit,
                )
                await session.commit()
            return rendered
        except BaseException as exc:
            # A committed hold survives server loss. Unknown upstream spend never retries.
            async with self._sessions() as session:
                row = await self._row(session, identity)
                _save(row, _execution(row), pending=False, uncertain=True)
                await session.commit()
            if not isinstance(exc, Exception):
                raise
            raise HTTPException(
                502, "Model outcome is uncertain; reservation retained"
            ) from None
