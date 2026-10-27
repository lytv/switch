"""Audited budget extensions do not resume tasks or reconcile uncertain usage."""

from datetime import UTC, datetime
from typing import Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, model_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from switch_core.bridges.jira.accounting import accounting_lock
from switch_core.bridges.jira.contracts import (
    USD,
    Budget,
    Count,
    Positive,
    Text,
)
from switch_core.bridges.jira.target import delivery_state
from switch_core.config import SwitchConfig
from switch_core.db.models import JiraWorkerEvent, JiraWorkerTicket, User


class BudgetDelta(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    ticket_tokens: Count
    ticket_estimated_usd: USD
    day_estimated_usd: USD

    @model_validator(mode="after")
    def positive_extension(self) -> Self:
        if not any(
            (self.ticket_tokens, self.ticket_estimated_usd, self.day_estimated_usd)
        ):
            raise ValueError("Budget extension must increase at least one limit")
        return self


class BudgetRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    request_id: UUID
    reason: Text
    previous_generation: Positive
    delta: BudgetDelta


async def extend_budget(
    sessions: async_sessionmaker[AsyncSession],
    *,
    config: SwitchConfig,
    ticket_id: str,
    request: BudgetRequest,
    actor: str,
    now: datetime,
) -> JiraWorkerEvent:
    if now.tzinfo is None:
        raise ValueError("Budget time requires a timezone")
    async with sessions() as session:
        await accounting_lock(session)
        user = await session.get(User, actor)
        if user is None or user.role != "admin":
            raise PermissionError("Only an admin may extend a budget")
        ticket = await session.get(JiraWorkerTicket, ticket_id, with_for_update=True)
        if ticket is None:
            raise ValueError("ticket not found")
        if ticket.project_key not in config.jira_worker_enabled_projects.get(
            ticket.instance, []
        ):
            raise ValueError("project is not enabled")
        key = f"admin-budget:{request.request_id}"
        prior = await session.scalar(
            select(JiraWorkerEvent).where(
                JiraWorkerEvent.instance == ticket.instance,
                JiraWorkerEvent.idempotency_key == key,
            )
        )
        if prior is not None:
            payload = prior.payload or {}
            if (
                prior.issue_key != ticket.issue_key
                or payload.get("actor") != actor
                or payload.get("request") != request.model_dump(mode="json")
            ):
                raise ValueError("Budget request UUID was reused with different input")
            return prior
        state = delivery_state(ticket)
        if state is None or request.previous_generation != ticket.agent_generation:
            raise ValueError("Budget action requires the current delivery generation")
        old = state.budget
        new = Budget(
            ticket_tokens=old.ticket_tokens + request.delta.ticket_tokens,
            ticket_estimated_usd=old.ticket_estimated_usd
            + request.delta.ticket_estimated_usd,
            day_estimated_usd=old.day_estimated_usd,
        )
        ticket.workflow_state = {
            **state.model_dump(mode="json"),
            "budget_override": new.model_dump(mode="json"),
        }
        receipt = JiraWorkerEvent(
            instance=ticket.instance,
            issue_key=ticket.issue_key,
            project_key=ticket.project_key,
            idempotency_key=key,
            event_kind="admin_budget",
            webhook_event="worker:admin_budget",
            consumed=True,
            payload={
                "request": request.model_dump(mode="json"),
                "actor": actor,
                "request_id": str(request.request_id),
                "reason": request.reason,
                "previous_generation": ticket.agent_generation,
                "delta": request.delta.model_dump(mode="json"),
                "day": now.astimezone(UTC).date().isoformat(),
                "created_at": now.isoformat(),
                "previous_budget": old.model_dump(mode="json"),
                "budget": new.model_dump(mode="json"),
                "tokens_used": ticket.tokens_used,
                "usage_unknown": state.unknown_spend,
            },
        )
        session.add(receipt)
        await session.commit()
        return receipt
