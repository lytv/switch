"""Configuration-driven delivery intake and trusted triage result storage."""

from __future__ import annotations

import json
import re
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from switch_core.bridges.jira.contracts import Repository
from switch_core.bridges.jira.model_upstream import model_prices
from switch_core.bridges.jira.outbox import park_ticket
from switch_core.bridges.jira.packs import load_pack
from switch_core.bridges.jira.parse import _plain_text, _text
from switch_core.bridges.jira.state import DeliveryWorkflowState, workflow_state_adapter
from switch_core.config import SwitchConfig
from switch_core.db.models import JiraWorkerEvent, JiraWorkerTicket

UNACCEPTED_SERVICE = (
    "Delivery intake requires a mapped Bug, Feature, Docs or Other Service"
)


def ticket_repository_url(fields: dict[str, Any], field: str) -> str:
    value = fields.get(field)
    if field != "description":
        return value.strip() if isinstance(value, str) else ""
    matches = re.findall(
        r"^\s*Repository URL:\s*(\S+)\s*$",
        _plain_text(value),
        flags=re.MULTILINE | re.IGNORECASE,
    )
    return matches[0] if len(matches) == 1 else ""


def delivery_state(ticket: JiraWorkerTicket) -> DeliveryWorkflowState | None:
    state = workflow_state_adapter.validate_json(json.dumps(ticket.workflow_state))
    return state if isinstance(state, DeliveryWorkflowState) else None


def initialize_target(
    ticket: JiraWorkerTicket, fields: dict[str, Any], config: SwitchConfig
) -> None:
    mapping = config.jira_worker_delivery_projects.get(ticket.instance, {}).get(
        ticket.project_key
    )
    if mapping is None:
        return
    service = _text(fields.get(mapping.service_field)).strip()
    if service not in {"Bug", "Feature", "Docs", "Other"}:
        if ticket.worker_parked_reason != UNACCEPTED_SERVICE:
            park_ticket(ticket, UNACCEPTED_SERVICE)
        return
    supplied_url = ticket_repository_url(fields, mapping.repository_url_field)
    repository = (
        Repository(
            supplied_url=supplied_url,
            outcome="unknown",
            canonical_url=None,
            base_branch=None,
            base_sha=None,
            evidence="Ticket URL awaits trusted repository triage",
        )
        if supplied_url
        else None
    )
    pack = load_pack(config)
    prices = (
        model_prices(config, pack.snapshot)
        if not pack.snapshot.fixture_only
        and (
            config.jira_worker_delivery_runtime is not None
            or config.jira_worker_delivery_upstreams
        )
        else None
    )
    state = DeliveryWorkflowState.model_validate_json(
        json.dumps(
            {
                "pack": pack.model_dump(mode="json"),
                "model_prices": prices.model_dump(mode="json") if prices else None,
                "service": service,
                "service_field": mapping.service_field,
                "repository_url_field": mapping.repository_url_field,
                "repository": repository.model_dump(mode="json")
                if repository
                else None,
                "statuses": [],
                "transitions": [],
            }
        )
    )
    ticket.service_value = service
    ticket.workflow_state = state.model_dump(mode="json")


def reset_delivery_scope(
    ticket: JiraWorkerTicket,
    state: DeliveryWorkflowState,
    *,
    service: str,
    supplied_url: str,
) -> None:
    """Retain cumulative accounting and frozen policy, but require new triage and approval."""
    data = {
        key: state.model_dump(mode="json")[key]
        for key in (
            "pack",
            "model_prices",
            "service_field",
            "repository_url_field",
            "statuses",
            "transitions",
            "reserved_tokens",
            "reserved_estimated_usd",
            "known_estimated_usd",
            "unknown_spend",
            "budget_override",
        )
    }
    data.update(
        service=service,
        repository=Repository(
            supplied_url=supplied_url,
            outcome="unknown",
            canonical_url=None,
            base_branch=None,
            base_sha=None,
            evidence="Fresh repository triage required before scope approval",
        ).model_dump(mode="json")
        if supplied_url
        else None,
    )
    ticket.workflow_state = DeliveryWorkflowState.model_validate_json(
        json.dumps(data)
    ).model_dump(mode="json")
    ticket.service_value = service
    ticket.agent_generation += 1
    intake = ticket.intake_state or {}
    ticket.intake_state = {
        "phase": "intake",
        "cycle": intake.get("cycle", 0) + 1,
        "questions": 0,
        "history": intake.get("history", []),
    }
    ticket.agent_state = "wake_requested"
    ticket.wait_anchor = None
    ticket.wait_fired = None
    ticket.wake_at = None


def check_target_fields(
    ticket: JiraWorkerTicket, fields: dict[str, Any], *, complete: bool
) -> None:
    state = delivery_state(ticket)
    if state is None or ticket.worker_parked_reason:
        return
    service_changed = (
        state.service_field is not None
        and (complete or state.service_field in fields)
        and _text(fields.get(state.service_field)).strip() != state.service
    )
    supplied_url = state.repository.supplied_url if state.repository else ""
    repository_changed = (
        state.service == "Bug"
        and (complete or state.repository_url_field in fields)
        and ticket_repository_url(fields, state.repository_url_field) != supplied_url
    )
    if service_changed or repository_changed:
        park_ticket(
            ticket, "Ticket Service or repository URL changed; target triage required"
        )


async def record_repository_triage(
    session: AsyncSession, *, ticket_id: str, generation: int, repository: Repository
) -> None:
    """Accept a trusted triage result, never a ticket/model claim of verification."""
    ticket = await session.get(
        JiraWorkerTicket, ticket_id, with_for_update=True, populate_existing=True
    )
    if ticket is None:
        raise ValueError("Triage ticket does not exist")
    state = delivery_state(ticket)
    if (
        state is None
        or state.service != "Bug"
        or state.step != "intake"
        or ticket.worker_parked_reason
        or ticket.agent_generation != generation
    ):
        raise ValueError("Repository triage does not match the active Bug intake")
    if (
        state.repository is None
        or repository.supplied_url != state.repository.supplied_url
    ):
        raise ValueError("Triage repository does not match the ticket URL")
    if state.repository.outcome == "verified":
        if repository != state.repository:
            raise ValueError("Verified repository target is frozen")
        return
    data = state.model_dump(mode="json")
    data["repository"] = repository.model_dump(mode="json")
    ticket.workflow_state = DeliveryWorkflowState.model_validate_json(
        json.dumps(data)
    ).model_dump(mode="json")
    intake = dict(ticket.intake_state or {})
    if repository.outcome == "verified" and intake.get("phase") == "triage":
        ticket.intake_state = {**intake, "phase": "scope_ready"}
        session.add(
            JiraWorkerEvent(
                instance=ticket.instance,
                issue_key=ticket.issue_key,
                project_key=ticket.project_key,
                idempotency_key=f"repository-verified:{ticket.id}:{generation}",
                event_kind="explicit_input",
                webhook_event="worker:repository_verified",
                payload={"reporter_input": True},
            )
        )
        if ticket.agent_state != "live":
            ticket.agent_state = "wake_requested"
    await session.flush()
