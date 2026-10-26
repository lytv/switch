"""Opt-in Bug supervision, from a Jira scope gate to one published draft."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any, Literal

from sqlalchemy import select

from switch_core.bridges.jira.contracts import (
    Approval,
    Contract,
    Gate,
    Job,
    Result,
    Run,
    digest,
)
from switch_core.bridges.jira.delivery_contracts import (
    ExecutionEnvelope,
    Handoff,
    ScopeContract,
    validate_candidate,
    validate_reproduction,
)
from switch_core.bridges.jira.observe import (
    Observation,
    _stamp,
    comment_actor,
    observe_jira,
)
from switch_core.bridges.jira.outbox import park_ticket
from switch_core.bridges.jira.publisher import delivery_lock
from switch_core.bridges.jira.state import DeliveryWorkflowState
from switch_core.bridges.jira.target import delivery_state
from switch_core.db.models import JiraWorkerJob, JiraWorkerTicket

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from switch_core.bridges.jira.activity import TicketActivity
    from switch_core.bridges.jira.firstmate_jobs import FirstmateJobs
    from switch_core.bridges.jira.publisher import DraftPublisher
    from switch_core.config import SwitchConfig

logger = logging.getLogger(__name__)


def replace_state(
    ticket: JiraWorkerTicket, state: DeliveryWorkflowState, **changes: Any
) -> None:
    ticket.workflow_state = DeliveryWorkflowState.model_validate_json(
        json.dumps(state.model_dump(mode="json") | changes)
    ).model_dump(mode="json")


def stage_scope(ticket: JiraWorkerTicket, scope: ScopeContract, gate: Gate) -> None:
    """Trusted admission supplies reviewed commands, profile content, and workflow proof."""
    state = delivery_state(ticket)
    if (
        state is None
        or state.service != "Bug"
        or state.step != "intake"
        or ticket.worker_parked_reason
        or scope.job.repository != state.repository
        or scope.job.pack_digest != state.pack.digest
        or scope.job.run.ticket_id != ticket.id
        or scope.job.run.instance != ticket.instance
        or scope.job.run.generation != ticket.agent_generation
        or gate.kind != "scope"
        or gate.run != scope.job.run
        or gate.contract_digest != digest(scope)
        or gate.transition_id != scope.approval_route.transition_id
        or gate.candidate_sha is not None
        or ticket.jira_read_updated is None
        or ticket.jira_read_changelog_id is None
    ):
        raise ValueError("Scope gate does not match the current verified Bug intake")
    if not any(
        item.jira_id == gate.transition_id for item in state.transitions
    ) or not {
        scope.approval_route.source_status_id,
        scope.approval_route.target_status_id,
    }.issubset({item.jira_id for item in state.statuses}):
        raise ValueError("Scope approval requires frozen transition identifiers")
    replace_state(
        ticket,
        state,
        scope=scope.model_dump(mode="json"),
        gate=gate.model_dump(mode="json"),
        step="approve_scope",
        gate_ready=False,
    )


class ScopeApproved(Contract):
    kind: Literal["scope_approved"] = "scope_approved"
    approval: Approval


class JobFinished(Contract):
    kind: Literal["job_finished"] = "job_finished"
    job: Job
    result: Result
    handoff: Handoff


def apply_event(ticket: JiraWorkerTicket, event: ScopeApproved | JobFinished) -> None:
    state = delivery_state(ticket)
    if state is None or state.scope is None or ticket.worker_parked_reason:
        raise ValueError("Delivery requires an active frozen scope")
    scope = state.scope
    if scope.job.run.generation != ticket.agent_generation:
        raise ValueError("Delivery event belongs to a stale generation")
    if isinstance(event, ScopeApproved):
        approval = event.approval
        if approval in state.approvals:
            return
        if state.step != "approve_scope" or approval.gate != state.gate:
            raise ValueError("Approval does not match the active scope gate")
        replace_state(
            ticket,
            state,
            approvals=[
                *(item.model_dump(mode="json") for item in state.approvals),
                approval.model_dump(mode="json"),
            ],
            gate=None,
            gate_ready=False,
            step="reproduce",
        )
        ticket.intake_state = {**(ticket.intake_state or {}), "phase": "delivery"}
        ticket.agent_state = "parked"
        ticket.wait_anchor = None
        ticket.wait_fired = None
        return
    job, result, handoff = event.job, event.result, event.handoff
    envelope = ExecutionEnvelope(
        scope=scope, reproduction=state.reproduction if job.run.step == "fix" else None
    )
    envelope.validate_job(job)
    if (
        state.step != job.run.step
        or result.run != job.run
        or result.input_digest != digest(job)
        or result.status != "completed"
        or result.usage.measurement != "complete"
        or state.unknown_spend
        or not state.approvals
    ):
        raise ValueError(
            "Job result lacks the current phase, approval, or complete usage"
        )
    if job.run.step == "reproduce":
        validate_reproduction(scope, handoff)
        replace_state(
            ticket, state, reproduction=handoff.model_dump(mode="json"), step="fix"
        )
    else:
        validate_candidate(scope, job, handoff)
        replace_state(
            ticket,
            state,
            candidate=handoff.model_dump(mode="json"),
            candidate_head=handoff.head,
        )


def observe_scope_approval(
    ticket: JiraWorkerTicket, version: dict[str, Any], observation: Observation
) -> Observation:
    """Status endpoints prove Approve only with a trusted unique-edge workflow receipt."""
    state = delivery_state(ticket)
    if (
        state is None
        or state.step != "approve_scope"
        or state.scope is None
        or state.gate is None
        or not state.gate_ready
        or ticket.worker_parked_reason
    ):
        return observation
    base = ticket.jira_read_changelog_id
    if base is None or ticket.jira_seen_comment_ids is None:
        return observation
    histories = version["histories"]
    index = next((i for i, item in enumerate(histories) if str(item["id"]) == base), -1)
    if base and index < 0:
        return observation
    humans = [
        item
        for item in histories[index + 1 :]
        if item.get("author", {}).get("accountId") != ticket.jira_worker_account_id
    ]
    if not humans:
        return observation
    edge = humans[0]
    route, gate = state.scope.approval_route, state.gate
    items = edge.get("items", [])
    actor = edge.get("author", {}).get("accountId")
    clean = (
        len(humans) == 1
        and actor in gate.approver_account_ids
        and actor != ticket.jira_worker_account_id
        and len(items) == 1
        and items[0].get("field") == "status"
        and items[0].get("from") == route.source_status_id
        and items[0].get("to") == route.target_status_id
        and version["fields"].get("status", {}).get("id") == route.target_status_id
        and version["fields"].get("status", {}).get("statusCategory", {}).get("key")
        != "done"
    )
    known = set(ticket.jira_seen_comment_ids)
    current = {str(item["id"]) for item in version["comments"]}
    dirty_comments = known - current or any(
        comment_actor(item) != ticket.jira_worker_account_id
        and (
            str(item["id"]) not in known
            or _stamp(item["updated"])
            > _stamp(ticket.jira_read_updated or ticket.last_event_at.isoformat())
        )
        for item in version["comments"]
    )
    if not clean or dirty_comments:
        park_ticket(ticket, "Dirty or unauthorized delivery approval history")
        return observation
    allowed = {
        **version,
        "histories": [
            {**item, "author": {"accountId": ticket.jira_worker_account_id}}
            if item is edge
            else item
            for item in histories
        ],
    }
    checked = observe_jira(
        allowed,
        seen_ids=known,
        changelog_id=base,
        base_stamp=_stamp(ticket.jira_read_updated or ticket.last_event_at.isoformat()),
        worker_account=ticket.jira_worker_account_id or "",
        reporter_account=ticket.reporter_account_id,
    )
    if checked.park:
        return observation
    apply_event(
        ticket,
        ScopeApproved(
            approval=Approval(
                gate=gate,
                actor_account_id=actor,
                actor_kind="human",
                history_id=str(edge["id"]),
            )
        ),
    )
    return checked


def fix_job(scope: ScopeContract) -> Job:
    data = scope.job.model_dump(mode="json")
    run = scope.job.run
    data["run"] = Run(
        instance=run.instance,
        ticket_id=run.ticket_id,
        generation=run.generation,
        step="fix",
        attempt=1,
        execution_key=f"{run.ticket_id}/{run.generation}/fix/1",
    ).model_dump(mode="json")
    return Job.model_validate_json(json.dumps(data))


class DeliverySupervisor:
    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        config: SwitchConfig,
        activity: TicketActivity,
        jobs: FirstmateJobs,
        publisher: DraftPublisher,
    ) -> None:
        self._sessions = session_factory
        self._config = config
        self._activity = activity
        self._jobs = jobs
        self._publisher = publisher

    async def run_once(self) -> None:
        async with self._sessions() as session:
            ids = list(
                (
                    await session.scalars(
                        select(JiraWorkerTicket.id).where(
                            JiraWorkerTicket.workflow_state["policy"].astext
                            == "delivery"
                        )
                    )
                ).all()
            )
        for ticket_id in ids:
            try:
                await self.reconcile(ticket_id)
            except ValueError as exc:
                async with self._sessions() as session:
                    ticket = await session.get(
                        JiraWorkerTicket, ticket_id, with_for_update=True
                    )
                    if ticket is not None and not ticket.worker_parked_reason:
                        park_ticket(ticket, str(exc))
                    await session.commit()
                logger.exception("Delivery evidence failed for %s", ticket_id)
            except Exception:
                logger.exception("Delivery reconciliation failed for %s", ticket_id)

    async def reconcile(self, ticket_id: str) -> None:
        async with delivery_lock(self._sessions, ticket_id, "execute") as acquired:
            if acquired:
                await self._reconcile(ticket_id)

    async def _reconcile(self, ticket_id: str) -> None:
        async with self._sessions() as session:
            ticket = await session.get(JiraWorkerTicket, ticket_id)
            if ticket is None:
                return
            active = list(
                (
                    await session.scalars(
                        select(JiraWorkerJob.execution_key).where(
                            JiraWorkerJob.ticket_id == ticket_id,
                            JiraWorkerJob.kind == "role_job",
                            JiraWorkerJob.status.in_(
                                ("claimed", "running", "uncertain", "cancel_requested")
                            ),
                        )
                    )
                ).all()
            )
            enabled = (
                self._config.jira_worker_delivery_execution_enabled
                and ticket.project_key
                in self._config.jira_worker_delivery_projects.get(ticket.instance, {})
                and ticket.project_key
                in self._config.jira_worker_enabled_projects.get(ticket.instance, [])
            )
            parked = bool(
                ticket.worker_parked_reason
            ) or ticket.status.strip().casefold() in {"done", "cancelled", "canceled"}
        if not enabled or parked:
            for key in active:
                assert key is not None
                await self._jobs.cancel(key)
            return
        if not await self._activity.read_ticket(ticket_id, force_jira=True):
            return
        async with self._sessions() as session:
            ticket = await session.get(JiraWorkerTicket, ticket_id)
            assert ticket is not None
            state = delivery_state(ticket)
            if ticket.worker_parked_reason:
                for key in active:
                    assert key is not None
                    await self._jobs.cancel(key)
                return
            if (
                state is None
                or state.scope is None
                or state.step not in {"reproduce", "fix"}
            ):
                return
            scope = state.scope
            if (
                state.publication is not None
                and state.publication.status == "published"
            ):
                return
            if state.candidate is not None:
                job = None
            else:
                job = scope.job if state.step == "reproduce" else fix_job(scope)
                envelope = ExecutionEnvelope(
                    scope=scope,
                    reproduction=state.reproduction if job.run.step == "fix" else None,
                )
                envelope.validate_job(job)
        if job is None:
            await self._publisher.publish(ticket_id)
            return
        view = await self._jobs.start(
            job,
            reservation=scope.reproduction_reservation
            if job.run.step == "reproduce"
            else scope.fix_reservation,
            estimate_seconds=scope.reproduction_estimate_seconds
            if job.run.step == "reproduce"
            else scope.fix_estimate_seconds,
            envelope=envelope,
        )
        if view.status != "done":
            return
        async with self._sessions() as session:
            ticket = await session.get(
                JiraWorkerTicket, ticket_id, with_for_update=True
            )
            assert ticket is not None
            try:
                if view.result is None or view.execution.handoff is None:
                    raise ValueError(
                        "Completed delivery job lacks its evidence handoff"
                    )
                apply_event(
                    ticket,
                    JobFinished(
                        job=job, result=view.result, handoff=view.execution.handoff
                    ),
                )
            except ValueError as exc:
                park_ticket(ticket, str(exc))
            await session.commit()
