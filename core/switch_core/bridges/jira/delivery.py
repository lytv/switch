"""Opt-in Bug supervision, from a Jira scope gate to one published draft."""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Literal, cast

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
    ExactTransition,
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
from switch_core.bridges.jira.outbox import enqueue_jira_command, park_ticket
from switch_core.bridges.jira.publisher import delivery_lock
from switch_core.bridges.jira.state import DeliveryWorkflowState
from switch_core.bridges.jira.target import delivery_state
from switch_core.bridges.jira.verification import validate_verification
from switch_core.db.models import JiraWorkerJob, JiraWorkerOutbox, JiraWorkerTicket

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


class CandidateAccepted(Contract):
    kind: Literal["candidate_accepted"] = "candidate_accepted"
    approval: Approval


class CandidateSummary(Contract):
    scope_digest: str
    head: str
    request_url: str
    review: Handoff
    qa: Handoff


class JobFinished(Contract):
    kind: Literal["job_finished"] = "job_finished"
    job: Job
    result: Result
    handoff: Handoff


def apply_event(
    ticket: JiraWorkerTicket, event: ScopeApproved | CandidateAccepted | JobFinished
) -> None:
    state = delivery_state(ticket)
    if state is None or state.scope is None or ticket.worker_parked_reason:
        raise ValueError("Delivery requires an active frozen scope")
    scope = state.scope
    if scope.job.run.generation != ticket.agent_generation:
        raise ValueError("Delivery event belongs to a stale generation")
    if isinstance(event, (ScopeApproved, CandidateAccepted)):
        approval = event.approval
        if approval in state.approvals:
            return
        acceptance = isinstance(event, CandidateAccepted)
        if (
            state.step != ("accept" if acceptance else "approve_scope")
            or approval.gate != state.gate
            or approval.gate.kind != ("acceptance" if acceptance else "scope")
            or not state.gate_ready
            or (
                acceptance
                and (
                    state.candidate_head != approval.gate.candidate_sha
                    or state.review_head != state.candidate_head
                    or state.qa_head != state.candidate_head
                    or state.publication is None
                    or state.publication.readiness != "ready"
                )
            )
        ):
            raise ValueError("Approval does not match the active tested gate")
        replace_state(
            ticket,
            state,
            approvals=[
                *(item.model_dump(mode="json") for item in state.approvals),
                approval.model_dump(mode="json"),
            ],
            gate=None,
            gate_ready=False,
            step="merge" if acceptance else "reproduce",
            accepted_head=state.candidate_head if acceptance else None,
        )
        ticket.intake_state = {**(ticket.intake_state or {}), "phase": "delivery"}
        ticket.agent_state = "parked"
        ticket.wait_anchor = None
        ticket.wait_fired = None
        return
    job, result, handoff = event.job, event.result, event.handoff
    envelope = ExecutionEnvelope(
        scope=scope,
        reproduction=state.reproduction if job.run.step != "reproduce" else None,
        candidate=state.candidate
        if job.run.step in {"review", "qa", "smoke"}
        else None,
        target_sha=state.merge_sha if job.run.step == "smoke" else state.candidate_head,
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
    elif job.run.step == "fix":
        validate_candidate(scope, job, handoff)
        replace_state(
            ticket,
            state,
            candidate=handoff.model_dump(mode="json"),
            candidate_head=handoff.head,
            candidate_job=job.model_dump(mode="json"),
        )
    else:
        target = state.merge_sha if job.run.step == "smoke" else state.candidate_head
        if target is None or job.run.attempt != state.attempts.get(job.run.step, 1):
            raise ValueError("Verification result belongs to a stale candidate attempt")
        validate_verification(scope, job, handoff, target)
        if handoff.verdict != "PASS":
            if handoff.verdict == "BLOCKED":
                raise ValueError(
                    "Verification environment is unsupported; no additional access is authorized"
                )
            if job.run.step == "smoke":
                raise ValueError("Merged-code smoke did not pass; closure is forbidden")
            attempts = dict(state.attempts)
            if state.rework_count >= 2 or any(
                attempts.get(step, 1) >= 3 for step in ("fix", "review", "qa")
            ):
                raise ValueError("Approved rework or attempt policy is exhausted")
            for step in ("fix", "review", "qa"):
                attempts[step] = attempts.get(step, 1) + 1
            replace_state(
                ticket,
                state,
                step="fix",
                attempts=attempts,
                rework_count=state.rework_count + 1,
                feedback=handoff.model_dump(mode="json"),
                review=None,
                qa=None,
                review_head=None,
                qa_head=None,
                accepted_head=None,
                gate=None,
                gate_ready=False,
                merge_sha=None,
                merge_receipt=None,
                smoke=None,
            )
            return
        if job.run.step == "review":
            replace_state(
                ticket,
                state,
                review=handoff.model_dump(mode="json"),
                review_head=target,
                step="qa",
            )
        elif job.run.step == "qa":
            replace_state(
                ticket,
                state,
                qa=handoff.model_dump(mode="json"),
                qa_head=target,
                step="accept",
            )
        else:
            if (
                state.accepted_head != state.candidate_head
                or state.merge_receipt is None
                or state.merge_receipt.outcome != "merged"
                or state.merge_receipt.merged_sha != target
            ):
                raise ValueError("Smoke lacks acceptance and the human merge receipt")
            replace_state(
                ticket, state, smoke=handoff.model_dump(mode="json"), step="close"
            )


def observe_scope_approval(
    ticket: JiraWorkerTicket, version: dict[str, Any], observation: Observation
) -> Observation:
    """Status endpoints prove Approve only with a trusted unique-edge workflow receipt."""
    state = delivery_state(ticket)
    if (
        state is None
        or state.step not in {"approve_scope", "accept"}
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
        (CandidateAccepted if state.step == "accept" else ScopeApproved)(
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
    return role_job(scope, "fix", 1)


def role_job(
    scope: ScopeContract,
    step: Literal["reproduce", "fix", "review", "qa", "smoke"],
    attempt: int,
) -> Job:
    data = scope.job.model_dump(mode="json")
    run = scope.job.run
    data["run"] = Run(
        instance=run.instance,
        ticket_id=run.ticket_id,
        generation=run.generation,
        step=step,
        attempt=attempt,
        execution_key=f"{run.ticket_id}/{run.generation}/{step}/{attempt}",
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
        clock: Callable[[], datetime],
    ) -> None:
        self._sessions = session_factory
        self._config = config
        self._activity = activity
        self._jobs = jobs
        self._publisher = publisher
        self._clock = clock

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
                try:
                    await self._reconcile(ticket_id)
                except ValueError as exc:
                    async with self._sessions() as session:
                        ticket = await session.get(
                            JiraWorkerTicket, ticket_id, with_for_update=True
                        )
                        if ticket is not None and not ticket.worker_parked_reason:
                            state = delivery_state(ticket)
                            if state is not None:
                                replace_state(
                                    ticket,
                                    state,
                                    review_head=None,
                                    qa_head=None,
                                    accepted_head=None,
                                    gate=None,
                                    gate_ready=False,
                                    merge_sha=None,
                                    merge_receipt=None,
                                    smoke=None,
                                )
                            park_ticket(ticket, str(exc))
                        await session.commit()
                    await self._cancel_active(ticket_id)

    async def _cancel_active(self, ticket_id: str) -> None:
        async with self._sessions() as session:
            keys = (
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
        for key in keys:
            assert key is not None
            await self._jobs.cancel(key)

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
                or state.step in {"intake", "approve_scope", "manual_handoff"}
            ):
                return
            scope = state.scope
            if (
                state.step == "fix"
                and state.candidate is not None
                and (
                    state.candidate_job is None
                    or state.candidate_job.run.attempt == state.attempts.get("fix", 1)
                )
            ):
                if (
                    state.publication is None
                    or state.publication.status != "published"
                    or state.publication.head != state.candidate_head
                ):
                    publish = True
                elif scope.verification is None:
                    return
                else:
                    replace_state(ticket, state, step="review")
                    await session.commit()
                    return
            else:
                publish = False
            if not publish and state.step in {
                "review",
                "qa",
                "accept",
                "merge",
                "smoke",
                "close",
            }:
                _, receipt = await self._publisher.inspect(ticket)
                if state.step == "merge":
                    if receipt is None or receipt.outcome != "merged":
                        return
                    replace_state(
                        ticket,
                        state,
                        step="smoke",
                        merge_sha=receipt.merged_sha,
                        merge_receipt=receipt.model_dump(mode="json"),
                    )
                    await session.commit()
                    return
                if state.step in {"smoke", "close"} and (
                    receipt is None
                    or receipt.outcome != "merged"
                    or receipt != state.merge_receipt
                ):
                    raise ValueError("Merged-code evidence changed or is inconclusive")
            step = state.step
            if not publish and step not in {"accept", "close"}:
                job = role_job(
                    scope,
                    cast(Literal["reproduce", "fix", "review", "qa", "smoke"], step),
                    state.attempts.get(step, 1),
                )
                envelope = ExecutionEnvelope(
                    scope=scope,
                    reproduction=state.reproduction if step != "reproduce" else None,
                    candidate=state.candidate,
                    target_sha=state.merge_sha
                    if step == "smoke"
                    else state.candidate_head,
                    feedback=state.feedback if step == "fix" else None,
                )
                envelope.validate_job(job)
        if publish:
            await self._publisher.publish(ticket_id)
            return
        if step == "accept":
            if await self._publisher.make_ready(ticket_id):
                await self._milestone(ticket_id, closing=False)
            return
        if step == "close":
            await self._milestone(ticket_id, closing=True)
            return
        verification = (
            getattr(scope.verification, step)
            if step in {"review", "qa", "smoke"} and scope.verification
            else None
        )
        view = await self._jobs.start(
            job,
            reservation=verification.reservation
            if verification
            else scope.reproduction_reservation
            if step == "reproduce"
            else scope.fix_reservation,
            estimate_seconds=verification.estimate_seconds
            if verification
            else scope.reproduction_estimate_seconds
            if step == "reproduce"
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

    async def _milestone(self, ticket_id: str, *, closing: bool) -> None:
        async with self._sessions() as session:
            ticket = await session.get(
                JiraWorkerTicket, ticket_id, with_for_update=True
            )
            assert ticket is not None
            state = delivery_state(ticket)
            if (
                state is None
                or state.scope is None
                or state.publication is None
                or state.scope.verification is None
            ):
                raise ValueError("Milestone lacks the frozen verification contract")
            if (
                ticket.worker_parked_reason
                or state.unknown_spend
                or state.scope.job.run.generation != ticket.agent_generation
            ):
                raise ValueError("Milestone authorization changed")
            live = await session.scalar(
                select(JiraWorkerJob.id)
                .where(
                    JiraWorkerJob.ticket_id == ticket_id,
                    JiraWorkerJob.kind == "role_job",
                    JiraWorkerJob.status.in_(
                        ("claimed", "running", "uncertain", "cancel_requested")
                    ),
                )
                .limit(1)
            )
            if live is not None:
                raise ValueError(
                    "Human waits and completion require every role to exit"
                )
            await self._publisher.inspect(ticket)
            if closing:
                if (
                    not await self._publisher.authorize_command(ticket)
                    or state.accepted_head != state.candidate_head
                ):
                    raise ValueError(
                        "Completion requires acceptance, human merge and merged-SHA smoke"
                    )
                body = f"Accepted candidate: {state.accepted_head}\nRequest: {state.publication.pr_url}\nHuman merge: {state.merge_sha}\nMerged-code smoke: PASS ({digest(state.smoke) if state.smoke else ''})\nDone means accepted and merged, not deployed. No deployment occurred."
                transition: str | None = state.scope.verification.complete_transition_id
                destination = next(
                    (
                        item.name.split("->")[1]
                        for item in state.transitions
                        if item.jira_id == transition
                        and item.name.startswith(
                            state.scope.approval_route.target_status_id + "->"
                        )
                    ),
                    None,
                )
                if destination is None:
                    raise ValueError("Frozen Complete delivery transition is missing")
            else:
                if (
                    state.review is None
                    or state.qa is None
                    or state.candidate_head is None
                    or state.publication.pr_url is None
                    or state.publication.readiness != "ready"
                ):
                    raise ValueError("Acceptance requires the tested ready candidate")
                summary = CandidateSummary(
                    scope_digest=digest(state.scope),
                    head=state.candidate_head,
                    request_url=state.publication.pr_url,
                    review=state.review,
                    qa=state.qa,
                )
                if state.gate is None:
                    gate = Gate(
                        run=state.candidate_job.run
                        if state.candidate_job
                        else fix_job(state.scope).run,
                        id=f"accept:{ticket_id}:{ticket.agent_generation}:{state.candidate_head}",
                        kind="acceptance",
                        contract_digest=digest(summary),
                        transition_id=state.scope.approval_route.transition_id,
                        candidate_sha=state.candidate_head,
                        approver_account_ids=state.approvals[
                            0
                        ].gate.approver_account_ids,
                    )
                    replace_state(
                        ticket,
                        state,
                        gate=gate.model_dump(mode="json"),
                        gate_ready=False,
                    )
                    state = delivery_state(ticket)
                assert (
                    state is not None
                    and state.scope is not None
                    and state.publication is not None
                )
                body = f"Delivery summary\nRepository: {state.scope.job.repository.canonical_url}\nTested candidate: {state.candidate_head}\nRequest: {state.publication.pr_url}\nReview: PASS ({digest(state.review) if state.review else ''})\nQA: PASS, original failure repeated at the approved base; acceptance checked at the candidate ({digest(state.qa) if state.qa else ''})\nAcceptance: {json.dumps(state.scope.job.acceptance)}\nSummary digest: {digest(summary)}\nAccept this exact candidate in Jira. Acceptance does not merge code."
                destination = state.scope.approval_route.source_status_id
                transition = next(
                    (
                        item.jira_id
                        for item in state.transitions
                        if item.name
                        == f"{state.scope.approval_route.target_status_id}->{destination}"
                    ),
                    None,
                )
                if transition is None:
                    raise ValueError("Frozen Request approval transition is missing")
            assert state.scope is not None and transition is not None
            target_name = next(
                (item.name for item in state.statuses if item.jira_id == destination),
                None,
            )
            if (
                target_name is None
                or ticket.jira_read_updated is None
                or ticket.jira_read_changelog_id is None
            ):
                raise ValueError(
                    "Milestone lacks frozen workflow IDs or a Jira baseline"
                )
            prefix = f"delivery:{ticket_id}:{ticket.agent_generation}:{state.step}:{state.candidate_head}"
            existing = await session.scalar(
                select(JiraWorkerOutbox).where(
                    JiraWorkerOutbox.instance == ticket.instance,
                    JiraWorkerOutbox.command_key == f"{prefix}:transition",
                )
            )
            if existing is not None:
                if (existing.payload or {}).get(
                    "candidate_sha"
                ) != state.candidate_head or (existing.payload or {}).get(
                    "scope_digest"
                ) != digest(state.scope):
                    raise ValueError(
                        "Existing milestone differs from the frozen candidate"
                    )
                return
            predecessor = None
            exact = ExactTransition(
                generation=ticket.agent_generation,
                pack_digest=state.pack.digest,
                step=state.step,
                gate_id=state.gate.id if state.gate else None,
                candidate_sha=state.candidate_head,
                transition_id=transition,
                source_status_id=state.scope.approval_route.target_status_id,
                target_status_id=destination,
                target_status_name=target_name,
            )
            for suffix, command, data in (
                ("summary", "comment", body),
                ("transition", "transition_by_id", exact.model_dump(mode="json")),
            ):
                row_id = await enqueue_jira_command(
                    session,
                    config=self._config,
                    ticket=ticket,
                    command_key=f"{prefix}:{suffix}",
                    command=command,
                    data=data,
                    based_updated=ticket.jira_read_updated,
                    based_changelog_id=ticket.jira_read_changelog_id,
                )
                if row_id is None:
                    raise ValueError("Delivery milestone admission is disabled")
                row = await session.get(JiraWorkerOutbox, row_id)
                assert row is not None
                row.due_at = self._clock()
                row.created_at = self._clock()
                row.payload = {
                    **(row.payload or {}),
                    "intake_generation": ticket.agent_generation,
                    "scope_digest": digest(state.scope),
                    "delivery_step": state.step,
                    "candidate_sha": state.candidate_head,
                    "after_command": predecessor,
                    "gate_digest": state.gate.contract_digest if state.gate else None,
                }
                if predecessor is not None:
                    previous = await session.get(JiraWorkerOutbox, predecessor)
                    assert previous is not None
                    row.created_at = previous.created_at + timedelta(microseconds=1)
                predecessor = row_id
            await session.commit()
