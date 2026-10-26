"""Durable role executions in the existing Jira job table, not a second queue.

Calls return after short Firstmate start/get/cancel operations, never after
model completion.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from decimal import Decimal
from typing import Literal, Protocol

from pydantic import Field
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from switch_core.bridges.jira.accounting import (
    LIVE_ROLES,
    CapacityUnavailable,
    accounting_lock,
    day_cost,
    day_limit,
    live_role_sessions,
    live_sessions,
)
from switch_core.bridges.jira.admin import _record_reason_alerts
from switch_core.bridges.jira.contracts import (
    USD,
    Contract,
    Count,
    Job,
    Pack,
    Positive,
    Reservation,
    Result,
    Text,
    digest,
)
from switch_core.bridges.jira.delivery_contracts import ExecutionEnvelope, Handoff
from switch_core.bridges.jira.firstmate_copy import SecretGuard, TaskObservation
from switch_core.bridges.jira.state import DeliveryWorkflowState, workflow_state_adapter
from switch_core.db.models import JiraWorkerJob, JiraWorkerTicket

_TERMINAL = {"done", "cancelled", "waiting_input", "error"}


class DeliveryCopy(Protocol):
    async def start(
        self,
        job: Job,
        pack: Pack,
        reservation: Reservation,
        estimate_seconds: int,
        *,
        envelope: ExecutionEnvelope | None = None,
    ) -> TaskObservation: ...

    async def get(self, job: Job) -> TaskObservation: ...

    async def cancel(self, job: Job) -> None: ...


class Execution(Contract):
    contract: Job
    envelope: ExecutionEnvelope | None = None
    handoff: Handoff | None = None
    pack: Pack
    reservation: Reservation
    estimate_seconds: Positive
    started_at: Text
    held_tokens: Count
    held_usd: USD
    known_tokens: Count = 0
    known_usd: USD = Decimal("0")
    cost_by_day: dict[str, USD] = Field(default_factory=dict)
    usage_unknown: bool = True
    exit_confirmed: bool = False
    progress: Handoff | None = None
    progress_signatures: tuple[tuple[str, str, int], ...] = ()
    prior_active_seconds: Count = 0
    active_seconds: Count = 0
    rejection: Literal["cancelled", "stale_generation", "parked"] | None = None


class ExecutionView(Contract):
    execution_key: Text
    status: Text
    result: Result | None
    execution: Execution


def _execution(row: JiraWorkerJob) -> Execution:
    return Execution.model_validate_json(json.dumps(row.payload))


def _state(ticket: JiraWorkerTicket) -> DeliveryWorkflowState:
    state = workflow_state_adapter.validate_json(json.dumps(ticket.workflow_state))
    if not isinstance(state, DeliveryWorkflowState):
        raise ValueError("Task accounting requires a frozen delivery contract")
    return state


def _replace[T: Contract](value: T, **changes: object) -> T:
    return type(value).model_validate_json(
        json.dumps(value.model_dump(mode="json") | changes, default=str)
    )


def _view(row: JiraWorkerJob) -> ExecutionView:
    return ExecutionView(
        execution_key=_execution(row).contract.run.execution_key,
        status=row.status,
        result=Result.model_validate_json(json.dumps(row.result))
        if row.result is not None
        else None,
        execution=_execution(row),
    )


def _reason(ticket: JiraWorkerTicket, key: str, detail: str) -> None:
    ticket.admin_reasons = dict(ticket.admin_reasons or {}) | {key: detail}


class FirstmateJobs:
    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        delivery_copy: DeliveryCopy,
        secrets: SecretGuard,
        clock: Callable[[], datetime],
    ) -> None:
        self._sessions = session_factory
        self._copy = delivery_copy
        self._secrets = secrets
        self._clock = clock

    @asynccontextmanager
    async def _execution_lock(self, key: str) -> AsyncIterator[None]:
        # Session locks do not hold ticket rows during a process-boundary call.
        while True:
            async with self._sessions() as session:
                acquired = await session.scalar(
                    text("SELECT pg_try_advisory_lock(hashtextextended(:key, 0))"),
                    {"key": "jira-firstmate:" + key},
                )
                if acquired:
                    try:
                        yield
                    finally:
                        await session.execute(
                            text(
                                "SELECT pg_advisory_unlock(hashtextextended(:key, 0))"
                            ),
                            {"key": "jira-firstmate:" + key},
                        )
                    return
            await asyncio.sleep(0.05)

    async def _budget_lock(self, session: AsyncSession) -> None:
        await accounting_lock(session)

    async def _row(self, session: AsyncSession, key: str) -> JiraWorkerJob:
        row = await session.scalar(
            select(JiraWorkerJob)
            .where(JiraWorkerJob.execution_key == key, JiraWorkerJob.kind == "role_job")
            .with_for_update()
        )
        if row is None:
            raise ValueError("Unknown Firstmate execution key")
        return row

    async def _ticket(
        self, session: AsyncSession, row: JiraWorkerJob
    ) -> JiraWorkerTicket:
        ticket = await session.get(
            JiraWorkerTicket, row.ticket_id, with_for_update=True
        )
        if ticket is None:
            raise ValueError("Execution ticket no longer exists")
        return ticket

    async def _day_cost(self, session: AsyncSession) -> Decimal:
        return await day_cost(session, self._clock())

    async def _reserve(
        self,
        job: Job,
        reservation: Reservation,
        estimate_seconds: int,
        envelope: ExecutionEnvelope | None,
    ) -> None:
        self._secrets.check(job.model_dump_json())
        if envelope is not None:
            envelope.validate_job(job)
            self._secrets.check(envelope.model_dump_json())
        self._secrets.check(reservation.model_dump_json())
        now = self._clock()
        if now.tzinfo is None:
            raise ValueError("Execution clock requires a timezone")
        async with self._sessions() as session:
            await self._budget_lock(session)
            existing = await session.scalar(
                select(JiraWorkerJob).where(
                    JiraWorkerJob.execution_key == job.run.execution_key
                )
            )
            if existing is not None:
                saved = _execution(existing)
                if (
                    saved.contract,
                    saved.reservation,
                    saved.estimate_seconds,
                    saved.envelope,
                ) != (
                    job,
                    reservation,
                    estimate_seconds,
                    envelope,
                ):
                    raise ValueError("Execution key was reused with different input")
                return
            ticket = await session.get(
                JiraWorkerTicket, job.run.ticket_id, with_for_update=True
            )
            if ticket is None:
                raise ValueError("Unknown delivery ticket")
            state = _state(ticket)
            if (
                state.service != "Bug"
                or ticket.instance != job.run.instance
                or ticket.agent_generation != job.run.generation
                or ticket.worker_parked_reason is not None
                or state.pack.digest != job.pack_digest
                or state.repository != job.repository
                or state.step != job.run.step
            ):
                raise ValueError("Job does not match the current frozen ticket fence")
            if state.scope is not None and (
                envelope is None
                or envelope.scope != state.scope
                or not state.approvals
                or state.unknown_spend
                or job.run.attempt != state.attempts.get(job.run.step, 1)
                or job.run.input_revision
                != state.input_revisions.get(f"{job.run.step}/{job.run.attempt}", 0)
                or envelope.resume_answer
                != (state.answer if job.run.input_revision else None)
                or envelope.reproduction_job != state.reproduction_job
                or (state.question is not None and state.answer is None)
                or (
                    job.run.step != "reproduce"
                    and envelope.reproduction != state.reproduction
                )
                or (
                    job.run.step in {"review", "qa", "smoke"}
                    and (
                        envelope.candidate != state.candidate
                        or envelope.target_sha
                        != (
                            state.merge_sha
                            if job.run.step == "smoke"
                            else state.candidate_head
                        )
                    )
                )
            ):
                raise ValueError(
                    "Execution envelope lacks the frozen approval or reproduction"
                )
            roles = await live_role_sessions(session)
            same_ticket = await session.scalar(
                select(JiraWorkerJob.id)
                .where(
                    JiraWorkerJob.kind == "role_job",
                    JiraWorkerJob.ticket_id == ticket.id,
                    JiraWorkerJob.status.in_(LIVE_ROLES),
                )
                .limit(1)
            )
            if (
                roles >= 2
                or same_ticket is not None
                or await live_sessions(session) >= 5
            ):
                ticket.queue_reason = "Delivery capacity: waiting for a model session"
                await session.commit()
                raise CapacityUnavailable(ticket.queue_reason)
            ticket.queue_reason = None
            budget = state.budget
            if (
                ticket.tokens_used + state.reserved_tokens + reservation.tokens
                > budget.ticket_tokens
                or state.known_estimated_usd
                + state.reserved_estimated_usd
                + reservation.estimated_usd
                > budget.ticket_estimated_usd
                or await self._day_cost(session) + reservation.estimated_usd
                > await day_limit(session, now, budget.day_estimated_usd)
            ):
                raise ValueError(
                    "Delivery usage reservation exceeds the approved budget"
                )
            prior_seconds = 0
            if envelope is not None and envelope.resume_answer is not None:
                prior = await session.scalar(
                    select(JiraWorkerJob).where(
                        JiraWorkerJob.execution_key
                        == envelope.resume_answer.question.run.execution_key
                    )
                )
                if (
                    prior is None
                    or prior.status != "done"
                    or not _execution(prior).exit_confirmed
                ):
                    raise ValueError(
                        "Checkpoint resume requires the exited prior execution"
                    )
                prior_seconds = _execution(prior).active_seconds
            execution = Execution(
                contract=job,
                prior_active_seconds=prior_seconds,
                envelope=envelope,
                pack=state.pack.snapshot,
                reservation=reservation,
                estimate_seconds=estimate_seconds,
                started_at=now.isoformat(),
                held_tokens=reservation.tokens,
                held_usd=reservation.estimated_usd,
            )
            session.add(
                JiraWorkerJob(
                    kind="role_job",
                    instance=ticket.instance,
                    project_key=ticket.project_key,
                    ticket_id=ticket.id,
                    generation=job.run.generation,
                    execution_key=job.run.execution_key,
                    due_at=now,
                    last_progress_at=now,
                    status="claimed",
                    payload=execution.model_dump(mode="json"),
                )
            )
            ticket.workflow_state = _replace(
                state,
                reserved_tokens=state.reserved_tokens + reservation.tokens,
                reserved_estimated_usd=state.reserved_estimated_usd
                + reservation.estimated_usd,
                unknown_spend=True,
            ).model_dump(mode="json")
            await session.commit()

    async def _snapshot(self, key: str) -> tuple[ExecutionView, Pack]:
        async with self._sessions() as session:
            row = await self._row(session, key)
            return _view(row), _execution(row).pack

    async def start(
        self,
        job: Job,
        *,
        reservation: Reservation,
        estimate_seconds: int,
        envelope: ExecutionEnvelope | None = None,
    ) -> ExecutionView:
        async with self._execution_lock(job.run.execution_key):
            await self._reserve(job, reservation, estimate_seconds, envelope)
            current, pack = await self._snapshot(job.run.execution_key)
            if current.status not in _TERMINAL:
                # The durable row and reservation precede every external operation.
                async with self._sessions() as session:
                    row = await self._row(session, job.run.execution_key)
                    ticket = await self._ticket(session, row)
                    cancelled = (
                        row.cancel_requested_at is not None
                        or ticket.agent_generation != row.generation
                        or ticket.worker_parked_reason is not None
                    )
                if not cancelled:
                    try:
                        if envelope is None:
                            await self._copy.start(
                                job, pack, reservation, estimate_seconds
                            )
                        else:
                            await self._copy.start(
                                job,
                                pack,
                                reservation,
                                estimate_seconds,
                                envelope=envelope,
                            )
                    except Exception:
                        await self._uncertain(
                            job.run.execution_key, "Firstmate start outcome is unknown"
                        )
                        raise
            return await self._reconcile(job.run.execution_key)

    async def _uncertain(self, key: str, detail: str) -> None:
        async with self._sessions() as session:
            await self._budget_lock(session)
            row = await self._row(session, key)
            if row.status not in _TERMINAL:
                row.status = "uncertain"
            ticket = await self._ticket(session, row)
            previous = set(ticket.admin_reasons or {})
            _reason(ticket, "delivery_uncertain", detail)
            await _record_reason_alerts(session, ticket, previous)
            await session.commit()

    async def get(self, execution_key: str) -> ExecutionView:
        async with self._execution_lock(execution_key):
            return await self._reconcile(execution_key)

    async def cancel(self, execution_key: str) -> ExecutionView:
        # Commit intent before waiting for an in-flight launch or control call.
        async with self._sessions() as session:
            await self._budget_lock(session)
            row = await self._row(session, execution_key)
            if row.status not in _TERMINAL and row.result is None:
                row.cancel_requested_at = row.cancel_requested_at or self._clock()
                row.status = "cancel_requested"
            await session.commit()
        async with self._execution_lock(execution_key):
            return await self._reconcile(execution_key)

    async def _reconcile(self, key: str) -> ExecutionView:
        current, pack = await self._snapshot(key)
        job = current.execution.contract
        try:
            # Cancellation cannot depend on parsing an untrusted result artifact.
            async with self._sessions() as session:
                row = await self._row(session, key)
                ticket = await self._ticket(session, row)
                must_stop = (
                    row.cancel_requested_at is not None
                    or ticket.agent_generation != row.generation
                    or ticket.worker_parked_reason is not None
                )
            stopped = current.execution.exit_confirmed
            if not stopped and must_stop:
                await self._copy.cancel(job)
                stopped = True
            observed = (
                TaskObservation(
                    task_id=key,
                    status="running",
                    result=current.result,
                    handoff=current.execution.handoff,
                )
                if current.result is not None and stopped
                else await self._copy.get(job)
            )
            if not stopped and observed.result is not None:
                await self._copy.cancel(job)
                stopped = True
                observed = await self._copy.get(job)
            view, budget_stop = await self._apply(key, observed, pack, stopped)
            if budget_stop and not stopped:
                await self._copy.cancel(job)
                return (await self._apply(key, await self._copy.get(job), pack, True))[
                    0
                ]
            return view
        except Exception:
            await self._uncertain(
                key,
                "Firstmate observation or cancellation failed; reservation retained",
            )
            raise

    async def _apply(
        self, key: str, observed: TaskObservation, pack: Pack, stopped: bool
    ) -> tuple[ExecutionView, bool]:
        async with self._sessions() as session:
            await self._budget_lock(session)
            row = await self._row(session, key)
            ticket = await self._ticket(session, row)
            previous = set(ticket.admin_reasons or {})
            state, execution = _state(ticket), _execution(row)
            terminal = row.status in _TERMINAL or row.result is not None
            result = observed.result
            handoff = observed.handoff
            progress = execution.progress
            progress_signatures = set(execution.progress_signatures)
            if observed.progress is not None and not terminal:
                incoming = observed.progress
                self._secrets.check(incoming.model_dump_json())
                checks = (
                    {check.id: check for check in execution.envelope.scope.checks}
                    if execution.envelope
                    else {}
                )
                if incoming.input_digest != digest(execution.contract) or any(
                    check.check_id not in checks
                    or check.expected != checks[check.check_id].expected
                    for check in incoming.checks
                ):
                    raise ValueError("Progress lacks approved execution evidence")

                def signatures(value: Handoff | None) -> set[tuple[str, str, int]]:
                    return (
                        {
                            (check.check_id, check.commit, check.exit_code)
                            for check in value.checks
                        }
                        if value
                        else set()
                    )

                progress_signatures.update(signatures(progress))
                incoming_signatures = signatures(incoming)
                if incoming_signatures - progress_signatures:
                    row.last_progress_at = self._clock()
                    progress = incoming
                progress_signatures.update(incoming_signatures)
            row.last_heartbeat_at = self._clock()
            if handoff is not None:
                self._secrets.check(handoff.model_dump_json())
                if handoff.input_digest != digest(execution.contract):
                    raise ValueError("Handoff does not match the frozen execution")
                if execution.handoff is not None and execution.handoff != handoff:
                    raise ValueError("Terminal evidence handoff is immutable")
            if result is not None:
                self._secrets.check(result.model_dump_json())
                if (
                    result.run != execution.contract.run
                    or result.input_digest != digest(execution.contract)
                ):
                    raise ValueError("Result does not match the frozen execution")
                if row.result is not None and row.result != result.model_dump(
                    mode="json"
                ):
                    raise ValueError("Terminal execution result is immutable")
            usage = result.usage if result is not None else observed.usage
            known_tokens, known_usd = execution.known_tokens, execution.known_usd
            complete = execution.exit_confirmed and not execution.usage_unknown
            if usage is not None:
                self._secrets.check(usage.model_dump_json())
                if (usage.provider is not None and usage.provider != pack.provider) or (
                    usage.model is not None and usage.model != pack.model
                ):
                    raise ValueError(
                        "Usage attribution does not match the frozen model"
                    )
                counts = (
                    usage.input_tokens,
                    usage.output_tokens,
                    usage.cache_read_tokens,
                    usage.cache_write_tokens,
                )
                measured_tokens = sum(count for count in counts if count is not None)
                measured_usd = usage.estimated_usd
                if usage.measurement != "unknown" and (
                    measured_tokens < known_tokens
                    or (measured_usd is not None and measured_usd < known_usd)
                ):
                    raise ValueError("Cumulative usage must not regress")
                known_tokens = max(known_tokens, measured_tokens)
                if measured_usd is not None:
                    if usage.price_source != execution.reservation.price_source:
                        raise ValueError(
                            "Usage price source differs from the reservation"
                        )
                    known_usd = max(known_usd, measured_usd)
                complete = (
                    stopped
                    and usage.measurement == "complete"
                    and measured_usd is not None
                )
            held_tokens = (
                0 if complete else max(0, execution.reservation.tokens - known_tokens)
            )
            held_usd = (
                Decimal("0")
                if complete
                else max(Decimal("0"), execution.reservation.estimated_usd - known_usd)
            )
            day = self._clock().astimezone(UTC).date().isoformat()
            daily = dict(execution.cost_by_day)
            if known_usd != execution.known_usd:
                daily[day] = (
                    daily.get(day, Decimal("0")) + known_usd - execution.known_usd
                )
            ticket.tokens_used += known_tokens - execution.known_tokens
            state = _replace(
                state,
                reserved_tokens=state.reserved_tokens
                + held_tokens
                - execution.held_tokens,
                reserved_estimated_usd=state.reserved_estimated_usd
                + held_usd
                - execution.held_usd,
                known_estimated_usd=state.known_estimated_usd
                + known_usd
                - execution.known_usd,
            )
            rejection = execution.rejection
            if not terminal:
                if row.cancel_requested_at is not None:
                    rejection = "cancelled"
                elif ticket.agent_generation != row.generation:
                    rejection = "stale_generation"
                elif ticket.worker_parked_reason is not None:
                    rejection = "parked"
            saved_handoff = execution.handoff
            if result is not None and stopped and handoff is not None:
                saved_handoff = handoff
            execution = _replace(
                execution,
                known_tokens=known_tokens,
                known_usd=known_usd,
                active_seconds=execution.active_seconds
                if execution.exit_confirmed
                else execution.prior_active_seconds
                + max(
                    0,
                    int(
                        (
                            self._clock() - datetime.fromisoformat(execution.started_at)
                        ).total_seconds()
                    ),
                ),
                held_tokens=held_tokens,
                held_usd=held_usd,
                cost_by_day=daily,
                usage_unknown=not complete,
                exit_confirmed=stopped,
                rejection=rejection,
                progress=progress.model_dump(mode="json") if progress else None,
                progress_signatures=sorted(progress_signatures),
                handoff=saved_handoff.model_dump(mode="json")
                if saved_handoff
                else None,
            )
            row.payload = execution.model_dump(mode="json")
            await session.flush()
            budget_stop = False
            if not terminal:
                budget = state.budget
                exceeded = (
                    known_tokens > execution.reservation.tokens
                    or known_usd > execution.reservation.estimated_usd
                    or ticket.tokens_used + state.reserved_tokens > budget.ticket_tokens
                    or state.known_estimated_usd + state.reserved_estimated_usd
                    > budget.ticket_estimated_usd
                    or await self._day_cost(session)
                    > await day_limit(session, self._clock(), budget.day_estimated_usd)
                )
                budget_stop = exceeded or (
                    not stopped
                    and (
                        known_tokens >= execution.reservation.tokens
                        or known_usd >= execution.reservation.estimated_usd
                    )
                )
                if budget_stop:
                    row.cancel_requested_at = row.cancel_requested_at or self._clock()
                    row.status = "cancelled" if stopped else "cancel_requested"
                    _reason(
                        ticket,
                        "delivery_budget",
                        "Delivery spend reached its reservation or approved budget",
                    )
                    ticket.worker_parked_reason = "delivery_budget"
                elif rejection is not None:
                    row.status = "cancelled" if stopped else "cancel_requested"
                elif result is not None and stopped:
                    if complete and result.status in {
                        "completed",
                        "needs_input",
                        "cancelled",
                    }:
                        row.result = result.model_dump(mode="json")
                        row.status = {
                            "completed": "done",
                            "needs_input": "waiting_input",
                            "cancelled": "cancelled",
                        }[result.status]
                    else:
                        row.status = "uncertain"
                        if complete:
                            ticket.worker_parked_reason = (
                                ticket.worker_parked_reason
                                or "delivery_outcome_uncertain"
                            )
                            _reason(
                                ticket,
                                "delivery_uncertain",
                                "Task result reports an uncertain outcome",
                            )
                elif observed.status == "running":
                    row.status = "running"
                else:
                    row.status = "uncertain"
                    _reason(
                        ticket,
                        "delivery_uncertain",
                        "Task dispatch has no confirmed running receipt",
                    )
            if (
                row.status == "running"
                and row.last_progress_at is not None
                and (self._clock() - row.last_progress_at).total_seconds() >= 1200
            ):
                ticket.worker_parked_reason = "delivery_idle"
                _reason(
                    ticket,
                    "delivery_idle",
                    "No new check evidence for twenty active minutes; admin review required",
                )
                row.cancel_requested_at = row.cancel_requested_at or self._clock()
                row.status = "cancel_requested"
                budget_stop = True
            if stopped and not complete:
                ticket.worker_parked_reason = (
                    ticket.worker_parked_reason or "delivery_usage_unknown"
                )
                _reason(
                    ticket,
                    "delivery_usage_unknown",
                    "Usage is incomplete; the reservation remains held",
                )
            if (
                row.status == "running"
                and execution.active_seconds >= 2 * execution.estimate_seconds
            ):
                _reason(
                    ticket,
                    "delivery_time_overrun",
                    "Delivery exceeded twice its estimate; it continues within its budget",
                )
            unknown = await session.scalar(
                select(func.count()).where(
                    JiraWorkerJob.ticket_id == ticket.id,
                    JiraWorkerJob.kind.in_(("role_job", "orchestrator_turn")),
                    JiraWorkerJob.payload["usage_unknown"].astext == "true",
                )
            )
            ticket.workflow_state = _replace(
                state, unknown_spend=bool(unknown)
            ).model_dump(mode="json")
            await _record_reason_alerts(session, ticket, previous)
            await session.commit()
            return _view(row), budget_stop
