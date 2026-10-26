"""Public delivery boundaries with disposable PostgreSQL and controlled remote peers."""

from __future__ import annotations

import asyncio
import hashlib
import json
from decimal import Decimal

import httpx
import pytest
from sqlalchemy import select

from switch_core.bridges.jira.contracts import digest
from switch_core.bridges.jira.delivery import fix_job, role_job
from switch_core.bridges.jira.delivery_contracts import (
    ScopeContract,
    VerificationContract,
    VerificationRole,
)
from switch_core.bridges.jira.firstmate_copy import SecretGuard
from switch_core.bridges.jira.firstmate_jobs import FirstmateJobs
from switch_core.bridges.jira.outbox import JiraOutboxSender
from switch_core.bridges.jira.publisher import PublicationUncertain
from switch_core.bridges.jira.state import WorkflowMapping
from switch_core.bridges.jira.target import delivery_state
from switch_core.bridges.jira.verification import MergeReceipt
from switch_core.db.models import JiraWorkerJob, JiraWorkerOutbox, JiraWorkerTicket
from tests.switch_core.bridges.jira.test_delivery import (
    BASE_SHA,
    HEAD,
    NOW,
    URL,
    DeliveryJira,
    DeliveryProcess,
    ForgeDouble,
    config,
    current,
    handoff,
    seed,
    services,
)
from tests.switch_core.bridges.jira.test_outbox import client_for

MERGED = "f" * 40


class ClosingJira(DeliveryJira):
    async def handle(self, request):
        if request.url.path.endswith("/transitions"):
            if request.method == "GET":
                response = await super().handle(request)
                body = response.json()
                body["transitions"].append(
                    {
                        "id": "complete",
                        "to": {
                            "id": "done",
                            "name": "Done",
                            "statusCategory": {"key": "done"},
                        },
                    }
                )
                return type(response)(200, json=body)
            if json.loads(request.content)["transition"]["id"] == "complete":
                self.writes.append(request)
                self.status = "Done"
                self.change("worker-account")
                self.histories[-1]["items"] = [
                    {
                        "field": "status",
                        "from": "progress",
                        "to": "done",
                        "fromString": "In Progress",
                        "toString": "Done",
                    }
                ]
                return httpx.Response(204)
        return await super().handle(request)


class Verifier(DeliveryProcess):
    def complete(self, job, *, verdict="PASS", bad=None):
        super().complete(job)
        if job.run.step not in {"review", "qa", "smoke"}:
            return
        observed = self.tasks[job.run.execution_key]
        envelope = self.envelopes[job.run.execution_key]
        checks = tuple(
            check.model_copy(
                update={
                    "commit": envelope.target_sha,
                    "exit_code": 0 if verdict == "PASS" else 1,
                }
            )
            for check in envelope.candidate.checks
        )
        evidence = handoff(job).model_copy(
            update={
                "head": envelope.target_sha,
                "verdict": verdict,
                "changed_paths": (),
                "checks": checks,
                "regression_checks": envelope.reproduction.checks
                if job.run.step == "qa"
                else (),
            }
        )
        if bad == "head":
            evidence = evidence.model_copy(update={"head": BASE_SHA})
        if bad == "unit-only":
            evidence = evidence.model_copy(update={"regression_checks": ()})
        if bad == "mutation":
            evidence = evidence.model_copy(
                update={"changed_paths": ("src/operation.py",)}
            )
        self.tasks[job.run.execution_key] = observed.model_copy(
            update={"handoff": evidence}
        )


class ClosingForge(ForgeDouble):
    def __init__(self):
        super().__init__()
        self.ready_calls = []
        self.receipt = None
        self.lose_ready = False
        self.hide_ready = False
        self.read_error = False
        self.updates = []

    async def find_requests(self, url, branch):
        if self.read_error:
            raise TimeoutError("Forge observation is unavailable")
        return await super().find_requests(url, branch)

    async def ready_request(self, url, request_url, head):
        assert self.requests[0].url == request_url and head == self.head
        self.ready_calls.append((url, request_url, head))
        if not self.hide_ready:
            self.requests = (self.requests[0].model_copy(update={"draft": False}),)
        if self.lose_ready:
            raise TimeoutError("Readiness outcome is unknown")

    async def merge_receipt(self, url, request_url):
        return self.receipt or MergeReceipt(
            repository_url=url,
            request_url=request_url,
            candidate_sha=self.head,
            base_branch="main",
            outcome="open",
            merged_sha=None,
            actor_id=None,
            human=False,
        )

    def merge(self, *, outcome="merged", human=True, candidate_sha=HEAD):
        self.receipt = MergeReceipt(
            repository_url=URL,
            request_url=self.requests[0].url,
            candidate_sha=candidate_sha,
            base_branch="main",
            outcome=outcome,
            merged_sha=MERGED if outcome == "merged" else None,
            actor_id="repository-owner" if human else None,
            human=human,
        )
        if outcome in {"merged", "closed_unmerged"}:
            self.requests = (self.requests[0].model_copy(update={"open": False}),)

    async def update_branch(self, url, branch, previous, head):
        assert self.head == previous
        self.updates.append((previous, head))
        self.head = head
        self.requests = (self.requests[0].model_copy(update={"head": head}),)

    async def verify_candidate(self, scope, candidate):
        assert candidate.verdict == "PASS"


async def setup(sessions, jira):
    scope = await seed(sessions)
    roles = {}
    for step in ("review", "qa", "smoke"):
        content = f"Switch {step} profile. Return typed evidence. Never edit or push."
        profile = scope.profile.model_copy(
            update={
                "content": content,
                "sha256": hashlib.sha256(content.encode()).hexdigest(),
            }
        )
        roles[step] = VerificationRole(
            profile=profile, reservation=scope.fix_reservation, estimate_seconds=60
        )
    verification = VerificationContract(
        **roles,
        behavior_check_ids=("regression",),
        smoke_check_ids=("regression",),
        complete_transition_id="complete",
    )
    data = scope.model_dump(mode="json")
    data["verification"] = verification.model_dump(mode="json")
    data["checks"][0]["kind"] = "behavior"
    scope = ScopeContract.model_validate_json(json.dumps(data))
    async with sessions() as session:
        ticket = await session.get(JiraWorkerTicket, "ticket")
        state = delivery_state(ticket)
        gate = state.gate.model_copy(update={"contract_digest": digest(scope)})
        ticket.workflow_state = state.model_copy(
            update={
                "scope": scope,
                "gate": gate,
                "statuses": (
                    *state.statuses,
                    WorkflowMapping(name="Done", jira_id="done"),
                ),
                "transitions": (
                    *state.transitions,
                    WorkflowMapping(name="progress->done", jira_id="complete"),
                ),
            }
        ).model_dump(mode="json")
        await session.commit()
    process, forge = Verifier(), ClosingForge()
    activity, supervisor, _, _, publisher = services(
        sessions, jira, process=process, forge=forge
    )
    jira.approve()
    await supervisor.reconcile("ticket")
    for job in (scope.job, fix_job(scope)):
        process.complete(job)
        await supervisor.reconcile("ticket")
        await supervisor.reconcile("ticket")
    await supervisor.reconcile("ticket")
    await supervisor.reconcile("ticket")
    return scope, activity, supervisor, process, forge, publisher


def sender(sessions, jira, publisher, *, trusted=True):
    return JiraOutboxSender(
        session_factory=sessions,
        config=config(),
        clients={"acme": client_for(jira)},
        clock=lambda: NOW,
        authorize_delivery=publisher.authorize_command if trusted else None,
    )


async def _tested(sessions, jira):
    bundle = await setup(sessions, jira)
    scope, _, supervisor, process, _, _ = bundle
    for step in ("review", "qa"):
        process.complete(role_job(scope, step, 1))
        await supervisor.reconcile("ticket")
        await supervisor.reconcile("ticket")
    await supervisor.reconcile("ticket")
    await supervisor.reconcile("ticket")
    return bundle


async def accepted(sessions, jira):
    bundle = await _tested(sessions, jira)
    scope, activity, supervisor, process, forge, publisher = bundle
    send = sender(sessions, jira, publisher)
    for _ in range(3):
        await send.run_once()
    await activity.read_ticket("ticket", force_jira=True)
    ticket, state = await current(sessions)
    async with sessions() as session:
        commands = (await session.scalars(select(JiraWorkerOutbox))).all()
        assert state.gate_ready and state.gate.kind == "acceptance", (
            ticket.worker_parked_reason,
            state.step,
            [(row.command, row.status, row.error) for row in commands],
        )
    assert state.gate.candidate_sha == HEAD
    assert state.accepted_head is None
    jira.approve()
    await supervisor.reconcile("ticket")
    ticket, state = await current(sessions)
    assert state.accepted_head == HEAD and state.step == "merge"
    return bundle


async def test_accept_merge_smoke_close_orders_every_public_boundary(session_factory):
    jira = ClosingJira()
    scope, activity, supervisor, process, forge, publisher = await accepted(
        session_factory, jira
    )
    assert len(forge.ready_calls) == 1
    assert process.starts == [
        "ticket/1/reproduce/1",
        "ticket/1/fix/1",
        "ticket/1/review/1",
        "ticket/1/qa/1",
    ]
    assert len(set(process.starts)) == 4
    assert process.envelopes["ticket/1/review/1"].target_sha == HEAD
    assert process.envelopes["ticket/1/qa/1"].target_sha == HEAD
    for _ in range(3):
        await supervisor.reconcile("ticket")
    assert len(process.starts) == 4 and jira.status == "In Progress"
    forge.merge()
    await supervisor.reconcile("ticket")
    await supervisor.reconcile("ticket")
    assert process.envelopes["ticket/1/smoke/1"].target_sha == MERGED
    process.complete(role_job(scope, "smoke", 1))
    await supervisor.reconcile("ticket")
    await asyncio.gather(supervisor.reconcile("ticket"), supervisor.reconcile("ticket"))
    send = sender(session_factory, jira, publisher)
    for _ in range(3):
        await send.run_once()
    assert jira.status == "Done"
    await activity.read_ticket("ticket", force_jira=True)
    ticket, state = await current(session_factory)
    assert state.outcome == "delivered" and ticket.status == "Done"
    assert ticket.worker_parked_reason == "Jira ticket completed or cancelled"
    async with session_factory() as session:
        rows = (
            await session.scalars(
                select(JiraWorkerOutbox)
                .where(JiraWorkerOutbox.channel == "jira")
                .order_by(JiraWorkerOutbox.created_at)
            )
        ).all()
        assert len(rows) == 4
        assert [request.url.path.rsplit("/", 1)[1] for request in jira.writes] == [
            "comment",
            "transitions",
            "comment",
            "transitions",
        ]
        assert all(row.status == "done" for row in rows)
        sync = (
            await session.scalars(
                select(JiraWorkerOutbox).where(JiraWorkerOutbox.channel == "switch")
            )
        ).all()
        assert any(row.command == "sync_ticket_room" for row in sync)
        jobs = (
            await session.scalars(
                select(JiraWorkerJob).where(JiraWorkerJob.kind == "role_job")
            )
        ).all()
        assert all(row.payload["exit_confirmed"] for row in jobs)
    assert len(forge.creates) == len(forge.pushes) == 1
    assert not hasattr(forge, "deploy") and not hasattr(forge, "perform_merge")


@pytest.mark.parametrize(
    "step,bad",
    [
        ("review", "head"),
        ("review", "mutation"),
        ("qa", "unit-only"),
        ("qa", "head"),
        ("qa", "mutation"),
    ],
)
async def test_verification_rejects_stale_or_nonbehavioral_evidence(
    session_factory, step, bad
):
    jira = ClosingJira()
    scope, _, supervisor, process, forge, _ = await setup(session_factory, jira)
    if step == "qa":
        process.complete(role_job(scope, "review", 1))
        await supervisor.reconcile("ticket")
        await supervisor.reconcile("ticket")
    process.complete(role_job(scope, step, 1), bad=bad)
    await supervisor.reconcile("ticket")
    ticket, state = await current(session_factory)
    assert ticket.worker_parked_reason and state.accepted_head is None
    assert not forge.ready_calls and jira.status != "Done"
    async with session_factory() as session:
        job = await session.scalar(
            select(JiraWorkerJob).where(
                JiraWorkerJob.execution_key == f"ticket/1/{step}/1"
            )
        )
        assert job.payload["known_tokens"] == 20 and job.payload["exit_confirmed"]


@pytest.mark.parametrize("phase", ["review", "qa", "accept", "merge", "smoke", "close"])
async def test_changed_candidate_invalidates_all_evidence_and_cancels_live_role(
    session_factory, phase
):
    jira = ClosingJira()
    if phase in {"merge", "smoke", "close"}:
        bundle = await accepted(session_factory, jira)
    elif phase == "accept":
        bundle = await _tested(session_factory, jira)
    else:
        bundle = await setup(session_factory, jira)
    scope, _, supervisor, process, forge, _ = bundle
    if phase == "qa":
        process.complete(role_job(scope, "review", 1))
        await supervisor.reconcile("ticket")
        await supervisor.reconcile("ticket")
    if phase in {"smoke", "close"}:
        forge.merge()
        await supervisor.reconcile("ticket")
        await supervisor.reconcile("ticket")
        if phase == "close":
            process.complete(role_job(scope, "smoke", 1))
            await supervisor.reconcile("ticket")
    forge.head = "9" * 40
    forge.requests = (forge.requests[0].model_copy(update={"head": forge.head}),)
    await supervisor.reconcile("ticket")
    ticket, state = await current(session_factory)
    assert ticket.worker_parked_reason and ticket.agent_generation == 2
    assert state.review_head is state.qa_head is state.accepted_head is None
    assert state.smoke is None and jira.status != "Done"
    if phase in {"review", "qa", "smoke"}:
        assert f"ticket/1/{phase}/1" in process.cancelled


@pytest.mark.parametrize(
    "outcome,human",
    [
        ("open", False),
        ("unknown", False),
        ("closed_unmerged", False),
        ("merged", False),
    ],
)
async def test_no_confirmed_human_merge_never_starts_smoke_or_completes(
    session_factory, outcome, human
):
    jira = ClosingJira()
    _, _, supervisor, process, forge, _ = await accepted(session_factory, jira)
    forge.merge(outcome=outcome, human=human)
    for _ in range(2):
        await supervisor.reconcile("ticket")
    assert "ticket/1/smoke/1" not in process.starts and jira.status != "Done"


@pytest.mark.parametrize("failure", ["FAIL", "BLOCKED", "unknown"])
async def test_failed_or_unknown_smoke_blocks_closure(session_factory, failure):
    jira = ClosingJira()
    scope, _, supervisor, process, forge, _ = await accepted(session_factory, jira)
    forge.merge()
    await supervisor.reconcile("ticket")
    await supervisor.reconcile("ticket")
    if failure != "unknown":
        process.complete(role_job(scope, "smoke", 1), verdict=failure)
    await supervisor.reconcile("ticket")
    ticket, state = await current(session_factory)
    assert state.step != "close" and state.outcome is None
    assert jira.status != "Done"
    async with session_factory() as session:
        rows = (
            await session.scalars(
                select(JiraWorkerOutbox).where(
                    JiraWorkerOutbox.command_key.like("%:close:%")
                )
            )
        ).all()
        assert not rows


@pytest.mark.parametrize("visible", [True, False])
async def test_uncertain_ready_outcomes_never_repeat_writes_or_create_requests(
    session_factory, visible
):
    jira = ClosingJira()
    scope, _, supervisor, process, forge, publisher = await setup(session_factory, jira)
    for step in ("review", "qa"):
        process.complete(role_job(scope, step, 1))
        await supervisor.reconcile("ticket")
        if step == "review":
            await supervisor.reconcile("ticket")
    forge.lose_ready, forge.hide_ready = True, not visible
    with pytest.raises(PublicationUncertain):
        await supervisor.reconcile("ticket")
    for _ in range(3):
        await supervisor.reconcile("ticket")
    _, state = await current(session_factory)
    assert len(forge.ready_calls) == len(forge.creates) == 1
    assert state.publication.readiness == ("ready" if visible else "uncertain")
    if not visible:
        assert state.gate is None
    assert publisher is not None


async def test_send_time_head_change_blocks_acceptance_request(session_factory):
    jira = ClosingJira()
    _, _, _, _, forge, publisher = await _tested(session_factory, jira)
    forge.head = "9" * 40
    await sender(session_factory, jira, publisher).run_once()
    ticket, state = await current(session_factory)
    assert ticket.worker_parked_reason and not state.gate_ready
    assert not jira.writes


async def test_missing_trusted_send_authorizer_blocks_acceptance(session_factory):
    jira = ClosingJira()
    _, _, _, _, _, publisher = await _tested(session_factory, jira)
    await sender(session_factory, jira, publisher, trusted=False).run_once()
    ticket, state = await current(session_factory)
    assert ticket.worker_parked_reason and not state.gate_ready
    assert not jira.writes


async def test_review_failure_starts_bounded_fresh_rework_with_only_findings(
    session_factory,
):
    jira = ClosingJira()
    scope, _, supervisor, process, forge, _ = await setup(session_factory, jira)
    process.complete(role_job(scope, "review", 1), verdict="FAIL")
    await supervisor.reconcile("ticket")
    await supervisor.reconcile("ticket")
    _, state = await current(session_factory)
    assert state.step == "fix" and state.rework_count == 1
    assert process.starts[-1] == "ticket/1/fix/2"
    envelope = process.envelopes[process.starts[-1]]
    assert envelope.feedback.verdict == "FAIL" and envelope.target_sha == HEAD
    assert envelope.candidate.head == HEAD
    assert not forge.ready_calls


async def test_human_close_during_review_is_not_agent_delivery(session_factory):
    jira = ClosingJira()
    _, activity, supervisor, process, _, _ = await setup(session_factory, jira)
    jira.status = "Done"
    jira.change("approver")
    jira.histories[-1]["items"] = [
        {
            "field": "status",
            "from": "progress",
            "to": "done",
            "fromString": "In Progress",
            "toString": "Done",
        }
    ]
    await activity.read_ticket("ticket", force_jira=True)
    await supervisor.reconcile("ticket")
    ticket, state = await current(session_factory)
    assert state.outcome == "closed_by_human" and ticket.status == "Done"
    assert "ticket/1/review/1" in process.cancelled
    assert not jira.writes


@pytest.mark.parametrize("phase", ["review", "qa", "accept", "merge", "smoke", "close"])
async def test_fresh_supervisor_resumes_without_duplicate_roles_or_publication(
    session_factory, phase
):
    jira = ClosingJira()
    if phase in {"merge", "smoke", "close"}:
        bundle = await accepted(session_factory, jira)
    elif phase == "accept":
        bundle = await _tested(session_factory, jira)
    else:
        bundle = await setup(session_factory, jira)
    scope, _, supervisor, process, forge, _ = bundle
    if phase == "qa":
        process.complete(role_job(scope, "review", 1))
        await supervisor.reconcile("ticket")
        await supervisor.reconcile("ticket")
    if phase in {"smoke", "close"}:
        forge.merge()
        await supervisor.reconcile("ticket")
        await supervisor.reconcile("ticket")
        if phase == "close":
            process.complete(role_job(scope, "smoke", 1))
            await supervisor.reconcile("ticket")
    starts = tuple(process.starts)
    for _ in range(2):
        _, restarted, _, _, _ = services(
            session_factory, jira, process=process, forge=forge
        )
        await restarted.reconcile("ticket")
    ticket, state = await current(session_factory)
    assert not ticket.worker_parked_reason, ticket.worker_parked_reason
    assert (
        tuple(process.starts) == starts and len(forge.creates) == len(forge.pushes) == 1
    )
    assert state.step == phase


@pytest.mark.parametrize("dirty", ["actor", "comment", "field", "chat"])
async def test_candidate_acceptance_needs_its_own_clean_human_edge(
    session_factory, dirty
):
    jira = ClosingJira()
    _, activity, supervisor, _, _, publisher = await _tested(session_factory, jira)
    await sender(session_factory, jira, publisher).run_once()
    await activity.read_ticket("ticket", force_jira=True)
    if dirty == "chat":
        jira.change("approver", comment=True)
    else:
        jira.approve(actor="untrusted" if dirty == "actor" else "approver")
        if dirty == "comment":
            jira.change("approver", comment=True)
        if dirty == "field":
            jira.histories[-1]["items"].append(
                {"field": "summary", "fromString": "old", "toString": "changed"}
            )
    await supervisor.reconcile("ticket")
    ticket, state = await current(session_factory)
    assert state.accepted_head is None and state.step == "accept"
    assert jira.status != "Done"


async def test_late_stale_review_usage_stays_accounted_after_candidate_invalidation(
    session_factory,
):
    jira = ClosingJira()
    scope, _, supervisor, process, forge, _ = await setup(session_factory, jira)
    forge.head = "9" * 40
    await supervisor.reconcile("ticket")
    process.complete(role_job(scope, "review", 1))
    jobs = FirstmateJobs(
        session_factory=session_factory,
        delivery_copy=process,
        secrets=SecretGuard(forbidden_values=()),
        clock=lambda: NOW,
    )
    await jobs.get("ticket/1/review/1")
    ticket, state = await current(session_factory)
    assert ticket.tokens_used == 60 and state.known_estimated_usd == Decimal("0.3")
    assert state.accepted_head is None and state.review_head is None
    async with session_factory() as session:
        row = await session.scalar(
            select(JiraWorkerJob).where(
                JiraWorkerJob.execution_key == "ticket/1/review/1"
            )
        )
        assert row.status == "cancelled" and row.payload["held_tokens"] == 0


async def test_rework_updates_only_the_original_draft_and_uses_new_attempts(
    session_factory,
):
    jira = ClosingJira()
    scope, _, supervisor, process, forge, _ = await setup(session_factory, jira)
    process.complete(role_job(scope, "review", 1), verdict="FAIL")
    await supervisor.reconcile("ticket")
    await supervisor.reconcile("ticket")
    new = "8" * 40
    fixed = role_job(scope, "fix", 2)
    process.complete(fixed)
    observation = process.tasks[fixed.run.execution_key]
    evidence = observation.handoff.model_copy(
        update={
            "head": new,
            "fix_commit": new,
            "checks": tuple(
                check.model_copy(update={"commit": new})
                for check in observation.handoff.checks
            ),
        }
    )
    process.tasks[fixed.run.execution_key] = observation.model_copy(
        update={"handoff": evidence}
    )
    await supervisor.reconcile("ticket")
    for _ in range(4):
        await supervisor.reconcile("ticket")
    ticket, state = await current(session_factory)
    assert not ticket.worker_parked_reason, ticket.worker_parked_reason
    assert state.step == "review" and state.candidate_head == new
    assert process.starts[-1] == "ticket/1/review/2"
    assert forge.updates == [(HEAD, new)]
    assert len(forge.creates) == len(forge.pushes) == 1


@pytest.mark.parametrize("changed", ["base_branch", "branch", "url"])
async def test_rework_cannot_push_after_the_draft_contract_changes(
    session_factory, changed
):
    jira = ClosingJira()
    scope, _, supervisor, process, forge, _ = await setup(session_factory, jira)
    process.complete(role_job(scope, "review", 1), verdict="FAIL")
    await supervisor.reconcile("ticket")
    await supervisor.reconcile("ticket")
    fixed = role_job(scope, "fix", 2)
    process.complete(fixed)
    observation = process.tasks[fixed.run.execution_key]
    new = "8" * 40
    evidence = observation.handoff.model_copy(
        update={
            "head": new,
            "fix_commit": new,
            "checks": tuple(
                check.model_copy(update={"commit": new})
                for check in observation.handoff.checks
            ),
        }
    )
    process.tasks[fixed.run.execution_key] = observation.model_copy(
        update={"handoff": evidence}
    )
    await supervisor.reconcile("ticket")
    value = (
        "https://example.invalid/team/project/pull/2"
        if changed == "url"
        else "human-branch"
    )
    forge.requests = (forge.requests[0].model_copy(update={changed: value}),)
    await supervisor.reconcile("ticket")
    ticket, _ = await current(session_factory)
    assert ticket.worker_parked_reason and not forge.updates
    assert len(forge.creates) == 1 and forge.head == HEAD


async def test_send_time_merge_change_blocks_complete_after_summary(session_factory):
    jira = ClosingJira()
    scope, _, supervisor, process, forge, publisher = await accepted(
        session_factory, jira
    )
    forge.merge()
    await supervisor.reconcile("ticket")
    await supervisor.reconcile("ticket")
    process.complete(role_job(scope, "smoke", 1))
    await supervisor.reconcile("ticket")
    await supervisor.reconcile("ticket")
    forge.receipt = forge.receipt.model_copy(update={"merged_sha": "7" * 40})
    await sender(session_factory, jira, publisher).run_once()
    ticket, state = await current(session_factory)
    assert ticket.worker_parked_reason and jira.status != "Done"
    assert state.outcome is None


async def test_role_attempt_policy_exhausts_without_a_fourth_fix(session_factory):
    jira = ClosingJira()
    scope, _, supervisor, process, forge, _ = await setup(session_factory, jira)
    for attempt in (1, 2, 3):
        process.complete(role_job(scope, "review", attempt), verdict="FAIL")
        await supervisor.reconcile("ticket")
        if attempt == 3:
            break
        await supervisor.reconcile("ticket")
        fixed = role_job(scope, "fix", attempt + 1)
        process.complete(fixed)
        observation = process.tasks[fixed.run.execution_key]
        new = str(attempt + 4) * 40
        evidence = observation.handoff.model_copy(
            update={
                "head": new,
                "fix_commit": new,
                "checks": tuple(
                    check.model_copy(update={"commit": new})
                    for check in observation.handoff.checks
                ),
            }
        )
        process.tasks[fixed.run.execution_key] = observation.model_copy(
            update={"handoff": evidence}
        )
        for _ in range(5):
            await supervisor.reconcile("ticket")
    ticket, state = await current(session_factory)
    assert (
        ticket.worker_parked_reason == "Approved rework or attempt policy is exhausted"
    )
    assert state.rework_count == 2
    assert "ticket/1/fix/4" not in process.starts and not forge.ready_calls


async def test_unavailable_forge_keeps_the_existing_execution_and_never_certifies_delivery(
    session_factory,
):
    jira = ClosingJira()
    _, _, supervisor, process, forge, _ = await setup(session_factory, jira)
    starts = tuple(process.starts)
    forge.read_error = True
    for _ in range(2):
        await supervisor.run_once()
    ticket, state = await current(session_factory)
    assert tuple(process.starts) == starts and len(forge.creates) == 1
    assert (
        not forge.ready_calls and state.accepted_head is None and state.outcome is None
    )
    assert ticket.status != "Done"


async def test_unsupported_qa_environment_parks_without_more_access_or_rework(
    session_factory,
):
    jira = ClosingJira()
    scope, _, supervisor, process, forge, _ = await setup(session_factory, jira)
    process.complete(role_job(scope, "review", 1))
    await supervisor.reconcile("ticket")
    await supervisor.reconcile("ticket")
    process.complete(role_job(scope, "qa", 1), verdict="BLOCKED")
    await supervisor.reconcile("ticket")
    ticket, state = await current(session_factory)
    assert "unsupported" in ticket.worker_parked_reason
    assert state.rework_count == 0 and not forge.ready_calls
