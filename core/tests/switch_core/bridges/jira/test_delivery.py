"""Jira HTTP -> approval -> real job records -> draft, with process/forge doubles."""

from __future__ import annotations

import asyncio
import hashlib
import json
import subprocess
from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest
from sqlalchemy import select

from switch_core.bridges.jira.activity import TicketActivity, TicketTurn
from switch_core.bridges.jira.contracts import (
    Gate,
    Job,
    Repository,
    Reservation,
    Result,
    Run,
    Usage,
    digest,
)
from switch_core.bridges.jira.delivery import DeliverySupervisor, fix_job, stage_scope
from switch_core.bridges.jira.delivery_contracts import (
    ApprovalRoute,
    ApprovedCheck,
    CheckEvidence,
    ExactTransition,
    ExecutionEnvelope,
    Handoff,
    PreparedExecutionEnvelope,
    RoleProfile,
    ScopeContract,
    validate_candidate,
)
from switch_core.bridges.jira.firstmate_copy import SecretGuard, TaskObservation
from switch_core.bridges.jira.firstmate_jobs import FirstmateJobs
from switch_core.bridges.jira.forge import GitHubDraftForge
from switch_core.bridges.jira.intake import IntakeOrchestrator
from switch_core.bridges.jira.outbox import JiraOutboxSender, enqueue_jira_command
from switch_core.bridges.jira.packs import FrozenPack, load_pack
from switch_core.bridges.jira.publisher import (
    DraftPublisher,
    DraftRequest,
    GitCandidateObjects,
    PublicationUncertain,
)
from switch_core.bridges.jira.state import DeliveryWorkflowState, WorkflowMapping
from switch_core.bridges.jira.target import delivery_state
from switch_core.bridges.jira.worker import JiraWorkerScheduler
from switch_core.db.models import JiraWorkerJob, JiraWorkerOutbox, JiraWorkerTicket
from tests.switch_core.bridges.jira import test_firstmate_jobs
from tests.switch_core.bridges.jira.test_activity import ThreadRooms, message
from tests.switch_core.bridges.jira.test_admin import FakeAdminBoard, _worker_config
from tests.switch_core.bridges.jira.test_intake import ModelMock
from tests.switch_core.bridges.jira.test_outbox import BASE, JiraMock, client_for
from tests.switch_core.bridges.jira.test_worker import _config

lifecycle_lab = test_firstmate_jobs.lab

NOW = datetime(2026, 10, 1, tzinfo=UTC)
URL = "https://example.invalid/team/project"
BASE_SHA, TEST_SHA, HEAD = "a" * 40, "b" * 40, "c" * 40
PROFILE = (
    "Switch is the supervisor. Return Result and separate handoff.json. Do not publish."
)


def config(enabled=True):
    return _config(
        jira_worker_enabled_projects={"acme": ["TEST"]},
        jira_worker_delivery_projects={
            "acme": {"TEST": {"service_field": "customfield_12345"}}
        },
        jira_worker_delivery_execution_enabled=enabled,
    )


class DeliveryJira(JiraMock):
    def __init__(self):
        super().__init__()
        self.status = "Waiting for approval"
        self.fields["description"] = f"Repository URL: {URL}"

    def approve(self, actor="approver"):
        self.change(actor)
        self.status = "In Progress"
        self.histories[-1]["items"] = [
            {
                "field": "status",
                "from": "approval",
                "to": "progress",
                "fromString": "Waiting for approval",
                "toString": "In Progress",
            }
        ]

    async def handle(self, request):
        if request.method == "GET" and request.url.path.endswith("/transitions"):
            return httpx.Response(
                200,
                json={
                    "transitions": [
                        {
                            "id": "request-approval",
                            "to": {"id": "approval", "name": "Waiting for approval"},
                        }
                    ]
                },
            )
        if request.method == "POST" and request.url.path.endswith("/transitions"):
            self.writes.append(request)
            assert json.loads(request.content) == {
                "transition": {"id": "request-approval"}
            }
            self.status = "Waiting for approval"
            self.change("worker-account")
            self.histories[-1]["items"] = [
                {
                    "field": "status",
                    "from": "progress",
                    "to": "approval",
                    "fromString": "In Progress",
                    "toString": self.status,
                }
            ]
            return httpx.Response(204)
        response = await super().handle(request)
        if (
            request.method == "GET"
            and "/issue/" in request.url.path
            and not request.url.path.endswith(("/changelog", "/comment"))
        ):
            body = response.json()
            if "fields" in body:
                body["fields"]["status"]["id"] = (
                    "approval" if self.status == "Waiting for approval" else "progress"
                )
                response = httpx.Response(200, json=body)
        return response


async def seed(sessions, *, base_sha=BASE_SHA, pack=None, ready=True):
    pack = pack or load_pack(config())
    repository = Repository(
        supplied_url=URL,
        canonical_url=URL,
        outcome="verified",
        base_branch="main",
        base_sha=base_sha,
        evidence="Trusted fixture triage",
    )
    job = Job(
        schema_version=1,
        run=Run(
            instance="acme",
            ticket_id="ticket",
            generation=1,
            step="reproduce",
            attempt=1,
            execution_key="ticket/1/reproduce/1",
        ),
        pack_digest=pack.digest,
        service="Bug",
        repository=repository,
        acceptance=("The public operation succeeds",),
    )
    reservation = Reservation(
        tokens=1000, estimated_usd=Decimal("1"), price_source="fixture-prices"
    )
    scope = ScopeContract(
        job=job,
        branch="switch/ticket/1",
        allowed_paths=("src", "tests"),
        forbidden_paths=("src/secrets",),
        checks=(
            ApprovedCheck(
                id="regression",
                argv=("python", "-m", "pytest", "tests"),
                expected="The public operation succeeds",
            ),
        ),
        approval_route=ApprovalRoute(
            source_status_id="approval",
            target_status_id="progress",
            transition_id="approve-scope",
            unique_edge_verified=True,
            evidence="Trusted workflow test proves one edge and unauthorized denial",
        ),
        profile=RoleProfile(
            commit="d" * 40,
            content=PROFILE,
            sha256=hashlib.sha256(PROFILE.encode()).hexdigest(),
        ),
        reproduction_reservation=reservation,
        fix_reservation=reservation,
        reproduction_estimate_seconds=60,
        fix_estimate_seconds=120,
    )
    gate = Gate(
        run=job.run,
        id="scope-gate",
        kind="scope",
        contract_digest=digest(scope),
        transition_id="approve-scope",
        candidate_sha=None,
        approver_account_ids=("approver",),
    )
    state = DeliveryWorkflowState(
        pack=pack,
        service="Bug",
        repository=repository,
        statuses=(
            WorkflowMapping(name="Waiting for approval", jira_id="approval"),
            WorkflowMapping(name="In Progress", jira_id="progress"),
        ),
        transitions=(
            WorkflowMapping(name="Approve", jira_id="approve-scope"),
            WorkflowMapping(name="progress->approval", jira_id="request-approval"),
        ),
    )
    async with sessions() as session:
        ticket = JiraWorkerTicket(
            id="ticket",
            instance="acme",
            issue_key="TEST-1",
            project_key="TEST",
            status="Waiting for approval",
            agent_generation=1,
            reporter_account_id="reporter-account",
            jira_worker_account_id="worker-account",
            jira_read_updated=BASE,
            jira_read_changelog_id="",
            jira_seen_comment_ids=[],
            workflow_state=state.model_dump(mode="json"),
            intake_state={"phase": "approval"},
        )
        stage_scope(ticket, scope, gate)
        if ready:
            ticket.workflow_state = {**ticket.workflow_state, "gate_ready": True}
        session.add(ticket)
        await session.commit()
    return scope


def handoff(job, *, base=BASE_SHA):
    reproduce = job.run.step == "reproduce"
    check = CheckEvidence(
        check_id="regression",
        commit=base if reproduce else HEAD,
        exit_code=1 if reproduce else 0,
        expected=job.acceptance[0],
        actual="Observed original failure"
        if reproduce
        else "Original failure no longer occurs",
        environment="Isolated fixture",
        artifact_digest="e" * 64,
    )
    return Handoff(
        input_digest=digest(job),
        verdict="PASS",
        head=base if reproduce else HEAD,
        findings=("Behavior checked",),
        changed_paths=()
        if reproduce
        else ("src/operation.py", "tests/test_operation.py"),
        checks=(check,),
        regression_checks=()
        if reproduce
        else (check.model_copy(update={"commit": TEST_SHA, "exit_code": 1}),),
        regression_commit=None if reproduce else TEST_SHA,
        fix_commit=None if reproduce else HEAD,
    )


class DeliveryProcess:
    def __init__(self):
        self.tasks = {}
        self.starts = []
        self.envelopes = {}
        self.cancelled = []

    async def start(self, job, pack, reservation, estimate_seconds, *, envelope=None):
        envelope.validate_job(job)
        key = job.run.execution_key
        if key not in self.tasks:
            self.starts.append(key)
            self.tasks[key] = TaskObservation(task_id=key, status="running")
            self.envelopes[key] = envelope
        return self.tasks[key]

    async def get(self, job):
        return self.tasks[job.run.execution_key]

    async def cancel(self, job):
        self.cancelled.append(job.run.execution_key)

    def complete(self, job, *, evidence=True):
        usage = Usage(
            measurement="complete",
            provider="fixture-provider",
            model="fixture-model",
            input_tokens=10,
            output_tokens=10,
            cache_read_tokens=0,
            cache_write_tokens=0,
            estimated_usd=Decimal("0.1"),
            price_source="fixture-prices",
            billing="estimate_not_subscription_invoice",
        )
        # The immutable fixture pack supplies the actual pinned names.
        pack = load_pack(config()).snapshot
        usage = usage.model_copy(
            update={"provider": pack.provider, "model": pack.model}
        )
        self.tasks[job.run.execution_key] = TaskObservation(
            task_id=job.run.execution_key,
            status="running",
            result=Result(
                run=job.run,
                input_digest=digest(job),
                status="completed",
                question=None,
                usage=usage,
            ),
            handoff=handoff(job, base=job.repository.base_sha) if evidence else None,
        )


class ForgeDouble:
    def __init__(self):
        self.head = None
        self.requests = ()
        self.pushes = []
        self.creates = []
        self.branch_reads = []
        self.fail_push = False
        self.lose_push = False
        self.lose_create = False
        self.before_create = None

    async def branch_head(self, url, branch):
        assert url == URL and branch == "switch/ticket/1"
        self.branch_reads.append(self.head)
        return self.head

    async def push_new_branch(self, url, branch, head):
        assert self.head is None
        self.pushes.append((url, branch, head))
        if self.fail_push:
            self.fail_push = False
            raise TimeoutError("Push failed before remote write")
        self.head = head
        if self.lose_push:
            self.lose_push = False
            raise TimeoutError("Unknown push outcome")

    async def find_requests(self, url, branch):
        return self.requests

    async def create_draft(self, url, branch, base, head, key):
        self.creates.append(key)
        request = DraftRequest(
            url="https://example.invalid/team/project/pull/1",
            branch=branch,
            base_branch=base,
            head=head,
            draft=True,
            open=True,
        )
        self.requests = (request,)
        if self.lose_create:
            self.lose_create = False
            raise TimeoutError("Unknown creation outcome")
        return request

    async def verify_candidate(self, scope, candidate):
        validate_candidate(scope, fix_job(scope), candidate)


def services(sessions, jira, *, enabled=True, process=None, forge=None, rooms=None):
    settings = config(enabled)
    activity = TicketActivity(
        session_factory=sessions,
        config=settings,
        clients={"acme": client_for(jira)},
        rooms=rooms,
        clock=lambda: NOW,
    )
    process = process or DeliveryProcess()
    forge = forge or ForgeDouble()
    jobs = FirstmateJobs(
        session_factory=sessions,
        delivery_copy=process,
        secrets=SecretGuard(forbidden_values=("PLACEHOLDER_FORGE_CREDENTIAL",)),
        clock=lambda: NOW,
    )

    async def authorize(ticket_id):
        return (
            settings.jira_worker_delivery_execution_enabled
            and await activity.read_ticket(ticket_id, force_jira=True)
        )

    publisher = DraftPublisher(
        session_factory=sessions, forge=forge, authorize=authorize
    )
    supervisor = DeliverySupervisor(
        session_factory=sessions,
        config=settings,
        activity=activity,
        jobs=jobs,
        publisher=publisher,
    )
    return activity, supervisor, process, forge, publisher


async def current(sessions):
    async with sessions() as session:
        ticket = await session.get(JiraWorkerTicket, "ticket")
        return ticket, delivery_state(ticket)


async def candidate(sessions, jira):
    scope = await seed(sessions)
    activity, supervisor, process, forge, publisher = services(sessions, jira)
    jira.approve()
    await supervisor.reconcile("ticket")
    process.complete(scope.job)
    await supervisor.reconcile("ticket")
    job = fix_job(scope)
    await supervisor.reconcile("ticket")
    process.complete(job)
    await supervisor.reconcile("ticket")
    return scope, supervisor, process, forge, publisher


async def test_approve_runs_one_reproduction_then_fresh_fix_and_one_draft(
    session_factory,
):
    jira = DeliveryJira()
    scope, supervisor, process, forge, _ = await candidate(session_factory, jira)
    await asyncio.gather(supervisor.reconcile("ticket"), supervisor.reconcile("ticket"))
    await supervisor.reconcile("ticket")
    ticket, state = await current(session_factory)
    assert process.starts == ["ticket/1/reproduce/1", "ticket/1/fix/1"]
    assert process.envelopes[process.starts[0]].reproduction is None
    assert process.envelopes[process.starts[1]].reproduction == handoff(scope.job)
    assert state.publication.status == "published"
    assert len(forge.pushes) == len(forge.creates) == 1
    assert ticket.status == "In Progress" and ticket.worker_parked_reason is None
    assert len(state.approvals) == 1
    assert jira.writes == []
    async with session_factory() as session:
        assert (
            len(
                (
                    await session.scalars(
                        select(JiraWorkerJob).where(JiraWorkerJob.kind == "role_job")
                    )
                ).all()
            )
            == 2
        )


@pytest.mark.parametrize(
    "dirty", ["extra-history", "same-entry", "comment", "rank", "actor", "endpoint"]
)
async def test_dirty_or_unauthorized_gate_parks_without_job(session_factory, dirty):
    await seed(session_factory)
    jira = DeliveryJira()
    jira.approve("stranger" if dirty == "actor" else "approver")
    if dirty == "extra-history":
        jira.change("approver")
        jira.histories[-1]["items"] = [{"field": "summary"}]
    elif dirty == "same-entry":
        jira.histories[-1]["items"].append({"field": "summary"})
    elif dirty == "comment":
        jira.change("reporter-account", comment=True)
    elif dirty == "rank":
        jira.change("approver")
        jira.histories[-1]["items"] = [{"field": "Rank", "fieldtype": "custom"}]
    elif dirty == "endpoint":
        jira.histories[-1]["items"][0]["from"] = "other"
    _, supervisor, process, _, _ = services(session_factory, jira)
    await supervisor.reconcile("ticket")
    ticket, state = await current(session_factory)
    assert ticket.worker_parked_reason and ticket.agent_generation > 1
    assert process.starts == [] and state.approvals == ()


async def test_jira_chat_approval_does_not_start_work(session_factory):
    await seed(session_factory)
    jira = DeliveryJira()
    jira.change("reporter-account", comment=True)
    jira.comments[-1]["body"] = "Approved. Start the fix now."
    _, supervisor, process, _, _ = services(session_factory, jira)
    await supervisor.reconcile("ticket")
    ticket, state = await current(session_factory)
    assert process.starts == [] and state.step == "approve_scope"
    assert ticket.status == "Waiting for approval"


async def test_summary_and_exact_request_must_finish_before_gate_approval(
    session_factory,
):
    scope = await seed(session_factory, ready=False)
    jira = DeliveryJira()
    jira.status = "In Progress"
    settings = config()
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, "ticket")
        ticket.status = jira.status
        ticket.intake_state = {"phase": "intake"}
        ticket.wait_channel = "jira_comments"
        await session.commit()
    intake = IntakeOrchestrator(
        session_factory=session_factory,
        config=settings,
        model=ModelMock("complete"),
        board=FakeAdminBoard(),
        clock=lambda: NOW,
    )
    await intake(
        TicketTurn(
            "ticket",
            "TEST-1",
            jira.status,
            {
                "ticket_text": "Fixture bug",
                "description": "Fixture description",
                "events": [],
            },
            1000,
        )
    )
    _, state = await current(session_factory)
    assert not state.gate_ready
    sender = JiraOutboxSender(
        session_factory=session_factory,
        config=settings,
        clients={"acme": client_for(jira)},
        clock=lambda: NOW,
    )
    await sender.run_once()
    _, state = await current(session_factory)
    assert state.gate_ready
    assert len(jira.writes) == 2 and jira.writes[0].url.path.endswith("/comment")
    assert digest(scope) in jira.writes[0].content.decode()
    assert jira.writes[1].url.path.endswith("/transitions")
    activity, supervisor, process, _, _ = services(session_factory, jira)
    await activity.read_ticket("ticket", force_jira=True)
    jira.approve()
    await supervisor.reconcile("ticket")
    assert process.starts == [scope.job.run.execution_key]


@pytest.mark.parametrize("action", ["question", "complete"])
@pytest.mark.parametrize("channel", ["jira_comments", "switch"])
@pytest.mark.parametrize("missing", [None, "source", "destination", "transition"])
async def test_intake_fences_missing_frozen_mappings_through_turn(
    session_factory, action, channel, missing
):
    await seed(session_factory, ready=False)
    jira = DeliveryJira()
    jira.status = "Outside frozen workflow" if missing == "source" else "In Progress"
    destination = (
        "Waiting for input" if action == "question" else "Waiting for approval"
    )
    edge = "progress->input" if action == "question" else "progress->approval"
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, "ticket")
        state = delivery_state(ticket)
        statuses = [
            *state.statuses,
            WorkflowMapping(name="Waiting for input", jira_id="input"),
        ]
        transitions = [
            *state.transitions,
            WorkflowMapping(name="progress->input", jira_id="request-input"),
        ]
        ticket.workflow_state = {
            **ticket.workflow_state,
            "statuses": [
                item.model_dump(mode="json")
                for item in statuses
                if missing != "destination" or item.name != destination
            ],
            "transitions": [
                item.model_dump(mode="json")
                for item in transitions
                if missing != "transition" or item.name != edge
            ],
        }
        ticket.status = jira.status
        ticket.intake_state = {"phase": "intake"}
        ticket.wait_channel = channel
        ticket.room_claimed = True
        ticket.card_event_id = "fixture-card"
        ticket.agent_state = "wake_requested"
        await session.commit()
    activity, _, process, forge, _ = services(session_factory, jira, enabled=False)
    intake = IntakeOrchestrator(
        session_factory=session_factory,
        config=_worker_config(
            jira_worker_enabled_projects={"acme": ["TEST"]},
            jira_worker_delivery_execution_enabled=False,
        ),
        model=ModelMock(action),
        board=FakeAdminBoard(),
        clock=lambda: NOW,
    )
    assert await activity.run_one_turn(intake) == "ticket"
    ticket, _ = await current(session_factory)
    assert ticket.tokens_used == 10
    async with session_factory() as session:
        commands = list((await session.scalars(select(JiraWorkerOutbox))).all())
        job = await session.scalar(select(JiraWorkerJob))
        assert job.status == "done"
    if missing is None:
        assert ticket.worker_parked_reason is None
        assert ticket.intake_state["phase"] == (
            "question" if action == "question" else "approval"
        )
        intake_commands = [
            command
            for command in commands
            if "intake_generation" in (command.payload or {})
        ]
        assert len(intake_commands) == 2
        assert any(command.command == "transition_by_id" for command in intake_commands)
    else:
        assert "transition fence changed" in ticket.worker_parked_reason
        assert ticket.agent_state == "parked"
        assert ticket.intake_state == {"phase": "intake"}
        assert commands == []
    assert not jira.writes and not process.starts and not forge.pushes


async def test_gate_without_displayed_summary_never_starts_work(session_factory):
    await seed(session_factory, ready=False)
    jira = DeliveryJira()
    jira.approve()
    _, supervisor, process, _, _ = services(session_factory, jira)
    await supervisor.reconcile("ticket")
    ticket, _ = await current(session_factory)
    assert process.starts == [] and ticket.worker_parked_reason


async def test_room_approval_text_never_starts_work(session_factory):
    await seed(session_factory)
    rooms = ThreadRooms()
    rooms.human_senders.add("reporter-user")
    rooms.messages = [{**message(1), "body": "Approve. Start reproduction."}]
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, "ticket")
        ticket.room_id = "ticket-room"
        ticket.card_event_id = "ticket-card"
        ticket.wait_channel = "switch"
        ticket.reporter_switch_user_id = "reporter-user"
        await session.commit()
    _, supervisor, process, _, _ = services(
        session_factory, DeliveryJira(), rooms=rooms
    )
    await supervisor.reconcile("ticket")
    ticket, state = await current(session_factory)
    assert process.starts == [] and state.approvals == ()
    assert state.step == "approve_scope" and ticket.thread_cursor == 1


async def test_missing_reproduction_parks_and_never_starts_fix(session_factory):
    scope = await seed(session_factory)
    jira = DeliveryJira()
    jira.approve()
    _, supervisor, process, _, _ = services(session_factory, jira)
    await supervisor.reconcile("ticket")
    process.complete(scope.job, evidence=False)
    await supervisor.reconcile("ticket")
    await supervisor.reconcile("ticket")
    ticket, _ = await current(session_factory)
    assert ticket.worker_parked_reason and process.starts == [
        scope.job.run.execution_key
    ]


async def test_wrong_base_failure_prevents_fix(session_factory):
    scope = await seed(session_factory)
    jira = DeliveryJira()
    jira.approve()
    _, supervisor, process, _, _ = services(session_factory, jira)
    await supervisor.reconcile("ticket")
    process.complete(scope.job)
    observation = process.tasks[scope.job.run.execution_key]
    process.tasks[scope.job.run.execution_key] = observation.model_copy(
        update={"handoff": handoff(scope.job, base=HEAD)}
    )
    await supervisor.reconcile("ticket")
    ticket, _ = await current(session_factory)
    assert ticket.worker_parked_reason and process.starts == [
        scope.job.run.execution_key
    ]


async def test_disabled_and_unprovisioned_execution_never_starts(session_factory):
    await seed(session_factory)
    jira = DeliveryJira()
    jira.approve()
    _, supervisor, process, _, _ = services(session_factory, jira, enabled=False)
    await supervisor.reconcile("ticket")
    assert process.starts == []
    with pytest.raises(ValueError, match="explicitly provisioned"):
        JiraWorkerScheduler(session_factory=session_factory, config=config())


@pytest.mark.parametrize("operation", ["push", "create"])
async def test_uncertain_publication_discovers_existing_side_effects(
    session_factory, operation
):
    jira = DeliveryJira()
    _, _, _, forge, publisher = await candidate(session_factory, jira)
    setattr(forge, "lose_" + operation, True)
    with pytest.raises(PublicationUncertain):
        await publisher.publish("ticket")
    await publisher.publish("ticket")
    await publisher.publish("ticket")
    _, state = await current(session_factory)
    assert state.publication.status == "published"
    assert len(forge.pushes) == len(forge.creates) == 1


@pytest.mark.parametrize("status", ["uncertain", "pushing"])
async def test_uncertain_push_retries_only_after_fresh_absence(session_factory, status):
    jira = DeliveryJira()
    scope, _, _, forge, publisher = await candidate(session_factory, jira)
    forge.fail_push = True
    with pytest.raises(PublicationUncertain, match="push requires reconciliation"):
        await publisher.publish("ticket")
    _, state = await current(session_factory)
    assert state.publication.status == "uncertain"
    assert state.publication.uncertain_operation == "push"
    assert forge.head is None and forge.creates == []
    if status == "pushing":
        async with session_factory() as session:
            ticket = await session.get(JiraWorkerTicket, "ticket")
            ticket.workflow_state = {
                **ticket.workflow_state,
                "publication": {
                    **ticket.workflow_state["publication"],
                    "status": "pushing",
                    "uncertain_operation": None,
                },
            }
            await session.commit()
    reads = len(forge.branch_reads)
    await publisher.publish("ticket")
    assert forge.branch_reads[reads] is None
    await publisher.publish("ticket")
    _, state = await current(session_factory)
    assert state.publication.status == "published"
    assert forge.pushes == [(URL, scope.branch, HEAD)] * 2
    assert forge.head == HEAD and forge.creates == ["ticket/1/fix/1"]


@pytest.mark.parametrize("change", ["remote_head", "candidate_head", "authorization"])
async def test_uncertain_push_retry_preserves_publication_fences(
    session_factory, change
):
    jira = DeliveryJira()
    _, _, _, forge, publisher = await candidate(session_factory, jira)
    forge.fail_push = True
    with pytest.raises(PublicationUncertain):
        await publisher.publish("ticket")
    if change == "remote_head":
        forge.head = "f" * 40
    elif change == "candidate_head":
        async with session_factory() as session:
            ticket = await session.get(JiraWorkerTicket, "ticket")
            ticket.workflow_state = {
                **ticket.workflow_state,
                "candidate_head": "f" * 40,
                "candidate": {
                    **ticket.workflow_state["candidate"],
                    "head": "f" * 40,
                    "fix_commit": "f" * 40,
                    "checks": [
                        {**check, "commit": "f" * 40}
                        for check in ticket.workflow_state["candidate"]["checks"]
                    ],
                },
            }
            await session.commit()
    else:
        jira.change("approver")
        jira.histories[-1]["items"] = [{"field": "summary"}]
    with pytest.raises(ValueError):
        await publisher.publish("ticket")
    assert forge.pushes == [(URL, "switch/ticket/1", HEAD)]
    assert forge.creates == []


async def test_human_action_after_push_fences_request_creation(session_factory):
    jira = DeliveryJira()
    _, _, _, forge, publisher = await candidate(session_factory, jira)
    original = forge.push_new_branch

    async def interrupted(*args):
        await original(*args)
        jira.change("approver")
        jira.histories[-1]["items"] = [{"field": "summary"}]

    forge.push_new_branch = interrupted
    with pytest.raises(ValueError, match="authorization"):
        await publisher.publish("ticket")
    assert len(forge.pushes) == 1 and forge.creates == []


async def test_transient_observation_lock_defers_without_start_or_park(
    session_factory, monkeypatch
):
    await seed(session_factory)
    jira = DeliveryJira()
    jira.approve()
    activity, supervisor, process, _, _ = services(session_factory, jira)

    async def unavailable(*args, **kwargs):
        return False

    monkeypatch.setattr(activity, "read_ticket", unavailable)
    await supervisor.reconcile("ticket")
    ticket, _ = await current(session_factory)
    assert process.starts == [] and ticket.worker_parked_reason is None


async def test_human_done_cancels_a_live_execution(session_factory):
    scope = await seed(session_factory)
    jira = DeliveryJira()
    jira.approve()
    _, supervisor, process, _, _ = services(session_factory, jira)
    await supervisor.reconcile("ticket")
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, "ticket")
        ticket.status = "Done"
        await session.commit()
    await supervisor.reconcile("ticket")
    assert process.cancelled == [scope.job.run.execution_key]
    assert len(process.starts) == 1


async def test_publication_defers_without_fresh_authorization(
    session_factory, monkeypatch
):
    jira = DeliveryJira()
    _, _, _, forge, publisher = await candidate(session_factory, jira)
    original = publisher._authorize

    async def unavailable(ticket_id):
        return False

    monkeypatch.setattr(publisher, "_authorize", unavailable)
    with pytest.raises(RuntimeError, match="deferred"):
        await publisher.publish("ticket")
    assert forge.pushes == forge.creates == []
    monkeypatch.setattr(publisher, "_authorize", original)
    await publisher.publish("ticket")
    assert len(forge.pushes) == len(forge.creates) == 1


async def test_existing_human_branch_is_not_changed(session_factory):
    jira = DeliveryJira()
    _, _, _, forge, publisher = await candidate(session_factory, jira)
    forge.head = "f" * 40
    with pytest.raises(ValueError, match="another head"):
        await publisher.publish("ticket")
    assert forge.pushes == forge.creates == []


@pytest.mark.parametrize(
    "mutate", ["generation", "step", "gate", "transition", "source"]
)
async def test_exact_transition_fences_and_identifiers(session_factory, mutate):
    scope = await seed(session_factory)
    jira = DeliveryJira()
    jira.status = "In Progress"
    settings = config()
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, "ticket")
        state = delivery_state(ticket)
        command = ExactTransition(
            generation=1,
            pack_digest=scope.job.pack_digest,
            step="approve_scope",
            gate_id=state.gate.id,
            candidate_sha=None,
            transition_id="request-approval",
            source_status_id="progress",
            target_status_id="approval",
            target_status_name="Waiting for approval",
        )
        row_id = await enqueue_jira_command(
            session,
            config=settings,
            ticket=ticket,
            command_key="exact",
            command="transition_by_id",
            data=command.model_dump(mode="json"),
            based_updated=BASE,
            based_changelog_id="",
        )
        (await session.get(JiraWorkerOutbox, row_id)).due_at = NOW
        await session.commit()
        if mutate in {"generation", "step", "gate"}:
            if mutate == "generation":
                ticket.agent_generation += 1
            else:
                data = ticket.workflow_state.copy()
                data["step" if mutate == "step" else "gate"] = (
                    "reproduce" if mutate == "step" else None
                )
                ticket.workflow_state = data
            await session.commit()
        elif mutate == "source":
            jira.status = "Waiting for approval"
        elif mutate == "transition":

            async def wrong(request):
                if request.url.path.endswith("/transitions"):
                    return httpx.Response(
                        200,
                        json={
                            "transitions": [
                                {
                                    "id": "different",
                                    "to": {
                                        "id": "approval",
                                        "name": "Waiting for approval",
                                    },
                                }
                            ]
                        },
                    )
                return await jira.handle(request)

            client = client_for(jira)
            client._client = httpx.AsyncClient(transport=httpx.MockTransport(wrong))
        sender = JiraOutboxSender(
            session_factory=session_factory,
            config=settings,
            clients={"acme": client if mutate == "transition" else client_for(jira)},
            clock=lambda: NOW,
        )
        await sender.run_once()
        await session.refresh(await session.get(JiraWorkerOutbox, row_id))
    assert jira.writes == []


async def test_exact_transition_sends_frozen_id_once(session_factory):
    scope = await seed(session_factory)
    jira = DeliveryJira()
    jira.status = "In Progress"
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, "ticket")
        row_id = await enqueue_jira_command(
            session,
            config=config(),
            ticket=ticket,
            command_key="exact",
            command="transition_by_id",
            data=ExactTransition(
                generation=1,
                pack_digest=scope.job.pack_digest,
                step="approve_scope",
                gate_id="scope-gate",
                candidate_sha=None,
                transition_id="request-approval",
                source_status_id="progress",
                target_status_id="approval",
                target_status_name="Waiting for approval",
            ).model_dump(mode="json"),
            based_updated=BASE,
            based_changelog_id="",
        )
        await session.commit()
    async with session_factory() as session:
        (await session.get(JiraWorkerOutbox, row_id)).due_at = NOW
        await session.commit()
    sender = JiraOutboxSender(
        session_factory=session_factory,
        config=config(),
        clients={"acme": client_for(jira)},
        clock=lambda: NOW,
    )
    await sender.run_once()
    await sender.run_once()
    assert len(jira.writes) == 1


def git(path, *args):
    return (
        subprocess.check_output(
            [
                "git",
                "-C",
                str(path),
                "-c",
                "user.name=Fixture",
                "-c",
                "user.email=fixture@example.invalid",
                "-c",
                "commit.gpgsign=false",
                *args,
            ],
            stderr=subprocess.DEVNULL,
        )
        .decode()
        .strip()
    )


async def test_sealed_bare_repository_verifies_commit_order_and_actual_paths(
    session_factory, tmp_path
):
    work = tmp_path / "work"
    work.mkdir()
    git(work, "init", "-q")
    (work / "src").mkdir()
    (work / "src/operation.py").write_text("broken\n")
    git(work, "add", ".")
    git(work, "commit", "-qm", "base")
    base = git(work, "rev-parse", "HEAD")
    scope = await seed(session_factory, base_sha=base)
    (work / "tests").mkdir()
    (work / "tests/test_operation.py").write_text("regression\n")
    git(work, "add", ".")
    git(work, "commit", "-qm", "regression")
    regression = git(work, "rev-parse", "HEAD")
    (work / "src/operation.py").write_text("fixed\n")
    git(work, "commit", "-qam", "fix")
    head = git(work, "rev-parse", "HEAD")
    bare = tmp_path / "objects.git"
    git(tmp_path, "clone", "--bare", str(work), str(bare))
    evidence = handoff(fix_job(scope)).model_copy(
        update={"head": head, "regression_commit": regression, "fix_commit": head}
    )
    objects = GitCandidateObjects(bare)
    await objects.verify(scope, evidence)
    with pytest.raises(ValueError, match="changed paths"):
        await objects.verify(
            scope, evidence.model_copy(update={"changed_paths": ("src/operation.py",)})
        )
    with pytest.raises(ValueError, match="verification failed"):
        await objects.verify(
            scope,
            evidence.model_copy(
                update={
                    "regression_commit": head,
                    "fix_commit": regression,
                    "head": regression,
                }
            ),
        )


async def test_native_envelopes_keep_fresh_context_and_separate_evidence(
    session_factory, lifecycle_lab, monkeypatch
):
    lab = lifecycle_lab
    monkeypatch.setenv("GH_TOKEN", "PLACEHOLDER_FORGE_CREDENTIAL")
    pack = FrozenPack(snapshot=lab.pack, digest=digest(lab.pack))
    scope = await seed(session_factory, base_sha=lab.base_sha, pack=pack)
    reproduce = ExecutionEnvelope(scope=scope, reproduction=None)
    fixed = fix_job(scope)
    repair = ExecutionEnvelope(
        scope=scope, reproduction=handoff(scope.job, base=lab.base_sha)
    )
    git(lab.copy._project, "checkout", "-b", scope.branch)
    git(lab.copy._project, "remote", "add", "origin", URL)
    for job, envelope in ((scope.job, reproduce), (fixed, repair)):
        observation = await lab.copy.start(
            job, lab.pack, scope.reproduction_reservation, 60, envelope=envelope
        )
        assert observation.status == "running"
        home = lab.copy.location(job)
        task = home / "data" / lab.copy.task_id(job)
        prepared = PreparedExecutionEnvelope.model_validate_json(
            (task / "envelope.json").read_text()
        )
        assert (
            prepared.scope == envelope.scope
            and prepared.reproduction == envelope.reproduction
        )
        assert prepared.worktree_path == str(lab.copy._project)
        assert prepared.branch == scope.branch
        assert prepared.report_path == str(task / "result.json")
        assert (
            json.loads((task / "handoff-schema.json").read_text())["title"] == "Handoff"
        )
        assert "GH_TOKEN" not in json.loads((home / "environment.json").read_text())
        assert PROFILE in (task / "brief.md").read_text()
        assert "handoff.json" in (task / "brief.md").read_text()
        assert "PLACEHOLDER_FORGE_CREDENTIAL" not in (task / "brief.md").read_text()
    assert lab.copy.location(scope.job) != lab.copy.location(fixed)
    with pytest.raises(ValueError, match="requires reproduction"):
        ExecutionEnvelope(scope=scope, reproduction=None).validate_job(fixed)


async def test_uncertain_creation_never_replays_when_request_is_not_visible(
    session_factory,
):
    jira = DeliveryJira()
    _, _, _, forge, publisher = await candidate(session_factory, jira)
    forge.lose_create = True
    with pytest.raises(PublicationUncertain):
        await publisher.publish("ticket")
    requests = forge.requests
    forge.requests = ()
    await publisher.publish("ticket")
    await publisher.publish("ticket")
    _, state = await current(session_factory)
    assert state.publication.status == "uncertain"
    assert len(forge.creates) == 1
    forge.requests = requests
    await publisher.publish("ticket")
    _, state = await current(session_factory)
    assert state.publication.status == "published"


async def test_github_transport_scopes_git_and_api_without_worker_credentials(
    session_factory, monkeypatch, tmp_path
):
    scope = await seed(session_factory)
    calls, git_calls = [], []
    remote = None
    requests = []

    def draft_body():
        return {
            "html_url": "https://example.invalid/team/project/pull/1",
            "head": {
                "ref": scope.branch,
                "sha": HEAD,
                "repo": {"full_name": "team/project"},
            },
            "base": {"ref": "main"},
            "draft": True,
            "state": "open",
        }

    async def handle(request):
        calls.append(request)
        assert request.headers["Authorization"] == "Bearer PLACEHOLDER_FORGE_CREDENTIAL"
        assert request.url.host == "api.example.invalid"
        if "/git/ref/heads/" in request.url.path:
            return (
                httpx.Response(200, json={"object": {"sha": remote}})
                if remote
                else httpx.Response(404)
            )
        if request.method == "POST":
            body = json.loads(request.content)
            assert (
                body["draft"]
                and body["head"] == scope.branch
                and body["base"] == "main"
            )
            requests.append(draft_body())
            return httpx.Response(201, json=requests[-1])
        return httpx.Response(200, json=requests)

    objects = GitCandidateObjects(tmp_path / "trusted-objects.git")

    async def run_git(*args, authorization=None):
        nonlocal remote
        git_calls.append((args, authorization))
        remote = HEAD
        return b""

    monkeypatch.setattr(objects, "_git", run_git)
    async with httpx.AsyncClient(
        base_url="https://api.example.invalid", transport=httpx.MockTransport(handle)
    ) as client:
        forge = GitHubDraftForge(
            scope=scope,
            objects=objects,
            client=client,
            authorization="Bearer PLACEHOLDER_FORGE_CREDENTIAL",
        )
        await forge.push_new_branch(URL, scope.branch, HEAD)
        request = await forge.create_draft(
            URL, scope.branch, "main", HEAD, "ticket/1/fix/1"
        )
        assert (await forge.find_requests(URL, scope.branch)) == (request,)
        with pytest.raises(ValueError, match="deletion is forbidden"):
            await forge.push_new_branch(URL, scope.branch, "")
        with pytest.raises(ValueError, match="existing branch"):
            await forge.push_new_branch(URL, scope.branch, HEAD)
        with pytest.raises(ValueError, match="approved repository or branch"):
            await forge.push_new_branch(URL, "human/branch", HEAD)
    argv, auth = git_calls[0]
    assert argv == (
        "push",
        "--porcelain",
        "--no-verify",
        "--",
        URL + ".git",
        HEAD + ":refs/heads/" + scope.branch,
    )
    assert auth == "Bearer PLACEHOLDER_FORGE_CREDENTIAL"
    assert len(git_calls) == 1


async def test_github_transport_refuses_credential_redirects(session_factory, tmp_path):
    scope = await seed(session_factory)
    seen = []

    async def handle(request):
        seen.append(request)
        return httpx.Response(
            302, headers={"Location": "https://other.example.invalid/"}
        )

    async with httpx.AsyncClient(
        base_url="https://api.example.invalid", transport=httpx.MockTransport(handle)
    ) as client:
        forge = GitHubDraftForge(
            scope=scope,
            objects=GitCandidateObjects(tmp_path / "trusted-objects.git"),
            client=client,
            authorization="Bearer PLACEHOLDER_FORGE_CREDENTIAL",
        )
        with pytest.raises(ValueError, match="redirects"):
            await forge.branch_head(URL, scope.branch)
    assert len(seen) == 1
