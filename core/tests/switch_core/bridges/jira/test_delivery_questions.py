"""Delivery waits through public supervisor, turn, sender and admin interfaces."""

from __future__ import annotations

import asyncio
import json
from datetime import timedelta
from decimal import Decimal
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import select

from switch_core.bridges.jira.accounting import CapacityUnavailable, live_sessions
from switch_core.bridges.jira.activity import TicketActivity
from switch_core.bridges.jira.admin import release_parked_ticket, sync_admin_card
from switch_core.bridges.jira.budget import BudgetDelta, BudgetRequest, extend_budget
from switch_core.bridges.jira.contracts import Question, Result, digest
from switch_core.bridges.jira.delivery import DeliverySupervisor, role_job
from switch_core.bridges.jira.firstmate_copy import SecretGuard, TaskObservation
from switch_core.bridges.jira.firstmate_jobs import FirstmateJobs
from switch_core.bridges.jira.intake import IntakeDecision, IntakeOrchestrator
from switch_core.bridges.jira.outbox import JiraOutboxSender, park_ticket
from switch_core.bridges.jira.publisher import DraftPublisher
from switch_core.bridges.jira.state import WorkflowMapping
from switch_core.bridges.jira.target import delivery_state, record_repository_triage
from switch_core.config import JiraWorkerCredentials, JiraWorkerLanguagePricing
from switch_core.db.models import (
    JiraWorkerEvent,
    JiraWorkerJob,
    JiraWorkerOutbox,
    JiraWorkerTicket,
    User,
)
from tests.switch_core.bridges.jira.test_activity import message
from tests.switch_core.bridges.jira.test_admin import FakeAdminBoard
from tests.switch_core.bridges.jira.test_delivery import (
    NOW,
    DeliveryJira,
    DeliveryProcess,
    ForgeDouble,
    config,
    handoff,
    seed,
)
from tests.switch_core.bridges.jira.test_intake import IntakeRooms
from tests.switch_core.bridges.jira.test_outbox import client_for


class RelayJira(DeliveryJira):
    def __init__(self):
        super().__init__()
        self.fields["project"] = {"key": "TEST"}

    routes = {
        "request-input": ("input", "Waiting for input"),
        "resume": ("progress", "In Progress"),
        "request-approval": ("approval", "Waiting for approval"),
        "block": ("blocked", "BLOCKED"),
    }

    async def handle(self, request):
        if request.url.path.endswith("/transitions"):
            if request.method == "GET":
                return httpx.Response(
                    200,
                    json={
                        "transitions": [
                            {"id": key, "to": {"id": value[0], "name": value[1]}}
                            for key, value in self.routes.items()
                        ]
                    },
                )
            self.writes.append(request)
            transition = json.loads(request.content)["transition"]["id"]
            old = self.status
            target, self.status = self.routes[transition]
            self.change("worker-account")
            self.histories[-1]["items"] = [
                {
                    "field": "status",
                    "fromString": old,
                    "toString": self.status,
                    "from": "input" if old == "Waiting for input" else "progress",
                    "to": target,
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
                body["fields"].update(
                    {
                        key: value
                        for key, value in self.fields.items()
                        if key.startswith("customfield_")
                    }
                )
                body["fields"]["status"]["id"] = next(
                    (
                        value[0]
                        for value in self.routes.values()
                        if value[1] == self.status
                    ),
                    "progress",
                )
                return httpx.Response(200, json=body)
        return response


class RelayModel:
    def __init__(self):
        self.calls = []
        self.action = "unrelated"

    async def relay(self, data, token_budget):
        self.calls.append(data)
        return IntakeDecision(
            "question" if not data["replies"] else self.action,
            "Which browser version shows the failure?",
            50,
        )

    async def decide(self, data, token_budget):
        return IntakeDecision(
            "complete", "The reporter supplied the changed scope.", 50
        )


class QueueProcess(DeliveryProcess):
    async def start(self, job, pack, reservation, estimate_seconds, *, envelope=None):
        key = job.run.execution_key
        if key not in self.tasks:
            self.starts.append(key)
            self.tasks[key] = TaskObservation(task_id=key, status="running")
            self.envelopes[key] = envelope
        return self.tasks[key]


async def setup(sessions, *, channel="switch"):
    scope = await seed(sessions)
    settings = config().model_copy(
        update={
            "jira_worker_delivery_language_pricing": JiraWorkerLanguagePricing(
                max_usd_per_token=Decimal("0.00001"), price_source="fixture-upper-bound"
            ),
            "jira_worker_credentials": {
                "acme": JiraWorkerCredentials(
                    base_url="https://example.invalid",
                    email="worker@example.invalid",
                    api_token="PLACEHOLDER",
                )
            },
        }
    )
    clock = [NOW]
    jira, rooms, process, board = (
        RelayJira(),
        IntakeRooms(),
        QueueProcess(),
        FakeAdminBoard(),
    )
    activity = TicketActivity(
        session_factory=sessions,
        config=settings,
        clients={"acme": client_for(jira)},
        rooms=rooms,
        clock=lambda: clock[0],
    )
    jobs = FirstmateJobs(
        session_factory=sessions,
        delivery_copy=process,
        secrets=SecretGuard(forbidden_values=()),
        clock=lambda: clock[0],
    )

    async def authorize(ticket_id):
        return await activity.read_ticket(ticket_id, force_jira=True)

    publisher = DraftPublisher(
        session_factory=sessions, forge=ForgeDouble(), authorize=authorize
    )
    supervisor = DeliverySupervisor(
        session_factory=sessions,
        config=settings,
        activity=activity,
        jobs=jobs,
        publisher=publisher,
        clock=lambda: clock[0],
    )
    model = RelayModel()
    orchestrator = IntakeOrchestrator(
        session_factory=sessions,
        config=settings,
        model=model,
        board=board,
        clock=lambda: clock[0],
    )
    sender = JiraOutboxSender(
        session_factory=sessions,
        config=settings,
        clients={"acme": client_for(jira)},
        rooms=rooms,
        clock=lambda: clock[0],
    )
    async with sessions() as session:
        ticket = await session.get(JiraWorkerTicket, "ticket")
        state = delivery_state(ticket)
        data = state.model_dump(mode="json")
        data["statuses"].extend(
            [
                WorkflowMapping(name="Waiting for input", jira_id="input").model_dump(),
                WorkflowMapping(name="BLOCKED", jira_id="blocked").model_dump(),
            ]
        )
        data["transitions"].extend(
            WorkflowMapping(name=name, jira_id=value).model_dump()
            for name, value in (
                ("progress->input", "request-input"),
                ("approval->input", "request-input"),
                ("input->progress", "resume"),
                ("input->approval", "request-approval"),
                ("input->blocked", "block"),
            )
        )
        ticket.workflow_state = data
        ticket.room_id = "room-1"
        ticket.card_event_id = "card-room-1"
        ticket.reporter_switch_user_id = "reporter-user"
        ticket.room_claimed = True
        ticket.wait_channel = channel
        await session.commit()
    jira.approve()
    await supervisor.reconcile("ticket")
    return (
        scope,
        settings,
        clock,
        jira,
        rooms,
        process,
        board,
        activity,
        jobs,
        supervisor,
        model,
        orchestrator,
        sender,
    )


async def ask(process, job):
    process.complete(job)
    usage = process.tasks[job.run.execution_key].result.usage
    process.tasks[job.run.execution_key] = TaskObservation(
        task_id=job.run.execution_key,
        status="running",
        result=Result(
            run=job.run,
            input_digest=digest(job),
            status="needs_input",
            question=Question(
                run=job.run,
                input_digest=digest(job),
                id=f"browser-{job.run.input_revision}",
                checkpoint_digest="e" * 64,
                text="Which browser version shows the failure?",
                evidence=(
                    "The approved failing check needs the original browser version.",
                ),
            ),
            usage=usage,
        ),
    )


async def snapshot(sessions):
    async with sessions() as session:
        ticket = await session.get(JiraWorkerTicket, "ticket")
        return ticket, delivery_state(ticket)


async def send_all(sender):
    for _ in range(4):
        await sender.run_once()


async def open_question(sessions, *, channel="switch"):
    lab = await setup(sessions, channel=channel)
    scope, _, _, _, _, process, _, activity, _, supervisor, _, orchestrator, sender = (
        lab
    )
    await ask(process, scope.job)
    await supervisor.reconcile("ticket")
    ticket, state = await snapshot(sessions)
    assert state.question is not None and ticket.agent_state == "wake_requested"
    assert await activity.run_one_turn(orchestrator) == "ticket"
    await send_all(sender)
    return lab


@pytest.mark.parametrize("channel", ["switch", "jira_comments"])
async def test_no_mention_answer_resumes_only_its_checkpoint_once(
    session_factory, channel
):
    lab = await open_question(session_factory, channel=channel)
    (
        scope,
        _,
        _,
        jira,
        rooms,
        process,
        _,
        activity,
        _,
        supervisor,
        model,
        orchestrator,
        sender,
    ) = lab
    model.action = "answer"
    if channel == "switch":
        rooms.messages.append({**message(1), "body": "Browser version 123"})
    else:
        jira.change("reporter-account", comment=True)
        jira.comments[-1]["body"] = "Browser version 123"
    assert await activity.run_one_turn(orchestrator) == "ticket"
    ticket, state = await snapshot(session_factory)
    assert state.answer.classification == "answer"
    assert state.answer.actor_account_id == "reporter-account"
    assert ticket.wait_anchor is None
    await supervisor.reconcile("ticket")
    assert len(process.starts) == 1  # The exact Resume transition must finish first.
    await send_all(sender)
    await supervisor.reconcile("ticket")
    resumed = role_job(scope, "reproduce", 1, 1)
    assert process.starts == [scope.job.run.execution_key, resumed.run.execution_key]
    assert (
        process.envelopes[
            resumed.run.execution_key
        ].resume_answer.question.checkpoint_digest
        == "e" * 64
    )
    await activity.read_ticket("ticket", force_jira=True)
    await supervisor.reconcile("ticket")
    assert process.starts.count(resumed.run.execution_key) == 1
    process.complete(resumed)
    await supervisor.reconcile("ticket")
    _, state = await snapshot(session_factory)
    assert state.step == "fix" and state.reproduction.input_digest == digest(resumed)
    async with session_factory() as session:
        replies = list(
            await session.scalars(
                select(JiraWorkerEvent).where(
                    JiraWorkerEvent.event_kind == "delivery_answer"
                )
            )
        )
        assert len(replies) == 1


@pytest.mark.parametrize(
    "action,text", [("unrelated", "hello"), ("clarify", "Which browser do you mean?")]
)
async def test_unrelated_or_clarifying_reply_keeps_the_wait_and_authority(
    session_factory, action, text
):
    lab = await open_question(session_factory)
    (
        scope,
        _,
        _,
        _,
        rooms,
        process,
        _,
        activity,
        _,
        supervisor,
        model,
        orchestrator,
        sender,
    ) = lab
    before, state_before = await snapshot(session_factory)
    model.action = action
    rooms.messages.append({**message(1), "body": text})
    assert await activity.run_one_turn(orchestrator) == "ticket"
    await send_all(sender)
    await supervisor.reconcile("ticket")
    after, state = await snapshot(session_factory)
    assert state.question == state_before.question and state.answer is None
    assert after.wait_anchor == before.wait_anchor
    assert (
        state.scope == state_before.scope and state.repository == scope.job.repository
    )
    assert state.budget == state_before.budget
    assert process.starts == [scope.job.run.execution_key]


async def test_scope_change_invalidates_evidence_and_requires_new_triage_and_approval(
    session_factory,
):
    lab = await open_question(session_factory)
    _, _, _, _, rooms, process, _, activity, _, supervisor, model, orchestrator, _ = lab
    _, before = await snapshot(session_factory)
    model.action = "scope_change"
    rooms.messages.append(
        {
            **message(1),
            "body": "Support a different browser and change the expected behavior.",
        }
    )
    await activity.run_one_turn(orchestrator)
    await supervisor.reconcile("ticket")
    ticket, state = await snapshot(session_factory)
    assert ticket.agent_generation == 2 and state.step == "intake"
    assert (
        state.scope is None
        and state.approvals == ()
        and state.repository.outcome == "unknown"
    )
    assert state.known_estimated_usd > before.known_estimated_usd
    assert len(process.starts) == 1


async def test_restart_preserves_question_calendar_reminder_and_existing_card(
    session_factory,
):
    lab = await open_question(session_factory)
    _, settings, clock, _, _, process, board, _, _, supervisor, _, _, _ = lab
    ticket, before = await snapshot(session_factory)
    anchor = ticket.wait_anchor
    # Start inside the configured work window, then cross exactly two work hours.
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, "ticket")
        ticket.wait_anchor = NOW.replace(hour=9)
        await session.commit()
    clock[0] = NOW.replace(hour=11)
    for _ in range(2):
        await sync_admin_card(
            session_factory,
            engine=session_factory.kw["bind"],
            board=board,
            config=settings,
            ticket_id="ticket",
            now=clock[0],
        )
    ticket, state = await snapshot(session_factory)
    card_id = ticket.admin_card_event_id
    assert state.question == before.question and "reminder" in ticket.wait_fired
    # Replace all worker-side objects while the PostgreSQL and chat stores remain.
    jira, rooms = lab[3], lab[4]
    restarted_activity = TicketActivity(
        session_factory=session_factory,
        config=settings,
        clients={"acme": client_for(jira)},
        rooms=rooms,
        clock=lambda: clock[0],
    )
    restarted_jobs = FirstmateJobs(
        session_factory=session_factory,
        delivery_copy=process,
        secrets=SecretGuard(forbidden_values=()),
        clock=lambda: clock[0],
    )

    async def authorize(ticket_id):
        return await restarted_activity.read_ticket(ticket_id, force_jira=True)

    restarted = DeliverySupervisor(
        session_factory=session_factory,
        config=settings,
        activity=restarted_activity,
        jobs=restarted_jobs,
        publisher=DraftPublisher(
            session_factory=session_factory, forge=ForgeDouble(), authorize=authorize
        ),
        clock=lambda: clock[0],
    )
    await restarted.reconcile("ticket")
    await sync_admin_card(
        session_factory,
        engine=session_factory.kw["bind"],
        board=board,
        config=settings,
        ticket_id="ticket",
        now=clock[0],
    )
    ticket, _ = await snapshot(session_factory)
    assert ticket.admin_card_event_id == card_id
    async with session_factory() as session:
        reminders = list(
            await session.scalars(
                select(JiraWorkerOutbox).where(
                    JiraWorkerOutbox.command == "wait_reminder"
                )
            )
        )
        assert len(reminders) == 1
    assert len(process.starts) == 1 and anchor is not None


async def test_delivery_question_limit_is_separate_from_intake_limit(session_factory):
    lab = await setup(session_factory)
    scope, _, _, _, _, process, _, _, _, supervisor, _, _, _ = lab
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, "ticket")
        ticket.workflow_state = {**ticket.workflow_state, "question_count": 5}
        ticket.intake_state = {"phase": "delivery", "questions": 0}
        await session.commit()
    await ask(process, scope.job)
    await supervisor.reconcile("ticket")
    ticket, state = await snapshot(session_factory)
    assert ticket.worker_parked_reason and state.question_count == 5
    assert ticket.intake_state["questions"] == 0


async def clone_queued_ticket(sessions, scope, ticket_id):
    async with sessions() as session:
        ticket, state = await snapshot(sessions)
        session.add(
            JiraWorkerTicket(
                id=ticket_id,
                instance="acme",
                project_key="TEST",
                issue_key=f"TEST-{ticket_id}",
                status="In Progress",
                agent_generation=1,
                workflow_state={
                    **state.model_dump(mode="json"),
                    "scope": None,
                    "question": None,
                    "answer": None,
                    "reserved_tokens": 0,
                    "reserved_estimated_usd": "0",
                    "known_estimated_usd": "0",
                    "unknown_spend": False,
                },
            )
        )
        await session.commit()
    data = scope.job.model_dump(mode="json")
    data["run"].update(ticket_id=ticket_id, execution_key=f"{ticket_id}/1/reproduce/1")
    return type(scope.job).model_validate_json(json.dumps(data))


async def test_global_role_capacity_queues_the_third_ticket(session_factory):
    scope, _, _, _, _, process, _, _, jobs, _, _, _, _ = await setup(session_factory)
    second = await clone_queued_ticket(session_factory, scope, "second")
    third = await clone_queued_ticket(session_factory, scope, "third")
    results = await asyncio.gather(
        *(
            jobs.start(
                job, reservation=scope.reproduction_reservation, estimate_seconds=60
            )
            for job in (second, third)
        ),
        return_exceptions=True,
    )
    assert len(process.starts) == 2
    assert sum(isinstance(item, CapacityUnavailable) for item in results) == 1
    async with session_factory() as session:
        queued = list(
            await session.scalars(
                select(JiraWorkerTicket).where(
                    JiraWorkerTicket.queue_reason.is_not(None)
                )
            )
        )
        assert len(queued) == 1 and queued[0].status == "In Progress"


async def test_combined_intake_and_role_sessions_never_exceed_five(session_factory):
    scope, _, _, _, _, process, _, _, jobs, _, _, _, _ = await setup(session_factory)
    second = await clone_queued_ticket(session_factory, scope, "second")
    async with session_factory() as session:
        for index in range(4):
            session.add(
                JiraWorkerJob(
                    kind="orchestrator_turn",
                    instance="acme",
                    project_key="TEST",
                    status="claimed",
                    due_at=NOW,
                    payload={"ticket_id": f"intake-{index}"},
                )
            )
        await session.commit()
    with pytest.raises(CapacityUnavailable):
        await jobs.start(
            second, reservation=scope.reproduction_reservation, estimate_seconds=60
        )
    async with session_factory() as session:
        assert await live_sessions(session) == 5
    assert len(process.starts) == 1


async def test_progress_is_evidence_based_deduplicated_and_heartbeat_is_not_progress(
    session_factory,
):
    lab = await setup(session_factory)
    scope, _, clock, _, _, process, _, _, _, supervisor, _, _, _ = lab
    job = scope.job
    process.tasks[job.run.execution_key] = TaskObservation(
        task_id=job.run.execution_key, status="running", progress=handoff(job)
    )
    clock[0] += timedelta(minutes=10)
    await supervisor.reconcile("ticket")
    await supervisor.reconcile("ticket")
    async with session_factory() as session:
        notes = list(
            await session.scalars(
                select(JiraWorkerOutbox).where(
                    JiraWorkerOutbox.command_key.like("delivery-progress:%")
                )
            )
        )
        assert len(notes) == 1 and "regression" in notes[0].payload["data"]
    clock[0] += timedelta(minutes=20)
    await supervisor.reconcile("ticket")
    ticket, _ = await snapshot(session_factory)
    assert ticket.worker_parked_reason == "delivery_idle"
    assert process.cancelled


async def test_replayed_check_evidence_does_not_extend_the_idle_deadline(
    session_factory,
):
    lab = await setup(session_factory)
    scope, _, clock, _, _, process, _, _, _, supervisor, _, _, _ = lab
    evidence = handoff(scope.job)
    for minute, exit_code in ((5, 1), (10, 0), (25, 1), (30, 1)):
        clock[0] = NOW + timedelta(minutes=minute)
        progress = evidence.model_copy(
            update={
                "checks": (
                    evidence.checks[0].model_copy(update={"exit_code": exit_code}),
                )
            }
        )
        process.tasks[scope.job.run.execution_key] = TaskObservation(
            task_id=scope.job.run.execution_key,
            status="running",
            progress=progress,
        )
        await supervisor.reconcile("ticket")
    ticket, _ = await snapshot(session_factory)
    assert ticket.worker_parked_reason == "delivery_idle"
    assert process.cancelled


@pytest.mark.parametrize(
    "phase,change",
    [
        ("triage", None),
        ("scope_ready", None),
        ("manual_handoff", None),
        ("approval", "repository"),
        ("approval", "service"),
    ],
)
async def test_phase_aware_release_rereads_target_and_never_replays_approval(
    session_factory, phase, change
):
    (
        scope,
        settings,
        clock,
        jira,
        _,
        process,
        _,
        _,
        jobs,
        supervisor,
        _,
        _,
        sender,
    ) = await setup(session_factory)
    process.complete(scope.job)
    await jobs.get(scope.job.run.execution_key)
    async with session_factory() as session:
        session.add(
            User(id="admin", name="admin", role="admin", email="admin@example.invalid")
        )
        ticket = await session.get(JiraWorkerTicket, "ticket")
        ticket.intake_state = {
            "phase": phase,
            "cycle": 0,
            "history": [{"complete": "An existing intake summary."}],
        }
        jira.fields["customfield_12345"] = {"value": "Bug"}
        ticket.workflow_state = {
            **ticket.workflow_state,
            "service_field": "customfield_12345",
        }
        if phase == "manual_handoff":
            ticket.workflow_state = {
                **ticket.workflow_state,
                "service": "Docs",
                "step": "manual_handoff",
            }
            jira.fields["customfield_12345"] = {"value": "Docs"}
        if phase in {"triage", "scope_ready"}:
            ticket.workflow_state = {
                **ticket.workflow_state,
                "step": "intake",
                "scope": None,
                "approvals": [],
            }
        if phase == "approval":
            ticket.workflow_state = {
                **ticket.workflow_state,
                "step": "approve_scope",
                "gate": ticket.workflow_state["approvals"][0]["gate"],
                "gate_ready": True,
            }
            if change == "repository":
                jira.fields["description"] = (
                    "Repository URL: https://example.invalid/team/replacement"
                )
            else:
                jira.fields["customfield_12345"] = {"value": "Docs"}
            jira.status = "Waiting for approval"
        park_ticket(ticket, "Human Jira action")
        await session.commit()
        generation = ticket.agent_generation
        used = ticket.tokens_used
    request = str(uuid4())
    receipt = await release_parked_ticket(
        session_factory,
        config=settings,
        client=client_for(jira),
        ticket_id="ticket",
        request_id=request,
        admin_user_id="admin",
        now=clock[0],
        release_reason="Recheck the reporter target",
        previous_generation=generation,
    )
    repeated = await release_parked_ticket(
        session_factory,
        config=settings,
        client=client_for(jira),
        ticket_id="ticket",
        request_id=request,
        admin_user_id="admin",
        now=clock[0],
        release_reason="Recheck the reporter target",
        previous_generation=generation,
    )
    assert repeated.id == receipt.id
    await send_all(sender)
    await supervisor.reconcile("ticket")
    ticket, state = await snapshot(session_factory)
    assert ticket.worker_parked_reason is None and ticket.tokens_used == used
    assert state.scope is None and state.approvals == () and state.gate is None
    assert state.repository.outcome == "unknown"
    assert len(process.starts) == 1
    if phase == "approval":
        if change == "repository":
            assert state.repository.supplied_url.endswith("/replacement")
        else:
            assert state.service == "Docs"
        assert jira.status == "Waiting for input"
    if state.service == "Bug":
        async with session_factory() as session:
            repository = scope.job.repository.model_copy(
                update={
                    "supplied_url": state.repository.supplied_url,
                    "canonical_url": state.repository.supplied_url,
                }
            )
            await record_repository_triage(
                session,
                ticket_id="ticket",
                generation=ticket.agent_generation,
                repository=repository,
            )
            await session.commit()
        await supervisor.reconcile("ticket")
        assert (
            len(process.starts) == 1
        )  # A verified repository alone is not scope approval.


async def test_budget_receipt_is_idempotent_and_never_resets_unknown_usage(
    session_factory,
):
    _, settings, clock, _, _, _, _, _, _, _, _, _, _ = await setup(session_factory)
    async with session_factory() as session:
        session.add(
            User(id="admin", name="admin", role="admin", email="admin@example.invalid")
        )
        ticket = await session.get(JiraWorkerTicket, "ticket")
        park_ticket(ticket, "Usage is uncertain")
        generation = ticket.agent_generation
        await session.commit()
    request = BudgetRequest(
        request_id=uuid4(),
        reason="Approve additional bounded investigation",
        previous_generation=generation,
        delta=BudgetDelta(
            ticket_tokens=1000,
            ticket_estimated_usd=Decimal("1"),
            day_estimated_usd=Decimal("2"),
        ),
    )
    receipt, duplicate = await asyncio.gather(
        *(
            extend_budget(
                session_factory,
                config=settings,
                ticket_id="ticket",
                request=request,
                actor="admin",
                now=clock[0],
            )
            for _ in range(2)
        )
    )
    assert receipt.id == duplicate.id
    ticket, state = await snapshot(session_factory)
    assert (
        state.budget.ticket_tokens == 201000
        and state.budget.ticket_estimated_usd == Decimal("6")
    )
    assert (
        state.reserved_tokens == 1000
        and state.unknown_spend
        and ticket.worker_parked_reason
    )
    with pytest.raises(ValueError, match="different input"):
        await extend_budget(
            session_factory,
            config=settings,
            ticket_id="ticket",
            request=request.model_copy(update={"reason": "Different reason"}),
            actor="admin",
            now=clock[0],
        )
