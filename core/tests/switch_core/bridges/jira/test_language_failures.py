"""Failed language responses settle only validated usage through public turns."""

import json
from datetime import timedelta
from decimal import Decimal
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import select

from switch_core.bridges.jira.accounting import day_cost
from switch_core.bridges.jira.activity import MeasuredTurnError
from switch_core.bridges.jira.admin import release_parked_ticket
from switch_core.bridges.jira.budget import BudgetDelta, BudgetRequest, extend_budget
from switch_core.bridges.jira.intake import (
    IntakeModel,
    IntakeOrchestrator,
    RetryableModelError,
)
from switch_core.db.models import JiraWorkerJob, JiraWorkerTicket, User
from tests.switch_core.bridges.jira.test_activity import message
from tests.switch_core.bridges.jira.test_delivery_questions import (
    ask,
    open_question,
    setup,
    snapshot,
)
from tests.switch_core.bridges.jira.test_outbox import client_for


async def language_lab(sessions, stage):
    lab = await (open_question(sessions) if stage == "reply" else setup(sessions))
    scope, _, _, _, rooms, process, _, _, jobs, supervisor, _, _, _ = lab
    if stage == "reply":
        rooms.messages.append({**message(1), "body": "Browser 123"})
    elif stage == "question":
        await ask(process, scope.job)
        await supervisor.reconcile("ticket")
    else:
        process.complete(scope.job)
        await jobs.get(scope.job.run.execution_key)
        async with sessions() as session:
            ticket = await session.get(JiraWorkerTicket, "ticket")
            ticket.workflow_state = {
                **ticket.workflow_state,
                "step": "intake",
                "scope": None,
                "approvals": [],
            }
            ticket.intake_state = {"phase": "intake"}
            ticket.agent_state = "wake_requested"
            await session.commit()
    async with sessions() as session:
        session.add(
            User(id="admin", name="admin", role="admin", email="admin@example.invalid")
        )
        await session.commit()
    return lab


def model_double(monkeypatch, response):
    real_client = httpx.AsyncClient
    calls = []

    def handle(request):
        calls.append(request)
        if response == "timeout":
            raise httpx.ReadTimeout("Model response lost", request=request)
        return httpx.Response(200, json=response)

    def client(**options):
        if "transport" not in options:
            options["transport"] = httpx.MockTransport(handle)
        return real_client(**options)

    monkeypatch.setattr(httpx, "AsyncClient", client)
    return (
        IntakeModel(
            base_url="https://model.example.invalid/v1",
            api_key="placeholder",
            model="fixture-model",
        ),
        calls,
    )


def provider_response(failure):
    response = {
        "usage": {"prompt_tokens": 100, "completion_tokens": 10, "total_tokens": 110},
        "choices": [
            {
                "finish_reason": "stop",
                "message": {"content": '{"action":"invalid","content":"Invalid"}'},
            }
        ],
    }
    if failure == "incomplete":
        response["choices"][0]["finish_reason"] = "length"
    elif failure == "json":
        response["choices"][0]["message"]["content"] = "{"
    elif failure == "missing_choice":
        response["choices"] = []
    elif failure == "wrong_context":
        response["choices"][0]["message"]["content"] = json.dumps(
            {"action": "question", "content": "Another question"}
        )
    return response


@pytest.mark.parametrize(
    ("stage", "failure"),
    [
        (stage, failure)
        for stage in ("question", "reply", "intake")
        for failure in ("invalid", "incomplete", "json", "missing_choice")
    ]
    + [("question", "wrong_context"), ("reply", "wrong_context")],
)
async def test_measured_language_failure_settles_and_requires_admin_release(
    session_factory, monkeypatch, stage, failure
):
    lab = await language_lab(session_factory, stage)
    _, settings, clock, jira, _, process, board, activity, _, supervisor, _, _, _ = lab
    before, state_before = await snapshot(session_factory)
    async with session_factory() as session:
        daily_before = await day_cost(session, clock[0])
    response = provider_response(failure)
    if stage == "question" and failure == "wrong_context":
        response["choices"][0]["message"]["content"] = json.dumps(
            {"action": "answer", "content": "Unrequested classification"}
        )
    model, calls = model_double(monkeypatch, response)
    orchestrator = IntakeOrchestrator(
        session_factory=session_factory,
        config=settings,
        model=model,
        board=board,
        clock=lambda: clock[0],
    )
    with pytest.raises(MeasuredTurnError) as failure_receipt:
        await activity.run_one_turn(orchestrator)
    assert failure_receipt.value.tokens_used == 110
    ticket, state = await snapshot(session_factory)
    assert ticket.worker_parked_reason
    assert not state.unknown_spend
    assert state.reserved_tokens == state_before.reserved_tokens == 0
    assert state.reserved_estimated_usd == state_before.reserved_estimated_usd == 0
    assert ticket.tokens_used == before.tokens_used + 110
    measured_cost = Decimal("0.00110")
    assert state.known_estimated_usd == state_before.known_estimated_usd + measured_cost
    assert state.answer is None
    async with session_factory() as session:
        assert await day_cost(session, clock[0]) == daily_before + measured_cost
        failed = await session.scalar(
            select(JiraWorkerJob).where(
                JiraWorkerJob.kind == "orchestrator_turn",
                JiraWorkerJob.status == "error",
            )
        )
        assert failed.payload["held_tokens"] == 0
        assert Decimal(failed.payload["held_usd"]) == 0
        assert failed.payload["usage_unknown"] is False
        assert failed.payload["failed_response"] == response
    await supervisor.reconcile("ticket")
    assert await activity.run_one_turn(orchestrator) is None
    assert len(calls) == 1 and len(process.starts) == 1
    clock[0] += timedelta(seconds=1)
    request_id = str(uuid4())
    receipt = await release_parked_ticket(
        session_factory,
        config=settings,
        client=client_for(jira),
        ticket_id="ticket",
        request_id=request_id,
        admin_user_id="admin",
        now=clock[0],
        release_reason="Review failed language result and recheck scope",
        previous_generation=ticket.agent_generation,
    )
    assert receipt.event_kind == "admin_release"
    released, released_state = await snapshot(session_factory)
    assert released.worker_parked_reason is None
    assert released.tokens_used == ticket.tokens_used
    assert released_state.scope is None and released_state.approvals == ()
    assert released_state.answer is None
    assert len(calls) == 1 and len(process.starts) == 1
    async with session_factory() as session:
        failed = await session.get(JiraWorkerJob, failed.id)
        assert (
            failed.status == "error" and failed.payload["failed_response"] == response
        )


@pytest.mark.parametrize(
    "failure", ["timeout", "missing_usage", "malformed_usage", "incomplete_unknown"]
)
async def test_unknown_provider_outcome_keeps_hold_and_blocks_admin_release(
    session_factory, monkeypatch, failure
):
    lab = await language_lab(session_factory, "question")
    _, settings, clock, jira, _, _, board, activity, _, _, _, _, _ = lab
    response = provider_response("invalid")
    if failure == "timeout":
        response = "timeout"
    elif failure == "missing_usage":
        response.pop("usage")
    elif failure == "malformed_usage":
        response["usage"]["total_tokens"] = "110"
    else:
        response["usage"] = None
        response["choices"][0]["finish_reason"] = "length"
    model, _ = model_double(monkeypatch, response)
    orchestrator = IntakeOrchestrator(
        session_factory=session_factory,
        config=settings,
        model=model,
        board=board,
        clock=lambda: clock[0],
    )
    before, state_before = await snapshot(session_factory)
    async with session_factory() as session:
        daily_before = await day_cost(session, clock[0])
    with pytest.raises((RetryableModelError, TypeError, ValueError)):
        await activity.run_one_turn(orchestrator)
    ticket, state = await snapshot(session_factory)
    assert state.unknown_spend and ticket.worker_parked_reason
    assert state.reserved_tokens == 8192
    assert state.reserved_estimated_usd == Decimal("0.08192")
    assert state.known_estimated_usd == state_before.known_estimated_usd
    assert ticket.tokens_used == before.tokens_used
    async with session_factory() as session:
        assert await day_cost(session, clock[0]) == daily_before + Decimal("0.08192")
    await extend_budget(
        session_factory,
        config=settings,
        ticket_id="ticket",
        actor="admin",
        request=BudgetRequest(
            request_id=uuid4(),
            reason="Explicit budget extension cannot reconcile unknown usage",
            previous_generation=ticket.agent_generation,
            delta=BudgetDelta(
                ticket_tokens=1000,
                ticket_estimated_usd=Decimal("1"),
                day_estimated_usd=Decimal("1"),
            ),
        ),
        now=clock[0],
    )
    with pytest.raises(ValueError, match="reconciliation"):
        await release_parked_ticket(
            session_factory,
            config=settings,
            client=client_for(jira),
            ticket_id="ticket",
            request_id=str(uuid4()),
            admin_user_id="admin",
            now=clock[0],
            release_reason="Cannot prove model spend",
            previous_generation=ticket.agent_generation,
        )
    _, state = await snapshot(session_factory)
    assert state.unknown_spend and state.reserved_tokens == 8192
    assert state.reserved_estimated_usd == Decimal("0.08192")
    async with session_factory() as session:
        assert await day_cost(session, clock[0]) == daily_before + Decimal("0.08192")
