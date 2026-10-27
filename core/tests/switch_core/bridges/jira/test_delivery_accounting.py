"""Mixed role/language admission and unknown-usage receipts on PostgreSQL."""

import asyncio
from decimal import Decimal

import pytest

from switch_core.bridges.jira.accounting import live_sessions
from switch_core.bridges.jira.activity import TicketTurn, TurnResult
from switch_core.bridges.jira.contracts import Reservation
from switch_core.bridges.jira.intake import IntakeOrchestrator
from switch_core.db.models import JiraWorkerJob, JiraWorkerTicket
from tests.switch_core.bridges.jira.test_activity import message
from tests.switch_core.bridges.jira.test_delivery_questions import (
    RelayModel,
    ask,
    clone_queued_ticket,
    open_question,
    setup,
    snapshot,
)


class HeldRelay(RelayModel):
    def __init__(self):
        super().__init__()
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def relay(self, data, token_budget):
        self.started.set()
        await self.release.wait()
        return await super().relay(data, token_budget)


async def test_language_and_role_reservations_share_the_same_daily_limit(
    session_factory,
):
    lab = await open_question(session_factory)
    scope, settings, clock, _, rooms, process, board, activity, jobs, _, _, _, _ = lab
    second = await clone_queued_ticket(session_factory, scope, "second")
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, "second")
        ticket.workflow_state = {
            **ticket.workflow_state,
            "budget_override": {
                "ticket_tokens": 200000,
                "ticket_estimated_usd": "30",
                "day_estimated_usd": "25",
            },
        }
        await session.commit()
    model = HeldRelay()
    orchestrator = IntakeOrchestrator(
        session_factory=session_factory,
        config=settings,
        model=model,
        board=board,
        clock=lambda: clock[0],
    )
    rooms.messages.append({**message(1), "body": "hello"})
    turn = asyncio.create_task(activity.run_one_turn(orchestrator))
    await asyncio.wait_for(model.started.wait(), 10)
    reservation = Reservation(
        tokens=1000, estimated_usd=Decimal("24.85"), price_source="fixture-prices"
    )
    try:
        with pytest.raises(ValueError, match="budget"):
            await jobs.start(second, reservation=reservation, estimate_seconds=60)
    finally:
        model.release.set()
        assert await turn == "ticket"
    # Once the language reservation reconciles to actual known usage, it fits.
    await jobs.start(second, reservation=reservation, estimate_seconds=60)
    assert second.run.execution_key in process.starts


async def test_unknown_language_usage_retains_its_reservation_and_parks(
    session_factory,
):
    lab = await setup(session_factory)
    scope, settings, clock, _, _, process, board, activity, _, supervisor, _, _, _ = lab
    await ask(process, scope.job)
    await supervisor.reconcile("ticket")

    class LostRelay(RelayModel):
        async def relay(self, data, token_budget):
            raise TimeoutError("No model usage receipt")

    orchestrator = IntakeOrchestrator(
        session_factory=session_factory,
        config=settings,
        model=LostRelay(),
        board=board,
        clock=lambda: clock[0],
    )
    with pytest.raises(TimeoutError):
        await activity.run_one_turn(orchestrator)
    ticket, state = await snapshot(session_factory)
    assert ticket.worker_parked_reason and state.unknown_spend
    assert state.reserved_tokens == 8192 and state.reserved_estimated_usd == Decimal(
        "0.08192"
    )
    await supervisor.reconcile("ticket")
    assert await activity.run_one_turn(orchestrator) is None
    assert len(process.starts) == 1


async def test_intake_admission_counts_live_roles_even_for_other_tickets(
    session_factory,
):
    lab = await setup(session_factory)
    scope, _, clock, jira, _, _, _, activity, jobs, _, _, _, _ = lab
    second = await clone_queued_ticket(session_factory, scope, "second")
    await jobs.start(
        second, reservation=scope.reproduction_reservation, estimate_seconds=60
    )
    async with session_factory() as session:
        session.add(
            JiraWorkerTicket(
                id="intake",
                instance="acme",
                project_key="TEST",
                issue_key="TEST-intake",
                status="In Progress",
                room_claimed=True,
                room_id="room-intake",
                card_event_id="card-intake",
                agent_state="wake_requested",
                jira_read_updated=jira.updated,
                jira_read_changelog_id=str(jira.histories[-1]["id"]),
                jira_seen_comment_ids=[],
                jira_worker_account_id="worker-account",
                reporter_account_id="reporter-account",
            )
        )
        for index in range(3):
            session.add(
                JiraWorkerJob(
                    kind="orchestrator_turn",
                    instance="acme",
                    project_key="TEST",
                    due_at=clock[0],
                    status="claimed",
                    payload={"ticket_id": f"other-{index}"},
                )
            )
        await session.commit()

    async def unreachable(turn: TicketTurn) -> TurnResult:
        pytest.fail("The sixth model session must not start")

    assert await activity.run_one_turn(unreachable) is None
    async with session_factory() as session:
        assert await live_sessions(session) == 5
        ticket = await session.get(JiraWorkerTicket, "intake")
        assert ticket.queue_reason and ticket.status == "In Progress"
