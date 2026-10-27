"""Step-8 fault matrix: durable ingress, isolation, compatibility, human wins."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError

from switch_core.bridges.jira.delivery import role_job
from switch_core.bridges.jira.service import JiraBridgeService
from switch_core.bridges.jira.worker import JiraWorkerScheduler
from switch_core.db.models import JiraWorkerJob, JiraWorkerOutbox, JiraWorkerTicket
from tests.switch_core.bridges.jira.test_delivery import current, seed, services
from tests.switch_core.bridges.jira.test_delivery_close import (
    ClosingJira,
    _tested,
    accepted,
    setup,
)
from tests.switch_core.bridges.jira.test_rooms import (
    FakeCards,
    FakeRooms,
    _webhook_event,
)
from tests.switch_core.bridges.jira.test_worker import _config

INSTANCE = "acme"
PROJECT = "KAN"


def _service(session_factory: Any, *, rooms: FakeRooms | None = None) -> Any:
    return JiraBridgeService(
        session_factory=session_factory,
        trigger_store=cast(Any, None),
        agent_store=cast(Any, None),
        room_store=cast(Any, None),
        protocol=cast(Any, None),
        config=_config(jira_worker_enabled_projects={INSTANCE: [PROJECT]}),
        ticket_rooms=rooms,
        card_updater=FakeCards(rooms) if rooms is not None else None,
    )


async def _ticket_count(session_factory: Any) -> int:
    async with session_factory() as session:
        return int(
            (await session.scalar(select(func.count()).select_from(JiraWorkerTicket)))
            or 0
        )


async def test_acknowledged_event_is_durable_and_idempotent(session_factory) -> None:
    service = _service(session_factory)
    event = _webhook_event(event_id="durable-1")
    assert (
        await service.record_worker_intake(
            instance=INSTANCE, event=event, webhook_identifier="atlassian-1"
        )
        is True
    )
    assert await _ticket_count(session_factory) == 1
    assert (
        await service.record_worker_intake(
            instance=INSTANCE, event=event, webhook_identifier="atlassian-1"
        )
        is False
    )
    assert await _ticket_count(session_factory) == 1


async def test_restart_recovers_a_recorded_event_without_a_room(
    session_factory,
) -> None:
    service = _service(session_factory)
    await service.record_worker_intake(
        instance=INSTANCE,
        event=_webhook_event(event_id="restart-1"),
        webhook_identifier="atlassian-restart",
    )
    async with session_factory() as session:
        ticket = await session.scalar(select(JiraWorkerTicket))
        assert ticket is not None and ticket.room_id is None

    rooms = FakeRooms()
    scheduler = JiraWorkerScheduler(
        session_factory=session_factory,
        config=_config(jira_worker_enabled_projects={INSTANCE: [PROJECT]}),
        ticket_rooms=rooms,
        card_updater=FakeCards(rooms),
    )
    assert await scheduler.sweep_ticket_rooms() == 1
    async with session_factory() as session:
        ticket = await session.scalar(select(JiraWorkerTicket))
        assert ticket is not None and ticket.room_id in rooms.rooms
        pending = (
            await session.scalars(
                select(JiraWorkerOutbox).where(
                    JiraWorkerOutbox.command == "sync_ticket_room"
                )
            )
        ).all()
        assert all(row.status == "done" for row in pending)


async def test_old_claim_path_cannot_see_role_jobs(session_factory) -> None:
    now = datetime.now(UTC)
    async with session_factory() as session:
        session.add(JiraWorkerTicket(id="ticket", instance=INSTANCE, issue_key="KAN-1"))
        await session.flush()
        session.add(
            JiraWorkerJob(
                kind="role_job",
                instance=INSTANCE,
                project_key=PROJECT,
                ticket_id="ticket",
                generation=1,
                execution_key="ticket/1/reproduce/1",
                due_at=now,
                status="claimed",
            )
        )
        await session.commit()
    async with session_factory() as session:
        old_rows = (
            await session.scalars(
                text(
                    "SELECT id FROM jira_worker_job_record "
                    "WHERE status = 'pending' AND due_at <= :now"
                ),
                {"now": now},
            )
        ).all()
        assert old_rows == []


async def test_pending_role_job_is_rejected_by_the_database(
    session_factory,
) -> None:
    async with session_factory() as session:
        session.add(JiraWorkerTicket(id="ticket", instance=INSTANCE, issue_key="KAN-1"))
        await session.flush()
        session.add(
            JiraWorkerJob(
                kind="role_job",
                ticket_id="ticket",
                generation=1,
                execution_key="ticket/1/reproduce/1",
                due_at=datetime.now(UTC),
                status="pending",
            )
        )
        with pytest.raises(IntegrityError, match="ck_jira_worker_role_not_pending"):
            await session.commit()
        await session.rollback()


async def _at_phase(session_factory: Any, phase: str) -> tuple[Any, ...]:
    jira = ClosingJira()
    if phase in {"merge", "smoke", "close"}:
        scope, activity, supervisor, process, forge, publisher = await accepted(
            session_factory, jira
        )
    elif phase == "accept":
        scope, activity, supervisor, process, forge, publisher = await _tested(
            session_factory, jira
        )
    elif phase in {"review", "qa"}:
        scope, activity, supervisor, process, forge, publisher = await setup(
            session_factory, jira
        )
        if phase == "qa":
            process.complete(role_job(scope, "review", 1))
            await supervisor.reconcile("ticket")
            await supervisor.reconcile("ticket")
        return jira, scope, activity, supervisor, process, forge, publisher
    else:
        scope = await seed(session_factory)
        activity, supervisor, process, forge, publisher = services(
            session_factory, jira
        )
        jira.approve()
        await supervisor.reconcile("ticket")
        if phase == "fix":
            process.complete(scope.job)
            await supervisor.reconcile("ticket")
            await supervisor.reconcile("ticket")
        return jira, scope, activity, supervisor, process, forge, publisher

    if phase in {"smoke", "close"}:
        forge.merge()
        await supervisor.reconcile("ticket")
        await supervisor.reconcile("ticket")
        if phase == "close":
            process.complete(role_job(scope, "smoke", 1))
            await supervisor.reconcile("ticket")
    return jira, scope, activity, supervisor, process, forge, publisher


@pytest.mark.parametrize(
    "phase",
    [
        "reproduce",
        "fix",
        "review",
        "qa",
        "accept",
        "merge",
        "smoke",
        "close",
    ],
)
async def test_human_done_wins_in_every_delivery_phase(
    session_factory, phase: str
) -> None:
    jira, _, activity, supervisor, process, forge, _ = await _at_phase(
        session_factory, phase
    )
    starts = tuple(process.starts)
    creates = tuple(forge.creates)
    pushes = tuple(forge.pushes)
    writes = len(jira.writes)

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
    assert await activity.read_ticket("ticket", force_jira=True) is True
    for _ in range(3):
        await supervisor.reconcile("ticket")

    ticket, state = await current(session_factory)
    assert ticket.status == "Done"
    assert state is not None and state.outcome == "closed_by_human"
    assert tuple(process.starts) == starts
    assert tuple(forge.creates) == creates
    assert tuple(forge.pushes) == pushes
    assert len(jira.writes) == writes


@pytest.mark.parametrize("phase", ["reproduce", "fix", "review", "qa", "smoke"])
async def test_human_park_cancels_the_live_role_in_every_running_phase(
    session_factory, phase: str
) -> None:
    jira, _, activity, supervisor, process, _, _ = await _at_phase(
        session_factory, phase
    )
    live = tuple(process.starts)
    assert live

    jira.status = "Blocked"
    jira.change("approver")
    jira.histories[-1]["items"] = [
        {
            "field": "status",
            "from": "progress",
            "to": "blocked",
            "fromString": "In Progress",
            "toString": "Blocked",
        }
    ]
    await activity.read_ticket("ticket", force_jira=True)
    for _ in range(2):
        await supervisor.reconcile("ticket")

    ticket, _ = await current(session_factory)
    assert ticket.worker_parked_reason
    assert set(live).issubset(set(process.cancelled))
