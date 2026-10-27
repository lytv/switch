"""Step-1 state and execution proofs against throwaway PostgreSQL."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from switch_core.bridges.jira import packs
from switch_core.bridges.jira.contracts import Pack, Result, Run
from switch_core.bridges.jira.packs import FrozenPack, load_pack
from switch_core.bridges.jira.state import (
    DeliveryWorkflowState,
    WorkflowMapping,
    workflow_state_adapter,
)
from switch_core.bridges.jira.worker import claim_due_jobs
from switch_core.db.models import (
    JiraWorkerJob,
    JiraWorkerTicket,
)
from tests.switch_core.bridges.jira.test_worker import (
    _config,
)


def test_configured_budget_is_frozen(monkeypatch: pytest.MonkeyPatch) -> None:
    initial = load_pack(_config())
    assert initial.snapshot.budget.ticket_tokens == 200000
    assert initial.snapshot.budget.ticket_estimated_usd == Decimal("5")
    assert initial.snapshot.budget.day_estimated_usd == Decimal("25")
    monkeypatch.setenv("JIRA_WORKER_DELIVERY_TICKET_TOKENS", "123456")
    monkeypatch.setenv("JIRA_WORKER_DELIVERY_TICKET_ESTIMATED_USD", "7.25")
    monkeypatch.setenv("JIRA_WORKER_DELIVERY_DAY_ESTIMATED_USD", "50")
    changed = load_pack(_config())
    assert changed.snapshot.budget.ticket_tokens == 123456
    assert changed.snapshot.budget.ticket_estimated_usd == Decimal("7.25")
    assert changed.snapshot.budget.day_estimated_usd == Decimal("50")
    assert changed.digest != initial.digest
    assert initial.snapshot.budget.ticket_tokens == 200000
    assert _config().jira_worker_limits.tokens_per_ticket == 50000
    with pytest.raises(ValidationError, match="frozen"):
        initial.snapshot.budget.ticket_tokens = 1


@pytest.mark.parametrize(
    "field,value",
    [
        ("firstmate_commit", "main"),
        ("firstmate_commit", "0" * 40),
        ("firstmate_commit", ""),
        ("version", "latest"),
        ("harness_version", "latest"),
        ("provider", "default"),
        ("model", "latest"),
        ("model", " "),
        ("human_merge", False),
        ("deploy", True),
        ("bug_path", ["intake", "fix", "close"]),
    ],
)
def test_invalid_pack_pins_fail(field: str, value: object) -> None:
    data = load_pack(_config()).snapshot.model_dump(mode="json")
    data[field] = value
    with pytest.raises(ValidationError):
        Pack.model_validate_json(json.dumps(data))


@pytest.mark.parametrize(
    "field", ["firstmate_commit", "harness_version", "provider", "model"]
)
def test_missing_pack_pins_fail(field: str) -> None:
    data = load_pack(_config()).snapshot.model_dump(mode="json")
    del data[field]
    with pytest.raises(ValidationError):
        Pack.model_validate_json(json.dumps(data))


@pytest.mark.parametrize(
    "setting,value",
    [
        ("jira_worker_delivery_ticket_tokens", 0),
        ("jira_worker_delivery_ticket_estimated_usd", "-1"),
        ("jira_worker_delivery_day_estimated_usd", "NaN"),
    ],
)
def test_invalid_budget_config_fails(setting: str, value: object) -> None:
    with pytest.raises(ValidationError):
        _config(**{setting: value})


async def test_pack_file_edits_cannot_change_running_ticket(
    session_factory, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    state = DeliveryWorkflowState(
        pack=load_pack(_config()),
        service="Bug",
        statuses=(WorkflowMapping(name="approval", jira_id="status-1"),),
        transitions=(WorkflowMapping(name="approve", jira_id="transition-1"),),
    )
    async with session_factory() as session:
        ticket = JiraWorkerTicket(
            instance="fixture",
            issue_key="TEST-1",
            service_value="Bug",
            agent_generation=1,
            workflow_state=state.model_dump(mode="json"),
        )
        session.add(ticket)
        await session.commit()
        ticket_id = ticket.id
    edited = state.pack.snapshot.model_dump(mode="json")
    edited["model"] = "different-model"
    path = tmp_path / "packs/delivery/2.0.0-draft.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(edited))
    monkeypatch.setattr(packs, "files", lambda package: tmp_path)
    with pytest.raises(ValueError, match="Historical.*changed"):
        load_pack(_config())
    async with session_factory() as session:
        stored = await session.get(JiraWorkerTicket, ticket_id)
        recovered = workflow_state_adapter.validate_json(
            json.dumps(stored.workflow_state)
        )
        assert recovered == state
        assert recovered.pack.snapshot.model == "gpt-6.1-sol"
        assert stored.agent_generation == 1
    edited_state = state.model_dump(mode="json")
    edited_state["pack"]["snapshot"]["model"] = "different-model"
    with pytest.raises(ValidationError, match="Pack snapshot changed"):
        workflow_state_adapter.validate_json(json.dumps(edited_state))
    with pytest.raises(ValidationError, match="frozen"):
        state.statuses[0].jira_id = "changed"
    assert FrozenPack.model_validate_json(state.pack.model_dump_json()) == state.pack


@pytest.mark.parametrize("service", ["Feature", "Docs", "Other"])
def test_non_bug_state_cannot_select_development_step(service: str) -> None:
    data = {
        "pack": load_pack(_config()).model_dump(mode="json"),
        "service": service,
        "step": "fix",
        "statuses": [],
        "transitions": [],
    }
    with pytest.raises(ValidationError, match="Service path"):
        DeliveryWorkflowState.model_validate_json(json.dumps(data))
    data["step"] = "manual_handoff"
    assert (
        DeliveryWorkflowState.model_validate_json(json.dumps(data)).step
        == "manual_handoff"
    )


async def test_generic_claims_never_consume_role_jobs(session_factory) -> None:
    run = Run(
        instance="fixture",
        ticket_id="ticket-1",
        generation=1,
        step="reproduce",
        attempt=1,
        execution_key="ticket-1/1/reproduce/1",
    )
    now = datetime.now(UTC)
    async with session_factory() as session:
        session.add(
            JiraWorkerTicket(id=run.ticket_id, instance="fixture", issue_key="TEST-1")
        )
        await session.flush()
        session.add(
            JiraWorkerJob(
                kind="role_job",
                instance=run.instance,
                ticket_id=run.ticket_id,
                generation=run.generation,
                execution_key=run.execution_key,
                due_at=now,
                status="claimed",
                payload={"run": run.model_dump(mode="json")},
            )
        )
        await session.commit()

    async def claim(owner: str) -> list[str]:
        async with session_factory() as session:
            jobs = await claim_due_jobs(
                session, now=now, owner=owner, interval_seconds=300, kinds=("role_job",)
            )
            await session.commit()
            return [job.execution_key for job in jobs]

    claimed = await asyncio.gather(claim("first"), claim("second"))
    assert claimed == [[], []]
    async with session_factory() as session:
        row = await session.scalar(select(JiraWorkerJob))
        assert row is not None and row.status == "claimed" and row.claimed_at is None


async def test_pending_role_job_is_rejected(session_factory) -> None:
    async with session_factory() as session:
        session.add(
            JiraWorkerTicket(id="ticket", instance="fixture", issue_key="TEST-1")
        )
        await session.flush()
        session.add(
            JiraWorkerJob(
                kind="role_job",
                ticket_id="ticket",
                generation=1,
                execution_key="ticket/1/reproduce/1",
                status="pending",
                due_at=datetime.now(UTC),
            )
        )
        with pytest.raises(IntegrityError, match="ck_jira_worker_role_not_pending"):
            await session.commit()
        await session.rollback()


@pytest.mark.parametrize(
    "status",
    [
        "claimed",
        "running",
        "waiting_input",
        "paused",
        "cancel_requested",
        "uncertain",
    ],
)
async def test_only_one_active_execution_per_ticket_generation(
    session_factory, status: str
) -> None:
    async with session_factory() as session:
        session.add(
            JiraWorkerTicket(id="ticket", instance="fixture", issue_key="TEST-1")
        )
        await session.flush()
        session.add(
            JiraWorkerJob(
                kind="role_job",
                ticket_id="ticket",
                generation=1,
                execution_key="ticket/1/reproduce/1",
                due_at=datetime.now(UTC),
                status=status,
            )
        )
        await session.commit()
    async with session_factory() as session:
        session.add(
            JiraWorkerJob(
                kind="role_job",
                ticket_id="ticket",
                generation=1,
                execution_key="ticket/1/fix/1",
                status="claimed",
                due_at=datetime.now(UTC),
            )
        )
        with pytest.raises(
            IntegrityError, match="uq_jira_worker_job_active_generation"
        ):
            await session.commit()
        await session.rollback()
    async with session_factory() as session:
        session.add(
            JiraWorkerJob(
                kind="role_job",
                ticket_id="ticket",
                generation=2,
                execution_key="ticket/2/reproduce/1",
                status="claimed",
                due_at=datetime.now(UTC),
            )
        )
        await session.commit()


async def test_execution_key_stays_unique_after_completion(session_factory) -> None:
    async with session_factory() as session:
        session.add(
            JiraWorkerTicket(id="ticket", instance="fixture", issue_key="TEST-1")
        )
        await session.flush()
        session.add(
            JiraWorkerJob(
                kind="role_job",
                ticket_id="ticket",
                generation=1,
                execution_key="ticket/1/reproduce/1",
                status="done",
                due_at=datetime.now(UTC),
            )
        )
        await session.commit()
    async with session_factory() as session:
        session.add(
            JiraWorkerJob(
                kind="role_job",
                ticket_id="ticket",
                generation=1,
                execution_key="ticket/1/reproduce/1",
                status="claimed",
                due_at=datetime.now(UTC),
            )
        )
        with pytest.raises(IntegrityError, match="uq_jira_worker_job_execution_key"):
            await session.commit()
        await session.rollback()


@pytest.mark.parametrize(
    "change,constraint",
    [
        ({"ticket_id": None}, "ck_jira_worker_role_identity"),
        ({"generation": None}, "ck_jira_worker_role_identity"),
        ({"generation": 0}, "ck_jira_worker_role_identity"),
        ({"execution_key": None}, "ck_jira_worker_role_identity"),
        ({"execution_key": "other/1/fix/1"}, "ck_jira_worker_role_identity"),
        ({"execution_key": "ticket/2/fix/1"}, "ck_jira_worker_role_identity"),
        ({"execution_key": "ticket/1/fix/0"}, "ck_jira_worker_role_identity"),
        ({"execution_key": "ticket/1/unknown/1"}, "ck_jira_worker_role_identity"),
        ({"status": "unrecognized"}, "ck_jira_worker_role_status"),
        ({"status": "pending"}, "ck_jira_worker_role_not_pending"),
    ],
)
async def test_invalid_role_identity_and_status_fail(
    session_factory, change, constraint
) -> None:
    async with session_factory() as session:
        session.add(
            JiraWorkerTicket(id="ticket", instance="fixture", issue_key="TEST-1")
        )
        await session.commit()
    data = {
        "kind": "role_job",
        "ticket_id": "ticket",
        "generation": 1,
        "execution_key": "ticket/1/fix/1",
        "status": "claimed",
        "due_at": datetime.now(UTC),
    } | change
    async with session_factory() as session:
        session.add(JiraWorkerJob(**data))
        with pytest.raises(IntegrityError, match=constraint):
            await session.commit()
        await session.rollback()


@pytest.mark.parametrize(
    "change",
    [
        {"schema_version": 2},
        {"policy": "unknown"},
        {"generation": 1},
    ],
)
def test_workflow_state_rejects_unknown_schema_or_competing_generation(change) -> None:
    data = {
        "schema_version": 1,
        "policy": "legacy_intake",
        "pack_id": "delivery",
        "pack_version": "1",
    } | change
    with pytest.raises(ValidationError):
        workflow_state_adapter.validate_json(json.dumps(data))


async def test_job_result_round_trips_through_jsonb(session_factory) -> None:
    result = Result.model_validate_json(
        json.dumps(
            {
                "run": {
                    "instance": "fixture",
                    "ticket_id": "ticket",
                    "generation": 1,
                    "step": "reproduce",
                    "attempt": 1,
                    "execution_key": "ticket/1/reproduce/1",
                },
                "input_digest": "1" * 64,
                "status": "completed",
                "question": None,
                "usage": {
                    "measurement": "unknown",
                    "provider": None,
                    "model": None,
                    "input_tokens": None,
                    "output_tokens": None,
                    "cache_read_tokens": None,
                    "cache_write_tokens": None,
                    "estimated_usd": None,
                    "price_source": None,
                    "billing": "estimate_not_subscription_invoice",
                },
            }
        )
    )
    async with session_factory() as session:
        session.add(
            JiraWorkerTicket(id="ticket", instance="fixture", issue_key="TEST-1")
        )
        await session.flush()
        job = JiraWorkerJob(
            kind="role_job",
            ticket_id="ticket",
            generation=1,
            execution_key=result.run.execution_key,
            status="done",
            due_at=datetime.now(UTC),
            result=result.model_dump(mode="json"),
        )
        session.add(job)
        await session.commit()
        job_id = job.id
    async with session_factory() as session:
        stored = await session.get(JiraWorkerJob, job_id)
        assert Result.model_validate_json(json.dumps(stored.result)) == result
