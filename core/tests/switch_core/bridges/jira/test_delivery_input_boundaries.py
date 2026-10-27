"""Native checkpoint, authenticated budget API and additive migration boundaries."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import insert, text
from sqlalchemy.ext.asyncio import create_async_engine

from switch_core.bridges.jira.contracts import Answer, Question, digest
from switch_core.bridges.jira.delivery import role_job
from switch_core.bridges.jira.delivery_contracts import ExecutionEnvelope
from switch_core.bridges.jira.packs import FrozenPack
from switch_core.db.models import JiraWorkerJob, JiraWorkerTicket, User
from switch_core.gateway.auth import require_admin
from switch_core.gateway.dependencies import (
    get_config,
    get_session,
    get_session_factory,
)
from switch_core.gateway.jira_triggers import router
from tests.switch_core.bridges.jira import test_firstmate_jobs, test_worker
from tests.switch_core.bridges.jira.test_delivery import URL, config, git, seed

lifecycle_lab = test_firstmate_jobs.lab
mig_url = test_worker.mig_url


async def test_native_checkpoint_answer_reaches_one_new_task_without_replay(
    session_factory, lifecycle_lab
):
    lab = lifecycle_lab
    scope = await seed(
        session_factory,
        base_sha=lab.base_sha,
        pack=FrozenPack(snapshot=lab.pack, digest=digest(lab.pack)),
    )
    job = scope.job
    git(lab.copy._project, "checkout", "-b", scope.branch)
    git(lab.copy._project, "remote", "add", "origin", URL)
    await lab.copy.start(
        job,
        lab.pack,
        scope.reproduction_reservation,
        60,
        envelope=ExecutionEnvelope(scope=scope, reproduction=None),
    )
    question = Question(
        run=job.run,
        input_digest=digest(job),
        id="browser",
        checkpoint_digest="a" * 64,
        text="Which browser version?",
        evidence=("The failing check requires the browser version.",),
    )
    test_firstmate_jobs._result(
        lab, job, status="needs_input", question=question.model_dump(mode="json")
    )
    observed = await lab.copy.get(job)
    await lab.copy.cancel(job)
    answer = Answer(
        question=observed.result.question,
        revision=1,
        actor_account_id="reporter-account",
        classification="answer",
        text="Browser 123",
    )
    resumed = role_job(scope, "reproduce", 1, 1)
    envelope = ExecutionEnvelope(scope=scope, reproduction=None, resume_answer=answer)
    for _ in range(2):
        await lab.copy.start(
            resumed, lab.pack, scope.reproduction_reservation, 60, envelope=envelope
        )
    home = lab.copy.location(resumed)
    assert (home / "starts").read_text() == "1"
    forwarded = Answer.model_validate_json(
        (home / "state" / f"{lab.copy.task_id(resumed)}.inbox" / "001.msg").read_text()
    )
    checkpoint = (
        home / "data" / lab.copy.task_id(resumed) / "checkpoint.json"
    ).read_bytes()
    assert forwarded == answer
    assert hashlib.sha256(checkpoint).hexdigest() == answer.question.checkpoint_digest
    inbox = home / "state" / f"{lab.copy.task_id(resumed)}.inbox"
    (inbox / "handled").mkdir()
    (inbox / "001.msg").rename(inbox / "handled" / "001.msg")
    await lab.copy.start(
        resumed, lab.pack, scope.reproduction_reservation, 60, envelope=envelope
    )
    assert not (inbox / "001.msg").exists()
    checkpoint_path = home / "data" / lab.copy.task_id(resumed) / "checkpoint.json"
    next_checkpoint = b'{"checkpoint":"next missing fact"}'
    checkpoint_path.write_bytes(next_checkpoint)
    await lab.copy.start(
        resumed, lab.pack, scope.reproduction_reservation, 60, envelope=envelope
    )
    assert checkpoint_path.read_bytes() == next_checkpoint
    assert not (inbox / "001.msg").exists()
    original = (
        lab.copy.location(job) / "data" / lab.copy.task_id(job) / "checkpoint.json"
    )
    original.write_text("changed checkpoint")
    with pytest.raises(ValueError, match="Checkpoint artifact"):
        await lab.copy.start(
            resumed, lab.pack, scope.reproduction_reservation, 60, envelope=envelope
        )
    assert (home / "starts").read_text() == "1"


async def test_budget_http_request_is_idempotent_and_keeps_usage_and_park(
    session_factory,
):
    await seed(session_factory)
    async with session_factory() as session:
        admin = User(
            id="admin", name="admin", email="admin@example.invalid", role="admin"
        )
        session.add(admin)
        ticket = await session.get(JiraWorkerTicket, "ticket")
        ticket.tokens_used = 800
        ticket.worker_parked_reason = "Usage outcome uncertain"
        ticket.workflow_state = {
            **ticket.workflow_state,
            "unknown_spend": True,
            "reserved_tokens": 1000,
            "reserved_estimated_usd": "1",
        }
        await session.commit()
    app = FastAPI()
    app.include_router(router, prefix="/jira-triggers")

    async def session_dep():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_session] = session_dep
    app.dependency_overrides[get_session_factory] = lambda: session_factory
    app.dependency_overrides[get_config] = config
    app.dependency_overrides[require_admin] = lambda: admin
    path = "/jira-triggers/worker/tickets/TEST-1/budget?instance=acme"
    body = {
        "request_id": str(uuid4()),
        "previous_generation": 1,
        "reason": "Explicit additional budget",
        "delta": {
            "ticket_tokens": 1000,
            "ticket_estimated_usd": "1.25",
            "day_estimated_usd": "0",
        },
    }
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        first = await client.post(path, json=body)
        assert first.status_code == 200, first.text
        repeat = await client.post(path, json=body)
        assert repeat.json() == first.json()
        invalid = await client.post(path, json={**body, "reason": "Different reason"})
        assert invalid.status_code == 400
        stale = await client.post(
            path, json={**body, "request_id": str(uuid4()), "previous_generation": 9}
        )
        assert stale.status_code == 400
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, "ticket")
        assert (
            ticket.tokens_used == 800
            and ticket.worker_parked_reason == "Usage outcome uncertain"
        )
        assert (
            ticket.workflow_state["reserved_tokens"] == 1000
            and ticket.workflow_state["unknown_spend"]
        )
        assert Decimal(
            ticket.workflow_state["budget_override"]["ticket_estimated_usd"]
        ) == Decimal("6.25")


async def test_checkpoint_migration_keeps_old_records_and_admits_only_valid_revisions(
    mig_url,
):
    engine = create_async_engine(mig_url)
    try:
        async with engine.begin() as connection:
            await connection.run_sync(lambda c: test_worker._run_migrations(c, "head"))
            await connection.run_sync(
                lambda c: test_worker._run_migrations(c, "7b1e2f3a4c59")
            )
            await connection.execute(
                insert(JiraWorkerTicket).values(
                    id="ticket", instance="acme", issue_key="TEST-1", project_key="TEST"
                )
            )
            await connection.execute(
                insert(JiraWorkerJob).values(
                    id="original",
                    kind="role_job",
                    instance="acme",
                    project_key="TEST",
                    ticket_id="ticket",
                    generation=1,
                    execution_key="ticket/1/reproduce/1",
                    status="done",
                    due_at=datetime.now(UTC),
                )
            )
            await connection.run_sync(lambda c: test_worker._run_migrations(c, "head"))
            assert (
                await connection.scalar(
                    text(
                        "SELECT execution_key FROM jira_worker_job_record WHERE id = 'original'"
                    )
                )
                == "ticket/1/reproduce/1"
            )
            await connection.execute(
                insert(JiraWorkerJob).values(
                    id="resumed",
                    kind="role_job",
                    instance="acme",
                    project_key="TEST",
                    ticket_id="ticket",
                    generation=1,
                    execution_key="ticket/1/reproduce/1/input/1",
                    status="running",
                    due_at=datetime.now(UTC),
                )
            )
        with pytest.raises(RuntimeError, match="checkpoint revision"):
            async with engine.begin() as connection:
                await connection.run_sync(
                    lambda c: test_worker._run_migrations(c, "7b1e2f3a4c59")
                )
        async with engine.connect() as connection:
            assert (
                await connection.scalar(
                    text("SELECT count(*) FROM jira_worker_job_record")
                )
                == 2
            )
    finally:
        await engine.dispose()
