"""Public lifecycle calls, real PostgreSQL, and a fake delivery process only."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import signal
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import select

from switch_core.bridges.jira.contracts import (
    Job,
    Pack,
    Repository,
    Reservation,
    Result,
    Run,
    Usage,
    digest,
)
from switch_core.bridges.jira.firstmate_copy import FirstmateCopy, SecretGuard
from switch_core.bridges.jira.firstmate_jobs import FirstmateJobs
from switch_core.bridges.jira.packs import FrozenPack, load_pack
from switch_core.bridges.jira.state import DeliveryWorkflowState
from switch_core.bridges.jira.worker import JiraWorkerScheduler, set_identity_mapping
from switch_core.db.models import JiraWorkerJob, JiraWorkerOutbox, JiraWorkerTicket
from tests.switch_core.bridges.jira.test_activity import ThreadRooms
from tests.switch_core.bridges.jira.test_admin import (
    FakeAdminBoard,
    _seed,
    _sync_at,
    _worker_config,
)
from tests.switch_core.bridges.jira.test_outbox import JiraMock, client_for
from tests.switch_core.bridges.jira.test_worker import _config

NOW = datetime(2026, 10, 23, 8, tzinfo=UTC)
SECRET = "PLACEHOLDER_PRIVATE_VALUE"
GUARD = SecretGuard(forbidden_values=(SECRET,))
RESERVATION = Reservation(
    tokens=1000, estimated_usd=Decimal("1"), price_source="fixture-prices-v1"
)

SPAWN = """
import json, os, pathlib, subprocess, sys, time
home = pathlib.Path(os.environ["FM_HOME"])
task = sys.argv[1]
count = home / "starts"
count.write_text(str(int(count.read_text()) + 1) if count.exists() else "1")
(home / "environment.json").write_text(json.dumps(dict(os.environ)))
(home / "launch-entered").touch()
while (pathlib.Path(__file__).parent.parent / "wait-launch").exists():
    time.sleep(0.01)
if (pathlib.Path(__file__).parent.parent / "fail-before").exists():
    sys.exit(17)
child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"], start_new_session=True)
(home / "child.pid").write_text(str(child.pid))
(home / "state" / (task + ".meta")).write_text("endpoint_task_id=" + task + "\\n")
if (pathlib.Path(__file__).parent.parent / "fail-after").exists():
    sys.exit(17)
"""
CONTROL = """
import json, os, pathlib, signal, sys, time
home = pathlib.Path(os.environ["FM_HOME"])
assert sys.argv[2] == "exit"
(home / "cancel-reached").write_text(sys.argv[1])
(home / "cancel-environment.json").write_text(json.dumps(dict(os.environ)))
pid = int((home / "child.pid").read_text())
try:
    os.killpg(pid, signal.SIGTERM)
except ProcessLookupError:
    pass
for _ in range(100):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        break
    time.sleep(0.01)
"""


def _script(path: Path, code: str) -> None:
    path.write_text(f"#!{sys.executable}\n" + code)
    path.chmod(0o700)


def _commit(path: Path) -> str:
    for args in (
        ("init", "-q"),
        ("add", "."),
        (
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "-qm",
            "fixture",
        ),
    ):
        subprocess.run(["git", "-C", str(path), *args], check=True, capture_output=True)
    return subprocess.check_output(
        ["git", "-C", str(path), "rev-parse", "HEAD"], text=True
    ).strip()


@dataclass
class Lab:
    copy: FirstmateCopy
    checkout: Path
    homes: Path
    pack: Pack
    base_sha: str


@pytest.fixture
def lab(tmp_path: Path):
    checkout, homes, project, tools = (
        tmp_path / name for name in ("copy", "homes", "project", "tools")
    )
    for path in (checkout, homes, project, tools):
        path.mkdir(mode=0o700)
    for root, name in (
        (checkout, ".switch-delivery-copy"),
        (homes, ".switch-delivery-workers"),
    ):
        (root / name).write_text("switch-firstmate-delivery-v1\n")
    (checkout / "bin").mkdir()
    _script(checkout / "bin/fm-spawn.sh", SPAWN)
    _script(checkout / "bin/fm-control.sh", CONTROL)
    _script(tools / "tmux", "pass\n")
    (project / "fixture.txt").write_text("fixture\n")
    base_sha = _commit(project)
    firstmate_commit = _commit(checkout)
    data = load_pack(_config()).snapshot.model_dump(mode="json")
    data["firstmate_commit"] = firstmate_commit
    pack = Pack.model_validate_json(json.dumps(data))
    sockets = tempfile.TemporaryDirectory(prefix="sw-", dir=Path("/tmp").resolve())
    copy = FirstmateCopy(
        checkout=checkout,
        tmux_sockets=Path(sockets.name),
        homes=homes,
        project=project,
        executable_path=f"{tools}:/usr/bin:/bin",
        secrets=GUARD,
    )
    yield Lab(copy, checkout, homes, pack, base_sha)
    for file in homes.glob("*/child.pid"):
        try:
            os.killpg(int(file.read_text()), signal.SIGTERM)
        except ProcessLookupError:
            pass
    sockets.cleanup()


async def _job(
    session_factory, lab: Lab, *, ticket_id: str = "ticket", **budget
) -> Job:
    pack = lab.pack
    if budget:
        data = pack.model_dump(mode="json")
        data["budget"].update(budget)
        pack = Pack.model_validate_json(json.dumps(data))
    repository = Repository(
        supplied_url="https://example.invalid/repo",
        outcome="verified",
        canonical_url="https://example.invalid/repo",
        base_branch="main",
        base_sha=lab.base_sha,
        evidence="fixture triage",
    )
    job = Job(
        schema_version=1,
        run=Run(
            instance="fixture",
            ticket_id=ticket_id,
            generation=1,
            step="reproduce",
            attempt=1,
            execution_key=f"{ticket_id}/1/reproduce/1",
        ),
        pack_digest=digest(pack),
        service="Bug",
        repository=repository,
        acceptance=("Reproduce the fixture failure",),
    )
    state = DeliveryWorkflowState(
        pack=FrozenPack(snapshot=pack, digest=digest(pack)),
        repository=repository,
        service="Bug",
        step="reproduce",
        statuses=(),
        transitions=(),
    )
    async with session_factory() as session:
        session.add(
            JiraWorkerTicket(
                id=ticket_id,
                instance="fixture",
                issue_key=f"TEST-{ticket_id}",
                project_key="TEST",
                agent_generation=1,
                service_value="Bug",
                workflow_state=state.model_dump(mode="json"),
            )
        )
        await session.commit()
    return job


def _service(session_factory, lab: Lab, *, clock=lambda: NOW) -> FirstmateJobs:
    return FirstmateJobs(
        session_factory=session_factory,
        delivery_copy=lab.copy,
        secrets=GUARD,
        clock=clock,
    )


def _usage(*, tokens: int = 100, usd: str = "0.1", measurement="complete") -> Usage:
    return Usage(
        measurement=measurement,
        provider="openai-codex",
        model="gpt-6.1-sol",
        input_tokens=tokens,
        output_tokens=0,
        cache_read_tokens=25,
        cache_write_tokens=0,
        estimated_usd=Decimal(usd),
        price_source="fixture-prices-v1",
        billing="estimate_not_subscription_invoice",
    )


def _result(lab: Lab, job: Job, *, usage: Usage | None = None, **changes) -> None:
    result = Result(
        run=job.run,
        input_digest=digest(job),
        status="completed",
        question=None,
        usage=usage if usage is not None else _usage(),
    )
    data = result.model_dump(mode="json") | changes
    task = lab.copy.location(job) / "data" / lab.copy.task_id(job)
    if data.get("question") is not None:
        checkpoint = b'{"checkpoint":"fixture"}'
        (task / "checkpoint.json").write_bytes(checkpoint)
        data["question"]["checkpoint_digest"] = hashlib.sha256(checkpoint).hexdigest()
    raw = json.dumps(data).encode()
    (task / "result.json").write_bytes(raw)
    (task / "result.sha256").write_text(hashlib.sha256(raw).hexdigest())


async def _start(service: FirstmateJobs, job: Job):
    return await service.start(job, reservation=RESERVATION, estimate_seconds=60)


async def test_start_retry_and_restart_reuse_one_process(
    session_factory, lab, monkeypatch
):
    monkeypatch.setenv("JIRA_API_TOKEN", SECRET)
    monkeypatch.setenv("SWITCH_DB_PASSWORD", SECRET)
    job = await _job(session_factory, lab)
    service = _service(session_factory, lab)
    views = await asyncio.gather(_start(service, job), _start(service, job))
    assert [view.status for view in views] == ["running", "running"]
    assert (await _start(_service(session_factory, lab), job)).status == "running"
    home = lab.copy.location(job)
    assert (home / "starts").read_text() == "1"
    environment = json.loads((home / "environment.json").read_text())
    assert (
        "JIRA_API_TOKEN" not in environment and "SWITCH_DB_PASSWORD" not in environment
    )
    assert environment["HOME"] == str(home / "os-home")
    assert environment["FM_HOME"] == str(home)
    assert SECRET not in json.dumps(environment)
    assert all(
        SECRET not in path.read_text() for path in home.rglob("*") if path.is_file()
    )
    _result(lab, job)
    completed = await service.get(job.run.execution_key)
    assert completed.status == "done"
    assert completed.result is not None
    assert completed.execution.held_tokens == 0
    assert completed.execution.known_tokens == 125  # Cache reads count too.
    assert (home / "cancel-reached").read_text() == lab.copy.task_id(job)
    assert (await _start(service, job)).status == "done"
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, job.run.ticket_id)
        assert ticket.tokens_used == 125
        assert ticket.workflow_state["known_estimated_usd"] == "0.1"
        assert not ticket.workflow_state["unknown_spend"]
        assert len((await session.scalars(select(JiraWorkerJob))).all()) == 1


@pytest.mark.parametrize("fault", ["fail-before", "fail-after"])
async def test_process_failure_never_repeats_ambiguous_start(
    session_factory, lab, fault
):
    job = await _job(session_factory, lab)
    (lab.checkout / fault).touch()
    service = _service(session_factory, lab)
    with pytest.raises(RuntimeError, match="failed"):
        await _start(service, job)
    (lab.checkout / fault).unlink()
    restarted = await _start(_service(session_factory, lab), job)
    assert restarted.status == ("uncertain" if fault == "fail-before" else "running")
    assert (lab.copy.location(job) / "starts").read_text() == "1"
    assert restarted.execution.held_tokens == 1000
    assert restarted.execution.held_usd == Decimal("1")


async def test_crash_after_reservation_before_process_is_recoverable(
    session_factory, lab
):
    job = await _job(session_factory, lab)

    class BeforeStart:
        async def start(self, job, pack, reservation, estimate_seconds):
            raise RuntimeError("fixture crash before process boundary")

    broken = FirstmateJobs(
        session_factory=session_factory,
        delivery_copy=BeforeStart(),
        secrets=GUARD,
        clock=lambda: NOW,
    )
    with pytest.raises(RuntimeError, match="fixture crash"):
        await _start(broken, job)
    assert not lab.copy.location(job).exists()
    recovered = await _start(_service(session_factory, lab), job)
    assert recovered.status == "running"
    assert (lab.copy.location(job) / "starts").read_text() == "1"


async def test_cancelled_unknown_usage_blocks_new_attempt(session_factory, lab):
    job = await _job(session_factory, lab)
    service = _service(session_factory, lab)
    await _start(service, job)
    await service.cancel(job.run.execution_key)
    data = job.model_dump(mode="json")
    data["run"].update(attempt=2, execution_key=f"{job.run.ticket_id}/1/reproduce/2")
    with pytest.raises(ValueError, match="fence"):
        await _start(service, Job.model_validate_json(json.dumps(data)))
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, job.run.ticket_id)
        assert ticket.worker_parked_reason == "delivery_usage_unknown"
        assert ticket.workflow_state["reserved_tokens"] == 1000


async def test_accepted_result_survives_artifact_loss(session_factory, lab):
    job = await _job(session_factory, lab)
    service = _service(session_factory, lab)
    await _start(service, job)
    _result(lab, job)
    accepted = await service.get(job.run.execution_key)
    task = lab.copy.location(job) / "data" / lab.copy.task_id(job)
    (task / "result.json").unlink()
    (task / "result.sha256").unlink()
    recovered = await _service(session_factory, lab).get(job.run.execution_key)
    assert recovered.status == "done" and recovered.result == accepted.result
    assert recovered.execution.held_tokens == 0
    assert recovered.execution.known_tokens == 125


async def test_prepared_checkout_requires_frozen_base_commit(session_factory, lab):
    lab.base_sha = "1" * 40
    job = await _job(session_factory, lab)
    with pytest.raises(ValueError, match="frozen commit"):
        await _start(_service(session_factory, lab), job)
    assert not (lab.copy.location(job) / "starts").exists()


async def test_key_input_and_estimates_are_immutable(session_factory, lab):
    job = await _job(session_factory, lab)
    service = _service(session_factory, lab)
    await _start(service, job)
    data = job.model_dump(mode="json")
    data["acceptance"] = ["Changed input"]
    with pytest.raises(ValueError, match="different input"):
        await _start(service, Job.model_validate_json(json.dumps(data)))
    with pytest.raises(ValueError, match="different input"):
        await service.start(job, reservation=RESERVATION, estimate_seconds=61)


@pytest.mark.parametrize("state", ["done", "running"])
async def test_zero_spend_reads_do_not_add_daily_entries(session_factory, lab, state):
    job = await _job(session_factory, lab)
    service = _service(
        session_factory, lab, clock=lambda: NOW.replace(hour=23, minute=59, second=50)
    )
    await _start(service, job)
    if state == "done":
        _result(lab, job)
    accepted = await service.get(job.run.execution_key)
    async with session_factory() as session:
        stored = (
            await session.scalar(
                select(JiraWorkerJob).where(
                    JiraWorkerJob.execution_key == job.run.execution_key
                )
            )
        ).payload
    next_day = _service(
        session_factory,
        lab,
        clock=lambda: (NOW + timedelta(days=1)).replace(hour=0, minute=0, second=0),
    )
    for _ in range(2):
        read = await next_day.get(job.run.execution_key)
        restarted = await _start(next_day, job)
        if state == "done":
            assert read == restarted == accepted
        else:
            assert read.status == restarted.status == "running"
            assert (
                read.execution.cost_by_day
                == restarted.execution.cost_by_day
                == accepted.execution.cost_by_day
            )
    if state == "done":
        assert await next_day.cancel(job.run.execution_key) == accepted
    async with session_factory() as session:
        row = await session.scalar(
            select(JiraWorkerJob).where(
                JiraWorkerJob.execution_key == job.run.execution_key
            )
        )
        ticket = await session.get(JiraWorkerTicket, job.run.ticket_id)
        assert {
            key: value for key, value in row.payload.items() if key != "active_seconds"
        } == {key: value for key, value in stored.items() if key != "active_seconds"}
        assert row.result == (
            accepted.result.model_dump(mode="json")
            if accepted.result is not None
            else None
        )
        assert ticket.tokens_used == (125 if state == "done" else 0)
        assert ticket.workflow_state["reserved_tokens"] == (
            0 if state == "done" else 1000
        )
        assert Decimal(ticket.workflow_state["known_estimated_usd"]) == Decimal(
            "0.1" if state == "done" else "0"
        )
        assert Decimal(ticket.workflow_state["reserved_estimated_usd"]) == Decimal(
            "0" if state == "done" else "1"
        )
    assert (lab.copy.location(job) / "starts").read_text() == "1"


@pytest.mark.parametrize("outcome", ["completed", "needs_input", "cancelled"])
@pytest.mark.parametrize("change", ["cancel", "generation", "park", "day_budget"])
async def test_terminal_receipt_survives_later_ticket_and_budget_changes(
    session_factory, lab, outcome, change
):
    job = await _job(session_factory, lab, day_estimated_usd="1")
    service = _service(session_factory, lab)
    await _start(service, job)
    question = (
        {
            "run": job.run.model_dump(mode="json"),
            "input_digest": digest(job),
            "id": "question-1",
            "checkpoint_digest": "0" * 64,
            "text": "Which fixture should I use?",
        }
        if outcome == "needs_input"
        else None
    )
    _result(lab, job, status=outcome, question=question)
    accepted = await service.get(job.run.execution_key)
    assert (
        accepted.status
        == {
            "completed": "done",
            "needs_input": "waiting_input",
            "cancelled": "cancelled",
        }[outcome]
    )
    assert accepted.result is not None
    if change == "cancel":
        assert await service.cancel(job.run.execution_key) == accepted
    elif change == "day_budget":
        other = await _job(session_factory, lab, ticket_id="other")
        await _start(service, other)
        _result(lab, other, usage=_usage(usd="2"))
        assert (await service.get(other.run.execution_key)).status == "cancelled"
    else:
        async with session_factory() as session:
            ticket = await session.get(JiraWorkerTicket, job.run.ticket_id)
            if change == "generation":
                ticket.agent_generation += 1
            else:
                ticket.worker_parked_reason = "human_action"
            await session.commit()
    restarted = _service(session_factory, lab)
    assert await restarted.get(job.run.execution_key) == accepted
    assert await _start(restarted, job) == accepted
    assert await restarted.cancel(job.run.execution_key) == accepted
    async with session_factory() as session:
        row = await session.scalar(
            select(JiraWorkerJob).where(
                JiraWorkerJob.execution_key == job.run.execution_key
            )
        )
        ticket = await session.get(JiraWorkerTicket, job.run.ticket_id)
        assert row.result == accepted.result.model_dump(mode="json")
        assert row.cancel_requested_at is None
        assert ticket.tokens_used == 125
        assert ticket.workflow_state["reserved_tokens"] == 0
        assert Decimal(ticket.workflow_state["reserved_estimated_usd"]) == 0
        assert Decimal(ticket.workflow_state["known_estimated_usd"]) == Decimal("0.1")
        assert not ticket.workflow_state["unknown_spend"]
        assert "delivery_budget" not in (ticket.admin_reasons or {})
    assert (lab.copy.location(job) / "starts").read_text() == "1"


@pytest.mark.parametrize("fence", ["cancel", "generation", "park", "service"])
async def test_late_result_is_fenced_but_usage_is_reconciled(
    session_factory, lab, fence
):
    job = await _job(session_factory, lab)
    service = _service(session_factory, lab)
    await _start(service, job)
    if fence == "cancel":
        cancelled = await service.cancel(job.run.execution_key)
        assert cancelled.status == "cancelled"
        assert cancelled.execution.held_tokens == 1000
    else:
        async with session_factory() as session:
            ticket = await session.get(JiraWorkerTicket, job.run.ticket_id)
            if fence in {"generation", "service"}:
                ticket.agent_generation += 1
                if fence == "service":
                    state = dict(ticket.workflow_state)
                    state.update(service="Feature", step="manual_handoff")
                    ticket.workflow_state = state
            else:
                ticket.worker_parked_reason = "human_action"
            await session.commit()
    terminal = await service.get(job.run.execution_key)
    assert terminal.status == "cancelled" and terminal.result is None
    assert terminal.execution.held_tokens == 1000
    assert terminal.execution.held_usd == Decimal("1")
    assert terminal.execution.usage_unknown
    task = lab.copy.location(job) / "data" / lab.copy.task_id(job)
    (task / "usage.json").write_text(_usage(measurement="partial").model_dump_json())
    partial = await service.get(job.run.execution_key)
    assert partial.status == "cancelled" and partial.result is None
    assert partial.execution.rejection == terminal.execution.rejection
    assert partial.execution.known_tokens == 125
    assert partial.execution.held_tokens == 875
    assert partial.execution.held_usd == Decimal("0.9")
    assert partial.execution.usage_unknown
    assert await service.get(job.run.execution_key) == partial
    if fence == "park":
        async with session_factory() as session:
            ticket = await session.get(JiraWorkerTicket, job.run.ticket_id)
            ticket.worker_parked_reason = None
            await session.commit()
    _result(lab, job, usage=_usage(tokens=1975, usd="2"))
    late = await service.get(job.run.execution_key)
    assert late.result is None and late.status == "cancelled"
    assert late.execution.rejection == terminal.execution.rejection
    assert late.execution.known_tokens == 2000
    assert late.execution.known_usd == Decimal("2")
    assert late.execution.held_tokens == 0
    assert late.execution.held_usd == 0
    assert not late.execution.usage_unknown
    assert await _start(service, job) == late
    assert await service.cancel(job.run.execution_key) == late
    assert await _service(session_factory, lab).get(job.run.execution_key) == late
    assert (lab.copy.location(job) / "cancel-reached").exists()
    assert (lab.copy.location(job) / "starts").read_text() == "1"
    async with session_factory() as session:
        row = await session.scalar(select(JiraWorkerJob))
        ticket = await session.get(JiraWorkerTicket, job.run.ticket_id)
        assert row.result is None
        assert ticket.tokens_used == 2000
        assert ticket.workflow_state["reserved_tokens"] == 0
        assert Decimal(ticket.workflow_state["reserved_estimated_usd"]) == 0
        assert Decimal(ticket.workflow_state["known_estimated_usd"]) == Decimal("2")
        assert not ticket.workflow_state["unknown_spend"]
        assert "delivery_budget" not in (ticket.admin_reasons or {})


@pytest.mark.parametrize("day_offset", [0, 1])
@pytest.mark.parametrize("artifact", ["usage", "result"])
@pytest.mark.parametrize("fence", ["active", "cancel", "generation"])
async def test_known_usd_never_regresses_on_unknown_usage(
    session_factory, lab, artifact, fence, day_offset
):
    job = await _job(session_factory, lab)
    service = _service(session_factory, lab)
    await _start(service, job)
    task = lab.copy.location(job) / "data" / lab.copy.task_id(job)
    (task / "usage.json").write_text(
        _usage(usd="0.5", measurement="partial").model_dump_json()
    )
    partial = await service.get(job.run.execution_key)
    assert partial.execution.known_usd == Decimal("0.5")
    if fence == "cancel":
        await service.cancel(job.run.execution_key)
    elif fence == "generation":
        async with session_factory() as session:
            ticket = await session.get(JiraWorkerTicket, job.run.ticket_id)
            ticket.agent_generation += 1
            await session.commit()
        await service.get(job.run.execution_key)
    unknown = Usage(
        measurement="unknown",
        provider=None,
        model=None,
        input_tokens=None,
        output_tokens=None,
        cache_read_tokens=None,
        cache_write_tokens=None,
        estimated_usd=Decimal("0.1"),
        price_source=RESERVATION.price_source,
        billing="estimate_not_subscription_invoice",
    )
    if artifact == "usage":
        (task / "usage.json").write_text(unknown.model_dump_json())
    else:
        _result(lab, job, usage=unknown)
    first_day = NOW.date().isoformat()
    for day in (day_offset, day_offset + 1):
        now = NOW + timedelta(days=day)
        restarted = _service(session_factory, lab, clock=lambda: now)
        for _ in range(2):
            view = await restarted.get(job.run.execution_key)
            assert view.result is None
            assert view.execution.known_tokens == 125
            assert view.execution.known_usd == Decimal("0.5")
            assert view.execution.held_tokens == 875
            assert view.execution.held_usd == Decimal("0.5")
            assert view.execution.usage_unknown
            assert view.execution.cost_by_day[first_day] == Decimal("0.5")
            assert sum(view.execution.cost_by_day.values()) == Decimal("0.5")
            assert all(cost >= 0 for cost in view.execution.cost_by_day.values())
            async with session_factory() as session:
                row = await session.scalar(select(JiraWorkerJob))
                ticket = await session.get(JiraWorkerTicket, job.run.ticket_id)
                assert row.result is None
                assert Decimal(row.payload["known_usd"]) == Decimal("0.5")
                assert Decimal(row.payload["held_usd"]) == Decimal("0.5")
                assert row.payload["usage_unknown"]
                assert ticket.tokens_used == 125
                state = ticket.workflow_state
                assert state["reserved_tokens"] == 875
                assert Decimal(state["known_estimated_usd"]) == Decimal("0.5")
                assert Decimal(state["reserved_estimated_usd"]) == Decimal("0.5")
                assert state["unknown_spend"]
    assert (await _start(restarted, job)).execution.known_usd == Decimal("0.5")
    assert (
        await restarted.cancel(job.run.execution_key)
    ).execution.known_usd == Decimal("0.5")
    assert (lab.copy.location(job) / "starts").read_text() == "1"


async def test_unknown_usage_retains_reservation_across_midnight(session_factory, lab):
    job = await _job(session_factory, lab, day_estimated_usd="1")
    service = _service(session_factory, lab)
    await _start(service, job)
    unknown = Usage(
        measurement="unknown",
        provider=None,
        model=None,
        input_tokens=None,
        output_tokens=None,
        cache_read_tokens=None,
        cache_write_tokens=None,
        estimated_usd=None,
        price_source=None,
        billing="estimate_not_subscription_invoice",
    )
    _result(lab, job, usage=unknown)
    view = await service.get(job.run.execution_key)
    assert view.status == "uncertain" and view.result is None
    assert view.execution.held_tokens == 1000
    other = await _job(session_factory, lab, ticket_id="other", day_estimated_usd="1")
    with pytest.raises(ValueError, match="budget"):
        await _start(
            _service(session_factory, lab, clock=lambda: NOW + timedelta(days=1)), other
        )
    assert not lab.copy.location(other).exists()


async def test_concurrent_daily_reservations_cannot_overbook(session_factory, lab):
    jobs = [
        await _job(session_factory, lab, ticket_id=name, day_estimated_usd="1")
        for name in ("one", "two")
    ]
    service = _service(session_factory, lab)
    outcomes = await asyncio.gather(
        *(_start(service, job) for job in jobs), return_exceptions=True
    )
    assert sum(not isinstance(outcome, Exception) for outcome in outcomes) == 1
    assert sum(isinstance(outcome, ValueError) for outcome in outcomes) == 1
    assert len(list(lab.homes.glob("*/starts"))) == 1


@pytest.mark.parametrize(
    "budget",
    [
        {"ticket_tokens": 999},
        {"ticket_estimated_usd": "0.99"},
        {"day_estimated_usd": "0.99"},
    ],
)
async def test_budget_refusal_occurs_before_dispatch(session_factory, lab, budget):
    job = await _job(session_factory, lab, **budget)
    with pytest.raises(ValueError, match="budget"):
        await _start(_service(session_factory, lab), job)
    assert not lab.copy.location(job).exists()


async def test_time_overrun_alerts_without_stopping_but_spend_stops(
    session_factory, lab
):
    job = await _job(session_factory, lab)
    await _start(_service(session_factory, lab), job)
    later = _service(session_factory, lab, clock=lambda: NOW + timedelta(seconds=121))
    view = await later.get(job.run.execution_key)
    assert view.status == "running"
    assert not (lab.copy.location(job) / "cancel-reached").exists()
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, job.run.ticket_id)
        assert "delivery_time_overrun" in ticket.admin_reasons
        alerts = list(
            (
                await session.scalars(
                    select(JiraWorkerOutbox).where(
                        JiraWorkerOutbox.command == "admin_reason_alert"
                    )
                )
            ).all()
        )
        assert (
            len(alerts) == 1 and alerts[0].payload["reason"] == "delivery_time_overrun"
        )
    task = lab.copy.location(job) / "data" / lab.copy.task_id(job)
    (task / "usage.json").write_text(
        _usage(tokens=975, usd="1", measurement="partial").model_dump_json()
    )
    capped = await later.get(job.run.execution_key)
    assert capped.status == "cancelled"
    assert (lab.copy.location(job) / "cancel-reached").exists()
    assert capped.execution.usage_unknown


async def test_cancel_during_start_persists_intent_before_launch_returns(
    session_factory, lab
):
    job = await _job(session_factory, lab)
    service = _service(session_factory, lab)
    gate = lab.checkout / "wait-launch"
    gate.touch()
    start = asyncio.create_task(_start(service, job))
    cancel = None
    try:
        async with asyncio.timeout(10):
            while not (lab.copy.location(job) / "launch-entered").exists():
                await asyncio.sleep(0.01)
            cancel = asyncio.create_task(service.cancel(job.run.execution_key))
            while True:
                async with session_factory() as session:
                    row = await session.scalar(select(JiraWorkerJob))
                    if row.cancel_requested_at is not None:
                        break
                await asyncio.sleep(0.01)
            gate.unlink()
            started, cancelled = await asyncio.gather(start, cancel)
        assert started.status == cancelled.status == "cancelled"
        assert (lab.copy.location(job) / "cancel-reached").exists()
        pid = int((lab.copy.location(job) / "child.pid").read_text())
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)
    finally:
        gate.unlink(missing_ok=True)
        for task in (start, cancel):
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)


async def test_cancel_and_get_wait_for_full_native_start(
    session_factory, lab, tmp_path
):
    job = await _job(session_factory, lab)
    service = _service(session_factory, lab)
    _script(
        tmp_path / "tools/tmux",
        "import os, pathlib, time\n"
        "home = pathlib.Path(os.environ['FM_HOME'])\n"
        "(home / 'tmux-entered').touch()\n"
        "while (pathlib.Path.cwd() / 'wait-tmux').exists():\n"
        "    time.sleep(0.01)\n",
    )
    tmux_gate, launch_gate = (
        lab.checkout / name for name in ("wait-tmux", "wait-launch")
    )
    tmux_gate.touch()
    launch_gate.touch()
    start = asyncio.create_task(_start(service, job))
    cancel = get = None
    try:
        async with asyncio.timeout(160):
            while not (lab.copy.location(job) / "tmux-entered").exists():
                await asyncio.sleep(0.01)
            cancel = asyncio.create_task(
                _service(session_factory, lab).cancel(job.run.execution_key)
            )
            get = asyncio.create_task(
                _service(session_factory, lab).get(job.run.execution_key)
            )
            while True:
                async with session_factory() as session:
                    row = await session.scalar(select(JiraWorkerJob))
                    if row.cancel_requested_at is not None:
                        break
                await asyncio.sleep(0.01)
            await asyncio.sleep(70)
            tmux_gate.unlink()
            while not (lab.copy.location(job) / "launch-entered").exists():
                await asyncio.sleep(0.01)
            await asyncio.sleep(65)
            assert not cancel.done() and not get.done()
            launch_gate.unlink()
            views = await asyncio.gather(start, cancel, get)
        assert all(view.status == "cancelled" for view in views)
        assert all(view.result is None for view in views)
        home = lab.copy.location(job)
        assert (home / "starts").read_text() == "1"
        assert (home / "cancel-reached").exists()
        pid = int((home / "child.pid").read_text())
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)
        assert (await _start(_service(session_factory, lab), job)).status == "cancelled"
        assert (home / "starts").read_text() == "1"
        async with session_factory() as session:
            rows = (await session.scalars(select(JiraWorkerJob))).all()
            assert len(rows) == 1
            assert rows[0].cancel_requested_at is not None
    finally:
        tmux_gate.unlink(missing_ok=True)
        launch_gate.unlink(missing_ok=True)
        for task in (start, cancel, get):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in (start, cancel, get) if task is not None),
            return_exceptions=True,
        )


async def test_corrupt_artifact_cannot_prevent_cancellation(session_factory, lab):
    job = await _job(session_factory, lab)
    service = _service(session_factory, lab)
    await _start(service, job)
    _result(lab, job)
    task = lab.copy.location(job) / "data" / lab.copy.task_id(job)
    (task / "result.sha256").write_text("incorrect")
    with pytest.raises(ValueError, match="digest"):
        await service.cancel(job.run.execution_key)
    assert (lab.copy.location(job) / "cancel-reached").exists()
    pid = int((lab.copy.location(job) / "child.pid").read_text())
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


async def test_scheduler_polls_and_sends_reminders_while_task_runs(
    session_factory, lab
):
    job = await _job(session_factory, lab)
    service = _service(session_factory, lab)
    await _start(service, job)
    async with session_factory() as session:
        await set_identity_mapping(
            session,
            instance="acme",
            jira_account_id="reporter-account",
            switch_user_id="user-1",
            enabled_projects=("KAN",),
        )
        await session.commit()
    now = datetime.now(UTC)
    waiting_id = await _seed(
        session_factory,
        status="Waiting for input",
        reporter_switch_user_id="user-1",
        wait_channel="switch",
        room_id="room-1",
        wait_anchor=now - timedelta(hours=3),
        wait_fired={},
        card_event_id="card-room-1",
        jira_read_updated=now.isoformat(),
        jira_read_changelog_id="",
    )
    board = FakeAdminBoard()
    await _sync_at(session_factory, waiting_id, board, now)
    jira = JiraMock()
    jira.updated = now.isoformat()
    jira.status = "Waiting for input"
    rooms = ThreadRooms()
    await rooms.create_ticket_room(
        name="fixture",
        description="fixture",
        owner_id="user-1",
        instance="acme",
        issue_key="KAN-1",
    )
    scheduler = JiraWorkerScheduler(
        session_factory=session_factory,
        config=_worker_config(),
        poll_clients={"acme": client_for(jira)},
        ticket_rooms=rooms,
        admin_board=board,
        clock=lambda: datetime.now(UTC) + timedelta(seconds=1),
    )
    async with asyncio.timeout(10):
        await scheduler.run_once()
    assert rooms.notices == [
        ("room-1", "Reminder: the worker is still waiting for a reply on KAN-1.")
    ]
    async with session_factory() as session:
        role = await session.scalar(
            select(JiraWorkerJob).where(JiraWorkerJob.kind == "role_job")
        )
        polls = list(
            (
                await session.scalars(
                    select(JiraWorkerJob).where(JiraWorkerJob.kind == "watermark_poll")
                )
            ).all()
        )
        assert role.status == "running"
        assert any(poll.status == "done" for poll in polls)
    assert not (lab.copy.location(job) / "cancel-reached").exists()
    assert (await service.get(job.run.execution_key)).status == "running"


@pytest.mark.parametrize(
    "damage", ["checksum", "input", "run", "oversize", "symlink", "hardlink", "secret"]
)
async def test_artifacts_fail_closed_with_reservation_retained(
    session_factory, lab, tmp_path, damage
):
    job = await _job(session_factory, lab)
    service = _service(session_factory, lab)
    await _start(service, job)
    _result(lab, job)
    task = lab.copy.location(job) / "data" / lab.copy.task_id(job)
    result_path = task / "result.json"
    if damage == "checksum":
        (task / "result.sha256").write_text("0" * 64)
    elif damage in {"input", "run", "secret"}:
        data = json.loads(result_path.read_text())
        if damage == "input":
            data["input_digest"] = "0" * 64
        elif damage == "run":
            data["run"]["ticket_id"] = "other"
            data["run"]["execution_key"] = "other/1/reproduce/1"
        else:
            data["usage"]["model"] = SECRET
        raw = json.dumps(data).encode()
        result_path.write_bytes(raw)
        (task / "result.sha256").write_text(hashlib.sha256(raw).hexdigest())
    elif damage == "oversize":
        result_path.write_text("x" * 65537)
    else:
        outside = tmp_path / "outside.json"
        result_path.rename(outside)
        if damage == "symlink":
            result_path.symlink_to(outside)
        else:
            os.link(outside, result_path)
    with pytest.raises((ValueError, OSError)):
        await service.get(job.run.execution_key)
    async with session_factory() as session:
        row = await session.scalar(select(JiraWorkerJob))
        assert row.status == "uncertain" and row.result is None
        assert row.payload["held_tokens"] == 1000


@pytest.mark.parametrize(
    "value",
    [
        SECRET,
        "https://user:pass@example.invalid/repo",
        "https://example.invalid/repo?token=placeholder",
        "Bearer placeholder-only",
    ],
)
async def test_credentials_never_enter_brief_or_environment(
    session_factory, lab, value
):
    job = await _job(session_factory, lab)
    data = job.model_dump(mode="json")
    data["acceptance"] = [value]
    with pytest.raises(ValueError, match="Credential"):
        await _start(
            _service(session_factory, lab), Job.model_validate_json(json.dumps(data))
        )
    assert not lab.copy.location(job).exists()
