"""Admin recovery uses native evidence and the real admission/accounting path."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import socket
import subprocess
import uuid
from decimal import Decimal
from unittest.mock import AsyncMock

import httpx
import pytest
from sqlalchemy import select

from switch_core.bridges.jira.accounting import (
    CapacityUnavailable,
    day_cost,
    live_role_sessions,
)
from switch_core.bridges.jira.activity import ticket_turn_lock
from switch_core.bridges.jira.contracts import Approval, Meter, ModelPrices
from switch_core.bridges.jira.delivery import ScopeApproved, apply_event
from switch_core.bridges.jira.delivery_contracts import ExecutionEnvelope
from switch_core.bridges.jira.firstmate_copy import FirstmateCopy, tmux_directory
from switch_core.bridges.jira.firstmate_jobs import Execution
from switch_core.bridges.jira.live import LocalRepositories, build_delivery_jobs
from switch_core.bridges.jira.outbox import park_ticket
from switch_core.bridges.jira.target import delivery_state
from switch_core.db.models import JiraWorkerEvent, JiraWorkerJob, JiraWorkerTicket, User
from switch_core.gateway import jira_triggers
from tests.switch_core.bridges.jira.test_firstmate_jobs import (
    NOW,
    RESERVATION,
    _commit,
    _job,
    _script,
    _service,
    _start,
)
from tests.switch_core.bridges.jira.test_firstmate_jobs import lab as lab
from tests.switch_core.bridges.jira.test_firstmate_tmux import _real_tmux_script
from tests.switch_core.bridges.jira.test_live import live_setup as live_setup
from tests.switch_core.bridges.jira.test_live import native_lab as native_lab
from tests.switch_core.bridges.jira.test_outbox import JiraMock, client_for
from tests.switch_core.bridges.jira.test_worker import _config
from tests.switch_core.gateway.test_jira_release import app_for


@pytest.mark.parametrize("human_closed", [False, True])
@pytest.mark.parametrize("launch_brief", [False, True])
async def test_release_unstarted_frees_capacity_without_restarting_ticket(
    session_factory, lab, monkeypatch, human_closed, launch_brief
):
    service = _service(session_factory, lab)
    jobs = [await _job(session_factory, lab, ticket_id=str(n)) for n in range(3)]
    original = lab.copy._command

    async def fail_launch(*args):
        raise RuntimeError("native launch failed before spawn")

    monkeypatch.setattr(lab.copy, "_command", fail_launch)
    for job in jobs[:2]:
        with pytest.raises(RuntimeError, match="before spawn"):
            await _start(service, job)
        home = lab.copy.location(job)
        assert (home / "data" / lab.copy.task_id(job) / "dispatch").exists()
        assert not (home / "state" / f"{lab.copy.task_id(job)}.meta").exists()
        if launch_brief:
            task = home / "data" / lab.copy.task_id(job)
            (task / "launch-brief.md").write_bytes(
                b"# Current worker role contract\n\n" + (task / "brief.md").read_bytes()
            )
    with pytest.raises(CapacityUnavailable):
        await _start(service, jobs[2])
    monkeypatch.setattr(lab.copy, "_command", original)

    config = _config().model_copy(
        update={"jira_worker_enabled_projects": {"fixture": ["TEST"]}}
    )
    monkeypatch.setattr(
        jira_triggers,
        "build_poll_clients",
        lambda _: {"fixture": client_for(JiraMock())},
    )
    monkeypatch.setattr(jira_triggers, "build_delivery_jobs", lambda **_: service)
    async with session_factory() as session:
        admin = User(name="recovery-admin", email="admin@example.invalid", role="admin")
        session.add(admin)
        await session.commit()
        ticket = await session.get(JiraWorkerTicket, jobs[0].run.ticket_id)
        ticket.wake_at = NOW
        if human_closed:
            park_ticket(ticket, "Human Jira action")
            ticket.status = "Done"
        await session.commit()
        generation = ticket.agent_generation
        before = ticket.workflow_state.copy()
        assert await live_role_sessions(session) == 2
        assert await day_cost(session, NOW) == Decimal("2")
    path = "/jira-triggers/worker/tickets/TEST-0/release?instance=fixture"
    request = {
        "request_id": str(uuid.uuid4()),
        "reason": "Release failed launch after checking native evidence",
        "previous_generation": generation,
    }
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app_for(session_factory, admin, config)),
        base_url="http://test",
    ) as client:
        refused = await client.post(path, json=request)
        assert refused.status_code == 400
        assert "require reconciliation" in refused.json()["detail"]
        request["execution_key"] = jobs[0].run.execution_key
        for missing in ("reason", "previous_generation"):
            invalid = {key: value for key, value in request.items() if key != missing}
            assert (await client.post(path, json=invalid)).status_code == 400
        recovered = await client.post(path, json=request)
        assert recovered.status_code == 200, recovered.text
        repeated = await client.post(path, json=request)
        assert repeated.json() == recovered.json()
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, jobs[0].run.ticket_id)
        assert ticket.agent_generation == generation
        assert ticket.worker_parked_reason == (
            "Human Jira action" if human_closed else "delivery_launch_released"
        )
        if human_closed:
            assert ticket.status == "Done"
        assert ticket.tokens_used == 0
        assert ticket.agent_state == "parked" and ticket.wake_at is None
        assert await live_role_sessions(session) == 1
        assert await day_cost(session, NOW) == Decimal("1")
        after = ticket.workflow_state.copy()
        assert after.pop("reserved_tokens") == 0
        assert after.pop("reserved_estimated_usd") == "0"
        assert after.pop("unknown_spend") is False
        for field in ("reserved_tokens", "reserved_estimated_usd", "unknown_spend"):
            before.pop(field)
        assert after == before
        row = await session.scalar(
            select(JiraWorkerJob).where(
                JiraWorkerJob.execution_key == request["execution_key"]
            )
        )
        assert row.status == "cancelled" and row.payload["exit_confirmed"]
        assert row.payload["held_tokens"] == 0 and not row.payload["usage_unknown"]
        assert (
            len(
                (
                    await session.scalars(
                        select(JiraWorkerEvent).where(
                            JiraWorkerEvent.event_kind == "admin_release_unstarted"
                        )
                    )
                ).all()
            )
            == 1
        )
    assert (await _start(service, jobs[2])).status == "running"
    assert (await service.get(jobs[0].run.execution_key)).status == "cancelled"
    task = lab.copy.location(jobs[0]) / "data" / lab.copy.task_id(jobs[0])
    if launch_brief:
        assert (task / "launch-brief.md").read_bytes() == (
            b"# Current worker role contract\n\n" + (task / "brief.md").read_bytes()
        )
    assert not (lab.copy.location(jobs[0]) / "starts").exists()
    assert recovered.json()["payload"]["previous_reason"] == (
        "Human Jira action" if human_closed else None
    )


async def _uncertain(session_factory, lab):
    job = await _job(session_factory, lab)
    service = _service(session_factory, lab)
    await service._reserve(job, RESERVATION, 60, None)
    lab.copy._prepare(job, lab.pack, RESERVATION, 60, None)
    await service._uncertain(job.run.execution_key, "Unconfirmed native launch")
    async with session_factory() as session:
        admin = User(name="recovery-admin", email="admin@example.invalid", role="admin")
        session.add(admin)
        await session.commit()
    return (
        service,
        job,
        {
            "config": _config().model_copy(
                update={"jira_worker_enabled_projects": {"fixture": ["TEST"]}}
            ),
            "ticket_id": job.run.ticket_id,
            "execution_key": job.run.execution_key,
            "request_id": str(uuid.uuid4()),
            "admin_user_id": admin.id,
            "reason": "Release never-started launch",
            "generation": 1,
        },
    )


@pytest.mark.parametrize(
    "artifact",
    [
        "data/usage.json",
        "data/result.json",
        "data/result.sha256",
        "data/handoff.json",
        "data/progress.json",
        "data/checkpoint.json",
        "state/.meta",
        "state/.status",
        "state/.meta.spawn.123",
        "inbox/001.msg",
        "missing-dispatch",
        "changed-dispatch",
        "symlink",
        "hardlink",
        "launch-symlink",
        "launch-hardlink",
        "launch-oversized",
        "launch-directory",
        "data/.launch-brief.md.123",
    ],
)
async def test_native_artifacts_keep_reservation(session_factory, lab, artifact):
    service, job, request = await _uncertain(session_factory, lab)
    home, task_id = lab.copy.location(job), lab.copy.task_id(job)
    task = home / "data" / task_id
    if artifact == "missing-dispatch":
        (task / "dispatch").unlink()
    elif artifact == "changed-dispatch":
        (task / "dispatch").write_text("changed")
    elif artifact == "launch-oversized":
        (task / "launch-brief.md").write_bytes(b"x" * 65537)
    elif artifact == "launch-directory":
        (task / "launch-brief.md").mkdir()
    elif artifact in {"symlink", "hardlink", "launch-symlink", "launch-hardlink"}:
        brief = task / (
            "launch-brief.md" if artifact.startswith("launch-") else "brief.md"
        )
        if artifact.startswith("launch-"):
            brief.write_text("native instruction overlay")
        target = home / "original-brief"
        brief.rename(target)
        if artifact.endswith("symlink"):
            brief.symlink_to(target)
        else:
            os.link(target, brief)
    else:
        kind, name = artifact.split("/", 1)
        path = (
            task / name
            if kind == "data"
            else home / "state" / f"{task_id}{name}"
            if kind == "state"
            else home / "state" / f"{task_id}.inbox" / name
        )
        path.write_text("native artifact")
    with pytest.raises((ValueError, OSError)):
        await service.release_unstarted(**request)
    async with session_factory() as session:
        row = await session.scalar(select(JiraWorkerJob))
        assert row.status == "uncertain" and row.payload["held_tokens"] == 1000
        ticket = await session.get(JiraWorkerTicket, job.run.ticket_id)
        assert ticket.workflow_state["unknown_spend"]
        assert ticket.workflow_state["reserved_tokens"] == 1000
        assert not await session.scalar(
            select(JiraWorkerEvent).where(
                JiraWorkerEvent.event_kind == "admin_release_unstarted"
            )
        )


@pytest.mark.parametrize(
    "fault",
    [
        "known_tokens",
        "known_usd",
        "cost_by_day",
        "running",
        "candidate_head",
        "candidate_job",
        "accepted_head",
        "review_head",
        "qa_head",
        "meter_pending",
        "meter_uncertain",
        "meter_input_tokens",
        "meter_output_tokens",
        "meter_cache_read_tokens",
        "meter_cache_write_tokens",
        "meter_pending_since",
    ],
)
async def test_recorded_spend_or_candidate_keeps_reservation(
    session_factory, lab, fault
):
    service, job, request = await _uncertain(session_factory, lab)
    async with session_factory() as session:
        row = await session.scalar(select(JiraWorkerJob))
        if fault == "running":
            row.status = "running"
        elif fault.startswith("candidate") or fault.endswith("_head"):
            ticket = await session.get(JiraWorkerTicket, job.run.ticket_id)
            ticket.workflow_state = ticket.workflow_state | {
                fault: lab.base_sha
                if fault.endswith("_head")
                else job.model_dump(mode="json")
            }
        elif fault.startswith("meter_"):
            meter = Meter(
                prices=ModelPrices(
                    input=Decimal("0.1"),
                    output=Decimal("0.1"),
                    cache_read=Decimal("0.1"),
                    cache_write=Decimal("0.1"),
                    source="fixture-prices-v1",
                )
            )
            field = fault.removeprefix("meter_")
            value = (
                NOW.isoformat()
                if field == "pending_since"
                else True
                if field in {"pending", "uncertain"}
                else 1
            )
            row.payload = row.payload | {
                "meter": meter.model_dump(mode="json") | {field: value}
            }
        else:
            value = {"2026-10-23": "0.1"} if fault == "cost_by_day" else 1
            row.payload = row.payload | {fault: value}
        await session.commit()
    with pytest.raises(ValueError, match="require reconciliation"):
        await service.release_unstarted(**request)
    async with session_factory() as session:
        row = await session.scalar(select(JiraWorkerJob))
        assert row.payload["held_tokens"] == 1000
        assert not await session.scalar(
            select(JiraWorkerEvent).where(
                JiraWorkerEvent.event_kind == "admin_release_unstarted"
            )
        )


async def test_recovery_authorization_fences_and_replay(session_factory, lab):
    service, job, request = await _uncertain(session_factory, lab)
    for change, message in [
        ({"generation": 2}, "current execution generation"),
        ({"ticket_id": "other"}, "does not belong"),
        ({"reason": " "}, "requires a reason"),
    ]:
        with pytest.raises(ValueError, match=message):
            await service.release_unstarted(**(request | change))
    async with session_factory() as session:
        member = User(name="member", email="member@example.invalid", role="member")
        session.add(member)
        await session.commit()
    with pytest.raises(PermissionError):
        await service.release_unstarted(**(request | {"admin_user_id": member.id}))
    async with ticket_turn_lock(
        session_factory.kw["bind"], job.run.ticket_id
    ) as locked:
        assert locked
        with pytest.raises(BlockingIOError, match="Ticket turn is active"):
            await service.release_unstarted(**request)
    receipt = await service.release_unstarted(**request)
    assert (await service.release_unstarted(**request)).id == receipt.id
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, job.run.ticket_id)
        ticket.agent_generation += 1
        await session.commit()
    assert (await service.release_unstarted(**request)).id == receipt.id
    for change in ({"reason": "different"}, {"generation": 2}):
        with pytest.raises(ValueError, match="reused with different input"):
            await service.release_unstarted(**(request | change))
    with pytest.raises(ValueError, match="require reconciliation"):
        await service.release_unstarted(
            **(request | {"request_id": str(uuid.uuid4()), "generation": 2})
        )


@pytest.mark.parametrize("native_task", [False, True, "only-task"])
async def test_native_session_check_ignores_only_bootstrap_shell(
    session_factory, lab, native_task
):
    tmux = shutil.which("tmux")
    if tmux is None:
        pytest.skip("tmux is not installed")
    service, job, request = await _uncertain(session_factory, lab)
    home = lab.copy.location(job)
    sockets = tmux_directory(home, lab.copy._tmux_sockets)
    endpoint = sockets / f"tmux-{os.getuid()}/default"
    env = lab.copy._environment(home)
    _real_tmux_script(lab.checkout.parent / "tools/tmux", tmux)
    subprocess.run(
        [
            str(lab.checkout.parent / "tools/tmux"),
            "-S",
            str(endpoint),
            "-f",
            "/dev/null",
            "new-session",
            "-d",
            "-s",
            "firstmate",
            "/bin/sh",
        ],
        env=env,
        check=True,
    )
    try:
        async with asyncio.timeout(5):
            while True:
                listing = (
                    subprocess.check_output(
                        [
                            tmux,
                            "-S",
                            str(endpoint),
                            "list-panes",
                            "-a",
                            "-F",
                            "#{window_name}:#{pane_current_command}",
                        ],
                        env=env,
                        text=True,
                    )
                    .strip()
                    .split(":")
                )
                if len(listing) == 2 and all(
                    name in {"sh", "bash", "zsh"} for name in listing
                ):
                    break
                await asyncio.sleep(0.01)
        if native_task:
            subprocess.run(
                [
                    tmux,
                    "-S",
                    str(endpoint),
                    "new-window",
                    "-d",
                    "-t",
                    "firstmate:",
                    "-n",
                    f"fm-{lab.copy.task_id(job)}",
                    "/bin/sh",
                ],
                env=env,
                check=True,
            )
            if native_task == "only-task":
                subprocess.run(
                    [tmux, "-S", str(endpoint), "kill-window", "-t", "firstmate:0"],
                    env=env,
                    check=True,
                )
                subprocess.run(
                    [
                        tmux,
                        "-S",
                        str(endpoint),
                        "move-window",
                        "-s",
                        "firstmate:1",
                        "-t",
                        "firstmate:0",
                    ],
                    env=env,
                    check=True,
                )
            with pytest.raises(ValueError, match="Live or ambiguous"):
                await service.release_unstarted(**request)
        else:
            assert (
                await service.release_unstarted(**request)
            ).event_kind == "admin_release_unstarted"
    finally:
        subprocess.run(
            [
                str(lab.checkout.parent / "tools/tmux"),
                "-S",
                str(endpoint),
                "kill-server",
            ],
            env=env,
            check=True,
        )


async def test_inflight_native_launch_keeps_reservation(session_factory, lab):
    service, job, request = await _uncertain(session_factory, lab)
    other = lab.checkout.parent / "fm-spawn.sh"
    _script(other, "import time\ntime.sleep(60)\n")
    process = subprocess.Popen([str(other), lab.copy.task_id(job)])
    try:
        await lab.copy.verify_unstarted(job, None)
    finally:
        process.terminate()
        process.wait()
    script = lab.checkout / "bin/fm-spawn.sh"
    _script(script, "import time\ntime.sleep(60)\n")
    process = subprocess.Popen([str(script), lab.copy.task_id(job)])
    try:
        with pytest.raises(ValueError, match="Native launcher is active"):
            await service.release_unstarted(**request)
    finally:
        process.terminate()
        process.wait()


@pytest.mark.parametrize(
    "fault",
    ["unknown-socket", "unreadable-session", "oversized-session", "split-session"],
)
async def test_native_probe_requires_complete_bounded_evidence(
    session_factory, lab, fault
):
    service, job, request = await _uncertain(session_factory, lab)
    sockets = tmux_directory(lab.copy.location(job), lab.copy._tmux_sockets)
    endpoint = sockets / f"tmux-{os.getuid()}/default"
    endpoint.parent.mkdir(mode=0o700)
    kind = socket.SOCK_DGRAM if fault == "unknown-socket" else socket.SOCK_STREAM
    code = {
        "oversized-session": "print('x' * 4097)\n",
        "split-session": "import sys, time\nsys.stdout.write('firstmate:0:1:'); sys.stdout.flush()\ntime.sleep(0.05)\nprint('sh:sh')\n",
    }.get(fault, "raise SystemExit(1)\n")
    _script(lab.checkout.parent / "tools/tmux", code)
    with socket.socket(socket.AF_UNIX, kind) as server:
        server.bind(str(endpoint))
        if kind == socket.SOCK_STREAM:
            server.listen()
        if fault == "split-session":
            assert (
                await service.release_unstarted(**request)
            ).event_kind == "admin_release_unstarted"
        else:
            with pytest.raises(ValueError, match="session"):
                await service.release_unstarted(**request)


@pytest.mark.parametrize(
    "remote",
    [
        "absent",
        "exists",
        "unavailable",
        "inaccessible-repository",
        "dirty-checkout",
        "advanced-head",
    ],
)
async def test_live_recovery_checks_forge_and_stops_meter(
    session_factory, live_setup, monkeypatch, remote
):
    settings, scope, _, lab = live_setup
    repositories = LocalRepositories(settings.jira_worker_delivery_runtime)
    project = repositories.project(scope.job)
    project.parent.mkdir(mode=0o700, parents=True)
    shutil.copytree(lab.copy._project, project)
    monkeypatch.setattr(LocalRepositories, "prepare", AsyncMock())
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, "ticket")
        state = delivery_state(ticket)
        apply_event(
            ticket,
            ScopeApproved(
                approval=Approval(
                    gate=state.gate,
                    actor_account_id="approver",
                    actor_kind="human",
                    history_id="approved",
                )
            ),
        )
        admin = User(name="recovery-admin", email="admin@example.invalid", role="admin")
        session.add(admin)
        await session.commit()
        frozen = ticket.workflow_state.copy()
    jobs = build_delivery_jobs(session_factory=session_factory, config=settings)

    async def fail_launch(*args):
        raise RuntimeError("native launch failed")

    monkeypatch.setattr(FirstmateCopy, "_command", fail_launch)
    with pytest.raises(RuntimeError, match="native launch failed"):
        await jobs.start(
            scope.job,
            reservation=scope.reproduction_reservation,
            estimate_seconds=60,
            envelope=ExecutionEnvelope(scope=scope, reproduction=None),
        )
    if remote in {"dirty-checkout", "advanced-head"}:
        (project / "fixture.txt").write_text("Unreconciled work\n")
        if remote == "advanced-head":
            _commit(project)
    original = httpx.AsyncClient
    calls = []

    async def forge(request):
        calls.append((request.method, request.url.path))
        assert request.url.host == "forge.example.invalid"
        if "/git/ref/heads/" not in request.url.path:
            return httpx.Response(
                404 if remote == "inaccessible-repository" else 200, json={}
            )
        return httpx.Response(
            200 if remote == "exists" else 503 if remote == "unavailable" else 404,
            json={"object": {"sha": lab.base_sha}},
        )

    client = original(
        transport=httpx.ASGITransport(app=app_for(session_factory, admin, settings)),
        base_url="http://test",
    )
    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda **kwargs: original(transport=httpx.MockTransport(forge), **kwargs),
    )
    request = {
        "request_id": str(uuid.uuid4()),
        "reason": "Recover native launch failure",
        "previous_generation": 1,
        "execution_key": scope.job.run.execution_key,
    }
    async with client:
        response = await client.post(
            "/jira-triggers/worker/tickets/TEST-1/release?instance=acme", json=request
        )
    assert response.status_code == (
        200
        if remote == "absent"
        else 503
        if remote in {"unavailable", "inaccessible-repository"}
        else 400
    ), response.text
    assert len(calls) == (1 if remote == "inaccessible-repository" else 2)
    assert all(method == "GET" for method, _ in calls)
    if remote != "inaccessible-repository":
        assert "/git/ref/heads/" in calls[-1][1]
    async with session_factory() as session:
        row = await session.scalar(
            select(JiraWorkerJob).where(JiraWorkerJob.kind == "role_job")
        )
        execution = Execution.model_validate_json(json.dumps(row.payload))
        ticket = await session.get(JiraWorkerTicket, "ticket")
        assert execution.meter.stopped == (remote == "absent")
        assert execution.held_tokens == (
            0 if remote == "absent" else scope.reproduction_reservation.tokens
        )
        assert ticket.agent_generation == 1
        for name in ("scope", "approvals", "step", "attempts"):
            assert ticket.workflow_state[name] == frozen[name]
