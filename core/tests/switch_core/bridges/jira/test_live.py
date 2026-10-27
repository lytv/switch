"""Local server integration with disposable PostgreSQL, native test processes and peers."""

from __future__ import annotations

import asyncio
import hashlib
import json
import subprocess
from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import select

from switch_core.bridges.jira import live
from switch_core.bridges.jira.contracts import (
    Approval,
    Meter,
    Pack,
    Repository,
    Result,
    digest,
)
from switch_core.bridges.jira.delivery import ScopeApproved, apply_event, role_job
from switch_core.bridges.jira.delivery_contracts import (
    CheckEvidence,
    Handoff,
    ScopeContract,
    ScopePolicy,
    VerificationContract,
    VerificationRole,
)
from switch_core.bridges.jira.firstmate_jobs import Execution
from switch_core.bridges.jira.intake import IntakeOrchestrator
from switch_core.bridges.jira.live import (
    AdmissionPolicy,
    LiveAdmission,
    LocalRepositories,
    build_delivery_factory,
)
from switch_core.bridges.jira.model_gateway import (
    DeliveryModelGateway,
    model_capability,
    model_identity,
)
from switch_core.bridges.jira.packs import FrozenPack, load_pack
from switch_core.bridges.jira.readiness import check_readiness
from switch_core.bridges.jira.state import WorkflowMapping
from switch_core.bridges.jira.target import delivery_state
from switch_core.bridges.jira.verification import MergeReceipt
from switch_core.bridges.jira.worker import JiraWorkerScheduler
from switch_core.db.models import (
    JiraWorkerEvent,
    JiraWorkerJob,
    JiraWorkerOutbox,
    JiraWorkerTicket,
)
from tests.switch_core.bridges.jira.test_admin import FakeAdminBoard, _worker_config
from tests.switch_core.bridges.jira.test_delivery import (
    NOW,
    URL,
    DeliveryJira,
    config,
    current,
    seed,
    services,
)
from tests.switch_core.bridges.jira.test_delivery_close import ClosingForge, ClosingJira
from tests.switch_core.bridges.jira.test_firstmate_jobs import _script
from tests.switch_core.bridges.jira.test_firstmate_jobs import (
    lab as native_lab,  # noqa: F401
)
from tests.switch_core.bridges.jira.test_intake import ModelMock
from tests.switch_core.bridges.jira.test_outbox import client_for


def git(path, *args):
    return subprocess.check_output(
        ["git", "-C", str(path), "-c", "commit.gpgsign=false", *args], text=True
    ).strip()


@pytest.fixture
async def live_setup(session_factory, request, monkeypatch, tmp_path):
    lab = request.getfixturevalue("native_lab")
    settings = config()
    for field in (
        "db_password",
        "jwt_secret_key",
        "agent_registration_token",
        "gateway_admin_password",
    ):
        setattr(settings, field, f"PLACEHOLDER_{field.upper()}_VALUE")
    repositories = tmp_path / "repositories"
    repositories.mkdir(mode=0o700)
    (repositories / ".switch-delivery-repositories").write_text(
        "switch-firstmate-delivery-v1\n"
    )
    _script(tmp_path / "tools/pi", 'print("1.4.0")\n')
    settings.jira_worker_delivery_pack = "2.0.0"
    values = settings.model_dump(mode="json")
    values.update(
        jira_worker_credentials={
            "acme": {
                "base_url": "https://jira.example.invalid",
                "email": "worker@example.invalid",
                "api_token": "PLACEHOLDER_JIRA_VALUE",
            }
        },
        jira_worker_llm_base_url="https://model.example.invalid/v1",
        jira_worker_llm_api_key="PLACEHOLDER_MODEL_VALUE",
    )
    values["jira_worker_delivery_runtime"] = {
        "checkout": str(lab.checkout),
        "homes": str(lab.homes),
        "repositories": str(repositories),
        "executable_path": lab.copy._path,
        "model_url": "http://127.0.0.1:8000",
        "forge_api_url": "https://forge.example.invalid",
        "forge_repository_host": "example.invalid",
        "forge_authorization": "PLACEHOLDER_FORGE_VALUE",
        "policies": {"acme": {"TEST": str(tmp_path / "policy.json")}},
        "prices": {
            "input": "0.000002",
            "output": "0.000008",
            "cache_read": "0.0000005",
            "cache_write": "0.000002",
            "source": "fixture-prices",
        },
    }
    settings = type(settings).model_validate(values)
    pack_data = load_pack(settings).snapshot.model_dump(mode="json")
    pack_data["firstmate_commit"] = git(lab.checkout, "rev-parse", "HEAD")
    snapshot = Pack.model_validate_json(json.dumps(pack_data))
    frozen = FrozenPack(snapshot=snapshot, digest=digest(snapshot))
    monkeypatch.setattr(live, "load_pack", lambda config: frozen)
    scope = await seed(session_factory, pack=frozen, base_sha=lab.base_sha)
    roles = {}
    for step in ("review", "qa", "smoke"):
        content = f"Pinned independent {step}. Return evidence. Never publish or merge."
        profile = scope.profile.model_copy(
            update={
                "content": content,
                "sha256": hashlib.sha256(content.encode()).hexdigest(),
            }
        )
        roles[step] = VerificationRole(
            profile=profile, reservation=scope.fix_reservation, estimate_seconds=60
        )
    data = scope.model_dump(mode="json")
    data.update(
        verification=VerificationContract(
            **roles,
            behavior_check_ids=("regression",),
            smoke_check_ids=("regression",),
            complete_transition_id="complete",
        ).model_dump(mode="json"),
        allowed_paths=["fixture.txt", "regression.txt"],
        forbidden_paths=[],
    )
    data["checks"][0]["kind"] = "behavior"
    scope = ScopeContract.model_validate_json(json.dumps(data))
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, "ticket")
        state = delivery_state(ticket)
        policy = AdmissionPolicy(
            scope=ScopePolicy.model_validate_json(
                scope.model_dump_json(exclude={"job", "branch"})
            ),
            statuses=(*state.statuses, WorkflowMapping(name="Done", jira_id="done")),
            transitions=(
                *state.transitions,
                WorkflowMapping(name="progress->done", jira_id="complete"),
            ),
            approver_account_ids=("approver",),
        )
        (tmp_path / "policy.json").write_text(policy.model_dump_json())
        ticket.workflow_state = state.model_copy(
            update={
                "scope": scope,
                "gate": state.gate.model_copy(
                    update={"contract_digest": digest(scope)}
                ),
                "statuses": policy.statuses,
                "transitions": policy.transitions,
            }
        ).model_dump(mode="json")
        await session.commit()
    return settings, scope, policy, lab


def test_live_and_fixture_pack_pins_stay_separate():
    settings = config(False)
    assert (
        load_pack(settings).digest
        == "965793653f39572b8b0a85134ef1ebeb5c0a77c19cbe81cc689354166d428fbf"
    )
    settings.jira_worker_delivery_pack = "2.0.0"
    pack = load_pack(settings)
    assert not pack.snapshot.fixture_only
    assert (
        pack.digest
        == "aa1e6a26ec686cd0097d6fe416258cac5989029511dca798fbb34e94314a07b6"
    )
    assert pack.snapshot.human_merge and not pack.snapshot.deploy
    assert build_delivery_factory(session_factory=None, config=settings) is None


async def test_live_preflight_only_leaves_human_evidence(live_setup, monkeypatch):
    settings, scope, _, _ = live_setup
    settings.jira_worker_delivery_execution_enabled = False
    settings.jira_webhook_secrets = {"acme": "PLACEHOLDER_WEBHOOK_VALUE"}
    monkeypatch.setattr("switch_core.bridges.jira.readiness.load_pack", live.load_pack)
    checks = check_readiness(settings, scope, "acme", "TEST")
    assert {name for name, reason in checks.items() if reason} == {"live_prerequisites"}
    settings.jira_worker_delivery_runtime.prices = (
        settings.jira_worker_delivery_runtime.prices.model_copy(
            update={"source": "changed-prices"}
        )
    )
    assert check_readiness(settings, scope, "acme", "TEST")["delivery_runtime"]


async def test_trusted_triage_stages_scope_without_checkout_or_approval(
    session_factory, live_setup, monkeypatch
):
    settings, scope, policy, lab = live_setup
    runtime = settings.jira_worker_delivery_runtime
    admission = LiveAdmission(session_factory, runtime, {("acme", "TEST"): policy})
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, "ticket")
        state = delivery_state(ticket)
        ticket.workflow_state = state.model_copy(
            update={
                "repository": Repository(
                    supplied_url=URL,
                    outcome="unknown",
                    canonical_url=None,
                    base_branch=None,
                    base_sha=None,
                    evidence="Awaiting trusted triage",
                ),
                "step": "intake",
                "scope": None,
                "gate": None,
                "gate_ready": False,
                "statuses": (),
                "transitions": (),
            }
        ).model_dump(mode="json")
        ticket.intake_state = {
            "phase": "triage",
            "history": [{"complete": "The approved operation succeeds"}],
        }
        await session.commit()

    async def handle(request):
        assert request.headers["authorization"] == "PLACEHOLDER_FORGE_VALUE"
        if "/missing/" in request.url.path:
            return httpx.Response(404)
        if "/flaky/" in request.url.path:
            return httpx.Response(503)
        if request.url.path.endswith("/commits/main"):
            return httpx.Response(200, json={"sha": lab.base_sha})
        return httpx.Response(200, json={"clone_url": URL, "default_branch": "main"})

    original = httpx.AsyncClient
    monkeypatch.setattr(
        live.httpx,
        "AsyncClient",
        lambda **kwargs: original(transport=httpx.MockTransport(handle), **kwargs),
    )
    await admission("ticket")
    await admission("ticket")
    ticket, state = await current(session_factory)
    assert state.repository.outcome == "verified" and state.scope is not None
    assert (
        state.step == "approve_scope" and not state.gate_ready and not state.approvals
    )
    assert ticket.intake_state["phase"] == "scope_ready"
    assert not list(runtime.repositories.glob("*/objects.git"))
    assert not list(lab.homes.glob("*/data"))
    with pytest.raises(ValueError, match="configured GitHub"):
        await admission.triage("https://other.example.invalid/team/project")
    with pytest.raises(ValueError, match="refused repository triage"):
        await admission.triage("https://example.invalid/missing/project")
    with pytest.raises(httpx.HTTPStatusError):
        await admission.triage("https://example.invalid/flaky/project")
    # Intake saw no verified target without its scope; the summary names the gate.
    async with session_factory() as session:
        events = (
            await session.scalars(
                select(JiraWorkerEvent.idempotency_key).where(
                    JiraWorkerEvent.issue_key == ticket.issue_key
                )
            )
        ).all()
    assert {"repository-verified:ticket:1", "live-scope:ticket:1"} <= set(events)


async def test_live_intake_waits_for_trusted_scope_before_approval_request(
    session_factory, live_setup
):
    settings, scope, policy, _ = live_setup
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, "ticket")
        state = delivery_state(ticket)
        ticket.workflow_state = state.model_copy(
            update={"step": "intake", "scope": None, "gate": None, "gate_ready": False}
        ).model_dump(mode="json")
        ticket.status = "In Progress"
        ticket.intake_state = {"phase": "intake"}
        ticket.wait_channel = "jira_comments"
        ticket.room_claimed = True
        ticket.card_event_id = "fixture-card"
        ticket.agent_state = "wake_requested"
        await session.commit()
    jira = DeliveryJira()
    jira.status = "In Progress"
    activity, *_ = services(session_factory, jira, enabled=False)
    intake = IntakeOrchestrator(
        session_factory=session_factory,
        config=_worker_config(jira_worker_enabled_projects={"acme": ["TEST"]}),
        model=ModelMock("complete"),
        board=FakeAdminBoard(),
        clock=lambda: NOW,
    )

    async def bodies():
        async with session_factory() as session:
            rows = (await session.scalars(select(JiraWorkerOutbox))).all()
        return [
            (row.command, json.dumps(row.payload))
            for row in rows
            if "intake_generation" in (row.payload or {})
        ]

    assert await activity.run_one_turn(intake) == "ticket"
    ticket, _ = await current(session_factory)
    assert ticket.intake_state["phase"] == "triage"
    [(command, body)] = await bodies()
    assert command == "comment" and "Trusted scope admission required" in body
    async with session_factory() as session:
        for row in (await session.scalars(select(JiraWorkerOutbox))).all():
            row.status = "done"
        await session.commit()
    await LiveAdmission(
        session_factory,
        settings.jira_worker_delivery_runtime,
        {("acme", "TEST"): policy},
    )("ticket")
    assert await activity.run_one_turn(intake) == "ticket"
    commands = await bodies()
    assert len(commands) == 3 and commands[2][0] == "transition_by_id"
    _, staged = await current(session_factory)
    assert f"Scope digest: {digest(staged.scope)}" in commands[1][1]
    assert staged.step == "approve_scope" and not staged.approvals


async def test_server_factory_drives_all_native_roles_to_merged_sha_smoke(
    session_factory, live_setup, monkeypatch
):
    settings, scope, _, lab = live_setup
    forge, jira = ClosingForge(), ClosingJira()
    repositories = LocalRepositories(settings.jira_worker_delivery_runtime)
    trusted_forge = live.LiveForge(session_factory, repositories)
    forge.verify_candidate = trusted_forge.verify_candidate
    monkeypatch.setattr(live, "LiveForge", lambda sessions, repositories: forge)
    sources = {lab.base_sha: lab.copy._project}
    original_git = live.GitCandidateObjects._git

    async def objects_git(self, *args, authorization=None):
        args = list(args)
        if args[0] == "fetch" and str(args[-2]).startswith("https://"):
            sha = args[-1].split(":")[0]
            args[-2] = str(sources[sha])
        return await original_git(self, *args)

    monkeypatch.setattr(live.GitCandidateObjects, "_git", objects_git)
    factory = build_delivery_factory(session_factory=session_factory, config=settings)
    scheduler = JiraWorkerScheduler(
        session_factory=session_factory,
        config=settings,
        poll_clients={"acme": client_for(jira)},
        clock=lambda: datetime.now(UTC),
        delivery_factory=factory,
    )
    app = FastAPI()
    app.include_router(DeliveryModelGateway(session_factory, settings).router)
    original_client = httpx.AsyncClient
    upstream_calls = []

    async def provider(request):
        body = json.loads(request.content)
        upstream_calls.append(body)
        assert request.headers["authorization"] == "Bearer PLACEHOLDER_MODEL_VALUE"
        assert body["stream"] is False and body["max_tokens"] <= 16384
        return httpx.Response(
            200,
            json={
                "id": "completion",
                "created": 1,
                "model": "gpt-4.1-2025-04-14",
                "choices": [
                    {
                        "message": {"role": "assistant", "content": "Checked"},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {
                    "prompt_tokens": 30,
                    "completion_tokens": 10,
                    "total_tokens": 40,
                    "prompt_tokens_details": {"cached_tokens": 10},
                },
            },
        )

    monkeypatch.setattr(
        "switch_core.bridges.jira.model_gateway.httpx.AsyncClient",
        lambda **kwargs: original_client(
            transport=httpx.MockTransport(provider), **kwargs
        ),
    )
    jira.approve()
    scheduler.start()
    try:
        for _ in range(600):
            async with session_factory() as session:
                started = await session.scalar(
                    select(JiraWorkerJob).where(
                        JiraWorkerJob.execution_key == scope.job.run.execution_key,
                        JiraWorkerJob.status == "running",
                    )
                )
            if started:
                break
            await asyncio.sleep(0.05)
        else:
            pytest.fail("The enabled server supervisor did not launch the native role")
    finally:
        await scheduler.stop()
    candidate = regression = None
    homes = []
    async with original_client(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        for step in ("reproduce", "fix", "review", "qa", "smoke"):
            job = role_job(scope, step, 1)
            for _ in range(12):
                async with session_factory() as session:
                    row = await session.scalar(
                        select(JiraWorkerJob).where(
                            JiraWorkerJob.execution_key == job.run.execution_key
                        )
                    )
                if row is not None and row.status == "running":
                    break
                await scheduler.run_once()
            else:
                ticket, state = await current(session_factory)
                pytest.fail(
                    f"{step} did not start: {state.step}, {ticket.worker_parked_reason}"
                )
            identity = model_identity(job)
            endpoint = (
                f"/integrations/jira/delivery/model/{identity}/v1/chat/completions"
            )
            assert (await client.post(endpoint, json={})).status_code == 403
            response = await client.post(
                endpoint,
                headers={
                    "Authorization": "Bearer " + model_capability(settings, identity)
                },
                json={
                    "model": "gpt-4.1-2025-04-14",
                    "messages": [
                        {
                            "role": "user",
                            "content": [
                                {"type": "text", "text": "Check the approved task"}
                            ],
                        }
                    ],
                    "stream": True,
                    "max_tokens": 4096,
                },
            )
            assert response.status_code == 200 and "[DONE]" in response.text
            project = repositories.project(job)
            if step == "fix":
                (project / "regression.txt").write_text("Regression check\n")
                git(project, "add", ".")
                git(project, "commit", "-qm", "Regression check")
                regression = git(project, "rev-parse", "HEAD")
                (project / "fixture.txt").write_text("Fixed\n")
                git(project, "add", ".")
                git(project, "commit", "-qm", "Fix")
                candidate = git(project, "rev-parse", "HEAD")
                sources[candidate] = project
            target = lab.base_sha if step == "reproduce" else candidate
            check = CheckEvidence(
                check_id="regression",
                commit=target,
                exit_code=1 if step == "reproduce" else 0,
                expected=scope.checks[0].expected,
                actual="Controlled behavioral check",
                environment="Isolated native test process",
                artifact_digest="a" * 64,
            )
            evidence = Handoff(
                input_digest=digest(job),
                verdict="PASS",
                head=target,
                findings=(),
                changed_paths=("fixture.txt", "regression.txt")
                if step == "fix"
                else (),
                checks=(check,),
                regression_checks=(
                    check.model_copy(update={"commit": regression, "exit_code": 1}),
                )
                if step == "fix"
                else (
                    check.model_copy(update={"commit": lab.base_sha, "exit_code": 1}),
                )
                if step == "qa"
                else (),
                regression_commit=regression if step == "fix" else None,
                fix_commit=candidate if step == "fix" else None,
            )
            home = lab.homes / identity
            homes.append(home)
            artifact = home / "data" / ("jira-" + identity[:32])
            usage = Meter.model_validate_json(
                json.dumps(
                    {
                        "prices": settings.jira_worker_delivery_runtime.prices.model_dump(
                            mode="json"
                        )
                    }
                )
            ).usage(live.load_pack(settings).snapshot)
            result = Result(
                run=job.run,
                input_digest=digest(job),
                status="completed",
                question=None,
                usage=usage,
            )
            for name, value in (("result", result), ("handoff", evidence)):
                content = value.model_dump_json().encode()
                (artifact / f"{name}.json").write_bytes(content)
                (artifact / f"{name}.sha256").write_text(
                    hashlib.sha256(content).hexdigest()
                )
            await scheduler.run_once()
            async with session_factory() as session:
                row = await session.scalar(
                    select(JiraWorkerJob).where(
                        JiraWorkerJob.execution_key == job.run.execution_key
                    )
                )
                assert row.status == "done"
                execution = Execution.model_validate_json(json.dumps(row.payload))
                assert execution.known_tokens == 40 and execution.known_usd == Decimal(
                    "0.000125"
                )
                assert execution.exit_confirmed and execution.meter.stopped
            assert (
                await client.post(
                    endpoint,
                    headers={
                        "Authorization": "Bearer "
                        + model_capability(settings, identity)
                    },
                    json={
                        "model": "gpt-4.1-2025-04-14",
                        "messages": [{"role": "user", "content": "More"}],
                    },
                )
            ).status_code == 403
            if step == "qa":
                for _ in range(6):
                    await scheduler.run_once()
                _, state = await current(session_factory)
                assert state.step == "accept" and state.gate_ready
                jira.approve()
                for _ in range(3):
                    await scheduler.run_once()
                _, state = await current(session_factory)
                assert state.step == "merge" and state.accepted_head == candidate
                forge.receipt = MergeReceipt(
                    repository_url=URL,
                    request_url=forge.requests[0].url,
                    candidate_sha=candidate,
                    base_branch="main",
                    outcome="merged",
                    merged_sha=candidate,
                    actor_id="human-owner",
                    human=True,
                )
                forge.requests = (forge.requests[0].model_copy(update={"open": False}),)
        for _ in range(6):
            await scheduler.run_once()
    ticket, state = await current(session_factory)
    assert ticket.status == "Done" and state.outcome == "delivered"
    assert state.smoke.head == state.merge_sha == candidate
    assert len(upstream_calls) == 5 and len(set(homes)) == 5
    assert all((home / "cancel-reached").exists() for home in homes)
    for home in homes:
        environment = json.loads((home / "environment.json").read_text())
        assert "PLACEHOLDER_MODEL_VALUE" not in json.dumps(environment)
        assert "PLACEHOLDER_FORGE_VALUE" not in json.dumps(environment)
    assert len(forge.pushes) == len(forge.creates) == 1


@pytest.mark.parametrize(
    "fault,expected",
    [
        ("wrong-model", 403),
        ("image", 400),
        ("budget", 429),
        ("stopped", 403),
        ("pending", 403),
        ("missing-usage", 502),
    ],
)
async def test_gateway_refuses_unbounded_or_uncertain_spend(
    session_factory, live_setup, monkeypatch, fault, expected
):
    settings, scope, _, _ = live_setup
    identity = model_identity(scope.job)
    runtime = settings.jira_worker_delivery_runtime
    meter_data = {"prices": runtime.prices.model_dump(mode="json")}
    if fault in {"stopped", "pending"}:
        meter_data[fault] = True
    if fault == "budget":
        meter_data["input_tokens"] = scope.reproduction_reservation.tokens - 1
    meter = Meter.model_validate_json(json.dumps(meter_data))
    execution = Execution(
        contract=scope.job,
        pack=live.load_pack(settings).snapshot,
        reservation=scope.reproduction_reservation,
        estimate_seconds=60,
        started_at=datetime.now(UTC).isoformat(),
        held_tokens=scope.reproduction_reservation.tokens,
        held_usd=scope.reproduction_reservation.estimated_usd,
        meter=meter,
        model_identity=identity,
    )
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
        session.add(
            JiraWorkerJob(
                kind="role_job",
                instance="acme",
                project_key="TEST",
                ticket_id="ticket",
                generation=1,
                execution_key=scope.job.run.execution_key,
                due_at=datetime.now(UTC),
                status="running",
                payload=execution.model_dump(mode="json"),
            )
        )
        await session.commit()
    original = httpx.AsyncClient
    calls = []

    async def upstream(request):
        calls.append(request)
        return httpx.Response(200, json={"model": scope.job.pack_digest})

    monkeypatch.setattr(
        "switch_core.bridges.jira.model_gateway.httpx.AsyncClient",
        lambda **kwargs: original(transport=httpx.MockTransport(upstream), **kwargs),
    )
    app = FastAPI()
    app.include_router(DeliveryModelGateway(session_factory, settings).router)
    body = {
        "model": "other-model" if fault == "wrong-model" else execution.pack.model,
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {"url": "https://example.invalid/image"},
                    }
                ]
                if fault == "image"
                else "Check",
            }
        ],
        "max_tokens": 100,
    }
    endpoint = f"/integrations/jira/delivery/model/{identity}/v1/chat/completions"
    headers = {"Authorization": "Bearer " + model_capability(settings, identity)}
    async with original(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        assert (
            await client.post(endpoint, headers=headers, json=body)
        ).status_code == expected
        if fault == "missing-usage":
            app = FastAPI()
            app.include_router(DeliveryModelGateway(session_factory, settings).router)
            async with original(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as restarted:
                assert (
                    await restarted.post(endpoint, headers=headers, json=body)
                ).status_code == 403
    assert len(calls) == (1 if fault == "missing-usage" else 0)
    async with session_factory() as session:
        row = await session.scalar(
            select(JiraWorkerJob).where(
                JiraWorkerJob.execution_key == scope.job.run.execution_key
            )
        )
        saved = Execution.model_validate_json(json.dumps(row.payload))
        assert (
            saved.held_tokens == execution.held_tokens
            and saved.held_usd == execution.held_usd
        )
        assert saved.meter.uncertain == (fault == "missing-usage")
