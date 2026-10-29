"""Local server integration with disposable PostgreSQL, native test processes and peers."""

from __future__ import annotations

import asyncio
import hashlib
import json
import subprocess
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import select

from switch_core.bridges.jira import live
from switch_core.bridges.jira.activity import TicketActivity
from switch_core.bridges.jira.contracts import (
    Approval,
    Meter,
    Pack,
    Repository,
    Result,
    digest,
)
from switch_core.bridges.jira.delivery import (
    DeliverySupervisor,
    ScopeApproved,
    apply_event,
    role_job,
    stage_scope,
)
from switch_core.bridges.jira.delivery_contracts import (
    CheckEvidence,
    Handoff,
    ScopeContract,
    ScopePolicy,
    VerificationContract,
    VerificationRole,
)
from switch_core.bridges.jira.firstmate_jobs import Execution
from switch_core.bridges.jira.intake import IntakeDecision, IntakeOrchestrator
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
from switch_core.bridges.jira.outbox import JiraOutboxSender
from switch_core.bridges.jira.packs import FrozenPack, load_pack
from switch_core.bridges.jira.publisher import GitCandidateObjects
from switch_core.bridges.jira.readiness import check_readiness
from switch_core.bridges.jira.rooms import reconcile_ticket_room
from switch_core.bridges.jira.state import WorkflowMapping
from switch_core.bridges.jira.target import delivery_state
from switch_core.bridges.jira.verification import MergeReceipt
from switch_core.bridges.jira.worker import (
    JiraPollClient,
    JiraWorkerScheduler,
    record_polled_issue,
)
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
from tests.switch_core.bridges.jira.test_rooms import FakeRooms


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
    settings.jira_worker_delivery_pack = getattr(request, "param", "2.0.0")
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
        "tmux_sockets": str(lab.copy._tmux_sockets),
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


@pytest.mark.parametrize("legacy", [False, True])
async def test_repository_preparation_keeps_native_reads_local_and_frozen(
    live_setup, monkeypatch, legacy
):
    settings, scope, _, lab = live_setup
    local = LocalRepositories(settings.jira_worker_delivery_runtime)
    job = scope.job
    envelope = live.ExecutionEnvelope(scope=scope, reproduction=None)
    original = GitCandidateObjects._git

    async def fetch(objects, *args, authorization=None):
        assert args[0] == "fetch" and args[3] == URL + ".git"
        assert authorization == "PLACEHOLDER_FORGE_VALUE"
        return await original(objects, *args[:3], str(lab.copy._project), *args[4:])

    monkeypatch.setattr(GitCandidateObjects, "_git", fetch)
    await local.prepare(job, envelope)
    project = local.project(job)
    source = project.parent / "native.git"
    if legacy:
        git(project, "config", "--unset-all", f"url.{source}.insteadOf")
    before = git(project, "rev-parse", "HEAD")
    for _ in range(2):
        await local.prepare(job, envelope)
        assert git(project, "rev-parse", "HEAD") == before == lab.base_sha
        assert git(project, "config", "--get", "remote.origin.url") == URL
        assert git(project, "remote", "get-url", "origin") == str(source)
        assert git(source, "rev-parse", "HEAD") == lab.base_sha
        assert git(source, "symbolic-ref", "HEAD") == "refs/heads/source"
        assert git(source, "rev-parse", "--is-bare-repository") == "true"
        assert not git(source, "remote")
        assert not (source / "objects/info/alternates").exists()
    git(source, "symbolic-ref", "HEAD", "refs/heads/pin-" + lab.base_sha)
    with pytest.raises(ValueError, match="differs from the frozen commit"):
        await local.prepare(job, envelope)
    stored = project.parent / "retained-native.git"
    source.rename(stored)
    source.symlink_to(stored, target_is_directory=True)
    with pytest.raises(ValueError, match="source contains a symlink"):
        await local.prepare(job, envelope)


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


@pytest.mark.parametrize("live_setup", ["2.0.0", "2.0.0-deepseek"], indirect=True)
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


async def test_readiness_rejects_overlong_tmux_root_before_dispatch(
    live_setup, monkeypatch, tmp_path
):
    settings, scope, _, lab = live_setup
    settings.jira_worker_delivery_execution_enabled = False
    monkeypatch.setattr("switch_core.bridges.jira.readiness.load_pack", live.load_pack)
    settings.jira_worker_delivery_runtime.tmux_sockets = tmp_path / ("x" * 100)
    with pytest.raises(ValueError, match="Configure a shorter tmux_sockets root"):
        live.runtime_config(settings)
    checks = check_readiness(settings, scope, "acme", "TEST")
    assert "shorter tmux_sockets" in checks["delivery_tmux"]
    assert checks["delivery_runtime"]
    assert not list(lab.homes.glob("*/data/*/dispatch"))
    settings.jira_worker_delivery_runtime.tmux_sockets = lab.copy._tmux_sockets
    assert check_readiness(settings, scope, "acme", "TEST")["delivery_tmux"] is None


@pytest.mark.parametrize("live_setup", ["2.0.0", "2.0.0-deepseek"], indirect=True)
async def test_readiness_uses_selected_delivery_upstream(live_setup, monkeypatch):
    settings, scope, _, _ = live_setup
    provider = live.load_pack(settings).snapshot.provider
    values = settings.model_dump()
    values["jira_worker_delivery_upstreams"] = {
        provider: {
            "base_url": "https://delivery.example.invalid/v1",
            "api_key": "PLACEHOLDER_DELIVERY_VALUE",
            "prices": settings.jira_worker_delivery_runtime.prices,
        }
    }
    values["jira_worker_llm_base_url"] = "http://invalid-legacy.example.invalid"
    values["jira_worker_llm_api_key"] = None
    settings = type(settings).model_validate(values)
    settings.jira_worker_delivery_execution_enabled = False
    monkeypatch.setattr("switch_core.bridges.jira.readiness.load_pack", live.load_pack)
    checks = check_readiness(settings, scope, "acme", "TEST")
    assert checks["model_upstream"] is None and checks["delivery_runtime"] is None
    upstream = settings.jira_worker_delivery_upstreams[provider]
    settings.jira_worker_delivery_upstreams[provider] = upstream.model_copy(
        update={"base_url": "http://invalid-delivery.example.invalid"}
    )
    checks = check_readiness(settings, scope, "acme", "TEST")
    assert checks["model_upstream"] and checks["delivery_runtime"]
    settings.jira_worker_delivery_upstreams = {"another-provider": upstream}
    checks = check_readiness(settings, scope, "acme", "TEST")
    assert checks["model_upstream"] and checks["delivery_runtime"]


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
    assert policy.product_markers == ()
    await admission("ticket")
    ticket, state = await current(session_factory)
    assert state.repository.outcome == "verified" and state.scope is not None
    assert state.repository_match is None
    assert state.gate is not None and state.gate.kind == "scope"
    assert (
        state.step == "approve_scope" and not state.gate_ready and not state.approvals
    )
    assert ticket.intake_state["phase"] == "scope_ready"
    assert not list(runtime.repositories.glob("*/objects.git"))
    assert not list(lab.homes.glob("*/data"))
    await admission("ticket")
    repeated_ticket, repeated_state = await current(session_factory)
    assert repeated_state == state
    assert repeated_ticket.intake_state == ticket.intake_state
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


class AgdJira:
    """Stateful AGD fake: issue reads plus worker comment/transition sends."""

    def __init__(self, issue):
        self.fields = dict(issue["fields"])
        self.status = "To Do"
        self.updated = NOW.isoformat()
        self.histories = []
        self.comments = []
        self._ticks = 0

    def _tick(self):
        self._ticks += 1
        self.updated = (NOW + timedelta(seconds=self._ticks)).isoformat()
        return self.updated

    def _status_id(self):
        return {
            "To Do": "todo",
            "Waiting for approval": "approval",
            "In Progress": "progress",
        }.get(self.status, "progress")

    def approve(self, actor="approver"):
        before = self.status
        self.status = "In Progress"
        self.histories.append(
            {
                "id": str(len(self.histories) + 1),
                "author": {"accountId": actor},
                "created": self._tick(),
                "items": [
                    {
                        "field": "status",
                        "from": "approval",
                        "to": "progress",
                        "fromString": before,
                        "toString": "In Progress",
                    }
                ],
            }
        )

    async def handle(self, request):
        path = request.url.path
        if request.method == "GET":
            if path.endswith("/myself"):
                return httpx.Response(200, json={"accountId": "worker-account"})
            if path.endswith("/transitions"):
                return httpx.Response(
                    200,
                    json={
                        "transitions": [
                            {
                                "id": "request-approval",
                                "to": {
                                    "id": "approval",
                                    "name": "Waiting for approval",
                                },
                            }
                        ]
                    },
                )
            if path.endswith("/changelog") or path.endswith("/comment"):
                key = "values" if path.endswith("/changelog") else "comments"
                entries = self.histories if key == "values" else self.comments
                start = int(request.url.params.get("startAt", "0"))
                return httpx.Response(
                    200, json={key: entries[start:], "total": len(entries)}
                )
            if "/issue/" in path:
                fields = dict(self.fields)
                fields["status"] = {
                    "name": self.status,
                    "id": self._status_id(),
                    "statusCategory": {"key": "new"},
                }
                fields["updated"] = self.updated
                return httpx.Response(200, json={"fields": fields})
        else:
            if path.endswith("/comment"):
                body = json.loads(request.content)
                self.comments.append(
                    {
                        "id": str(len(self.comments) + 1),
                        "author": {"accountId": "worker-account"},
                        "updateAuthor": {"accountId": "worker-account"},
                        "created": self._tick(),
                        "updated": self.updated,
                        "body": body,
                    }
                )
                return httpx.Response(201, json={})
            if path.endswith("/transitions"):
                assert json.loads(request.content) == {
                    "transition": {"id": "request-approval"}
                }
                before = self.status
                self.status = "Waiting for approval"
                self.histories.append(
                    {
                        "id": str(len(self.histories) + 1),
                        "author": {"accountId": "worker-account"},
                        "created": self._tick(),
                        "items": [
                            {
                                "field": "status",
                                "fromString": before,
                                "toString": self.status,
                            }
                        ],
                    }
                )
                return httpx.Response(204)
        raise AssertionError(f"unexpected Jira request: {request.method} {path}")


@pytest.mark.parametrize("live_setup", ["2.0.0-deepseek"], indirect=True)
@pytest.mark.parametrize("admission_before_intake", [False, True])
@pytest.mark.parametrize(
    ("product", "repository_description", "path", "outcome"),
    [
        (
            "Disposable booking sandbox",
            "Disposable booking sandbox",
            "booking-sandbox",
            "match",
        ),
        (
            "Disposable booking sandbox",
            "Agent orchestration service",
            "orchestration-service",
            "mismatch",
        ),
        (
            "Booking",
            "Agent sandbox for orchestration",
            "orchestration-service",
            "mismatch",
        ),
        ("", "Agent orchestration service", "booking-service", "unclear"),
        ("Booking", "Agent orchestration service", "booking-service", "mismatch"),
        ("", "Disposable booking sandbox", "booking-service", "unclear"),
        ("Booking", "Disposable booking sandbox", "unreadable-tree", "unclear"),
    ],
    ids=[
        "matched",
        "wrong-product",
        "split-markers",
        "identity-only",
        "repository-identity-only",
        "ticket-identity-only",
        "unreadable-tree",
    ],
)
async def test_product_mismatch_stops_before_scope(
    session_factory,
    live_setup,
    monkeypatch,
    admission_before_intake,
    product,
    repository_description,
    path,
    outcome,
):
    settings, _, policy, lab = live_setup
    settings.jira_worker_enabled_projects = {"acme": ["AGD"]}
    settings.jira_worker_delivery_projects = {
        "acme": {"AGD": settings.jira_worker_delivery_projects["acme"]["TEST"]}
    }
    runtime = settings.jira_worker_delivery_runtime
    policy = policy.model_copy(
        update={
            "product_markers": ("booking", "sandbox"),
            "statuses": (
                *policy.statuses,
                WorkflowMapping(name="To Do", jira_id="todo"),
            ),
            "transitions": (
                *policy.transitions,
                WorkflowMapping(
                    name="todo->approval",
                    jira_id="request-approval",
                ),
            ),
        }
    )
    url = f"https://example.invalid/team/{path}"
    issue = {
        "key": "AGD-19",
        "fields": {
            "summary": f"{product}: preview input 7 must return 14",
            "description": (
                f"Repository URL: {url}\n\n"
                f"Intended product: {product}.\n"
                "The preview returns 7 for input 7. It must return 14."
            ),
            "status": {"name": "To Do"},
            "project": {"key": "AGD"},
            "reporter": {"accountId": "reporter"},
            "updated": NOW.isoformat(),
            "customfield_12345": {"value": "Bug"},
        },
    }
    async with session_factory() as session:
        assert await record_polled_issue(
            session, issue, instance="acme", config=settings
        )
        await session.commit()
        ticket = await session.scalar(
            select(JiraWorkerTicket).where(JiraWorkerTicket.issue_key == "AGD-19")
        )
        ticket_id = ticket.id
        assert delivery_state(ticket).repository.outcome == "unknown"
        assert await reconcile_ticket_room(
            session,
            instance="acme",
            issue_key="AGD-19",
            rooms=FakeRooms(),
            jira_agent_name="jira",
            limits=settings.jira_worker_limits,
            now=NOW,
            enabled_projects=settings.jira_worker_enabled_projects,
        )
        # Isolate product matching from the separately tracked generation-zero defect.
        ticket.agent_generation = 1
        await session.commit()
    model = ModelMock("complete")
    intake = IntakeOrchestrator(
        session_factory=session_factory,
        config=settings,
        model=model,
        board=FakeAdminBoard(),
        clock=lambda: NOW,
    )

    jira = AgdJira(issue)
    http = httpx.AsyncClient(transport=httpx.MockTransport(jira.handle))
    send_http = httpx.AsyncClient(transport=httpx.MockTransport(jira.handle))
    activity = TicketActivity(
        session_factory=session_factory,
        config=settings,
        clients={
            "acme": JiraPollClient(
                base_url="https://jira.example.invalid",
                email="worker@example.invalid",
                api_token="PLACEHOLDER_JIRA_VALUE",
                client=http,
                extra_fields=("customfield_12345",),
            )
        },
        rooms=None,
        clock=lambda: NOW,
    )
    sender = JiraOutboxSender(
        session_factory=session_factory,
        config=settings,
        clients={
            "acme": JiraPollClient(
                base_url="https://jira.example.invalid",
                email="worker@example.invalid",
                api_token="PLACEHOLDER_JIRA_VALUE",
                client=send_http,
            )
        },
        clock=lambda: NOW,
    )
    reads = []

    async def handle(request):
        assert request.headers["authorization"] == "PLACEHOLDER_FORGE_VALUE"
        reads.append(request.url.path)
        if request.url.path == f"/repos/team/{path}":
            return httpx.Response(
                200,
                json={
                    "clone_url": url,
                    "default_branch": "main",
                    "description": repository_description,
                },
            )
        if request.url.path == f"/repos/team/{path}/commits/main":
            return httpx.Response(200, json={"sha": lab.base_sha})
        assert request.url.path == f"/repos/team/{path}/git/trees/{lab.base_sha}"
        if path == "unreadable-tree":
            return httpx.Response(403)
        names = (
            ["booking.py", "scenario.json", "tests"]
            if outcome == "match"
            else ["service.py", "rooms.py", "gateway"]
        )
        return httpx.Response(
            200,
            json={"tree": [{"path": name} for name in names], "truncated": False},
        )

    original = httpx.AsyncClient
    monkeypatch.setattr(
        live.httpx,
        "AsyncClient",
        lambda **kwargs: original(transport=httpx.MockTransport(handle), **kwargs),
    )
    admission = LiveAdmission(session_factory, runtime, {("acme", "AGD"): policy})
    jobs, publisher = AsyncMock(), AsyncMock()
    supervisor = DeliverySupervisor(
        session_factory=session_factory,
        config=settings,
        activity=activity,
        jobs=jobs,
        publisher=publisher,
        clock=lambda: NOW,
        admission=admission,
    )
    if admission_before_intake:
        await supervisor.reconcile(ticket_id)
        assert reads == []
    assert await activity.run_one_turn(intake) == ticket_id
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, ticket_id)
        assert ticket.intake_state["phase"] == "triage"
        assert delivery_state(ticket).scope is None
    assert model.calls[0][0]["description"] == issue["fields"]["description"]
    await sender.run_once()
    assert jira.status == "To Do"
    assert len(jira.comments) == 1
    assert "Trusted scope admission required" in json.dumps(jira.comments[0]) or (
        "Repository triage required" in json.dumps(jira.comments[0])
    )
    await supervisor.reconcile(ticket_id)
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, ticket_id)
        state = delivery_state(ticket)
        assert state.repository_match.facts_available == (path != "unreadable-tree")
        assert repository_description in state.repository_match.facts
        jobs.start.assert_not_called()
        publisher.publish.assert_not_called()
        commands = (
            await session.scalars(
                select(JiraWorkerOutbox).where(JiraWorkerOutbox.issue_key == "AGD-19")
            )
        ).all()
        assert not any(row.command.startswith("transition") for row in commands)
        assert not list(runtime.repositories.glob("*/objects.git"))
        assert not list(lab.homes.glob("*/data"))
        assert reads == [
            f"/repos/team/{path}",
            f"/repos/team/{path}/commits/main",
            f"/repos/team/{path}/git/trees/{lab.base_sha}",
        ]
        if outcome == "match":
            assert state.repository.outcome == "verified"
            assert state.repository_match is not None
            assert state.repository_match.outcome == "match"
            assert state.scope is not None
            assert state.step == "approve_scope"
            return
        assert state.scope is None, (
            "P19: trusted admission staged scope for a reachable repository "
            f"that belongs to a different product: {state.repository.evidence}"
        )
        assert state.repository.outcome == "verified"
        assert state.gate is not None and state.gate.kind == "repository"
        assert not state.gate_ready
        assert state.repository_match is not None
        assert state.repository_match.outcome == outcome
        assert state.repository_match.confirmation is None
        assert ticket.intake_state["phase"] == "scope_ready"
        if path == "unreadable-tree" and admission_before_intake:
            # Persist the legacy stall: verified pins, no facts, and no intake wake.
            match = live.evaluate_repository_match(
                generation=ticket.agent_generation,
                repository=state.repository,
                markers=policy.product_markers,
                approval_route=policy.scope.approval_route,
                facts=(),
                facts_available=False,
                ticket_text=ticket.summary,
            )
            gate = state.gate.model_copy(
                update={"contract_digest": match.contract_digest}
            )
            ticket.workflow_state = state.model_copy(
                update={"repository_match": match, "gate": gate}
            ).model_dump(mode="json")
            ticket.intake_state = {**ticket.intake_state, "phase": "triage"}
            ticket.agent_state = "waiting"
            for event in await session.scalars(
                select(JiraWorkerEvent).where(JiraWorkerEvent.issue_key == "AGD-19")
            ):
                event.consumed = True
            await session.commit()
            for _ in range(2):
                await supervisor.reconcile(ticket_id)
            await session.refresh(ticket)
            recovered = delivery_state(ticket)
            assert recovered.repository_match == match and recovered.gate == gate
            assert ticket.intake_state["phase"] == "scope_ready"
            assert ticket.agent_state == "wake_requested"
            events = list(
                await session.scalars(
                    select(JiraWorkerEvent).where(
                        JiraWorkerEvent.idempotency_key
                        == f"live-repository:{ticket_id}:1"
                    )
                )
            )
            assert len(events) == 1 and not events[0].consumed
            assert len(reads) == 3
        # The reporter-visible confirmation request goes through intake.
        assert await activity.run_one_turn(intake) == ticket_id
        async with session_factory() as session:
            ticket = await session.get(JiraWorkerTicket, ticket_id)
            rows = (
                await session.scalars(
                    select(JiraWorkerOutbox).where(
                        JiraWorkerOutbox.issue_key == "AGD-19"
                    )
                )
            ).all()
            assert ticket.intake_state["phase"] == "approval"
            comments = [row for row in rows if row.command == "comment"]
            assert len(comments) == 2
            assert "Repository confirmation required" in comments[1].payload["data"]
            if path != "unreadable-tree" or not admission_before_intake:
                assert repository_description in comments[1].payload["data"]
            assert (
                comments[1].payload.get("repository_digest")
                == delivery_state(ticket).gate.contract_digest
            )
            transitions = [row for row in rows if row.command == "transition_by_id"]
            assert len(transitions) == 1
            assert transitions[0].payload["data"]["step"] == "intake"
        # Both writes succeed before the gate becomes active.
        await sender.run_once()
        await sender.run_once()
        async with session_factory() as session:
            ticket = await session.get(JiraWorkerTicket, ticket_id)
            assert delivery_state(ticket).gate_ready
            assert ticket.status == "Waiting for approval"
        await supervisor.reconcile(ticket_id)
        await sender.run_once()
        assert await activity.run_one_turn(intake) is None
        assert len(jira.comments) == 2 and len(jira.histories) == 1
        # A read without the approver edge changes nothing.
        assert await activity.read_ticket(ticket_id, force_jira=True)
        async with session_factory() as session:
            ticket = await session.get(JiraWorkerTicket, ticket_id)
            assert delivery_state(ticket).repository_match.confirmation is None
        # An authorized approver confirms the repository through the same edge.
        jira.approve("approver")
        assert await activity.read_ticket(ticket_id, force_jira=True)
        async with session_factory() as session:
            ticket = await session.get(JiraWorkerTicket, ticket_id)
            state = delivery_state(ticket)
            assert state.repository_match.confirmation is not None
            assert state.repository_match.confirmation.actor_account_id == "approver"
            assert state.gate is None and not state.gate_ready
            assert ticket.intake_state["phase"] == "scope_ready"
            assert ticket.status == "In Progress"
            confirmed_history = ticket.intake_state["history"]
            confirmed_commands = set(
                await session.scalars(
                    select(JiraWorkerOutbox.id).where(
                        JiraWorkerOutbox.issue_key == "AGD-19"
                    )
                )
            )
            ticket.agent_state = "wake_requested"
            await session.commit()
        assert await activity.run_one_turn(intake) == ticket_id
        async with session_factory() as session:
            ticket = await session.get(JiraWorkerTicket, ticket_id)
            state = delivery_state(ticket)
            assert ticket.intake_state["phase"] == "scope_ready"
            assert ticket.intake_state["history"] == confirmed_history
            assert state.scope is None and state.gate is None
            assert state.repository_match.confirmation is not None
            assert (
                set(
                    await session.scalars(
                        select(JiraWorkerOutbox.id).where(
                            JiraWorkerOutbox.issue_key == "AGD-19"
                        )
                    )
                )
                == confirmed_commands
            )
        assert len(model.calls) == 1
        # Confirmation resumes normal scope staging; scope approval stays separate.
        await LiveAdmission(session_factory, runtime, {("acme", "AGD"): policy})(
            ticket_id
        )
        async with session_factory() as session:
            ticket = await session.get(JiraWorkerTicket, ticket_id)
            state = delivery_state(ticket)
            assert state.scope is not None
            assert state.gate is not None and state.gate.kind == "scope"
            assert state.step == "approve_scope"
        assert await activity.run_one_turn(intake) == ticket_id
        async with session_factory() as session:
            rows = (
                await session.scalars(
                    select(JiraWorkerOutbox).where(
                        JiraWorkerOutbox.issue_key == "AGD-19"
                    )
                )
            ).all()
            comments = [row for row in rows if row.command == "comment"]
            assert "Scope digest" in comments[-1].payload["data"]


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


@pytest.mark.parametrize("live_setup", ["2.0.0", "2.0.0-deepseek"], indirect=True)
async def test_scope_rejection_requires_fresh_corrected_generation(
    session_factory, live_setup, monkeypatch
):
    settings, rejected_scope, policy, _ = live_setup
    policy = policy.model_copy(
        update={
            "statuses": (
                *policy.statuses,
                WorkflowMapping(name="Waiting for input", jira_id="input"),
            ),
            "transitions": (
                *policy.transitions,
                WorkflowMapping(name="input->approval", jira_id="request-approval"),
                WorkflowMapping(name="Reject", jira_id="7"),
            ),
        }
    )
    jira = DeliveryJira()
    jira.status = "In Progress"
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, "ticket")
        ticket.status = jira.status
        ticket.intake_state = {"phase": "intake", "questions": 5}
        ticket.wait_channel = "jira_comments"
        ticket.room_claimed = True
        ticket.card_event_id = "fixture-card"
        ticket.agent_state = "wake_requested"
        ticket.tokens_used = 23
        ticket.workflow_state = {
            **ticket.workflow_state,
            "gate_ready": False,
            "statuses": [item.model_dump(mode="json") for item in policy.statuses],
            "transitions": [
                item.model_dump(mode="json") for item in policy.transitions
            ],
            "reserved_tokens": 100,
            "reserved_estimated_usd": "0.01",
            "known_estimated_usd": "0.25",
        }
        await session.commit()
    activity, supervisor, process, forge, _ = services(session_factory, jira)

    class ScopeModel(ModelMock):
        async def decide(self, data, token_budget):
            decision = await super().decide(data, token_budget)
            summary = (
                data["replies"][-1]
                if data["replies"]
                else rejected_scope.job.acceptance[0]
            )
            return IntakeDecision("complete", summary, decision.tokens_used)

    model = ScopeModel("complete")
    intake = IntakeOrchestrator(
        session_factory=session_factory,
        config=_worker_config(jira_worker_enabled_projects={"acme": ["TEST"]}),
        model=model,
        board=FakeAdminBoard(),
        clock=lambda: NOW,
    )
    sender = JiraOutboxSender(
        session_factory=session_factory,
        config=settings,
        clients={"acme": client_for(jira)},
        clock=lambda: NOW,
    )
    assert await activity.run_one_turn(intake) == "ticket"
    await sender.run_once()
    await activity.read_ticket("ticket", force_jira=True)
    ticket, rejected = await current(session_factory)
    assert rejected.gate_ready and ticket.intake_state["phase"] == "approval"
    assert ticket.worker_parked_reason is None
    rejected_approval = ScopeApproved(
        approval=Approval(
            gate=rejected.gate,
            actor_account_id="approver",
            actor_kind="human",
            history_id="stale-approval",
        )
    )
    tokens_before = ticket.tokens_used

    # Jira changelogs expose status endpoints, not the Reject transition id (7).
    jira.reject()
    await activity.read_ticket("ticket", force_jira=True)
    ticket, reset = await current(session_factory)
    assert ticket.worker_parked_reason is None
    assert ticket.status == "Waiting for input" and ticket.agent_generation == 2
    assert ticket.intake_state["cycle"] == 1 and ticket.intake_state["questions"] == 0
    assert any("jira_rejection" in item for item in ticket.intake_state["history"])
    assert reset.step == "intake"
    assert reset.scope is reset.gate is None and not reset.gate_ready
    assert reset.approvals == () and reset.checkpoints == ()
    assert reset.repository.outcome == "unknown"
    for field in (
        "pack",
        "model_prices",
        "statuses",
        "transitions",
        "known_estimated_usd",
        "reserved_tokens",
        "reserved_estimated_usd",
        "unknown_spend",
        "budget",
    ):
        assert getattr(reset, field) == getattr(rejected, field)
    assert ticket.tokens_used == tokens_before
    await activity.read_ticket("ticket", force_jira=True)
    ticket, repeated = await current(session_factory)
    assert ticket.agent_generation == 2 and repeated == reset
    with pytest.raises(ValueError, match="active frozen scope"):
        apply_event(ticket, rejected_approval)

    admission = LiveAdmission(
        session_factory,
        settings.jira_worker_delivery_runtime,
        {("acme", "TEST"): policy},
    )

    async def triage_evidence(supplied):
        assert supplied == URL
        return rejected_scope.job.repository, {}, (), True

    monkeypatch.setattr(admission, "_triage_evidence", triage_evidence)
    await admission("ticket")
    ticket, state = await current(session_factory)
    assert state.scope is None and state.gate is None
    with pytest.raises(ValueError, match="current verified Bug intake"):
        stage_scope(ticket, rejected_scope, rejected.gate)
    assert await activity.run_one_turn(intake) == "ticket"
    await sender.run_once()
    ticket, _ = await current(session_factory)
    assert ticket.intake_state["phase"] == "question"
    assert ticket.intake_state["questions"] == 1 and len(model.calls) == 1
    correction = (
        "Correct the export to preserve every column, not only the first column."
    )
    jira.change("reporter-account", comment=True)
    jira.comments[-1]["body"] = correction
    await activity.read_ticket("ticket", force_jira=True)
    assert await activity.run_one_turn(intake) == "ticket"
    await sender.run_once()
    ticket, state = await current(session_factory)
    assert model.calls[-1][0]["replies"] == [correction]
    assert ticket.intake_state["phase"] == "triage" and state.scope is None
    await admission("ticket")
    ticket, staged = await current(session_factory)
    assert staged.scope.job.acceptance == (correction,)
    assert staged.scope.job.run.generation == staged.gate.run.generation == 2
    assert staged.scope.branch == "switch/ticket/2"
    assert staged.gate.contract_digest == digest(staged.scope) != digest(rejected_scope)
    assert not staged.gate_ready and staged.approvals == ()
    with pytest.raises(ValueError, match="active tested gate"):
        apply_event(ticket, rejected_approval)
    assert await activity.run_one_turn(intake) == "ticket"
    await sender.run_once()
    ticket, state = await current(session_factory)
    assert (
        state.scope == staged.scope and state.gate == staged.gate and state.gate_ready
    )
    assert state.approvals == () and ticket.intake_state["cycle"] == 1
    displayed = json.dumps(jira.comments[-1]["body"])
    assert correction in displayed and digest(staged.scope) in displayed
    assert digest(rejected_scope) not in displayed
    with pytest.raises(ValueError, match="active tested gate"):
        apply_event(ticket, rejected_approval)
    assert not process.starts and not forge.pushes
    jira.approve()
    await supervisor.reconcile("ticket")
    _, approved = await current(session_factory)
    assert approved.approvals[0].gate == staged.gate != rejected.gate
    assert process.starts == ["ticket/2/reproduce/1"]


@pytest.mark.parametrize("live_setup", ["2.0.0", "2.0.0-deepseek"], indirect=True)
async def test_server_factory_drives_all_native_roles_to_merged_sha_smoke(
    session_factory, live_setup, monkeypatch
):
    settings, scope, _, lab = live_setup
    pack = live.load_pack(settings).snapshot
    values = settings.model_dump()
    values["jira_worker_delivery_upstreams"] = {
        provider: {
            "base_url": f"https://{provider}.example.invalid/v1",
            "api_key": f"PLACEHOLDER_{provider}_VALUE",
            "prices": settings.jira_worker_delivery_runtime.prices,
        }
        for provider in ("switch-delivery", "switch-delivery-deepseek")
    }
    settings = type(settings).model_validate(values)
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
        assert (
            str(request.url)
            == f"https://{pack.provider}.example.invalid/v1/chat/completions"
        )
        assert (
            request.headers["authorization"]
            == f"Bearer PLACEHOLDER_{pack.provider}_VALUE"
        )
        assert body["model"] == pack.model
        assert body["stream"] is False and body["max_tokens"] <= 16384
        return httpx.Response(
            200,
            json={
                "id": "completion",
                "created": 1,
                "model": pack.model,
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
                    **(
                        {"prompt_cache_hit_tokens": 10}
                        if pack.model == "deepseek-v4-pro"
                        else {"prompt_tokens_details": {"cached_tokens": 10}}
                    ),
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
    settings.jira_worker_delivery_pack = (
        "2.0.0" if pack.version == "2.0.0-deepseek" else "2.0.0-deepseek"
    )
    settings.jira_worker_delivery_runtime.prices = (
        settings.jira_worker_delivery_runtime.prices.model_copy(
            update={"output": Decimal("0.01"), "source": "other-provider-prices"}
        )
    )
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
                    "model": pack.model,
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
                assert execution.pack == pack
                if step == "reproduce":
                    ticket = await session.get(JiraWorkerTicket, "ticket")
                    state = delivery_state(ticket)
                    assert state.model_prices == execution.meter.prices
                    ticket.workflow_state = state.model_copy(
                        update={"model_prices": None}
                    ).model_dump(mode="json")
                    await session.commit()
                    upstream = settings.jira_worker_delivery_upstreams[pack.provider]
                    settings.jira_worker_delivery_upstreams[pack.provider] = (
                        upstream.model_copy(
                            update={
                                "prices": settings.jira_worker_delivery_runtime.prices
                            }
                        )
                    )
            assert (
                await client.post(
                    endpoint,
                    headers={
                        "Authorization": "Bearer "
                        + model_capability(settings, identity)
                    },
                    json={
                        "model": pack.model,
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
        for upstream in settings.jira_worker_delivery_upstreams.values():
            assert upstream.api_key.get_secret_value() not in json.dumps(environment)
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
        ("invalid-openai-cache", 502),
        ("invalid-deepseek-cache", 502),
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
        if fault.startswith("invalid-"):
            return httpx.Response(
                200,
                json={
                    "model": execution.pack.model,
                    "choices": [
                        {"message": {"role": "assistant", "content": "Checked"}}
                    ],
                    "usage": {
                        "prompt_tokens": 30,
                        "completion_tokens": 10,
                        "total_tokens": 40,
                        **(
                            {"prompt_cache_hit_tokens": 31}
                            if fault == "invalid-deepseek-cache"
                            else {"prompt_tokens_details": {"cached_tokens": 31}}
                        ),
                    },
                },
            )
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
        if expected == 502:
            app = FastAPI()
            app.include_router(DeliveryModelGateway(session_factory, settings).router)
            async with original(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as restarted:
                assert (
                    await restarted.post(endpoint, headers=headers, json=body)
                ).status_code == 403
    assert len(calls) == (1 if expected == 502 else 0)
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
        assert saved.meter.uncertain == (expected == 502)
