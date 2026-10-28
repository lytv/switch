"""Fresh Jira delivery ingestion through rooms, intake and trusted live admission."""

from __future__ import annotations

import hashlib
from decimal import Decimal
from unittest.mock import AsyncMock

import httpx
import pytest
from sqlalchemy import select

from switch_core.bridges.jira.activity import TicketActivity
from switch_core.bridges.jira.contracts import Reservation, digest
from switch_core.bridges.jira.delivery import DeliverySupervisor
from switch_core.bridges.jira.delivery_contracts import (
    ApprovalRoute,
    ApprovedCheck,
    RoleProfile,
    ScopePolicy,
)
from switch_core.bridges.jira.intake import IntakeOrchestrator
from switch_core.bridges.jira.live import AdmissionPolicy, LiveAdmission
from switch_core.bridges.jira.packs import load_pack
from switch_core.bridges.jira.parse import parse_jira_payload
from switch_core.bridges.jira.rooms import reconcile_ticket_room
from switch_core.bridges.jira.state import WorkflowMapping
from switch_core.bridges.jira.target import delivery_state
from switch_core.bridges.jira.worker import (
    JiraPollClient,
    JiraWorkerScheduler,
    record_webhook_event,
)
from switch_core.config import (
    JiraWorkerCredentials,
    JiraWorkerDeliveryRuntime,
    JiraWorkerDeliveryUpstream,
)
from switch_core.db.models import JiraWorkerJob, JiraWorkerOutbox, JiraWorkerTicket
from tests.switch_core.bridges.jira.test_activity import ThreadRooms
from tests.switch_core.bridges.jira.test_admin import FakeAdminBoard
from tests.switch_core.bridges.jira.test_delivery import (
    BASE_SHA,
    NOW,
    PROFILE,
    URL,
    config,
)
from tests.switch_core.bridges.jira.test_intake import ModelMock
from tests.switch_core.bridges.jira.test_target_contract import SERVICE_FIELD, issue


@pytest.mark.parametrize("source", ["poll", "webhook"])
@pytest.mark.parametrize("pack", ["2.0.0", "2.0.0-deepseek"])
async def test_fresh_delivery_stages_scope_after_intake(
    session_factory, tmp_path, monkeypatch, source, pack
):
    settings = config()
    settings.jira_worker_delivery_pack = pack
    settings.jira_worker_credentials = {
        "acme": JiraWorkerCredentials(
            base_url="https://jira.example.invalid",
            email="worker@example.invalid",
            api_token="PLACEHOLDER_JIRA_VALUE",
        )
    }
    settings.jira_worker_enabled_projects = {"acme": ["AGD"]}
    settings.jira_worker_delivery_projects = {
        "acme": {"AGD": settings.jira_worker_delivery_projects["acme"]["TEST"]}
    }
    runtime = JiraWorkerDeliveryRuntime(
        checkout=tmp_path / "checkout",
        homes=tmp_path / "homes",
        repositories=tmp_path / "repositories",
        executable_path="/usr/bin",
        model_url="http://127.0.0.1:8000",
        forge_api_url="https://forge.example.invalid",
        forge_repository_host="example.invalid",
        forge_authorization="PLACEHOLDER_FORGE_VALUE",
        policies={},
        prices={
            "input": "0.000002",
            "output": "0.000008",
            "cache_read": "0.0000005",
            "cache_write": "0.000002",
            "source": "fixture-prices",
        },
    )
    settings.jira_worker_delivery_runtime = runtime
    settings.jira_worker_delivery_upstreams = {
        load_pack(settings).snapshot.provider: JiraWorkerDeliveryUpstream(
            base_url="https://model.example.invalid/v1",
            api_key="PLACEHOLDER_MODEL_VALUE",
            prices=runtime.prices,
        )
    }
    reservation = Reservation(
        tokens=1000, estimated_usd=Decimal("1"), price_source="fixture-prices"
    )
    policy = AdmissionPolicy(
        scope=ScopePolicy(
            allowed_paths=("src", "tests"),
            forbidden_paths=("src/secrets",),
            checks=(
                ApprovedCheck(
                    id="regression",
                    argv=("python", "-m", "pytest", "tests"),
                    expected="The public operation succeeds",
                ),
            ),
            approval_route=ApprovalRoute(
                source_status_id="approval",
                target_status_id="progress",
                transition_id="approve-scope",
                unique_edge_verified=True,
                evidence="Trusted workflow fixture proves the approval edge",
            ),
            profile=RoleProfile(
                commit="d" * 40,
                content=PROFILE,
                sha256=hashlib.sha256(PROFILE.encode()).hexdigest(),
            ),
            reproduction_reservation=reservation,
            fix_reservation=reservation,
            reproduction_estimate_seconds=60,
            fix_estimate_seconds=120,
        ),
        statuses=(
            WorkflowMapping(name="To Do", jira_id="todo"),
            WorkflowMapping(name="Waiting for approval", jira_id="approval"),
            WorkflowMapping(name="In Progress", jira_id="progress"),
        ),
        transitions=(
            WorkflowMapping(name="Approve", jira_id="approve-scope"),
            WorkflowMapping(name="todo->approval", jira_id="request-approval"),
        ),
        approver_account_ids=("approver",),
    )
    payload = issue("AGD-5", url=URL)
    payload["fields"]["summary"] = "[V2-P01] The public operation fails"
    payload["fields"]["updated"] = NOW.isoformat()
    requests = []

    async def peer(request):
        requests.append(request)
        path = request.url.path
        if request.url.host == "forge.example.invalid":
            assert request.headers["authorization"] == "PLACEHOLDER_FORGE_VALUE"
            assert path in {"/repos/team/project", "/repos/team/project/commits/main"}
            return httpx.Response(
                200,
                json={"sha": BASE_SHA}
                if path.endswith("/commits/main")
                else {"clone_url": URL, "default_branch": "main"},
            )
        if path.endswith("/myself"):
            return httpx.Response(200, json={"accountId": "worker-account"})
        if path.endswith("/changelog"):
            return httpx.Response(200, json={"values": [], "total": 0})
        if path.endswith("/comment"):
            return httpx.Response(200, json={"comments": [], "total": 0})
        if path.endswith("/search/jql"):
            assert SERVICE_FIELD in request.url.params["fields"].split(",")
            return httpx.Response(200, json={"issues": [payload], "isLast": True})
        assert path.endswith("/issue/AGD-5")
        return httpx.Response(200, json=payload)

    original_client = httpx.AsyncClient
    monkeypatch.setattr(
        "switch_core.bridges.jira.live.httpx.AsyncClient",
        lambda **kwargs: original_client(transport=httpx.MockTransport(peer), **kwargs),
    )
    async with httpx.AsyncClient() as http:
        client = JiraPollClient(
            base_url="https://jira.example.invalid",
            email="worker@example.invalid",
            api_token="PLACEHOLDER_JIRA_VALUE",
            client=http,
            extra_fields=(SERVICE_FIELD,),
        )
        if source == "poll":
            scheduler = JiraWorkerScheduler(
                session_factory=session_factory,
                config=settings.model_copy(
                    update={"jira_worker_delivery_execution_enabled": False}
                ),
                poll_clients={"acme": client},
                clock=lambda: NOW,
            )
            await scheduler.run_once()
        else:
            async with session_factory() as session:
                assert await record_webhook_event(
                    session,
                    parse_jira_payload(
                        {"webhookEvent": "jira:issue_created", "issue": payload}
                    ),
                    instance="acme",
                    enabled_projects=["AGD"],
                    webhook_identifier="fresh-delivery",
                    config=settings,
                )
                await session.commit()
        rooms = ThreadRooms()
        async with session_factory() as session:
            ticket = await session.scalar(select(JiraWorkerTicket))
            ticket_id = ticket.id
            assert delivery_state(ticket).scope is None
            assert ticket.worker_parked_reason is None
            room_id = await reconcile_ticket_room(
                session,
                instance="acme",
                issue_key="AGD-5",
                rooms=rooms,
                jira_agent_name="jira",
                now=NOW,
                enabled_projects=settings.jira_worker_enabled_projects,
            )
            await session.commit()
            assert room_id and ticket.room_claimed and ticket.card_event_id
        activity = TicketActivity(
            session_factory=session_factory,
            config=settings,
            clients={"acme": client},
            rooms=rooms,
            clock=lambda: NOW,
        )
        model = ModelMock("complete")
        intake = IntakeOrchestrator(
            session_factory=session_factory,
            config=settings,
            model=model,
            board=FakeAdminBoard(),
            clock=lambda: NOW,
        )
        assert await activity.run_one_turn(intake) == ticket_id
        async with session_factory() as session:
            ticket = await session.get(JiraWorkerTicket, ticket_id)
            assert ticket.intake_state["phase"] == "triage"
            assert ticket.intake_state["history"][-1]["complete"]
        jobs, publisher = AsyncMock(), AsyncMock()
        supervisor = DeliverySupervisor(
            session_factory=session_factory,
            config=settings,
            activity=activity,
            jobs=jobs,
            publisher=publisher,
            clock=lambda: NOW,
            admission=LiveAdmission(
                session_factory, runtime, {("acme", "AGD"): policy}
            ),
        )
        await supervisor.reconcile(ticket_id)
        await supervisor.reconcile(ticket_id)
        async with session_factory() as session:
            ticket = await session.get(JiraWorkerTicket, ticket_id)
            assert ticket.worker_parked_reason is None, ticket.worker_parked_reason
            state = delivery_state(ticket)
            assert ticket.agent_generation == state.scope.job.run.generation == 1
            assert state.repository.outcome == "verified"
            assert state.scope.job.repository.canonical_url == URL
            assert state.scope.job.repository.base_sha == BASE_SHA
            assert state.step == "approve_scope" and not state.gate_ready
            assert state.gate.contract_digest == digest(state.scope)
            assert not state.approvals
            assert ticket.intake_state["phase"] == "scope_ready"
            assert not list(
                await session.scalars(
                    select(JiraWorkerJob).where(JiraWorkerJob.kind == "role_job")
                )
            )
            for row in await session.scalars(select(JiraWorkerOutbox)):
                if "intake_generation" in (row.payload or {}):
                    row.status = "done"
            await session.commit()
        assert await activity.run_one_turn(intake) == ticket_id
        async with session_factory() as session:
            commands = list(
                await session.scalars(
                    select(JiraWorkerOutbox).where(
                        JiraWorkerOutbox.command == "transition_by_id"
                    )
                )
            )
            assert len(commands) == 1
            assert commands[0].payload["data"]["transition_id"] == "request-approval"
            summary = await session.get(
                JiraWorkerOutbox, commands[0].payload["after_command"]
            )
            assert f"Scope digest: {digest(state.scope)}" in summary.payload["data"]
        assert len(model.calls) == 1
        jobs.start.assert_not_called()
        publisher.publish.assert_not_called()
        assert len([r for r in requests if r.url.host == "forge.example.invalid"]) == 2
        assert not list(tmp_path.iterdir())
