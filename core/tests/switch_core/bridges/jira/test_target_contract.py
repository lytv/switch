"""Delivery intake parity and repository fences, without live Jira or checkout."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi import FastAPI
from pydantic import ValidationError
from sqlalchemy import select

from switch_core.bridges.jira.activity import TicketActivity, TicketTurn
from switch_core.bridges.jira.contracts import Job, Repository, repository_identity
from switch_core.bridges.jira.intake import IntakeOrchestrator
from switch_core.bridges.jira.outbox import read_jira_version
from switch_core.bridges.jira.parse import parse_jira_payload
from switch_core.bridges.jira.routes import init_jira_routes, router
from switch_core.bridges.jira.service import JiraBridgeService
from switch_core.bridges.jira.target import (
    check_target_fields,
    delivery_state,
    record_repository_triage,
    ticket_repository_url,
)
from switch_core.bridges.jira.worker import (
    JiraPollClient,
    JiraWorkerScheduler,
    build_poll_clients,
    record_polled_issue,
    record_webhook_event,
)
from switch_core.db.models import (
    JiraWorkerEvent,
    JiraWorkerJob,
    JiraWorkerOutbox,
    JiraWorkerTicket,
)
from tests.switch_core.bridges.jira.test_admin import FakeAdminBoard
from tests.switch_core.bridges.jira.test_intake import (
    ModelMock,
)
from tests.switch_core.bridges.jira.test_intake import (
    config as intake_config,
)
from tests.switch_core.bridges.jira.test_worker import _config

NOW = datetime(2026, 10, 2, tzinfo=UTC)
URL = "https://forge.example/team/project"
SERVICE_FIELD = "customfield_12345"


def config(**overrides):
    return _config(
        jira_worker_enabled_projects={"acme": ["TEST", "KAN"]},
        jira_worker_delivery_projects={
            "acme": {"TEST": {"service_field": SERVICE_FIELD}}
        },
        jira_worker_credentials={
            "acme": {
                "base_url": "https://jira.example",
                "email": "bot@example.com",
                "api_token": "placeholder",
            }
        },
        **overrides,
    )


def issue(key="TEST-1", service="Bug", url=URL):
    return {
        "key": key,
        "fields": {
            "summary": "A reproducible bug",
            "description": f"Repository URL: {url}" if url else "No target supplied",
            "status": {"name": "To Do"},
            "updated": NOW.isoformat(),
            "project": {"key": key.split("-")[0]},
            "reporter": {"accountId": "reporter"},
            "labels": ["delivery"],
            SERVICE_FIELD: {"value": service},
        },
    }


async def ingest(session_factory, *, service="Bug", url=URL, key="TEST-1"):
    settings = config()
    async with session_factory() as session:
        assert await record_polled_issue(
            session,
            issue(key, service, url),
            instance="acme",
            config=settings,
        )
        await session.commit()
        return await session.scalar(
            select(JiraWorkerTicket).where(JiraWorkerTicket.issue_key == key)
        )


@pytest.mark.parametrize("service", ["Bug", "Feature", "Docs", "Other"])
async def test_webhook_route_and_scheduler_poll_select_same_path(
    session_factory,
    monkeypatch,
    service,
) -> None:
    settings = config()
    store = AsyncMock()
    store.list.return_value = []
    bridge = JiraBridgeService(
        session_factory=session_factory,
        trigger_store=store,
        agent_store=AsyncMock(),
        room_store=AsyncMock(),
        protocol=AsyncMock(),
        config=settings,
    )
    app = FastAPI()
    init_jira_routes(service=bridge, secrets_by_instance={"acme": "placeholder"})
    app.include_router(router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as http:
        response = await http.post(
            "/integrations/jira/acme",
            json={
                "webhookEvent": "jira:issue_created",
                "issue": issue(service=service),
            },
            headers={"X-Switch-Secret": "placeholder"},
        )
    assert response.status_code == 202
    polled = issue("TEST-2", service)

    async def handle(request):
        if request.url.path.endswith("/changelog"):
            return httpx.Response(200, json={"values": [], "total": 0})
        if request.url.path.endswith("/comment"):
            return httpx.Response(200, json={"comments": [], "total": 0})
        selected = {
            key: value
            for key, value in polled["fields"].items()
            if key in request.url.params["fields"].split(",")
        }
        result = {**polled, "fields": selected}
        return httpx.Response(
            200,
            json={"issues": [result]}
            if request.url.path.endswith("/search/jql")
            else result,
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        with monkeypatch.context() as patch:
            patch.setattr(httpx, "AsyncClient", lambda **kwargs: http)
            clients = build_poll_clients(settings)
        # Run only TEST here; KAN remains enabled for separate legacy proofs.
        poll_config = settings.model_copy(
            update={"jira_worker_enabled_projects": {"acme": ["TEST"]}}
        )
        worker = JiraWorkerScheduler(
            session_factory=session_factory,
            config=poll_config,
            poll_clients=clients,
            ticket_rooms=None,
            clock=lambda: NOW,
        )
        await worker.run_once()
        version = await read_jira_version(clients["acme"], "TEST-2")
    assert version["fields"] == polled["fields"]
    async with session_factory() as session:
        rows = (
            await session.scalars(
                select(JiraWorkerTicket).order_by(JiraWorkerTicket.issue_key)
            )
        ).all()
        assert len(rows) == 2
        assert rows[0].workflow_state == rows[1].workflow_state
        assert rows[0].service_value == rows[1].service_value == service
        target = delivery_state(rows[0])
        assert target.service == service
        assert target.repository.outcome == "unknown"
        assert target.repository.supplied_url == URL
        assert target.pack.snapshot.fixture_only is True
        assert not (
            await session.scalars(
                select(JiraWorkerJob).where(JiraWorkerJob.kind == "role_job")
            )
        ).all()


async def test_poll_and_version_reads_preserve_routing_fields() -> None:
    fields = issue()["fields"]

    async def handle(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/changelog"):
            return httpx.Response(200, json={"values": [], "total": 0})
        if request.url.path.endswith("/comment"):
            return httpx.Response(200, json={"comments": [], "total": 0})
        selected = {
            key: value
            for key, value in fields.items()
            if key in request.url.params["fields"].split(",")
        }
        result = {"key": "TEST-1", "fields": selected}
        return httpx.Response(
            200,
            json={"issues": [result]}
            if request.url.path.endswith("/search/jql")
            else result,
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        client = JiraPollClient(
            base_url="https://jira.example",
            email="bot@example.com",
            api_token="placeholder",
            client=http,
            extra_fields=(SERVICE_FIELD,),
        )
        issues = await client.search_updated(project_key="TEST", updated_since=None)
        version = await read_jira_version(client, "TEST-1")
    assert issues[0]["fields"] == fields
    assert version["fields"] == fields


def verified(url=URL):
    return Repository(
        supplied_url=url,
        outcome="verified",
        canonical_url=url + ".git",
        base_branch="main",
        base_sha="a" * 40,
        evidence="Trusted triage checked project and base",
    )


@pytest.mark.parametrize("url", ["", URL])
async def test_unverified_target_cannot_construct_checkout_job(session_factory, url):
    ticket = await ingest(session_factory, url=url)
    target = delivery_state(ticket)
    with pytest.raises(ValidationError):
        Job.model_validate_json(
            json.dumps(
                {
                    "schema_version": 1,
                    "run": {
                        "instance": "acme",
                        "ticket_id": ticket.id,
                        "generation": ticket.agent_generation,
                        "step": "reproduce",
                        "attempt": 1,
                        "execution_key": f"{ticket.id}/{ticket.agent_generation}/reproduce/1",
                    },
                    "pack_digest": target.pack.digest,
                    "service": "Bug",
                    "repository": target.repository.model_dump(mode="json")
                    if target.repository
                    else None,
                    "acceptance": ["Failure no longer occurs"],
                }
            )
        )


async def test_triage_checks_current_generation_even_with_cached_ticket(
    session_factory,
):
    stored = await ingest(session_factory)
    async with session_factory() as stale:
        cached = await stale.get(JiraWorkerTicket, stored.id)
        async with session_factory() as fresh:
            changed = await fresh.get(JiraWorkerTicket, stored.id)
            changed.agent_generation += 1
            await fresh.commit()
        with pytest.raises(ValueError, match="active Bug intake"):
            await record_repository_triage(
                stale,
                ticket_id=stored.id,
                generation=cached.agent_generation,
                repository=verified(),
            )


async def test_trusted_triage_persists_target_and_rejects_mismatch_and_stale_generation(
    session_factory,
):
    ticket = await ingest(session_factory)
    async with session_factory() as session:
        for generation, repository in [
            (ticket.agent_generation + 1, verified()),
            (ticket.agent_generation, verified("https://forge.example/wrong/repo")),
        ]:
            with pytest.raises(ValueError):
                await record_repository_triage(
                    session,
                    ticket_id=ticket.id,
                    generation=generation,
                    repository=repository,
                )
        await record_repository_triage(
            session,
            ticket_id=ticket.id,
            generation=ticket.agent_generation,
            repository=verified(),
        )
        await session.commit()
    async with session_factory() as session:
        stored = await session.get(JiraWorkerTicket, ticket.id)
        assert delivery_state(stored).repository == verified()
        assert stored.agent_generation == ticket.agent_generation
        assert stored.tokens_used == 0
        await record_repository_triage(
            session,
            ticket_id=ticket.id,
            generation=ticket.agent_generation,
            repository=verified(),
        )
        changed = verified().model_copy(update={"base_sha": "b" * 40})
        with pytest.raises(ValueError, match="frozen"):
            await record_repository_triage(
                session,
                ticket_id=ticket.id,
                generation=ticket.agent_generation,
                repository=changed,
            )


@pytest.mark.parametrize(
    "service,url", [("Bug", ""), ("Feature", URL), ("Docs", URL), ("Other", URL)]
)
async def test_missing_or_non_bug_target_never_accepts_triage(
    session_factory, service, url
):
    ticket = await ingest(session_factory, service=service, url=url)
    async with session_factory() as session:
        with pytest.raises(ValueError):
            await record_repository_triage(
                session,
                ticket_id=ticket.id,
                generation=ticket.agent_generation,
                repository=verified(),
            )


@pytest.mark.parametrize("via", ["webhook", "poll"])
@pytest.mark.parametrize("change", ["repository", "service"])
async def test_changed_target_parks_and_preserves_frozen_contract(
    session_factory, via, change
):
    ticket = await ingest(session_factory)
    async with session_factory() as session:
        await record_repository_triage(
            session,
            ticket_id=ticket.id,
            generation=ticket.agent_generation,
            repository=verified(),
        )
        await session.commit()
    changed = issue(
        service="Feature" if change == "service" else "Bug",
        url="https://forge.example/wrong/repo" if change == "repository" else URL,
    )
    changed["fields"]["updated"] = "2026-10-02T00:01:00Z"
    async with session_factory() as session:
        if via == "webhook":
            await record_webhook_event(
                session,
                parse_jira_payload({"issue": changed}),
                instance="acme",
                enabled_projects=["TEST"],
                webhook_identifier="changed",
                config=config(),
            )
        else:
            await record_polled_issue(
                session, changed, instance="acme", config=config()
            )
        await session.commit()
        stored = await session.get(JiraWorkerTicket, ticket.id)
        assert (
            stored.worker_parked_reason
            == "Ticket Service or repository URL changed; target triage required"
        )
        assert stored.agent_generation == ticket.agent_generation + 1
        assert delivery_state(stored).repository == verified()
        assert stored.service_value == "Bug"


async def test_new_kan_ticket_uses_legacy_policy(session_factory):
    stored = await ingest(session_factory, key="KAN-1")
    assert stored.workflow_state["policy"] == "legacy_intake"
    assert stored.service_value is None


async def test_flattened_webhook_uses_same_target_and_issue_baseline(session_factory):
    payload = issue()
    async with session_factory() as session:
        await record_webhook_event(
            session,
            parse_jira_payload(payload),
            instance="acme",
            enabled_projects=["TEST"],
            webhook_identifier="flattened",
            config=config(),
        )
        await session.commit()
        stored = await session.scalar(select(JiraWorkerTicket))
        assert delivery_state(stored).service == "Bug"
        assert delivery_state(stored).repository.supplied_url == URL
        assert stored.reporter_account_id == "reporter"
        assert stored.jira_read_updated == NOW.isoformat()


@pytest.mark.parametrize("missing", [SERVICE_FIELD, "description"])
async def test_complete_version_read_blocks_removed_target_fields(
    session_factory, missing
):
    stored = await ingest(session_factory)
    async with session_factory() as session:
        await record_repository_triage(
            session,
            ticket_id=stored.id,
            generation=stored.agent_generation,
            repository=verified(),
        )
        await session.commit()
    async with session_factory() as session:
        row = await session.get(JiraWorkerTicket, stored.id)
        fields = issue()["fields"]
        del fields[missing]
        check_target_fields(row, fields, complete=True)
        assert row.worker_parked_reason is not None
        assert delivery_state(row).repository == verified()


async def test_partial_webhook_preserves_target(session_factory):
    stored = await ingest(session_factory)
    async with session_factory() as session:
        row = await session.get(JiraWorkerTicket, stored.id)
        check_target_fields(row, {"updated": NOW.isoformat()}, complete=False)
        assert row.worker_parked_reason is None
        assert row.workflow_state == stored.workflow_state


@pytest.mark.parametrize("service", [None, "", "Unexpected"])
@pytest.mark.parametrize("bad_first", [False, True])
async def test_invalid_service_parks_only_its_ticket_in_poll_and_webhook(
    session_factory, service, bad_first
):
    settings = config().model_copy(
        update={"jira_worker_enabled_projects": {"acme": ["TEST"]}}
    )
    good, bad = issue("TEST-1"), issue("TEST-2", service=service)
    if service is None:
        del bad["fields"][SERVICE_FIELD]
    poll = AsyncMock(spec=JiraPollClient)
    poll.search_updated.return_value = [bad, good] if bad_first else [good, bad]
    worker = JiraWorkerScheduler(
        session_factory=session_factory,
        config=settings,
        poll_clients={"acme": poll},
        clock=lambda: NOW,
    )
    await worker.run_once()
    async with session_factory() as session:
        jobs = (await session.scalars(select(JiraWorkerJob))).all()
        assert len([job for job in jobs if job.status == "done"]) == 1
        next_poll = next(job for job in jobs if job.status == "pending")
        assert next_poll.payload["watermark"] == NOW.isoformat()

    store = AsyncMock()
    store.list.return_value = []
    bridge = JiraBridgeService(
        session_factory=session_factory,
        trigger_store=store,
        agent_store=AsyncMock(),
        room_store=AsyncMock(),
        protocol=AsyncMock(),
        config=settings,
    )
    app = FastAPI()
    init_jira_routes(service=bridge, secrets_by_instance={"acme": "placeholder"})
    app.include_router(router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as http:
        for source, key in [(good, "TEST-3"), (bad, "TEST-4")]:
            for _ in range(2):
                response = await http.post(
                    "/integrations/jira/acme",
                    json={
                        "webhookEvent": "jira:issue_created",
                        "issue": {**source, "key": key},
                    },
                    headers={"X-Switch-Secret": "placeholder"},
                )
                assert response.status_code == 202
    async with session_factory() as session:
        rows = {
            row.issue_key: row
            for row in await session.scalars(select(JiraWorkerTicket))
        }
        assert set(rows) == {"TEST-1", "TEST-2", "TEST-3", "TEST-4"}
        for key in ("TEST-1", "TEST-3"):
            assert rows[key].worker_parked_reason is None
            assert delivery_state(rows[key]).service == "Bug"
        for key in ("TEST-2", "TEST-4"):
            row = rows[key]
            assert "requires a mapped" in row.worker_parked_reason
            assert row.agent_state == "parked"
            assert row.agent_generation == 1
            assert await session.scalar(
                select(JiraWorkerOutbox.id).where(
                    JiraWorkerOutbox.issue_key == key,
                    JiraWorkerOutbox.command == "upsert_ticket_admin_card",
                )
            )
        assert not (
            await session.scalars(
                select(JiraWorkerOutbox).where(JiraWorkerOutbox.channel == "jira")
            )
        ).all()


@pytest.mark.parametrize("key", ["KAN-1", "TEST-1"])
async def test_existing_legacy_policy_is_not_upgraded(session_factory, key):
    async with session_factory() as session:
        await record_polled_issue(session, issue(key), instance="acme")
        await session.commit()
    updated = issue(key)
    updated["fields"]["updated"] = "2026-10-02T00:01:00Z"
    async with session_factory() as session:
        await record_polled_issue(session, updated, instance="acme", config=config())
        await session.commit()
        stored = await session.scalar(select(JiraWorkerTicket))
        assert stored.workflow_state["policy"] == "legacy_intake"
        assert stored.service_value is None


@pytest.mark.parametrize(
    "service,phase",
    [
        ("Bug", "triage"),
        ("Feature", "manual_handoff"),
        ("Docs", "manual_handoff"),
        ("Other", "manual_handoff"),
    ],
)
async def test_intake_completion_cannot_dispatch_development(
    session_factory, service, phase
):
    stored = await ingest(session_factory, service=service)
    async with session_factory() as session:
        row = await session.get(JiraWorkerTicket, stored.id)
        row.jira_read_updated = NOW.isoformat()
        row.jira_read_changelog_id = ""
        await session.commit()
    model = ModelMock("complete")
    settings = intake_config(
        **{
            "jira_worker_enabled_projects": {"acme": ["TEST"]},
            "jira_worker_delivery_projects": {
                "acme": {"TEST": {"service_field": SERVICE_FIELD}}
            },
        }
    )
    orchestrator = IntakeOrchestrator(
        session_factory=session_factory,
        config=settings,
        model=model,
        board=FakeAdminBoard(),
        clock=lambda: NOW,
    )
    turn = TicketTurn(
        stored.id,
        stored.issue_key,
        stored.status,
        {
            "ticket_text": stored.summary,
            "description": stored.description,
            "events": [],
        },
        10000,
    )
    await orchestrator(turn)
    await orchestrator(turn)
    async with session_factory() as session:
        row = await session.get(JiraWorkerTicket, stored.id)
        assert row.intake_state["phase"] == phase
        commands = (
            await session.scalars(
                select(JiraWorkerOutbox).where(
                    JiraWorkerOutbox.command_key.like("intake:%")
                )
            )
        ).all()
        assert len(commands) == 1
        assert commands[0].command == "comment"
        assert (await session.scalars(select(JiraWorkerJob))).all() == []
        assert len(model.calls) == 1
        if service != "Bug":
            assert delivery_state(row).step == "manual_handoff"
            assert "Manual handling required" in commands[0].payload["data"]


@pytest.mark.parametrize("verified_later", [False, True])
async def test_verified_repository_requests_scope_approval_once(
    session_factory, verified_later
):
    stored = await ingest(session_factory)
    settings = config()
    model = ModelMock("complete")
    orchestrator = IntakeOrchestrator(
        session_factory=session_factory,
        config=settings,
        model=model,
        board=FakeAdminBoard(),
        clock=lambda: NOW,
    )
    async with session_factory() as session:
        row = await session.get(JiraWorkerTicket, stored.id)
        row.jira_read_changelog_id = ""
        row.jira_seen_comment_ids = []
        row.room_claimed = True
        row.card_event_id = "card"
        row.agent_state = "wake_requested"
        if not verified_later:
            await record_repository_triage(
                session,
                ticket_id=row.id,
                generation=row.agent_generation,
                repository=verified(),
            )
        await session.commit()

    async def handle(request):
        if request.url.path.endswith("/myself"):
            return httpx.Response(200, json={"accountId": "worker"})
        if request.url.path.endswith("/changelog"):
            return httpx.Response(200, json={"values": [], "total": 0})
        if request.url.path.endswith("/comment"):
            return httpx.Response(200, json={"comments": [], "total": 0})
        return httpx.Response(200, json=issue())

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        activity = TicketActivity(
            session_factory=session_factory,
            config=settings,
            clients={
                "acme": JiraPollClient(
                    base_url="https://jira.example",
                    email="bot@example.com",
                    api_token="placeholder",
                    client=http,
                    extra_fields=(SERVICE_FIELD,),
                )
            },
            rooms=None,
            clock=lambda: NOW,
        )
        assert await activity.run_one_turn(orchestrator) == stored.id
        if verified_later:
            async with session_factory() as session:
                row = await session.get(JiraWorkerTicket, stored.id)
                assert row.intake_state["phase"] == "triage"
                assert row.agent_state == "waiting"
                commands = (
                    await session.scalars(
                        select(JiraWorkerOutbox).where(
                            JiraWorkerOutbox.command_key.like("intake:%")
                        )
                    )
                ).all()
                assert len(commands) == 1
                assert commands[0].command == "comment"
                commands[0].status = "done"
                for outcome in ("unknown", "invalid"):
                    await record_repository_triage(
                        session,
                        ticket_id=row.id,
                        generation=row.agent_generation,
                        repository=Repository(
                            supplied_url=URL,
                            outcome=outcome,
                            canonical_url=None,
                            base_branch=None,
                            base_sha=None,
                            evidence="Repository needs human investigation",
                        ),
                    )
                    assert row.intake_state["phase"] == "triage"
                    assert row.agent_state == "waiting"
                with pytest.raises(ValueError, match="does not match"):
                    await record_repository_triage(
                        session,
                        ticket_id=row.id,
                        generation=row.agent_generation,
                        repository=verified("https://forge.example/wrong/repo"),
                    )
                assert row.intake_state["phase"] == "triage"
                await session.commit()
            for _ in range(2):
                async with session_factory() as session:
                    await record_repository_triage(
                        session,
                        ticket_id=stored.id,
                        generation=stored.agent_generation,
                        repository=verified(),
                    )
                    await session.commit()
            async with session_factory() as session:
                row = await session.get(JiraWorkerTicket, stored.id)
                assert row.intake_state["phase"] == "scope_ready"
                assert row.agent_state == "wake_requested"
            assert await activity.run_one_turn(orchestrator) == stored.id
        async with session_factory() as session:
            await record_repository_triage(
                session,
                ticket_id=stored.id,
                generation=stored.agent_generation,
                repository=verified(),
            )
            await session.commit()
        assert await activity.run_one_turn(orchestrator) is None
    async with session_factory() as session:
        row = await session.get(JiraWorkerTicket, stored.id)
        assert row.intake_state["phase"] == "approval"
        assert delivery_state(row).step == "intake"
        assert row.status == "To Do"
        commands = (
            await session.scalars(
                select(JiraWorkerOutbox).where(
                    JiraWorkerOutbox.command_key.like("intake:%")
                )
            )
        ).all()
        assert len(commands) == (3 if verified_later else 2)
        approvals = [command for command in commands if command.command == "transition"]
        assert len(approvals) == 1
        assert (
            approvals[0].payload["data"]
            == settings.jira_worker_statuses.waiting_for_approval
        )
        summary = await session.get(
            JiraWorkerOutbox, approvals[0].payload["after_command"]
        )
        assert summary.command == "comment"
        assert "The reporter needs a clear outcome." in summary.payload["data"]
        assert verified().canonical_url in summary.payload["data"]
        assert verified().base_branch in summary.payload["data"]
        assert verified().base_sha in summary.payload["data"]
        assert "triage required" not in summary.payload["data"]
        events = (
            await session.scalars(
                select(JiraWorkerEvent).where(
                    JiraWorkerEvent.webhook_event == "worker:repository_verified"
                )
            )
        ).all()
        assert len(events) == int(verified_later)
        assert all(event.consumed for event in events)
        assert not (
            await session.scalars(
                select(JiraWorkerJob).where(JiraWorkerJob.kind == "role_job")
            )
        ).all()
        assert len(model.calls) == 1


@pytest.mark.parametrize(
    "canonical",
    ["https://other.example/team/project", "https://forge.example/team/other"],
)
def test_verified_repository_must_match_supplied_url(canonical):
    with pytest.raises(ValidationError, match="does not match"):
        Repository.model_validate_json(
            json.dumps(
                {**verified().model_dump(mode="json"), "canonical_url": canonical}
            )
        )


@pytest.mark.parametrize(
    "url",
    [
        "file:///tmp/repo",
        "https://user:secret@forge.example/team/repo",
        "https://forge.example/team/../repo",
        "https://forge.example/team/repo?token=hidden",
        "https://forge.example/team/repo#head",
        "https://forge.example/team/%2e%2e/repo",
        "https://forge.example/team/repo\n",
        "https://forge.example/",
    ],
)
def test_unsafe_urls_cannot_be_verified(url):
    with pytest.raises(ValidationError):
        verified(url)


@pytest.mark.parametrize(
    "url",
    [
        "https://forge.example/team/project.git",
        "ssh://git@forge.example/team/project",
        "git@forge.example:team/project.git",
    ],
)
def test_same_repository_transport_forms_match(url):
    assert repository_identity(url) == repository_identity(URL)


def test_description_adf_and_custom_url_field():
    description = {
        "type": "doc",
        "content": [
            {
                "type": "paragraph",
                "content": [{"type": "text", "text": f"Repository URL: {URL}"}],
            }
        ],
    }
    assert ticket_repository_url({"description": description}, "description") == URL
    assert ticket_repository_url({"customfield_23456": URL}, "customfield_23456") == URL
    assert (
        ticket_repository_url(
            {"description": f"Repository URL: {URL}\nRepository URL: {URL}"},
            "description",
        )
        == ""
    )
