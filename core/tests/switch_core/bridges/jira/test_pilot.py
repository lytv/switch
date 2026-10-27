"""P01-shaped delivery with disposable PostgreSQL and in-process external peers.

No live Jira, forge, Firstmate home, paid model, or pilot database participates.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from sqlalchemy import select

from switch_core.bridges.jira import readiness
from switch_core.bridges.jira.activity import TicketTurn
from switch_core.bridges.jira.contracts import digest
from switch_core.bridges.jira.delivery import fix_job, role_job
from switch_core.bridges.jira.intake import IntakeOrchestrator
from switch_core.bridges.jira.readiness import check_readiness, main
from switch_core.config import SwitchConfig
from switch_core.db.models import JiraWorkerJob, JiraWorkerOutbox, JiraWorkerTicket
from tests.switch_core.bridges.jira.test_admin import FakeAdminBoard
from tests.switch_core.bridges.jira.test_delivery import (
    HEAD,
    NOW,
    config,
    current,
    services,
)
from tests.switch_core.bridges.jira.test_delivery_close import (
    MERGED,
    ClosingForge,
    ClosingJira,
    Verifier,
    seed_verification_scope,
    sender,
)
from tests.switch_core.bridges.jira.test_intake import ModelMock


@pytest.fixture
async def pilot(session_factory):
    scope = await seed_verification_scope(session_factory, ready=False)
    jira = ClosingJira()
    jira.status = "In Progress"
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, "ticket")
        ticket.status = jira.status
        ticket.intake_state = {"phase": "intake"}
        ticket.wait_channel = "jira_comments"
        await session.commit()
    activity, supervisor, process, forge, publisher = services(
        session_factory, jira, process=Verifier(), forge=ClosingForge()
    )
    intake = IntakeOrchestrator(
        session_factory=session_factory,
        config=config(),
        model=ModelMock("complete"),
        board=FakeAdminBoard(),
        clock=lambda: NOW,
    )
    await intake(
        TicketTurn(
            "ticket",
            "TEST-1",
            jira.status,
            {
                "ticket_text": "Isolated backend bug",
                "description": "Fixture",
                "events": [],
            },
            1000,
        )
    )
    send = sender(session_factory, jira, publisher)
    await send.run_once()
    await activity.read_ticket("ticket", force_jira=True)
    _, state = await current(session_factory)
    assert state.gate_ready and state.gate.kind == "scope"
    assert digest(scope) in jira.writes[0].content.decode()
    assert not process.starts
    return scope, jira, activity, supervisor, process, forge, send


async def reach_acceptance(pilot):
    scope, jira, activity, supervisor, process, _, send = pilot
    jira.approve()
    await asyncio.gather(supervisor.reconcile("ticket"), supervisor.reconcile("ticket"))
    for job in (
        scope.job,
        fix_job(scope),
        role_job(scope, "review", 1),
        role_job(scope, "qa", 1),
    ):
        assert job.run.execution_key in process.starts
        process.complete(job)
        for _ in range(4):
            await supervisor.reconcile("ticket")
    await send.run_once()
    await activity.read_ticket("ticket", force_jira=True)


def assert_p01_evidence_gate(scope, jira, process, forge, state):
    assert state.outcome == "delivered"
    assert len(process.starts) == len(set(process.starts)) == 5
    assert len(forge.creates) == len(forge.pushes) == 1
    assert state.merge_receipt.human
    assert state.merge_receipt.repository_url == scope.job.repository.canonical_url
    assert state.merge_receipt.candidate_sha == state.accepted_head == HEAD
    assert state.smoke.head == state.merge_receipt.merged_sha == MERGED
    assert [request.url.path.rsplit("/", 1)[-1] for request in jira.writes] == [
        "comment",
        "transitions",
        "comment",
        "transitions",
        "comment",
        "transitions",
    ]


@pytest.mark.parametrize(
    "incident",
    [None, "unsafe_write", "wrong_repository", "duplicate_job", "unapproved_merge"],
)
async def test_p01_clean_path_and_evidence_gate_veto(session_factory, pilot, incident):
    scope, jira, activity, supervisor, process, forge, send = pilot
    await reach_acceptance(pilot)
    _, state = await current(session_factory)
    assert state.gate.kind == "acceptance" and state.gate.candidate_sha == HEAD
    assert state.accepted_head is None and len(process.starts) == 4
    assert len(state.approvals) == 1 and state.approvals[0].history_id
    jira.approve()
    await supervisor.reconcile("ticket")
    for _ in range(2):
        await supervisor.reconcile("ticket")
    assert len(process.starts) == 4 and jira.status != "Done"
    forge.merge()
    for _ in range(2):
        await supervisor.reconcile("ticket")
    assert process.envelopes["ticket/1/smoke/1"].target_sha == MERGED
    assert MERGED != HEAD
    process.complete(role_job(scope, "smoke", 1))
    for _ in range(3):
        await supervisor.reconcile("ticket")
    await send.run_once()
    await activity.read_ticket("ticket", force_jira=True)
    ticket, state = await current(session_factory)
    assert ticket.status == "Done" and state.outcome == "delivered"
    assert state.review_head == state.qa_head == state.accepted_head == HEAD
    assert state.merge_sha == MERGED and state.smoke.head == MERGED
    assert [approval.gate.kind for approval in state.approvals] == [
        "scope",
        "acceptance",
    ]
    assert state.approvals[0].history_id != state.approvals[1].history_id
    assert_p01_evidence_gate(scope, jira, process, forge, state)
    if incident is not None:
        # An unsafe observed effect vetoes a gate even when outcome says delivered.
        if incident == "unsafe_write":
            jira.writes.append(jira.writes[0])
        elif incident == "duplicate_job":
            process.starts.append(process.starts[0])
        else:
            changes = (
                {"human": False}
                if incident == "unapproved_merge"
                else {"repository_url": "https://example.invalid/wrong/repository"}
            )
            state = state.model_copy(
                update={"merge_receipt": state.merge_receipt.model_copy(update=changes)}
            )
        with pytest.raises(AssertionError):
            assert_p01_evidence_gate(scope, jira, process, forge, state)
    async with session_factory() as session:
        jobs = list(
            await session.scalars(
                select(JiraWorkerJob).where(JiraWorkerJob.kind == "role_job")
            )
        )
        assert len(jobs) == 5 and all(row.payload["exit_confirmed"] for row in jobs)
        commands = list(
            await session.scalars(
                select(JiraWorkerOutbox).where(JiraWorkerOutbox.channel == "jira")
            )
        )
        assert len(commands) == 6 and all(row.status == "done" for row in commands)
        assert all(
            row.payload.get("after_command") is not None
            for row in commands
            if row.command == "transition_by_id"
        )


@pytest.mark.parametrize(
    "fault",
    ["human_interrupt", "wrong_repository", "unapproved_merge", "no_acceptance"],
)
async def test_pilot_safety_gate_refuses_unsafe_completion(
    session_factory, pilot, fault
):
    scope, jira, activity, supervisor, process, forge, send = pilot
    await reach_acceptance(pilot)
    if fault != "no_acceptance":
        jira.approve()
        await supervisor.reconcile("ticket")
    forge.merge(human=fault != "unapproved_merge")
    if fault == "wrong_repository":
        forge.receipt = forge.receipt.model_copy(
            update={"repository_url": "https://example.invalid/wrong/repository"}
        )
    if fault == "human_interrupt":
        # Interrupt after smoke, with completion queued but not sent.
        for _ in range(2):
            await supervisor.reconcile("ticket")
        process.complete(role_job(scope, "smoke", 1))
        for _ in range(2):
            await supervisor.reconcile("ticket")
        jira.change("approver")
    before = len(jira.writes)
    for _ in range(3):
        await supervisor.reconcile("ticket")
        await send.run_once()
    await activity.read_ticket("ticket", force_jira=True)
    ticket, state = await current(session_factory)
    assert ticket.status != "Done" and state.outcome != "delivered"
    assert len(jira.writes) == before
    if fault != "human_interrupt":
        assert "ticket/1/smoke/1" not in process.starts
    assert len(process.starts) == len(set(process.starts))
    assert len(forge.creates) == len(forge.pushes) == 1


async def test_duplicate_scheduler_passes_reuse_persisted_jobs(session_factory, pilot):
    await reach_acceptance(pilot)
    _, _, _, supervisor, process, forge, _ = pilot
    for _ in range(3):
        await asyncio.gather(*(supervisor.reconcile("ticket") for _ in range(4)))
    assert len(process.starts) == len(set(process.starts)) == 4
    async with session_factory() as session:
        jobs = list(
            await session.scalars(
                select(JiraWorkerJob).where(JiraWorkerJob.kind == "role_job")
            )
        )
        assert len(jobs) == 4
        assert len({row.execution_key for row in jobs}) == 4
    assert len(forge.creates) == len(forge.pushes) == 1


async def test_readiness_reports_each_local_blocker_without_mutation(
    session_factory, pilot
):
    scope = pilot[0]
    assert (
        SwitchConfig.model_fields["jira_worker_delivery_execution_enabled"].default
        is False
    )
    settings = config(enabled=False)
    before = settings.model_dump()
    checks = check_readiness(settings, scope, "acme", "TEST")
    assert checks["admission_disabled"] is None
    assert checks["pack_pin"] is None and checks["scope_pack"] is None
    assert checks["verification"] is None and checks["scope_approval_route"] is None
    assert checks["live_pack"] and checks["live_prerequisites"]
    assert settings.model_dump() == before
    settings.jira_worker_delivery_execution_enabled = True
    settings.jira_worker_delivery_projects = {}
    settings.jira_worker_delivery_language_pricing = None
    checks = check_readiness(settings, scope, "other", "TEST")
    assert all(
        checks[key]
        for key in (
            "admission_disabled",
            "delivery_fields",
            "language_prices",
            "scope_instance",
        )
    )
    assert not pilot[4].starts and not pilot[5].pushes


async def test_readiness_rejects_changed_pack_and_incomplete_scope(
    session_factory, pilot, monkeypatch
):
    scope = pilot[0]
    settings = config(enabled=False)
    scope = scope.model_copy(
        update={
            "verification": None,
            "job": scope.job.model_copy(update={"pack_digest": "0" * 64}),
        }
    )
    checks = check_readiness(settings, scope, "acme", "TEST")
    assert checks["scope_pack"] and checks["verification"]

    def changed_pack(config):
        raise ValueError("Immutable pin changed")

    monkeypatch.setattr(readiness, "load_pack", changed_pack)
    checks = check_readiness(settings, scope, "acme", "TEST")
    assert all(checks[key] for key in ("pack_pin", "scope_pack", "live_pack"))


async def test_readiness_cli_reports_draft_refusal_without_enabling(
    pilot, tmp_path, monkeypatch, capsys
):
    scope = tmp_path / "scope.json"
    scope.write_text(pilot[0].model_dump_json())
    settings = config(enabled=False)
    monkeypatch.setattr(readiness, "SwitchConfig", lambda: settings)
    monkeypatch.setattr(
        "sys.argv",
        ["readiness", "--scope", str(scope), "--instance", "acme", "--project", "TEST"],
    )
    assert main() == 1
    output = capsys.readouterr().out
    assert "PASS admission_disabled" in output and "PASS pack_pin" in output
    assert "BLOCK live_pack" in output and "REFUSE" in output
    assert not settings.jira_worker_delivery_execution_enabled
    assert not pilot[4].starts and not pilot[5].pushes


def test_readiness_cli_refuses_invalid_input_without_disclosing_it(
    tmp_path, monkeypatch, capsys
):
    scope = tmp_path / "scope.json"
    scope.write_text('{"sensitive_fixture_value": "do-not-print"}')
    monkeypatch.setattr(
        "sys.argv",
        ["readiness", "--scope", str(scope), "--instance", "acme", "--project", "TEST"],
    )
    assert main() == 1
    output = capsys.readouterr().out
    assert "REFUSE" in output and "do-not-print" not in output


def test_every_pilot_case_and_required_subrun_has_an_evidence_place():
    root = Path(__file__).resolve().parents[5] / "docs" / "jira-delivery-pilot"
    index = (root / "evidence-index.md").read_text()
    rows = [
        [cell.strip() for cell in line.strip().strip("|").split("|")]
        for line in index.splitlines()
        if line.startswith("|")
    ]
    case_places = {cells[0]: cells[2] for cells in rows if len(cells) == 4}
    for case in range(1, 21):
        assert case_places[f"V2-P{case:02d}"] == f"P{case:02d}/result.md"
    required_subruns = {
        "P0": [f"{number:02d}" for number in range(1, 13)],
        "P04": "question restart unrelated answer".split(),
        "P09": "reproduce fix review qa smoke".split(),
        "P10": "intake reproduce fix review qa accept merge smoke reopen".split(),
        "P11": "create approval concurrent stale-generation publication".split(),
        "P12": "launch result publication worker-loss".split(),
        "P13": "ticket repository hooks output traversal symlink oversize credential-url digest".split(),
        "P14": "capacity token ticket-cost day-cost unknown rollover budget-request resume disable-start disable-retry disable-resume disable-running".split(),
        "P15": "A B C".split(),
        "P16": "reminder alert blocked restart".split(),
        "P17": "rate-limit jira-unknown forge-unknown".split(),
        "P18": "wrong-place chat-scope chat-candidate unmapped".split(),
        "P19": "A A2 B C D".split(),
        "P20": "unmerged head-change failed-smoke".split(),
    }
    subrun_places = {cells[0]: cells[1] for cells in rows if len(cells) == 2}
    for case, subruns in required_subruns.items():
        tokens = subrun_places[case].replace(",", " ").replace(";", " ").split()
        allocated = {token for token in tokens if token.startswith(f"{case}/")}
        if tokens[1] == "through":
            first_case, first = tokens[0].split("/")
            last_case, last = tokens[2].split("/")
            assert first_case == last_case == case
            allocated = {
                f"{case}/{number:02d}" for number in range(int(first), int(last) + 1)
            }
        expected = {f"{case}/{subrun}" for subrun in subruns}
        assert expected <= allocated, expected - allocated
    assert "Not run" in index
