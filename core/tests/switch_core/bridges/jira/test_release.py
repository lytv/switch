"""Admin release uses real PostgreSQL and HTTP-level Jira fixtures."""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timedelta

import httpx
import pytest
from sqlalchemy import select

from switch_core.bridges.jira.activity import TURN, ticket_turn_lock
from switch_core.bridges.jira.admin import release_parked_ticket
from switch_core.bridges.jira.outbox import (
    INTERRUPTED_TURN,
    STAMP_MISMATCH,
    TOKEN_LIMIT,
    TURN_FAILED,
    HumanChange,
    JiraOutboxSender,
    enqueue_jira_command,
    park_ticket,
)
from switch_core.db.models import (
    JiraWorkerEvent,
    JiraWorkerJob,
    JiraWorkerOutbox,
    JiraWorkerTicket,
    User,
)
from tests.switch_core.bridges.jira.test_admin import (
    NOW,
    FakeAdminBoard,
    _seed,
    _worker_config,
)
from tests.switch_core.bridges.jira.test_intake import (
    NOW as INTAKE_NOW,
)
from tests.switch_core.bridges.jira.test_intake import (
    IntakeJira,
    IntakeRooms,
    ModelMock,
    commands,
    scheduler,
    start_ticket,
    ticket,
)
from tests.switch_core.bridges.jira.test_intake import (
    config as intake_config,
)
from tests.switch_core.bridges.jira.test_outbox import BASE, client_for


async def setup_release(factory):
    key = await _seed(
        factory, status="Waiting for input", reporter_switch_user_id="reporter-user"
    )
    jira = IntakeJira()
    jira.status = "Waiting for input"
    jira.fields["summary"] = "First ticket"
    jira.change("worker-account", comment=True)
    jira.updated = (
        datetime.fromisoformat(jira.updated) + timedelta(milliseconds=52)
    ).isoformat()
    config = _worker_config()
    async with factory() as session:
        admin = User(
            name="release-admin", email="release-admin@example.invalid", role="admin"
        )
        member = User(
            name="release-member", email="release-member@example.invalid", role="user"
        )
        session.add_all([admin, member])
        ticket = await session.get(JiraWorkerTicket, key)
        ticket.tokens_used = 123
        ticket.jira_read_updated = BASE
        ticket.jira_read_changelog_id = ""
        ticket.jira_seen_comment_ids = []
        ticket.intake_state = {"phase": "approval", "questions": 3, "cycle": 0}
        summary_id = await enqueue_jira_command(
            session,
            config=config,
            ticket=ticket,
            command_key=f"intake:{key}:0:summary",
            command="comment",
            data="Intake summary (not approval):\nRecorded summary.",
            based_updated=BASE,
            based_changelog_id="",
            based_comment_ids=[],
        )
        summary = await session.get(JiraWorkerOutbox, summary_id)
        summary.status = "done"
        summary.created_at = NOW - timedelta(seconds=1)
        summary.payload = {
            **summary.payload,
            "intake_generation": ticket.agent_generation,
        }
        jira.comments[0]["body"] = summary.payload["data"]
        command_id = await enqueue_jira_command(
            session,
            config=config,
            ticket=ticket,
            command_key=f"intake:{key}:0:approval",
            command="transition",
            data="Waiting for approval",
            based_updated=BASE,
            based_changelog_id="",
            based_comment_ids=[],
        )
        command = await session.get(JiraWorkerOutbox, command_id)
        command.payload = {
            **command.payload,
            "intake_generation": ticket.agent_generation,
            "after_command": summary_id,
        }
        command.status = "parked"
        command.error = STAMP_MISMATCH
        park_ticket(ticket, STAMP_MISMATCH)
        ticket.admin_reasons = {"parked": STAMP_MISMATCH}
        ticket.admin_card_event_id = "existing-admin-card"
        session.add(
            JiraWorkerOutbox(
                channel="switch",
                instance="acme",
                issue_key="KAN-1",
                command="admin_reason_alert",
                command_key=f"admin-alert:{key}:parked",
                status="pending",
                payload={"reason": "parked"},
            )
        )
        await session.commit()
        return key, admin.id, member.id, command_id, jira, config


async def release(factory, setup, request_id):
    key, admin, _, _, jira, config = setup
    return await release_parked_ticket(
        factory,
        config=config,
        client=client_for(jira),
        ticket_id=key,
        request_id=request_id,
        admin_user_id=admin,
        now=NOW,
    )


async def release_intake_ticket(factory, key, jira):
    async with factory() as session:
        admin = User(name="release-admin", email="admin@example.invalid", role="admin")
        session.add(admin)
        await session.commit()
    return await release_parked_ticket(
        factory,
        config=intake_config(),
        client=client_for(jira),
        ticket_id=key,
        request_id=str(uuid.uuid4()),
        admin_user_id=admin.id,
        now=INTAKE_NOW,
    )


@pytest.mark.parametrize("status", ["To Do", "BACKLOG", "In Progress"])
async def test_never_started_release_keeps_configured_status_guard(
    session_factory, status
):
    key = await start_ticket(session_factory, status=status)
    jira = IntakeJira()
    jira.status = status
    jira.fields["status_category"] = (
        "new" if status != "In Progress" else "indeterminate"
    )
    assert intake_config().jira_worker_statuses.to_do == "To Do"
    async with session_factory() as session:
        row = await session.get(JiraWorkerTicket, key)
        assert row.intake_state is None and row.tokens_used == 0
        park_ticket(row, "Human Jira action")
        await session.commit()
    if status == "To Do":
        receipt = await release_intake_ticket(session_factory, key, jira)
        assert receipt.event_kind == "admin_release" and receipt.consumed
        row = await ticket(session_factory, key)
        assert row.worker_parked_reason is None
        assert row.jira_read_updated == jira.updated
    else:
        with pytest.raises(ValueError, match="Unknown intake phase/status combination"):
            await release_intake_ticket(session_factory, key, jira)
        row = await ticket(session_factory, key)
        assert row.worker_parked_reason == "Human Jira action"
        assert row.intake_state is None and row.tokens_used == 0
        async with session_factory() as session:
            assert not list(await session.scalars(select(JiraWorkerEvent)))
    assert not jira.writes


@pytest.mark.parametrize(
    "reason",
    [
        TOKEN_LIMIT,
        "Jira ticket completed or cancelled",
        INTERRUPTED_TURN,
        TURN_FAILED,
        "budget",
        "sending",
        "uncertain",
        "jira_terminal",
        "read_race",
        "project_changed",
        "reporter_changed",
        "regressed",
        "uncertain_comment",
        "uncertain_question",
        "custom_terminal",
    ],
)
async def test_release_refuses_limits_and_unknown_outcomes(session_factory, reason):
    setup = await setup_release(session_factory)
    key, _, _, command_id, jira, config = setup
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, key)
        if reason == "budget":
            ticket.tokens_used = config.jira_worker_limits.tokens_per_ticket
        elif reason in ("sending", "uncertain"):
            (await session.get(JiraWorkerOutbox, command_id)).status = reason
        elif reason in ("uncertain_comment", "uncertain_question"):
            row = await session.get(JiraWorkerOutbox, command_id)
            row.status = "uncertain"
            row.command = (
                "comment" if reason == "uncertain_comment" else "intake_question"
            )
            if reason == "uncertain_question":
                row.channel = "switch"
        elif reason == "custom_terminal":
            jira.status = "Closed"
            jira.fields["status_category"] = "done"
        elif reason == "jira_terminal":
            jira.status = "Done"
        elif reason == "read_race":
            jira.read_change = True
        elif reason == "project_changed":
            jira.fields["project"] = {"key": "OTHER"}
        elif reason == "reporter_changed":
            jira.fields["reporter"] = {"accountId": "new-reporter"}
        elif reason == "regressed":
            jira.updated = (
                datetime.fromisoformat(BASE) - timedelta(seconds=1)
            ).isoformat()
        else:
            ticket.worker_parked_reason = reason
        await session.commit()
        before_tokens = ticket.tokens_used
        before_reason = ticket.worker_parked_reason
    error_type = HumanChange if reason == "read_race" else ValueError
    with pytest.raises(
        error_type,
        match="limit|Terminal|completed|Unknown|changed while|refresh ticket|older than",
    ):
        await release(session_factory, setup, str(uuid.uuid4()))
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, key)
        assert ticket.tokens_used == before_tokens
        assert ticket.worker_parked_reason == before_reason
        assert ticket.agent_state == "parked"
        assert not list(await session.scalars(select(JiraWorkerEvent)))
    assert not jira.writes


async def test_release_requires_admin_and_enabled_project(session_factory):
    setup = await setup_release(session_factory)
    key, _, member, _, jira, config = setup
    with pytest.raises(PermissionError):
        await release_parked_ticket(
            session_factory,
            config=config,
            client=client_for(jira),
            ticket_id=key,
            request_id=str(uuid.uuid4()),
            admin_user_id=member,
            now=NOW,
        )
    assert not jira.requests
    disabled = (*setup[:-1], _worker_config(jira_worker_enabled_projects={}))
    with pytest.raises(ValueError, match="not enabled"):
        await release(session_factory, disabled, str(uuid.uuid4()))
    assert not jira.requests


async def test_concurrent_release_returns_retryable_conflict_then_same_receipt(
    session_factory,
):
    setup = await setup_release(session_factory)
    jira = setup[4]
    jira.issue_read_started, jira.issue_read_release = asyncio.Event(), asyncio.Event()
    request_id = str(uuid.uuid4())
    first = asyncio.create_task(release(session_factory, setup, request_id))
    await asyncio.wait_for(jira.issue_read_started.wait(), timeout=10)
    with pytest.raises(BlockingIOError, match="active"):
        await release(session_factory, setup, request_id)
    jira.issue_read_release.set()
    receipt = await first
    repeated = await release(session_factory, setup, request_id)
    assert repeated.id == receipt.id


@pytest.mark.parametrize("channel", ["jira_comments", "switch"])
@pytest.mark.parametrize("edited", [False, True])
async def test_release_preserves_new_reporter_input(session_factory, channel, edited):
    setup = await setup_release(session_factory)
    key, _, _, _, jira, _ = setup
    jira.change("reporter-account", comment=True)
    comment = jira.comments[-1]
    comment["body"] = "Fresh reporter answer."
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, key)
        ticket.wait_channel = channel
        if edited:
            ticket.jira_seen_comment_ids = [comment["id"]]
            comment["created"] = BASE
        await session.commit()
    receipt = await release(session_factory, setup, str(uuid.uuid4()))
    assert receipt.payload["previous_reason"] == STAMP_MISMATCH
    async with session_factory() as session:
        events = list(
            await session.scalars(
                select(JiraWorkerEvent).where(
                    JiraWorkerEvent.event_kind != "admin_release"
                )
            )
        )
        assert len(events) == 1
        assert events[0].event_kind == (
            "jira_comment" if channel == "jira_comments" else "ignored_input"
        )
        assert events[0].payload["reporter_input"] == (channel == "jira_comments")
        assert events[0].payload["untrusted_data"]["body"] == "Fresh reporter answer."
        assert not events[0].consumed


async def test_duplicate_old_release_does_not_clear_a_later_park(session_factory):
    setup = await setup_release(session_factory)
    key = setup[0]
    request_id = str(uuid.uuid4())
    receipt = await release(session_factory, setup, request_id)
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, key)
        park_ticket(ticket, "Human Jira action")
        await session.commit()
    repeated = await release(session_factory, setup, request_id)
    assert repeated.id == receipt.id
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, key)
        assert ticket.worker_parked_reason == "Human Jira action"
        assert ticket.tokens_used == 123


async def test_release_waits_for_an_older_admin_alert_delivery(session_factory):
    setup = await setup_release(session_factory)
    key, _, _, _, jira, _ = setup
    request_id, board = str(uuid.uuid4()), FakeAdminBoard()
    async with ticket_turn_lock(
        session_factory.kw["bind"], f"admin-card:{key}"
    ) as locked:
        assert locked
        with pytest.raises(BlockingIOError, match="admin card update is active"):
            await release(session_factory, setup, request_id)
        assert not jira.requests
    receipt = await release(session_factory, setup, request_id)
    assert receipt.payload["previous_reason"] == STAMP_MISMATCH
    assert not board.alerts


async def test_release_does_not_trust_a_foreign_event_with_its_request_key(
    session_factory,
):
    setup = await setup_release(session_factory)
    key, admin, _, _, jira, _ = setup
    request_id = str(uuid.uuid4())
    async with session_factory() as session:
        session.add(
            JiraWorkerEvent(
                instance="acme",
                issue_key="KAN-1",
                project_key="KAN",
                idempotency_key=f"admin-release:{key}:{request_id}",
                event_kind="created",
                payload={"released_by": admin},
            )
        )
        await session.commit()
    with pytest.raises(ValueError, match="another event"):
        await release(session_factory, setup, request_id)
    assert not jira.requests
    async with session_factory() as session:
        assert (
            await session.get(JiraWorkerTicket, key)
        ).worker_parked_reason == STAMP_MISMATCH


@pytest.mark.parametrize("error", [INTERRUPTED_TURN, TURN_FAILED])
@pytest.mark.parametrize("prior_release", [False, True])
async def test_release_refuses_unknown_turn_after_park_reason_overwrite(
    session_factory, error, prior_release
):
    setup = await setup_release(session_factory)
    key, _, _, _, jira, _ = setup
    async with session_factory() as session:
        if prior_release:
            session.add(
                JiraWorkerEvent(
                    instance="acme",
                    issue_key="KAN-1",
                    event_kind="admin_release",
                    idempotency_key="previous-release",
                    received_at=NOW - timedelta(days=2),
                    consumed=True,
                )
            )
        session.add(
            JiraWorkerJob(
                kind=TURN,
                instance="acme",
                project_key="KAN",
                status="error",
                due_at=NOW - timedelta(days=1),
                claimed_at=NOW - timedelta(days=1),
                payload={"ticket_id": key},
                error=error,
            )
        )
        park_ticket(await session.get(JiraWorkerTicket, key), "Human Jira action")
        await session.commit()
    with pytest.raises(ValueError, match="Unknown turn outcome"):
        await release(session_factory, setup, str(uuid.uuid4()))
    assert not jira.requests
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, key)
        assert (
            ticket.worker_parked_reason == "Human Jira action"
            and ticket.tokens_used == 123
        )


async def test_release_allows_reconciled_turn_before_last_release(session_factory):
    setup = await setup_release(session_factory)
    key = setup[0]
    async with session_factory() as session:
        session.add(
            JiraWorkerEvent(
                instance="acme",
                issue_key="KAN-1",
                event_kind="admin_release",
                idempotency_key="previous-release",
                received_at=NOW - timedelta(days=1),
                consumed=True,
            )
        )
        session.add(
            JiraWorkerJob(
                kind=TURN,
                instance="acme",
                project_key="KAN",
                status="error",
                due_at=NOW - timedelta(days=2),
                claimed_at=NOW - timedelta(days=2),
                payload={"ticket_id": key},
                error=INTERRUPTED_TURN,
            )
        )
        await session.commit()
    receipt = await release(session_factory, setup, str(uuid.uuid4()))
    assert receipt.payload["previous_reason"] == STAMP_MISMATCH


@pytest.mark.parametrize(
    "status", ["Waiting for approval", "Waiting for input", "In Progress", "To Do"]
)
async def test_release_completed_approval_never_undoes_human_status(
    session_factory, status
):
    key = await start_ticket(session_factory)
    jira, rooms, model = IntakeJira(), IntakeRooms(), ModelMock("complete")
    worker = scheduler(session_factory, jira, rooms, model)
    await worker.run_once()
    assert jira.status == "Waiting for approval"
    assert all(row.status == "done" for row in await commands(session_factory))
    jira.change("approver-account", comment=True)
    if status == "Waiting for input":
        jira.status = status
    elif status != "Waiting for approval":
        jira.transition(status, "approver-account")
    await worker.activity.read_ticket(key, force_jira=True)
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, key)
        before = dict(ticket.intake_state)
        control = {
            name: getattr(ticket, name)
            for name in (
                "status",
                "agent_state",
                "agent_generation",
                "tokens_used",
                "wake_at",
                "wait_anchor",
                "wait_fired",
                "summary",
                "description",
            )
        }
        assert ticket.worker_parked_reason == "Human Jira action"
    writes = len(jira.writes)
    if status in ("Waiting for approval", "In Progress"):
        await release_intake_ticket(session_factory, key, jira)
    else:
        with pytest.raises(ValueError, match="Unknown intake phase/status combination"):
            await release_intake_ticket(session_factory, key, jira)
    await worker._outbox.run_once()
    assert jira.status == status and len(jira.writes) == writes
    assert len(await commands(session_factory)) == 2
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, key)
        assert ticket.intake_state == before
        assert {name: getattr(ticket, name) for name in control} == control
        if status in ("Waiting for approval", "In Progress"):
            assert ticket.worker_parked_reason is None
        else:
            assert ticket.worker_parked_reason == "Human Jira action"
            assert not await session.scalar(
                select(JiraWorkerEvent.id).where(
                    JiraWorkerEvent.event_kind == "admin_release"
                )
            )


async def test_release_clears_only_park_and_baseline_without_replaying(session_factory):
    setup = await setup_release(session_factory)
    key, admin, _, command_id, jira, config = setup
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, key)
        ticket.jira_seen_comment_ids = ["deleted", "1"]
        ticket.wait_anchor = NOW - timedelta(hours=5)
        ticket.wait_fired = {"admin": NOW.isoformat()}
        ticket.admin_reasons = {"parked": STAMP_MISMATCH, "custom": "Keep this reason"}
        await session.commit()
        before = {
            column.name: getattr(ticket, column.name)
            for column in JiraWorkerTicket.__table__.columns
            if column.name
            not in (
                "worker_parked_reason",
                "jira_seen_comment_ids",
                "jira_read_updated",
                "jira_read_changelog_id",
                "jira_worker_account_id",
                "admin_reasons",
            )
        }
        command = await session.get(JiraWorkerOutbox, command_id)
        command_before = {
            column.name: getattr(command, column.name)
            for column in JiraWorkerOutbox.__table__.columns
        }
    request_id = str(uuid.uuid4())
    first = await release(session_factory, setup, request_id)
    reads = len(jira.requests)
    second = await release(session_factory, setup, request_id)
    assert first.id == second.id and len(jira.requests) == reads
    assert first.payload["released_by"] == admin
    assert first.payload["released_at"] == NOW.isoformat()
    await JiraOutboxSender(
        session_factory=session_factory,
        config=config,
        clients={"acme": client_for(jira)},
        clock=lambda: NOW,
    ).run_once()
    assert not jira.writes
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, key)
        assert ticket.worker_parked_reason is None
        assert ticket.jira_seen_comment_ids == ["1"]
        assert ticket.jira_read_updated == jira.updated
        assert ticket.admin_reasons == {"custom": "Keep this reason"}
        assert {name: getattr(ticket, name) for name in before} == before
        command = await session.get(JiraWorkerOutbox, command_id)
        assert {
            name: getattr(command, name) for name in command_before
        } == command_before


class FailedApprovalJira(IntakeJira):
    async def handle(self, request):
        if (
            request.method == "POST"
            and request.url.path.endswith("/transitions")
            and json.loads(request.content)["transition"]["id"]
            == "Waiting for approval"
        ):
            self.requests.append(request)
            return httpx.Response(400, json={"errorMessages": ["Transition rejected"]})
        return await super().handle(request)


@pytest.mark.parametrize(
    "change", ["none", "human_block", "missing_command", "wrong_failure"]
)
async def test_release_worker_blocked_after_failed_write(session_factory, change):
    key = await start_ticket(session_factory)
    jira, rooms, model = FailedApprovalJira(), IntakeRooms(), ModelMock("complete")
    worker = scheduler(session_factory, jira, rooms, model)
    await worker.run_once()
    sender = JiraOutboxSender(
        session_factory=session_factory,
        config=intake_config(),
        clients={"acme": client_for(jira)},
        clock=lambda: datetime.now(NOW.tzinfo) + timedelta(seconds=1),
    )
    for _ in range(3):
        await sender.run_once()
    assert jira.status == "BLOCKED"
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, key)
        reason = ticket.worker_parked_reason
        assert reason.startswith("Jira command failed: ")
        rows = list(await session.scalars(select(JiraWorkerOutbox)))
        failed = next(row for row in rows if row.status == "failed")
        assert reason == f"Jira command failed: {failed.id}"
        blocked = next(
            row for row in rows if (row.payload or {}).get("data") == "BLOCKED"
        )
        assert blocked.status == "done"
        if change == "human_block":
            jira.transition("BLOCKED", "human-account")
        elif change == "missing_command":
            (await session.get(JiraWorkerOutbox, blocked.id)).status = "cancelled"
        elif change == "wrong_failure":
            ticket.worker_parked_reason = "Jira command failed: another-command"
        await session.commit()
        before = dict(ticket.intake_state)
        generation, tokens = ticket.agent_generation, ticket.tokens_used
    writes = len(jira.writes)
    if change == "none":
        await release_intake_ticket(session_factory, key, jira)
    else:
        with pytest.raises(ValueError, match="Unknown intake phase/status"):
            await release_intake_ticket(session_factory, key, jira)
    await worker._outbox.run_once()
    assert jira.status == "BLOCKED" and len(jira.writes) == writes
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, key)
        assert ticket.intake_state == before
        assert ticket.agent_generation == generation and ticket.tokens_used == tokens
        assert (ticket.worker_parked_reason is None) == (change == "none")
        assert (await session.get(JiraWorkerOutbox, failed.id)).status == "failed"


async def test_release_deleted_comment_does_not_park_on_next_read(session_factory):
    key = await start_ticket(session_factory)
    jira, rooms, model = IntakeJira(), IntakeRooms(), ModelMock("complete")
    worker = scheduler(session_factory, jira, rooms, model)
    await worker.run_once()
    await worker.activity.read_ticket(key, force_jira=True)
    assert jira.comments
    deleted_ids = [str(comment["id"]) for comment in jira.comments]
    jira.comments.clear()
    jira.change("human-account")
    await worker.activity.read_ticket(key, force_jira=True)
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, key)
        assert ticket.worker_parked_reason
        assert ticket.jira_seen_comment_ids == deleted_ids
    await release_intake_ticket(session_factory, key, jira)
    await worker.activity.read_ticket(key, force_jira=True)
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, key)
        assert ticket.worker_parked_reason is None
        assert ticket.jira_seen_comment_ids == []


@pytest.mark.parametrize("status", ["pending", "claimed"])
async def test_release_refuses_unfinished_commands(session_factory, status):
    setup = await setup_release(session_factory)
    async with session_factory() as session:
        command = await session.get(JiraWorkerOutbox, setup[3])
        command.status = status
        await session.commit()
    with pytest.raises(ValueError, match="Unfinished commands"):
        await release(session_factory, setup, str(uuid.uuid4()))
    async with session_factory() as session:
        assert (await session.get(JiraWorkerTicket, setup[0])).worker_parked_reason
        assert (await session.get(JiraWorkerOutbox, setup[3])).status == status


@pytest.mark.parametrize(
    "change",
    ["worker", "missing_actor", "wrong_source", "no_history", "unfinished_approval"],
)
async def test_release_refuses_unproven_approval(session_factory, change):
    key = await start_ticket(session_factory)
    jira, rooms, model = IntakeJira(), IntakeRooms(), ModelMock("complete")
    worker = scheduler(session_factory, jira, rooms, model)
    await worker.run_once()
    jira.transition("In Progress", "approver-account")
    if change == "worker":
        jira.histories[-1]["author"] = {"accountId": "worker-account"}
    elif change == "missing_actor":
        jira.histories[-1]["author"] = {}
    elif change == "wrong_source":
        jira.histories[-1]["items"][0]["fromString"] = "To Do"
    elif change == "no_history":
        jira.histories.clear()
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, key)
        park_ticket(ticket, "Human Jira action")
        if change == "unfinished_approval":
            approval = await session.scalar(
                select(JiraWorkerOutbox).where(
                    JiraWorkerOutbox.command_key == f"intake:{key}:0:approval"
                )
            )
            approval.status = "parked"
        await session.commit()
    with pytest.raises(ValueError, match="Unknown intake phase/status"):
        await release_intake_ticket(session_factory, key, jira)
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, key)
        assert ticket.worker_parked_reason == "Human Jira action"
        assert not await session.scalar(
            select(JiraWorkerEvent.id).where(
                JiraWorkerEvent.event_kind == "admin_release"
            )
        )
