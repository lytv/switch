"""Admin release uses real PostgreSQL and HTTP-level Jira fixtures."""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta

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
from tests.switch_core.bridges.jira.test_activity import message
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


async def release(factory, setup, request_id, *, board):
    key, admin, _, _, jira, config = setup
    return await release_parked_ticket(
        factory,
        config=config,
        client=client_for(jira),
        board=board,
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
        board=FakeAdminBoard(),
        ticket_id=key,
        request_id=str(uuid.uuid4()),
        admin_user_id=admin.id,
        now=INTAKE_NOW,
    )


async def test_release_retries_matching_transition_once_after_restart(session_factory):
    setup = await setup_release(session_factory)
    key, admin, _, command_id, jira, config = setup
    board, request_id = FakeAdminBoard(), str(uuid.uuid4())
    first = await release(session_factory, setup, request_id, board=board)
    reads = len(jira.requests)
    second = await release(session_factory, setup, request_id, board=board)
    assert first.id == second.id and len(jira.requests) == reads
    assert first.payload["released_by"] == admin
    assert first.payload["released_at"] == NOW.isoformat()
    assert first.payload["retried_command_ids"] == [command_id]
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, key)
        command = await session.get(JiraWorkerOutbox, command_id)
        assert ticket.tokens_used == 123 and ticket.worker_parked_reason is None
        assert ticket.agent_state == "wake_requested"
        assert ticket.jira_seen_comment_ids == ["1"]
        assert ticket.jira_read_updated == jira.updated
        assert command.payload["based_updated"] == jira.updated
        assert command.payload["intake_generation"] == ticket.agent_generation
        assert "parked" not in (ticket.admin_reasons or {})
        assert "- none" in board.cards[ticket.admin_card_event_id]
        assert not list(
            await session.scalars(
                select(JiraWorkerOutbox).where(
                    JiraWorkerOutbox.command == "admin_reason_alert"
                )
            )
        )
        assert not board.alerts
    sender = JiraOutboxSender(
        session_factory=session_factory,
        config=config,
        clients={"acme": client_for(jira)},
        clock=lambda: NOW,
    )
    await asyncio.gather(sender.run_once(), sender.run_once())
    await JiraOutboxSender(
        session_factory=session_factory,
        config=config,
        clients={"acme": client_for(jira)},
        clock=lambda: NOW,
    ).run_once()
    assert jira.status == "Waiting for approval"
    assert len(jira.writes) == 1 and len(jira.comments) == 1
    async with session_factory() as session:
        assert (await session.get(JiraWorkerOutbox, command_id)).status == "done"
        receipts = list(
            await session.scalars(
                select(JiraWorkerEvent).where(
                    JiraWorkerEvent.event_kind == "admin_release"
                )
            )
        )
        assert len(receipts) == 1 and receipts[0].consumed


@pytest.mark.parametrize(
    "change",
    [
        "status",
        "human_field",
        "legacy_baseline",
        "failed",
        "deleted_comment",
        "cancelled_predecessor",
        "context_changed",
        "legacy_context",
        "field_command",
    ],
)
async def test_release_replans_stale_or_unprovable_commands(session_factory, change):
    setup = await setup_release(session_factory)
    key, _, _, command_id, jira, _ = setup
    if change == "status":
        jira.status = "In Progress"
    elif change == "human_field":
        jira.change("human-account")
    elif change == "context_changed":
        jira.fields["summary"] = "Changed Jira summary without a visible history row"
    async with session_factory() as session:
        command = await session.get(JiraWorkerOutbox, command_id)
        if change == "legacy_baseline":
            payload = dict(command.payload)
            payload.pop("based_status")
            command.payload = payload
        elif change == "failed":
            command.status = "failed"
        elif change == "legacy_context":
            payload = dict(command.payload)
            payload.pop("based_context")
            command.payload = payload
        elif change == "field_command":
            (await session.get(JiraWorkerTicket, key)).intake_state = None
            command.command_key = "release-fields"
            command.command = "fields"
            command.payload = {
                **command.payload,
                "data": {"summary": "Old planned summary"},
            }
        elif change == "deleted_comment":
            command.payload = {**command.payload, "based_comment_ids": ["deleted"]}
        elif change == "cancelled_predecessor":
            previous = JiraWorkerOutbox(
                channel="jira",
                instance="acme",
                issue_key="KAN-1",
                command="comment",
                status="cancelled",
            )
            session.add(previous)
            await session.flush()
            command.payload = {**command.payload, "after_command": previous.id}
        await session.commit()
    receipt = await release(
        session_factory, setup, str(uuid.uuid4()), board=FakeAdminBoard()
    )
    assert receipt.payload["replanned"] and receipt.payload["retried_command_ids"] == []
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, key)
        assert ticket.status == jira.status
        assert ticket.summary == jira.fields["summary"]
        assert ticket.intake_state == (
            None
            if change == "field_command"
            else {"phase": "approval", "questions": 3, "cycle": 0}
        )
        assert ticket.tokens_used == 123
        assert (await session.get(JiraWorkerOutbox, command_id)).status == (
            "cancelled" if change == "field_command" else "pending"
        )
    sender = JiraOutboxSender(
        session_factory=session_factory,
        config=setup[5],
        clients={"acme": client_for(jira)},
        clock=lambda: NOW,
    )
    await sender.run_once()
    assert jira.status == (
        "Waiting for input" if change == "field_command" else "Waiting for approval"
    )
    assert len(jira.comments) == 1


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
        await release(session_factory, setup, str(uuid.uuid4()), board=FakeAdminBoard())
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
            board=FakeAdminBoard(),
            ticket_id=key,
            request_id=str(uuid.uuid4()),
            admin_user_id=member,
            now=NOW,
        )
    assert not jira.requests
    disabled = (*setup[:-1], _worker_config(jira_worker_enabled_projects={}))
    with pytest.raises(ValueError, match="not enabled"):
        await release(
            session_factory, disabled, str(uuid.uuid4()), board=FakeAdminBoard()
        )
    assert not jira.requests


async def test_concurrent_release_returns_retryable_conflict_then_same_receipt(
    session_factory,
):
    setup = await setup_release(session_factory)
    jira = setup[4]
    jira.issue_read_started, jira.issue_read_release = asyncio.Event(), asyncio.Event()
    board, request_id = FakeAdminBoard(), str(uuid.uuid4())
    first = asyncio.create_task(
        release(session_factory, setup, request_id, board=board)
    )
    await asyncio.wait_for(jira.issue_read_started.wait(), timeout=10)
    with pytest.raises(BlockingIOError, match="active"):
        await release(session_factory, setup, request_id, board=board)
    jira.issue_read_release.set()
    receipt = await first
    repeated = await release(session_factory, setup, request_id, board=board)
    assert repeated.id == receipt.id


async def test_card_failure_does_not_repeat_release_and_retry_clears_alert(
    session_factory,
):
    setup = await setup_release(session_factory)
    board, request_id = FakeAdminBoard(), str(uuid.uuid4())

    async def unavailable():
        raise RuntimeError("board unavailable")

    original = board.ensure_room
    board.ensure_room = unavailable
    with pytest.raises(RuntimeError, match="board unavailable"):
        await release(session_factory, setup, request_id, board=board)
    board.ensure_room = original
    receipt = await release(session_factory, setup, request_id, board=board)
    assert receipt.payload["retried_command_ids"] == [setup[3]]
    assert len(board.cards) == 1


@pytest.mark.parametrize("channel", ["jira_comments", "switch"])
@pytest.mark.parametrize("edited", [False, True])
async def test_release_preserves_new_reporter_input_and_replans(
    session_factory, channel, edited
):
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
    receipt = await release(
        session_factory, setup, str(uuid.uuid4()), board=FakeAdminBoard()
    )
    assert receipt.payload["replanned"]
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
    request_id, board = str(uuid.uuid4()), FakeAdminBoard()
    receipt = await release(session_factory, setup, request_id, board=board)
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, key)
        park_ticket(ticket, "Human Jira action")
        await session.commit()
    repeated = await release(session_factory, setup, request_id, board=board)
    assert repeated.id == receipt.id
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, key)
        assert ticket.worker_parked_reason == "Human Jira action"
        assert ticket.tokens_used == 123


async def test_release_refuses_a_sender_with_an_active_http_write(session_factory):
    setup = await setup_release(session_factory)
    jira, config = setup[4:]
    board = FakeAdminBoard()
    await release(session_factory, setup, str(uuid.uuid4()), board=board)
    jira.write_started, jira.write_release = asyncio.Event(), asyncio.Event()
    sender = JiraOutboxSender(
        session_factory=session_factory,
        config=config,
        clients={"acme": client_for(jira)},
        clock=lambda: NOW,
    )
    sending = asyncio.create_task(sender.run_once())
    await asyncio.wait_for(jira.write_started.wait(), timeout=10)
    with pytest.raises(BlockingIOError, match="send is active"):
        await release(session_factory, setup, str(uuid.uuid4()), board=board)
    jira.write_release.set()
    await sending
    assert len(jira.writes) == 1


async def test_release_waits_for_an_older_admin_alert_delivery(session_factory):
    setup = await setup_release(session_factory)
    key, _, _, _, jira, _ = setup
    request_id, board = str(uuid.uuid4()), FakeAdminBoard()
    async with ticket_turn_lock(
        session_factory.kw["bind"], f"admin-card:{key}"
    ) as locked:
        assert locked
        with pytest.raises(BlockingIOError, match="admin card update is active"):
            await release(session_factory, setup, request_id, board=board)
        assert not jira.requests
    receipt = await release(session_factory, setup, request_id, board=board)
    assert receipt.payload["retried_command_ids"] == [setup[3]]
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
        await release(session_factory, setup, request_id, board=FakeAdminBoard())
    assert not jira.requests
    async with session_factory() as session:
        assert (
            await session.get(JiraWorkerTicket, key)
        ).worker_parked_reason == STAMP_MISMATCH


async def test_release_cancels_an_obsolete_wait_without_reusing_its_key(
    session_factory,
):
    setup = await setup_release(session_factory)
    key, _, _, command_id, _, config = setup
    old_anchor = NOW - timedelta(days=1)
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, key)
        ticket.wait_anchor = old_anchor
        ticket.wait_fired = {"blocked": old_anchor.isoformat()}
        wait_id = await enqueue_jira_command(
            session,
            config=config,
            ticket=ticket,
            command_key=f"wait-blocked:{key}:{old_anchor.isoformat()}",
            command="transition",
            data="BLOCKED",
            based_updated=BASE,
            based_changelog_id="",
            based_comment_ids=[],
        )
        (await session.get(JiraWorkerOutbox, wait_id)).status = "parked"
        await session.commit()
    receipt = await release(
        session_factory, setup, str(uuid.uuid4()), board=FakeAdminBoard()
    )
    assert receipt.payload["retried_command_ids"] == [command_id]
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, key)
        assert ticket.wait_anchor == NOW and ticket.wait_fired == {}
        assert ticket.tokens_used == 123
        assert (await session.get(JiraWorkerOutbox, wait_id)).status == "cancelled"
        assert (await session.get(JiraWorkerOutbox, command_id)).status == "pending"


@pytest.mark.parametrize("transition_status", ["parked", "cancelled"])
async def test_release_legacy_posted_summary_only_sends_approval(
    session_factory, transition_status
):
    setup = await setup_release(session_factory)
    key, _, _, command_id, jira, config = setup
    async with session_factory() as session:
        for row in await session.scalars(
            select(JiraWorkerOutbox).where(
                JiraWorkerOutbox.command_key.like(f"intake:{key}:%")
            )
        ):
            payload = dict(row.payload)
            payload.pop("based_status")
            payload.pop("based_context")
            row.payload = payload
        (await session.get(JiraWorkerOutbox, command_id)).status = transition_status
        await session.commit()
    receipt = await release(
        session_factory, setup, str(uuid.uuid4()), board=FakeAdminBoard()
    )
    assert receipt.payload["replanned"]
    sender = JiraOutboxSender(
        session_factory=session_factory,
        config=config,
        clients={"acme": client_for(jira)},
        clock=lambda: NOW,
    )
    await sender.run_once()
    await sender.run_once()
    assert jira.status == "Waiting for approval"
    assert len(jira.comments) == 1 and len(jira.writes) == 1
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, key)
        assert ticket.intake_state == {"phase": "approval", "questions": 3, "cycle": 0}
        assert ticket.wait_anchor == NOW and ticket.wait_fired == {}
        assert ticket.worker_parked_reason is None
        assert all(row.status == "done" for row in await commands(session_factory))


@pytest.mark.parametrize("action", ["complete", "question"])
async def test_release_restores_successors_cancelled_by_the_sender(
    session_factory, monkeypatch, action
):
    key = await start_ticket(session_factory)
    jira, rooms, model = IntakeJira(), IntakeRooms(), ModelMock(action)
    decide = model.decide

    async def change_before_send(data, budget):
        decision = await decide(data, budget)
        jira.updated = (
            datetime.fromisoformat(jira.updated) + timedelta(seconds=2)
        ).isoformat()
        return decision

    monkeypatch.setattr(model, "decide", change_before_send)
    await scheduler(session_factory, jira, rooms, model).run_once()
    rows = await commands(session_factory)
    assert [row.status for row in rows] == ["parked", "cancelled"]
    async with session_factory() as session:
        before = dict((await session.get(JiraWorkerTicket, key)).intake_state)
    receipt = await release_intake_ticket(session_factory, key, jira)
    assert receipt.payload["replanned"]
    async with session_factory() as session:
        assert (await session.get(JiraWorkerTicket, key)).intake_state == before
    sender = JiraOutboxSender(
        session_factory=session_factory,
        config=intake_config(),
        clients={"acme": client_for(jira)},
        clock=lambda: INTAKE_NOW,
        rooms=rooms,
    )
    await sender.run_once()
    await sender.run_once()
    assert all(row.status == "done" for row in await commands(session_factory))
    assert len(model.calls) == 1
    if action == "complete":
        assert jira.status == "Waiting for approval"
        assert len(jira.comments) == 1 and not rooms.questions
    else:
        assert jira.status == "Waiting for input"
        assert len(rooms.questions) == 1 and not jira.comments


async def test_release_question_without_pending_commands_keeps_replies_for_model(
    session_factory,
):
    key = await start_ticket(session_factory)
    jira, rooms, model = IntakeJira(), IntakeRooms(), ModelMock("question", "complete")
    worker = scheduler(session_factory, jira, rooms, model)
    await worker.run_once()
    async with session_factory() as session:
        ticket = await session.get(JiraWorkerTicket, key)
        before = {**ticket.intake_state, "pending_replies": ["Saved reporter reply"]}
        ticket.intake_state = before
        park_ticket(ticket, "Human Jira action")
        await session.commit()
    rooms.messages.append(message(1))
    await worker.activity.read_once()
    await release_intake_ticket(session_factory, key, jira)
    async with session_factory() as session:
        assert (await session.get(JiraWorkerTicket, key)).intake_state == before
    await scheduler(session_factory, jira, rooms, model).run_once()
    assert len(model.calls) == 2
    assert model.calls[-1][0]["history"] == before["history"]
    assert model.calls[-1][0]["replies"] == [
        "Saved reporter reply",
        rooms.messages[0]["body"],
    ]
    assert len(rooms.questions) == 1
    assert jira.status == "Waiting for approval" and len(jira.comments) == 1
    async with session_factory() as session:
        state = (await session.get(JiraWorkerTicket, key)).intake_state
        assert state["questions"] == 1 and state["cycle"] == 0
        assert "pending_replies" not in state


@pytest.mark.parametrize("changed_status", [False, True])
async def test_release_after_done_input_transition_only_sends_remaining_steps(
    session_factory, monkeypatch, changed_status
):
    key = await start_ticket(session_factory)
    jira, rooms, model = IntakeJira(), IntakeRooms(), ModelMock("question")
    handle = jira.handle

    async def drift_after_transition(request):
        response = await handle(request)
        if request.method == "POST" and request.url.path.endswith("/transitions"):
            jira.updated = (
                datetime.fromisoformat(jira.updated) + timedelta(milliseconds=1)
            ).isoformat()
        return response

    monkeypatch.setattr(jira, "handle", drift_after_transition)
    await scheduler(session_factory, jira, rooms, model).run_once()
    rows = await commands(session_factory)
    assert [row.status for row in rows] == ["done", "parked"]
    assert jira.status == "Waiting for input" and not rooms.questions
    monkeypatch.setattr(jira, "handle", handle)
    if changed_status:
        jira.transition("To Do", "human-account")
    await release_intake_ticket(session_factory, key, jira)
    sender = JiraOutboxSender(
        session_factory=session_factory,
        config=intake_config(),
        clients={"acme": client_for(jira)},
        clock=lambda: INTAKE_NOW,
        rooms=rooms,
    )
    await sender.run_once()
    assert (
        len(jira.writes) == (2 if changed_status else 1) and len(rooms.questions) == 1
    )
    assert jira.status == "Waiting for input"
    assert all(row.status == "done" for row in await commands(session_factory))
    assert len(model.calls) == 1
    async with session_factory() as session:
        state = (await session.get(JiraWorkerTicket, key)).intake_state
        assert (
            state["phase"] == "question"
            and state["questions"] == 1
            and state["cycle"] == 0
        )


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
        await release(session_factory, setup, str(uuid.uuid4()), board=FakeAdminBoard())
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
    receipt = await release(
        session_factory, setup, str(uuid.uuid4()), board=FakeAdminBoard()
    )
    assert receipt.payload["retried_command_ids"] == [setup[3]]
