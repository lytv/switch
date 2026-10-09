"""Real PostgreSQL claims and HTTP-level Jira mocks; no live Jira calls."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from switch_core.bridges.jira.outbox import (
    HumanChange,
    JiraOutboxSender,
    check_jira_version,
    enqueue_jira_command,
    read_jira_version,
)
from switch_core.bridges.jira.worker import JiraPollClient, JiraWorkerScheduler
from switch_core.config import JiraWorkerStatuses
from switch_core.db.models import JiraWorkerOutbox, JiraWorkerTicket
from tests.switch_core.bridges.jira import test_worker
from tests.switch_core.bridges.jira.test_worker import _config, _run_migrations

mig_url = test_worker.mig_url
BASE = "2026-10-01T00:00:00.000+00:00"


class JiraMock:
    def __init__(self) -> None:
        self.updated = BASE
        self.status = "To Do"
        self.histories: list[dict[str, Any]] = []
        self.comments: list[dict[str, Any]] = []
        self.fields: dict[str, Any] = {}
        self.requests: list[httpx.Request] = []
        self.writes: list[httpx.Request] = []
        self.failures = 0
        self.timeout_after_write = False
        self.crash_after_write = False
        self.server_error_after_write = False
        self.pre_send_server_errors = 0
        self.unavailable_transition = False
        self.read_failure = False
        self.read_change = False
        self.write_started: asyncio.Event | None = None
        self.write_release: asyncio.Event | None = None

    def change(self, author: str, *, comment: bool = False) -> None:
        self.updated = (
            datetime.fromisoformat(self.updated) + timedelta(seconds=1)
        ).isoformat()
        actor = {"accountId": author}
        if comment:
            self.comments.append(
                {
                    "id": str(len(self.comments) + 1),
                    "author": actor,
                    "updateAuthor": actor,
                    "created": self.updated,
                    "updated": self.updated,
                }
            )
        else:
            self.histories.append(
                {
                    "id": str(len(self.histories) + 1),
                    "author": actor,
                    "created": self.updated,
                }
            )

    async def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.path
        if request.method == "GET":
            if self.pre_send_server_errors:
                self.pre_send_server_errors -= 1
                return httpx.Response(500, json={"errorMessages": ["unavailable"]})
            if self.read_failure:
                raise httpx.ConnectError("mock read unavailable", request=request)
            if path.endswith("/myself"):
                return httpx.Response(200, json={"accountId": "worker-account"})
            if path.endswith("/transitions"):
                transitions = (
                    []
                    if self.unavailable_transition
                    else [
                        {
                            "id": "dynamic-block-id",
                            "name": "Stop work",
                            "to": {"name": "BLOCKED"},
                        },
                        {
                            "id": "dynamic-progress-id",
                            "name": "Begin",
                            "to": {"name": "In Progress"},
                        },
                        {
                            "id": "dynamic-custom-id",
                            "name": "Custom",
                            "to": {"name": "Needs help"},
                        },
                    ]
                )
                return httpx.Response(200, json={"transitions": transitions})
            if path.endswith("/changelog") or path.endswith("/comment"):
                key, entries = (
                    ("values", self.histories)
                    if path.endswith("/changelog")
                    else ("comments", self.comments)
                )
                start = int(request.url.params["startAt"])
                # Deliberately return one entry per page.
                return httpx.Response(
                    200, json={key: entries[start : start + 1], "total": len(entries)}
                )
            if path.endswith("/search/jql"):
                return httpx.Response(200, json={"issues": [], "isLast": True})
            if self.read_change:
                self.change("human-account")
            return httpx.Response(200, json={"fields": {"updated": self.updated}})
        self.writes.append(request)
        if self.write_started:
            self.write_started.set()
        if self.write_release:
            await self.write_release.wait()
        if self.failures:
            self.failures -= 1
            return httpx.Response(429, json={"errorMessages": ["Rate limited"]})
        body = json.loads(request.content)
        if path.endswith("/comment"):
            self.change("worker-account", comment=True)
            self.comments[-1]["body"] = body["body"]
        elif path.endswith("/transitions"):
            self.status = {
                "dynamic-block-id": "BLOCKED",
                "dynamic-progress-id": "In Progress",
                "dynamic-custom-id": "Needs help",
            }[body["transition"]["id"]]
            self.change("worker-account")
        else:
            self.fields.update(body["fields"])
            self.change("worker-account")
        if self.crash_after_write:
            raise asyncio.CancelledError()
        if self.timeout_after_write:
            raise httpx.ReadTimeout("mock lost response", request=request)
        if self.server_error_after_write:
            return httpx.Response(500, json={"errorMessages": ["applied but failed"]})
        return httpx.Response(201 if path.endswith("/comment") else 204)


def client_for(jira: JiraMock) -> JiraPollClient:
    return JiraPollClient(
        base_url="https://jira.example.invalid",
        email="worker@example.invalid",
        api_token="placeholder",
        client=httpx.AsyncClient(transport=httpx.MockTransport(jira.handle)),
    )


async def enqueue(
    factory: async_sessionmaker[AsyncSession],
    *,
    key: str = "summary-1",
    command: str = "comment",
    data: Any = "Intake summary",
    updated: str = BASE,
    changelog_id: str = "",
) -> str:
    async with factory() as session:
        ticket = await session.scalar(select(JiraWorkerTicket))
        if ticket is None:
            ticket = JiraWorkerTicket(
                instance="acme", project_key="KAN", issue_key="KAN-1"
            )
            session.add(ticket)
            await session.flush()
        command_id = await enqueue_jira_command(
            session,
            config=_config(jira_worker_enabled_projects={"acme": ["KAN"]}),
            ticket=ticket,
            command_key=key,
            command=command,
            data=data,
            based_updated=updated,
            based_changelog_id=changelog_id,
        )
        await session.commit()
        assert command_id is not None
        return command_id


async def rows(factory: async_sessionmaker[AsyncSession]) -> list[JiraWorkerOutbox]:
    async with factory() as session:
        return list(
            (
                await session.scalars(
                    select(JiraWorkerOutbox)
                    .where(JiraWorkerOutbox.channel == "jira")
                    .order_by(JiraWorkerOutbox.created_at, JiraWorkerOutbox.id)
                )
            ).all()
        )


def sender_for(
    factory: async_sessionmaker[AsyncSession], jira: JiraMock, **options: Any
) -> JiraOutboxSender:
    config = _config(jira_worker_enabled_projects={"acme": ["KAN"]}, **options)
    return JiraOutboxSender(
        session_factory=factory,
        config=config,
        clients={"acme": client_for(jira)},
        clock=lambda: datetime.now(UTC) + timedelta(seconds=1),
    )


async def test_restart_and_double_claim_send_once(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    jira = JiraMock()
    command_id = await enqueue(session_factory)
    assert await enqueue(session_factory) == command_id
    jira.write_started, jira.write_release = asyncio.Event(), asyncio.Event()
    first = asyncio.create_task(sender_for(session_factory, jira).run_once())
    await asyncio.wait_for(jira.write_started.wait(), timeout=5)
    await sender_for(session_factory, jira).run_once()
    jira.write_release.set()
    await first
    await sender_for(session_factory, jira).run_once()
    assert len(jira.writes) == 1
    assert (await rows(session_factory))[0].status == "done"


async def test_restart_after_remote_commit_never_replays(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    jira = JiraMock()
    await enqueue(session_factory)
    jira.crash_after_write = True
    with pytest.raises(asyncio.CancelledError):
        await sender_for(session_factory, jira).run_once()
    assert (await rows(session_factory))[0].status == "sending"
    jira.crash_after_write = False
    await sender_for(session_factory, jira).run_once()
    row = (await rows(session_factory))[0]
    assert row.status == "uncertain" and "unknown outcome" in (row.error or "")
    assert len(jira.comments) == len(jira.writes) == 1


async def test_restart_of_claimed_row_is_safe(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    jira = JiraMock()
    command_id = await enqueue(session_factory)
    async with session_factory() as session:
        row = await session.get(JiraWorkerOutbox, command_id)
        assert row is not None
        row.status, row.attempts = "claimed", 1
        await session.commit()
    await sender_for(session_factory, jira).run_once()
    assert len(jira.writes) == 1
    assert (await rows(session_factory))[0].attempts == 1


@pytest.mark.parametrize("comment", [False, True])
async def test_human_action_parks_ticket_and_all_commands(
    session_factory: async_sessionmaker[AsyncSession], comment: bool
) -> None:
    jira = JiraMock()
    await enqueue(session_factory)
    await enqueue(
        session_factory,
        key="other",
        command="fields",
        data={"summary": "Another summary"},
    )
    jira.change("human-account", comment=comment)
    jira.change("worker-account")  # Latest actor alone cannot establish safety.
    await sender_for(session_factory, jira).run_once()
    assert not jira.writes
    assert all(
        row.status == "parked" and row.error for row in await rows(session_factory)
    )
    async with session_factory() as session:
        ticket = await session.scalar(select(JiraWorkerTicket))
        assert ticket is not None and ticket.worker_parked_reason


@pytest.mark.parametrize("comment", [False, True])
async def test_own_change_is_not_human(
    session_factory: async_sessionmaker[AsyncSession], comment: bool
) -> None:
    jira = JiraMock()
    await enqueue(session_factory)
    jira.change("worker-account", comment=comment)
    await sender_for(session_factory, jira).run_once()
    assert len(jira.writes) == 1
    assert (await rows(session_factory))[0].status == "done"


async def test_transition_resolves_target_name_and_checks_version_last(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    jira = JiraMock()
    await enqueue(session_factory, command="transition", data="In Progress")
    await sender_for(session_factory, jira).run_once()
    assert jira.status == "In Progress"
    assert json.loads(jira.writes[0].content) == {
        "transition": {"id": "dynamic-progress-id"}
    }
    assert jira.requests[-2].url.path == "/rest/api/3/issue/KAN-1"


async def test_fields_write(session_factory: async_sessionmaker[AsyncSession]) -> None:
    jira = JiraMock()
    await enqueue(
        session_factory, command="fields", data={"customfield_10142": {"id": "10028"}}
    )
    await sender_for(session_factory, jira).run_once()
    assert jira.fields == {"customfield_10142": {"id": "10028"}}
    assert jira.writes[0].method == "PUT"


async def test_retry_backoff_then_blocked_and_summary(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    jira = JiraMock()
    jira.failures = 3
    command_id = await enqueue(session_factory)
    now = datetime.now(UTC) + timedelta(seconds=1)
    config = _config(
        jira_worker_enabled_projects={"acme": ["KAN"]},
        jira_worker_write_backoff_seconds=5,
    )
    sender = JiraOutboxSender(
        session_factory=session_factory,
        config=config,
        clients={"acme": client_for(jira)},
        clock=lambda: now,
    )
    await sender.run_once()
    row = (await rows(session_factory))[0]
    assert (
        row.attempts == 1
        and row.status == "pending"
        and row.due_at == now + timedelta(seconds=5)
    )
    await sender.run_once()
    assert len(jira.writes) == 1
    now += timedelta(seconds=5)
    await sender.run_once()
    assert (await rows(session_factory))[0].due_at == now + timedelta(seconds=10)
    now += timedelta(seconds=10)
    await sender.run_once()
    original = next(row for row in await rows(session_factory) if row.id == command_id)
    assert original.status == "failed" and original.attempts == 3 and original.error
    await sender.run_once()
    assert jira.status == "BLOCKED"
    assert len(jira.comments) == 1
    assert (
        "3 failed attempt"
        in jira.comments[0]["body"]["content"][0]["content"][0]["text"]
    )
    assert sorted(row.status for row in await rows(session_factory)) == [
        "done",
        "done",
        "failed",
    ]
    await sender_for(session_factory, jira).run_once()
    assert len(jira.writes) == 5


async def test_failed_blocked_write_stays_visible(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    jira = JiraMock()
    jira.unavailable_transition = True
    await enqueue(session_factory, command="transition", data="In Progress")
    sender = sender_for(session_factory, jira)
    await sender.run_once()
    await sender.run_once()
    children = [
        row
        for row in await rows(session_factory)
        if (row.payload or {}).get("failure_of")
    ]
    blocked = next(row for row in children if row.command == "transition")
    assert blocked.status == "failed" and blocked.error
    assert not any(request.url.path.endswith("/transitions") for request in jira.writes)


async def test_human_change_during_retry_prevents_blocked_write(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    jira = JiraMock()
    jira.failures = 3
    await enqueue(session_factory)
    now = datetime.now(UTC) + timedelta(seconds=1)
    sender = JiraOutboxSender(
        session_factory=session_factory,
        config=_config(jira_worker_enabled_projects={"acme": ["KAN"]}),
        clients={"acme": client_for(jira)},
        clock=lambda: now,
    )
    for _ in range(3):
        await sender.run_once()
        now += timedelta(seconds=100)
    jira.change("human-account")
    await sender.run_once()
    assert len(jira.writes) == 3
    assert all(
        row.status == "parked"
        for row in await rows(session_factory)
        if (row.payload or {}).get("failure_of")
    )


@pytest.mark.parametrize(
    ("command", "data"),
    [("comment", "Intake summary"), ("transition", "In Progress")],
)
async def test_applied_write_http_500_stays_uncertain_across_restart(
    session_factory: async_sessionmaker[AsyncSession], command: str, data: str
) -> None:
    jira = JiraMock()
    jira.server_error_after_write = True
    await enqueue(session_factory, command=command, data=data)
    now = datetime.now(UTC) + timedelta(seconds=1)

    def clock() -> datetime:
        return now

    def make_sender() -> JiraOutboxSender:
        return JiraOutboxSender(
            session_factory=session_factory,
            config=_config(
                jira_worker_enabled_projects={"acme": ["KAN"]},
                jira_worker_write_backoff_seconds=5,
            ),
            clients={"acme": client_for(jira)},
            clock=clock,
        )

    await make_sender().run_once()
    row = (await rows(session_factory))[0]
    assert row.status == "uncertain" and row.attempts == 1
    assert row.error and "outcome is unknown" in row.error
    async with session_factory() as session:
        ticket = await session.scalar(select(JiraWorkerTicket))
        assert ticket is not None and ticket.worker_parked_reason == row.error
    now += timedelta(days=2)
    await make_sender().run_once()
    stored = await rows(session_factory)
    assert [item.status for item in stored] == ["uncertain"]
    assert stored[0].attempts == 1 and len(jira.writes) == 1
    assert jira.status != "BLOCKED" and len(jira.comments) == (
        1 if command == "comment" else 0
    )
    if command == "transition":
        assert jira.status == "In Progress"
    async with session_factory() as session:
        ticket = await session.scalar(select(JiraWorkerTicket))
        assert ticket is not None and ticket.worker_parked_reason == stored[0].error


async def test_unknown_timeout_is_visible_and_never_replayed(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    jira = JiraMock()
    jira.timeout_after_write = True
    await enqueue(session_factory)
    await sender_for(session_factory, jira).run_once()
    await sender_for(session_factory, jira).run_once()
    assert (await rows(session_factory))[0].status == "uncertain"
    assert len(jira.comments) == len(jira.writes) == 1


async def test_disabled_project_unchanged(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    jira = JiraMock()
    await enqueue(session_factory)
    sender = JiraOutboxSender(
        session_factory=session_factory,
        config=_config(),
        clients={"acme": client_for(jira)},
        clock=lambda: datetime.now(UTC),
    )
    await sender.run_once()
    assert not jira.requests
    row = (await rows(session_factory))[0]
    assert row.status == "pending" and row.attempts == 0
    async with session_factory() as session:
        ticket = await session.scalar(select(JiraWorkerTicket))
        assert ticket is not None
        assert (
            await enqueue_jira_command(
                session,
                config=_config(),
                ticket=ticket,
                command_key="disabled",
                command="comment",
                data="Ignored",
                based_updated=BASE,
                based_changelog_id="",
            )
            is None
        )
    assert len(await rows(session_factory)) == 1


async def test_configured_blocked_status(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    jira = JiraMock()
    await enqueue(session_factory, command="transition", data="Unavailable")
    sender = sender_for(
        session_factory,
        jira,
        jira_worker_statuses=JiraWorkerStatuses(blocked="Needs help"),
    )
    await sender.run_once()
    await sender.run_once()
    assert jira.status == "Needs help"


async def test_scheduler_drives_outbox(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    jira = JiraMock()
    await enqueue(session_factory)
    scheduler = JiraWorkerScheduler(
        session_factory=session_factory,
        config=_config(jira_worker_enabled_projects={"acme": ["KAN"]}),
        poll_clients={"acme": client_for(jira)},
        clock=lambda: datetime.now(UTC) + timedelta(seconds=1),
    )
    await scheduler.run_once()
    assert len(jira.writes) == 1


async def test_version_reads_all_pages_and_checks_baseline_id(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    jira = JiraMock()
    jira.change("human-account")
    baseline = jira.updated
    jira.change("worker-account")
    jira.change("worker-account", comment=True)
    version = await read_jira_version(client_for(jira), "KAN-1")
    assert len(version["histories"]) == 2 and version["changelog_id"] == "2"
    await enqueue(session_factory, updated=baseline, changelog_id="1")
    await sender_for(session_factory, jira).run_once()
    assert (await rows(session_factory))[0].status == "done"


async def test_change_during_version_read_parks(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    jira = JiraMock()
    jira.read_change = True
    await enqueue(session_factory)
    await sender_for(session_factory, jira).run_once()
    assert not jira.writes and (await rows(session_factory))[0].status == "parked"


async def test_key_reuse_with_different_command_fails(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await enqueue(session_factory)
    with pytest.raises(ValueError, match="different command"):
        await enqueue(session_factory, data="Different summary")


async def test_outbox_migration_retains_switch_rows(mig_url: str) -> None:
    engine = create_async_engine(mig_url)
    try:
        async with engine.begin() as connection:
            await connection.run_sync(lambda c: _run_migrations(c, "head"))
            await connection.run_sync(lambda c: _run_migrations(c, "514c2211a4bebe57"))
            await connection.execute(
                text(
                    "INSERT INTO jira_worker_outbox (id, channel, command, issue_key, status) VALUES ('legacy', 'switch', 'sync_ticket_room', 'KAN-1', 'pending')"
                )
            )
            await connection.run_sync(lambda c: _run_migrations(c, "head"))
            row = (
                await connection.execute(
                    text(
                        "SELECT instance, command_key, attempts, status FROM jira_worker_outbox WHERE id = 'legacy'"
                    )
                )
            ).one()
            assert row == ("", None, 0, "pending")
            await connection.run_sync(lambda c: _run_migrations(c, "514c2211a4bebe57"))
            assert (
                await connection.execute(
                    text("SELECT channel FROM jira_worker_outbox WHERE id = 'legacy'")
                )
            ).scalar_one() == "switch"
    finally:
        await engine.dispose()


async def test_pre_send_server_error_retries_with_backoff(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    jira = JiraMock()
    jira.pre_send_server_errors = 1
    await enqueue(session_factory)
    now = datetime.now(UTC) + timedelta(seconds=1)
    sender = JiraOutboxSender(
        session_factory=session_factory,
        config=_config(
            jira_worker_enabled_projects={"acme": ["KAN"]},
            jira_worker_write_backoff_seconds=5,
        ),
        clients={"acme": client_for(jira)},
        clock=lambda: now,
    )
    await sender.run_once()
    row = (await rows(session_factory))[0]
    assert (
        row.attempts == 1
        and row.status == "pending"
        and row.due_at == now + timedelta(seconds=5)
    )
    assert not jira.writes
    async with session_factory() as session:
        ticket = await session.scalar(select(JiraWorkerTicket))
        assert ticket is not None and ticket.worker_parked_reason is None
    now += timedelta(seconds=5)
    await sender.run_once()
    done = (await rows(session_factory))[0]
    assert done.status == "done" and done.attempts == 2
    assert len(jira.writes) == 1 and len(jira.comments) == 1
    assert len(await rows(session_factory)) == 1


async def test_retry_after_read_failure_sends_once(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    jira = JiraMock()
    jira.read_failure = True
    await enqueue(session_factory)
    now = datetime.now(UTC) + timedelta(seconds=1)
    sender = JiraOutboxSender(
        session_factory=session_factory,
        config=_config(jira_worker_enabled_projects={"acme": ["KAN"]}),
        clients={"acme": client_for(jira)},
        clock=lambda: now,
    )
    await sender.run_once()
    assert not jira.writes
    assert (await rows(session_factory))[0].status == "pending"
    jira.read_failure = False
    now += timedelta(seconds=100)
    await sender.run_once()
    assert len(jira.writes) == 1
    assert (await rows(session_factory))[0].attempts == 2


async def test_concurrent_enqueue_deduplicates_key(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await enqueue(session_factory)
    ids = await asyncio.gather(
        enqueue(session_factory, key="second"), enqueue(session_factory, key="second")
    )
    assert ids[0] == ids[1]
    assert len(await rows(session_factory)) == 2


async def test_own_queued_writes_share_baseline(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    jira = JiraMock()
    await enqueue(session_factory)
    await enqueue(
        session_factory, key="progress", command="transition", data="In Progress"
    )
    await sender_for(session_factory, jira).run_once()
    assert len(jira.writes) == 2 and jira.status == "In Progress"
    assert all(row.status == "done" for row in await rows(session_factory))


@pytest.mark.parametrize(
    "scenario", ["missing_baseline", "unattributed", "edited_comment", "regressed"]
)
async def test_unverifiable_changes_park(scenario: str) -> None:
    jira = JiraMock()
    payload = {"based_updated": BASE, "based_changelog_id": ""}
    jira.change("worker-account")
    if scenario == "missing_baseline":
        payload["based_changelog_id"] = "unknown"
    elif scenario == "unattributed":
        jira.updated = (
            datetime.fromisoformat(jira.updated) + timedelta(seconds=1)
        ).isoformat()
    elif scenario == "edited_comment":
        jira.comments.append(
            {
                "created": BASE,
                "updated": jira.updated,
                "author": {"accountId": "worker-account"},
                "updateAuthor": {"accountId": "human-account"},
            }
        )
    else:
        payload["based_updated"] = "2026-10-02T00:00:00+00:00"
    version = await read_jira_version(client_for(jira), "KAN-1")
    with pytest.raises(HumanChange):
        check_jira_version(version, payload, "worker-account")


def test_invalid_config_fails_before_sending() -> None:
    with pytest.raises(ValueError):
        JiraWorkerStatuses(blocked=" ")
    with pytest.raises(ValueError):
        _config(jira_worker_write_backoff_seconds=-1)
