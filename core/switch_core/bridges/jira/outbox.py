"""Worker-only Jira writes with durable claims and compare-read-write checks.

Jira has no atomic idempotency key or compare-and-set API. A committed
``sending`` row survives a crash; an unknown outcome is visible and is never
replayed automatically. The per-ticket session lock spans database commits
and the HTTP request. It also serializes commands from overlapping schedulers.
A Jira action can still race between the final read and the write.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import quote

import httpx
from sqlalchemy import and_, or_, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from switch_core.config import SwitchConfig
from switch_core.db.models import JiraWorkerOutbox, JiraWorkerTicket

if TYPE_CHECKING:
    from switch_core.bridges.jira.worker import ClockFn, JiraPollClient

logger = logging.getLogger(__name__)
_ACTIVE = ("pending", "claimed", "sending")


def _stamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Jira updated stamp must include a timezone")
    return parsed


def _validate_command(command: str, data: Any) -> None:
    if command == "comment" and isinstance(data, str) and data.strip():
        return
    if command == "transition" and isinstance(data, str) and data.strip():
        return
    if (
        command == "fields"
        and isinstance(data, dict)
        and data
        and "comment" not in data
    ):
        return
    raise ValueError(
        "Expected a comment, target status name, or nonempty fields object"
    )


async def enqueue_jira_command(
    session: AsyncSession,
    *,
    config: SwitchConfig,
    ticket: JiraWorkerTicket,
    command_key: str,
    command: str,
    data: Any,
    based_updated: str,
    based_changelog_id: str,
) -> str | None:
    """Enqueue once per instance/key, in the caller's transaction."""
    if ticket.project_key not in config.jira_worker_enabled_projects.get(
        ticket.instance, []
    ):
        return None
    if not command_key.strip():
        raise ValueError("Jira command key is required")
    _validate_command(command, data)
    _stamp(based_updated)
    payload = {
        "data": data,
        "based_updated": based_updated,
        "based_changelog_id": based_changelog_id,
    }
    result = await session.execute(
        insert(JiraWorkerOutbox)
        .values(
            id=str(uuid.uuid4()),
            channel="jira",
            instance=ticket.instance,
            issue_key=ticket.issue_key,
            command_key=command_key,
            command=command,
            payload=payload,
        )
        .on_conflict_do_nothing(index_elements=["instance", "command_key"])
        .returning(JiraWorkerOutbox.id)
    )
    command_id = result.scalar_one_or_none()
    if command_id is not None:
        return command_id
    existing = (
        await session.execute(
            select(JiraWorkerOutbox).where(
                JiraWorkerOutbox.instance == ticket.instance,
                JiraWorkerOutbox.command_key == command_key,
            )
        )
    ).scalar_one()
    if (
        existing.issue_key != ticket.issue_key
        or existing.command != command
        or existing.payload != payload
    ):
        raise ValueError("Jira command key already identifies a different command")
    return existing.id


async def _pages(client: JiraPollClient, path: str, key: str) -> list[dict[str, Any]]:
    values: list[dict[str, Any]] = []
    start = 0
    while True:
        response = await client.request(
            "GET", path, params={"startAt": start, "maxResults": 100}
        )
        body = response.json()
        page = body[key]
        if not isinstance(page, list) or any(
            not isinstance(item, dict) for item in page
        ):
            raise ValueError("Jira returned an invalid page")
        values.extend(page)
        start += len(page)
        if start >= body["total"]:
            return values
        if not page:
            raise ValueError("Jira pagination stopped before total")


async def read_jira_version(client: JiraPollClient, issue_key: str) -> dict[str, Any]:
    """Read all history and comments, not the truncated issue expand."""
    path = f"/rest/api/3/issue/{quote(issue_key, safe='')}"
    before = (await client.request("GET", path, params={"fields": "updated"})).json()[
        "fields"
    ]["updated"]
    histories = await _pages(client, path + "/changelog", "values")
    comments = await _pages(client, path + "/comment", "comments")
    updated = (await client.request("GET", path, params={"fields": "updated"})).json()[
        "fields"
    ]["updated"]
    if _stamp(before) != _stamp(updated):
        raise HumanChange("Issue changed while the version check read its history")
    histories.sort(key=lambda item: (_stamp(item["created"]), int(item["id"])))
    return {
        "updated": updated,
        "changelog_id": str(histories[-1]["id"]) if histories else "",
        "histories": histories,
        "comments": comments,
    }


class HumanChange(Exception):
    """A non-worker or unattributed change must park the ticket."""


def check_jira_version(
    version: dict[str, Any], payload: dict[str, Any], account_id: str
) -> None:
    base = _stamp(payload["based_updated"])
    current = _stamp(version["updated"])
    base_id = payload["based_changelog_id"]
    if current == base and version["changelog_id"] == base_id:
        return
    if current < base:
        raise HumanChange("Jira version is older than the command baseline")
    histories = version["histories"]
    if base_id:
        index = next(
            (i for i, entry in enumerate(histories) if str(entry["id"]) == base_id),
            None,
        )
        if index is None:
            raise HumanChange("The command's changelog baseline is missing")
        histories = histories[index + 1 :]
    changes: list[tuple[datetime, str]] = [
        (_stamp(entry["created"]), entry.get("author", {}).get("accountId", ""))
        for entry in histories
    ]
    for comment in version["comments"]:
        created = _stamp(comment["created"])
        edited = _stamp(comment["updated"])
        if created > base:
            changes.append((created, comment.get("author", {}).get("accountId", "")))
        if edited > base:
            changes.append(
                (edited, comment.get("updateAuthor", {}).get("accountId", ""))
            )
    if not changes or any(author != account_id for _, author in changes):
        raise HumanChange("A non-worker Jira action superseded the command")
    if max(stamp for stamp, _ in changes) != current:
        raise HumanChange("Jira updated stamp has no attributable worker change")


def _failure_reason(command_id: str) -> str:
    return f"Jira command failed: {command_id}"


class JiraOutboxSender:
    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        config: SwitchConfig,
        clients: dict[str, JiraPollClient],
        clock: ClockFn,
    ) -> None:
        self._factory = session_factory
        self._engine = cast(AsyncEngine, session_factory.kw["bind"])
        self._config = config
        self._clients = clients
        self._clock = clock
        self._accounts: dict[str, str] = {}

    async def run_once(self) -> None:
        scopes = [
            and_(
                JiraWorkerTicket.instance == instance,
                JiraWorkerTicket.project_key.in_(projects),
            )
            for instance, projects in self._config.jira_worker_enabled_projects.items()
            if projects and instance in self._clients
        ]
        if not scopes:
            return
        async with self._factory() as session:
            commands = list(
                (
                    await session.scalars(
                        select(JiraWorkerOutbox.id)
                        .join(
                            JiraWorkerTicket,
                            and_(
                                JiraWorkerTicket.instance == JiraWorkerOutbox.instance,
                                JiraWorkerTicket.issue_key
                                == JiraWorkerOutbox.issue_key,
                            ),
                        )
                        .where(
                            JiraWorkerOutbox.channel == "jira",
                            JiraWorkerOutbox.status.in_(_ACTIVE),
                            JiraWorkerOutbox.due_at <= self._clock(),
                            or_(*scopes),
                        )
                        .order_by(JiraWorkerOutbox.created_at, JiraWorkerOutbox.id)
                        .limit(20)
                    )
                ).all()
            )
        for command_id in commands:
            try:
                await self._send(command_id)
            except Exception:
                # Database failures leave claimed/sending rows for restart recovery.
                logger.exception(
                    "Jira outbox command %s could not complete", command_id
                )

    async def _send(self, command_id: str) -> None:
        async with self._factory() as session:
            row = await session.get(JiraWorkerOutbox, command_id)
            if row is None:
                return
            lock_key = f"jira-outbox:{row.instance}:{row.issue_key}"
        async with self._engine.connect() as connection:
            try:
                locked = await connection.scalar(
                    text("SELECT pg_try_advisory_lock(hashtextextended(:key, 0))"),
                    {"key": lock_key},
                )
            except BaseException:
                await connection.invalidate()
                raise
            if not locked:
                return
            try:
                await connection.commit()
                async with self._factory(bind=connection) as session:
                    await self._send_locked(session, command_id)
            finally:
                try:
                    await connection.rollback()
                    await connection.execute(
                        text("SELECT pg_advisory_unlock(hashtextextended(:key, 0))"),
                        {"key": lock_key},
                    )
                    await connection.commit()
                except BaseException:
                    await connection.invalidate()
                    raise

    async def _send_locked(self, session: AsyncSession, command_id: str) -> None:
        row = await session.get(JiraWorkerOutbox, command_id, with_for_update=True)
        if row is None or row.status not in _ACTIVE or row.due_at > self._clock():
            return
        ticket = (
            await session.scalars(
                select(JiraWorkerTicket)
                .where(
                    JiraWorkerTicket.instance == row.instance,
                    JiraWorkerTicket.issue_key == row.issue_key,
                )
                .with_for_update()
            )
        ).one()
        if ticket.project_key not in self._config.jira_worker_enabled_projects.get(
            ticket.instance, []
        ):
            return
        if row.status == "sending":
            row.status = "uncertain"
            row.error = "Restart found a write with an unknown outcome; automatic replay is unsafe"
            ticket.worker_parked_reason = row.error
            await session.commit()
            return
        payload = row.payload or {}
        if (
            ticket.worker_parked_reason
            and ticket.worker_parked_reason
            != _failure_reason(payload.get("failure_of", ""))
        ):
            row.status = "parked"
            row.error = ticket.worker_parked_reason
            await session.commit()
            return
        # A later command cannot overtake a retry for the same ticket.
        earlier = await session.scalar(
            select(JiraWorkerOutbox.id)
            .where(
                JiraWorkerOutbox.instance == row.instance,
                JiraWorkerOutbox.issue_key == row.issue_key,
                JiraWorkerOutbox.channel == "jira",
                JiraWorkerOutbox.status.in_(_ACTIVE),
                or_(
                    JiraWorkerOutbox.created_at < row.created_at,
                    and_(
                        JiraWorkerOutbox.created_at == row.created_at,
                        JiraWorkerOutbox.id < row.id,
                    ),
                ),
            )
            .limit(1)
        )
        if earlier:
            return
        if row.status == "pending":
            row.attempts += 1
        row.status = "claimed"
        await session.commit()
        client = self._clients[row.instance]
        sending = False
        try:
            _validate_command(row.command, payload["data"])
            account_id = self._accounts.get(row.instance)
            if account_id is None:
                account_id = (await client.request("GET", "/rest/api/3/myself")).json()[
                    "accountId"
                ]
                if not isinstance(account_id, str) or not account_id:
                    raise ValueError("Jira worker accountId is required")
                self._accounts[row.instance] = account_id
            path = f"/rest/api/3/issue/{quote(row.issue_key, safe='')}"
            method = "POST"
            if row.command == "transition":
                available = (await client.request("GET", path + "/transitions")).json()[
                    "transitions"
                ]
                matches = [
                    item["id"]
                    for item in available
                    if item.get("to", {}).get("name") == payload["data"]
                ]
                if len(matches) != 1:
                    raise ValueError(
                        f"Expected one available transition to {payload['data']}"
                    )
                body = {"transition": {"id": matches[0]}}
                path += "/transitions"
            elif row.command == "comment":
                body = {
                    "body": {
                        "type": "doc",
                        "version": 1,
                        "content": [
                            {
                                "type": "paragraph",
                                "content": [{"type": "text", "text": payload["data"]}],
                            }
                        ],
                    }
                }
                path += "/comment"
            else:
                method = "PUT"
                body = {"fields": payload["data"]}
            # Resolve transitions first. The version check is the last Jira read.
            version = await read_jira_version(client, row.issue_key)
            check_jira_version(version, payload, account_id)
            row.status = "sending"
            await session.commit()
            sending = True
            await client.request(method, path, json=body)
        except HumanChange as exc:
            row.status = "parked"
            row.error = str(exc)
            ticket.worker_parked_reason = str(exc)
        except Exception as exc:
            if sending and isinstance(exc, httpx.TransportError):
                row.status = "uncertain"
                row.error = f"{type(exc).__name__}: Jira write outcome is unknown; automatic replay is unsafe"
                ticket.worker_parked_reason = row.error
            else:
                transient = isinstance(exc, httpx.TransportError) or (
                    isinstance(exc, httpx.HTTPStatusError)
                    and (
                        exc.response.status_code == 429
                        or exc.response.status_code >= 500
                    )
                )
                row.error = f"{type(exc).__name__}: " + (
                    f"HTTP {exc.response.status_code}"
                    if isinstance(exc, httpx.HTTPStatusError)
                    else str(exc)
                    if isinstance(exc, ValueError)
                    else "Jira command could not complete"
                )
                if transient and row.attempts < 3:
                    row.status = "pending"
                    row.due_at = self._clock() + timedelta(
                        seconds=self._config.jira_worker_write_backoff_seconds
                        * 2 ** (row.attempts - 1)
                    )
                else:
                    row.status = "failed"
                    if not payload.get("failure_of"):
                        ticket.worker_parked_reason = _failure_reason(row.id)
                    if (
                        not payload.get("failure_of")
                        and payload.get("based_updated")
                        and "based_changelog_id" in payload
                    ):
                        await self._enqueue_failure(session, ticket, row)
        else:
            row.status = "done"
            row.error = None
        await session.commit()

    async def _enqueue_failure(
        self, session: AsyncSession, ticket: JiraWorkerTicket, row: JiraWorkerOutbox
    ) -> None:
        payload = row.payload or {}
        # These are ordinary durable commands. They use the original baseline,
        # so a human action during retries also prevents the BLOCKED writes.
        for command, data in (
            ("transition", self._config.jira_worker_statuses.blocked),
            (
                "comment",
                f"Jira worker stopped after {row.attempts} failed attempt(s) to {row.command}. {row.error}. Command {row.command_key}.",
            ),
        ):
            command_id = await enqueue_jira_command(
                session,
                config=self._config,
                ticket=ticket,
                command_key=f"failure:{row.id}:{command}",
                command=command,
                data=data,
                based_updated=payload["based_updated"],
                based_changelog_id=payload["based_changelog_id"],
            )
            child = await session.get(JiraWorkerOutbox, command_id)
            assert child is not None
            child.payload = {**(child.payload or {}), "failure_of": row.id}
