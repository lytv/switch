"""Jira ticket worker: intake, identity mapping, and ticket-room scheduling.

One module inside the Jira bridge (option C). It records webhook and poll
events into the worker tables, resolves reporter identity, schedules ticket
room reconciliation, and runs the worker scheduler. Jira writes, the intake
orchestrator, and the outbox sender are later steps.

With no project enabled (the default) the intake hook returns immediately and
the existing Jira bridge behaves exactly as today.
"""

from __future__ import annotations

import asyncio
import logging
import re
import uuid
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import httpx
from sqlalchemy import and_, func, or_, select, true, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from switch_core.bridges.trigger_source import NormalizedTriggerEvent
from switch_core.config import SwitchConfig
from switch_core.db.models import (
    JiraWorkerEvent,
    JiraWorkerIdentity,
    JiraWorkerJob,
    JiraWorkerOutbox,
    JiraWorkerTicket,
)

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from switch_core.bridges.jira.rooms import CardUpdater, TicketRooms

POLL_JOB_KIND = "watermark_poll"
SWITCH_WAIT_CHANNEL = "switch"
JIRA_COMMENTS_WAIT_CHANNEL = "jira_comments"
SYNC_TICKET_ROOM_COMMAND = "sync_ticket_room"

_TERMINAL_TICKET_STATUSES = ("done", "cancelled", "canceled")


def is_terminal_ticket_status(status: str) -> bool:
    """Whether a Jira status ends the v1 ticket flow (Done, or cancelled)."""
    return status.strip().casefold() in _TERMINAL_TICKET_STATUSES


SleepFn = Callable[[float], Awaitable[None]]
ClockFn = Callable[[], datetime]

_PENDING_OR_CLAIMED = ("pending", "claimed")


def is_worker_enabled(config: SwitchConfig, instance: str, project_key: str) -> bool:
    """Per-Jira-project on/off switch. Off by default (empty list)."""
    return project_key in config.jira_worker_enabled_projects.get(instance, [])


def webhook_idempotency_key(payload: dict[str, Any], issue_key: str) -> str:
    """Key from raw webhookEvent + issue id/key + timestamp + changelog.id."""
    issue = payload.get("issue")
    issue_id = ""
    if isinstance(issue, dict):
        issue_id = str(issue.get("id") or "")
    changelog_id = ""
    changelog = payload.get("changelog")
    if isinstance(changelog, dict) and changelog.get("id") is not None:
        changelog_id = str(changelog.get("id"))
    parts = [
        str(payload.get("webhookEvent") or payload.get("event") or ""),
        issue_id,
        issue_key,
        str(payload.get("timestamp") or ""),
        changelog_id,
    ]
    return "|".join(parts)


def poll_idempotency_key(issue_key: str, updated: str) -> str:
    return f"poll|{issue_key}|{updated}"


def jira_reporter_account_id(payload: dict[str, Any]) -> str:
    """Return only Jira's immutable reporter account identifier."""
    issue = payload.get("issue")
    fields = issue.get("fields") if isinstance(issue, dict) else None
    reporter = fields.get("reporter") if isinstance(fields, dict) else None
    account_id = reporter.get("accountId") if isinstance(reporter, dict) else None
    return str(account_id).strip() if account_id is not None else ""


async def lookup_identity(
    session: AsyncSession, *, instance: str, jira_account_id: str
) -> str | None:
    """Resolve a manual Jira account mapping without name or email matching."""
    if not jira_account_id:
        return None
    result = await session.execute(
        select(JiraWorkerIdentity.switch_user_id).where(
            JiraWorkerIdentity.instance == instance,
            JiraWorkerIdentity.jira_account_id == jira_account_id,
        )
    )
    return result.scalar_one_or_none()


async def list_identity_mappings(
    session: AsyncSession, *, instance: str
) -> list[JiraWorkerIdentity]:
    result = await session.execute(
        select(JiraWorkerIdentity)
        .where(JiraWorkerIdentity.instance == instance)
        .order_by(JiraWorkerIdentity.jira_account_id)
    )
    return list(result.scalars().all())


async def _enqueue_switch_follow_up(
    session: AsyncSession,
    *,
    command: str,
    ticket: JiraWorkerTicket,
    switch_user_id: str | None,
) -> None:
    session.add(
        JiraWorkerOutbox(
            channel="switch",
            command=command,
            issue_key=ticket.issue_key,
            payload={
                "instance": ticket.instance,
                "project_key": ticket.project_key,
                "ticket_id": ticket.id,
                "switch_user_id": switch_user_id,
            },
        )
    )
    await session.flush()


async def _enqueue_room_sync(session: AsyncSession, ticket: JiraWorkerTicket) -> None:
    """Leave a pending row until room reconcile finishes."""
    await _enqueue_switch_follow_up(
        session,
        command=SYNC_TICKET_ROOM_COMMAND,
        ticket=ticket,
        switch_user_id=ticket.reporter_switch_user_id,
    )


async def _enqueue_room_sync_for_reporter(
    session: AsyncSession,
    *,
    instance: str,
    jira_account_id: str,
    enabled_projects: Sequence[str],
) -> None:
    """Queue reconcile for every ticket of this reporter in enabled projects.

    Includes Done and cancelled tickets. Step 2 does not rewrite their
    reporter column, but the room owner still has to follow the mapping.
    A project that is off is left unchanged.
    """
    if not enabled_projects:
        return
    result = await session.execute(
        select(JiraWorkerTicket).where(
            JiraWorkerTicket.instance == instance,
            JiraWorkerTicket.project_key.in_(enabled_projects),
            JiraWorkerTicket.reporter_account_id == jira_account_id,
        )
    )
    for ticket in result.scalars().all():
        await _enqueue_room_sync(session, ticket)


async def _set_open_ticket_reporter_resolution(
    session: AsyncSession,
    *,
    instance: str,
    jira_account_id: str,
    switch_user_id: str | None,
    enabled_projects: Sequence[str],
) -> int:
    if not enabled_projects:
        return 0
    result = await session.execute(
        select(JiraWorkerTicket)
        .where(
            JiraWorkerTicket.instance == instance,
            JiraWorkerTicket.project_key.in_(enabled_projects),
            JiraWorkerTicket.reporter_account_id == jira_account_id,
            func.lower(JiraWorkerTicket.status).not_in(_TERMINAL_TICKET_STATUSES),
        )
        .with_for_update()
    )
    tickets = list(result.scalars().all())
    wait_channel = SWITCH_WAIT_CHANNEL if switch_user_id else JIRA_COMMENTS_WAIT_CHANNEL
    changed = 0
    for ticket in tickets:
        if (
            ticket.reporter_switch_user_id == switch_user_id
            and ticket.wait_channel == wait_channel
        ):
            continue
        ticket.reporter_switch_user_id = switch_user_id
        ticket.wait_channel = wait_channel
        if switch_user_id:
            await _enqueue_switch_follow_up(
                session,
                command="invite_ticket_reporter",
                ticket=ticket,
                switch_user_id=switch_user_id,
            )
        await _enqueue_switch_follow_up(
            session,
            command="upsert_ticket_admin_card",
            ticket=ticket,
            switch_user_id=switch_user_id,
        )
        changed += 1
    await session.flush()
    return changed


async def set_identity_mapping(
    session: AsyncSession,
    *,
    instance: str,
    jira_account_id: str,
    switch_user_id: str,
    enabled_projects: Sequence[str],
) -> tuple[JiraWorkerIdentity, int]:
    """Create or update a manual mapping and wake matching open tickets."""
    result = await session.execute(
        select(JiraWorkerIdentity)
        .where(
            JiraWorkerIdentity.instance == instance,
            JiraWorkerIdentity.jira_account_id == jira_account_id,
        )
        .with_for_update()
    )
    mapping = result.scalar_one_or_none()
    if mapping is None:
        mapping = JiraWorkerIdentity(
            instance=instance,
            jira_account_id=jira_account_id,
            switch_user_id=switch_user_id,
        )
        session.add(mapping)
    else:
        mapping.switch_user_id = switch_user_id
    await session.flush()
    updated_tickets = await _set_open_ticket_reporter_resolution(
        session,
        instance=instance,
        jira_account_id=jira_account_id,
        switch_user_id=switch_user_id,
        enabled_projects=enabled_projects,
    )
    await _enqueue_room_sync_for_reporter(
        session,
        instance=instance,
        jira_account_id=jira_account_id,
        enabled_projects=enabled_projects,
    )
    return mapping, updated_tickets


async def remove_identity_mapping(
    session: AsyncSession,
    *,
    instance: str,
    jira_account_id: str,
    enabled_projects: Sequence[str],
) -> int:
    """Remove a mapping and return matching open tickets to Jira comments."""
    result = await session.execute(
        select(JiraWorkerIdentity)
        .where(
            JiraWorkerIdentity.instance == instance,
            JiraWorkerIdentity.jira_account_id == jira_account_id,
        )
        .with_for_update()
    )
    mapping = result.scalar_one_or_none()
    if mapping is None:
        raise ValueError("Jira worker identity mapping not found")
    await session.delete(mapping)
    await session.flush()
    updated = await _set_open_ticket_reporter_resolution(
        session,
        instance=instance,
        jira_account_id=jira_account_id,
        switch_user_id=None,
        enabled_projects=enabled_projects,
    )
    await _enqueue_room_sync_for_reporter(
        session,
        instance=instance,
        jira_account_id=jira_account_id,
        enabled_projects=enabled_projects,
    )
    return updated


def jira_updated_at(payload: dict[str, Any]) -> datetime:
    issue = payload.get("issue")
    fields = issue.get("fields") if isinstance(issue, dict) else None
    updated = fields.get("updated") if isinstance(fields, dict) else None
    if isinstance(updated, str):
        text = updated.strip()
        if text[-1:] in ("Z", "z"):
            text = text[:-1] + "+00:00"
        elif re.search(r"[+-]\d{4}$", text):
            text = text[:-2] + ":" + text[-2:]
        try:
            parsed = datetime.fromisoformat(text)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=UTC)
            return parsed
        except ValueError:
            pass
    timestamp = payload.get("timestamp")
    if isinstance(timestamp, (int, float)):
        return datetime.fromtimestamp(timestamp / 1000, UTC)
    return datetime.now(UTC)


async def record_event(
    session: AsyncSession,
    *,
    instance: str,
    idempotency_key: str,
    issue_key: str,
    project_key: str,
    event_kind: str,
    webhook_event: str,
    payload: dict[str, Any] | None,
) -> str | None:
    """Insert an event row; return its id, or None when the key already exists."""
    stmt = (
        insert(JiraWorkerEvent)
        .values(
            id=str(uuid.uuid4()),
            instance=instance,
            idempotency_key=idempotency_key,
            issue_key=issue_key,
            project_key=project_key,
            event_kind=event_kind,
            webhook_event=webhook_event,
            payload=payload,
        )
        .on_conflict_do_nothing(index_elements=["instance", "idempotency_key"])
        .returning(JiraWorkerEvent.id)
    )
    result = await session.execute(stmt)
    await session.flush()
    return result.scalar_one_or_none()


async def upsert_ticket(
    session: AsyncSession,
    *,
    instance: str,
    issue_key: str,
    reporter_account_id: str,
    reporter_switch_user_id: str | None,
    wait_channel: str,
    updated_at: datetime,
    issue_id: str = "",
    project_key: str = "",
    summary: str = "",
    status: str = "",
) -> tuple[JiraWorkerTicket, bool, bool]:
    stmt = insert(JiraWorkerTicket).values(
        id=str(uuid.uuid4()),
        instance=instance,
        issue_key=issue_key,
        issue_id=issue_id,
        project_key=project_key,
        summary=summary,
        status=status,
        reporter_account_id=reporter_account_id,
        reporter_switch_user_id=reporter_switch_user_id,
        wait_channel=wait_channel,
        last_event_at=updated_at,
    )
    result = await session.execute(
        stmt.on_conflict_do_nothing(index_elements=["instance", "issue_key"]).returning(
            JiraWorkerTicket
        )
    )
    ticket = result.scalar_one_or_none()
    if ticket is not None:
        await session.flush()
        await _enqueue_room_sync(session, ticket)
        return ticket, True, False

    ticket = await session.scalar(
        select(JiraWorkerTicket)
        .where(
            JiraWorkerTicket.instance == instance,
            JiraWorkerTicket.issue_key == issue_key,
        )
        .with_for_update()
    )
    assert ticket is not None
    if ticket.last_event_at > updated_at:
        return ticket, False, False

    if not reporter_account_id and ticket.reporter_account_id:
        reporter_account_id = ticket.reporter_account_id
        reporter_switch_user_id = ticket.reporter_switch_user_id
        wait_channel = ticket.wait_channel
    reporter_resolution_changed = (
        ticket.reporter_account_id != reporter_account_id
        or ticket.reporter_switch_user_id != reporter_switch_user_id
        or ticket.wait_channel != wait_channel
    )
    ticket.issue_id = issue_id
    ticket.project_key = project_key
    ticket.summary = summary
    ticket.status = status
    ticket.reporter_account_id = reporter_account_id
    ticket.reporter_switch_user_id = reporter_switch_user_id
    ticket.wait_channel = wait_channel
    ticket.last_event_at = updated_at
    await session.flush()
    await _enqueue_room_sync(session, ticket)
    return ticket, False, reporter_resolution_changed


async def _enqueue_ticket_intake_follow_ups(
    session: AsyncSession,
    *,
    ticket: JiraWorkerTicket,
    created: bool,
    reporter_resolution_changed: bool,
    switch_user_id: str | None,
) -> None:
    if ticket.status.lower() in _TERMINAL_TICKET_STATUSES:
        return
    if created:
        if switch_user_id:
            await _enqueue_switch_follow_up(
                session,
                command="invite_ticket_reporter",
                ticket=ticket,
                switch_user_id=switch_user_id,
            )
        else:
            await _enqueue_switch_follow_up(
                session,
                command="upsert_ticket_admin_card",
                ticket=ticket,
                switch_user_id=None,
            )
        return
    if reporter_resolution_changed:
        if switch_user_id:
            await _enqueue_switch_follow_up(
                session,
                command="invite_ticket_reporter",
                ticket=ticket,
                switch_user_id=switch_user_id,
            )
        await _enqueue_switch_follow_up(
            session,
            command="upsert_ticket_admin_card",
            ticket=ticket,
            switch_user_id=switch_user_id,
        )


async def record_webhook_event(
    session: AsyncSession,
    event: NormalizedTriggerEvent,
    *,
    instance: str,
    enabled_projects: Sequence[str],
    webhook_identifier: str | None,
) -> bool:
    """Record one parsed webhook event. Duplicate deliveries are no-ops.

    Returns True when the event was new, False for duplicates or when the
    event's project is not enabled.
    """
    if event.project not in enabled_projects:
        return False
    raw = event.raw if isinstance(event.raw, dict) else {}
    key = webhook_identifier or webhook_idempotency_key(raw, event.key)
    inserted = await record_event(
        session,
        instance=instance,
        idempotency_key=key,
        issue_key=event.key,
        project_key=event.project,
        event_kind=event.event_kind,
        webhook_event=event.webhook_event,
        payload=raw or None,
    )
    if inserted is None:
        return False
    reporter_account_id = jira_reporter_account_id(raw)
    reporter_switch_user_id = await lookup_identity(
        session,
        instance=instance,
        jira_account_id=reporter_account_id,
    )
    wait_channel = (
        SWITCH_WAIT_CHANNEL if reporter_switch_user_id else JIRA_COMMENTS_WAIT_CHANNEL
    )
    issue_id = ""
    if isinstance(raw.get("issue"), dict):
        issue_id = str(raw["issue"].get("id") or "")
    ticket, created, reporter_resolution_changed = await upsert_ticket(
        session,
        instance=instance,
        issue_key=event.key,
        issue_id=issue_id,
        project_key=event.project,
        summary=event.summary,
        status=event.status,
        reporter_account_id=reporter_account_id,
        reporter_switch_user_id=reporter_switch_user_id,
        wait_channel=wait_channel,
        updated_at=jira_updated_at(raw),
    )
    await _enqueue_ticket_intake_follow_ups(
        session,
        ticket=ticket,
        created=created,
        reporter_resolution_changed=reporter_resolution_changed,
        switch_user_id=reporter_switch_user_id,
    )
    return True


async def record_polled_issue(
    session: AsyncSession,
    issue: dict[str, Any],
    *,
    instance: str,
) -> bool:
    """Record one Jira search hit through the same event-log path as webhooks."""
    fields = issue.get("fields") if isinstance(issue.get("fields"), dict) else {}
    if not isinstance(fields, dict):
        fields = {}
    issue_key = str(issue.get("key") or "")
    if not issue_key:
        return False
    project = fields.get("project")
    project_key = (
        str(project.get("key"))
        if isinstance(project, dict) and project.get("key")
        else ""
    )
    status = fields.get("status")
    status_name = (
        str(status.get("name"))
        if isinstance(status, dict) and status.get("name")
        else ""
    )
    updated = str(fields.get("updated") or "")
    inserted = await record_event(
        session,
        instance=instance,
        idempotency_key=poll_idempotency_key(issue_key, updated),
        issue_key=issue_key,
        project_key=project_key,
        event_kind="updated",
        webhook_event="poll:issue_updated",
        payload=issue,
    )
    if inserted is None:
        return False
    reporter = fields.get("reporter")
    reporter_account_id = (
        str(reporter.get("accountId")).strip()
        if isinstance(reporter, dict) and reporter.get("accountId") is not None
        else ""
    )
    reporter_switch_user_id = await lookup_identity(
        session,
        instance=instance,
        jira_account_id=reporter_account_id,
    )
    wait_channel = (
        SWITCH_WAIT_CHANNEL if reporter_switch_user_id else JIRA_COMMENTS_WAIT_CHANNEL
    )
    ticket, created, reporter_resolution_changed = await upsert_ticket(
        session,
        instance=instance,
        issue_key=issue_key,
        issue_id=str(issue.get("id") or ""),
        project_key=project_key,
        summary=str(fields.get("summary") or ""),
        status=status_name,
        reporter_account_id=reporter_account_id,
        reporter_switch_user_id=reporter_switch_user_id,
        wait_channel=wait_channel,
        updated_at=jira_updated_at({"issue": issue}),
    )
    await _enqueue_ticket_intake_follow_ups(
        session,
        ticket=ticket,
        created=created,
        reporter_resolution_changed=reporter_resolution_changed,
        switch_user_id=reporter_switch_user_id,
    )
    return True


async def ensure_poll_job(
    session: AsyncSession,
    *,
    instance: str,
    project_key: str,
    interval_seconds: int,
    now: datetime | None = None,
) -> None:
    """Seed one pending watermark-poll job per project when none is unfinished."""
    clock = now or datetime.now(UTC)
    existing = await session.execute(
        select(JiraWorkerJob.id)
        .where(
            JiraWorkerJob.kind == POLL_JOB_KIND,
            JiraWorkerJob.instance == instance,
            JiraWorkerJob.project_key == project_key,
            JiraWorkerJob.status.in_(_PENDING_OR_CLAIMED),
        )
        .limit(1)
    )
    if existing.scalar_one_or_none() is not None:
        return
    session.add(
        JiraWorkerJob(
            kind=POLL_JOB_KIND,
            instance=instance,
            project_key=project_key,
            due_at=clock,
            status="pending",
            payload={"watermark": None},
        )
    )
    await session.flush()


async def schedule_next_poll(
    session: AsyncSession,
    *,
    instance: str,
    project_key: str,
    watermark: str | None,
    interval_seconds: int,
    predecessor_id: str,
    now: datetime | None = None,
) -> None:
    clock = now or datetime.now(UTC)
    due = clock + timedelta(seconds=interval_seconds)
    session.add(
        JiraWorkerJob(
            kind=POLL_JOB_KIND,
            instance=instance,
            project_key=project_key,
            due_at=due,
            status="pending",
            payload={"watermark": watermark, "predecessor_id": predecessor_id},
        )
    )
    await session.flush()


async def claim_due_jobs(
    session: AsyncSession,
    *,
    now: datetime | None = None,
    limit: int = 10,
    owner: str = "scheduler",
    interval_seconds: int,
    scopes: Sequence[tuple[str, str]] | None = None,
) -> Sequence[JiraWorkerJob]:
    """Claim due rows transactionally: pending → claimed in one statement.

    The caller commits. Two concurrent claimers never win the same row: the
    CTE locks candidates with SKIP LOCKED and the UPDATE only touches rows
    still pending.
    """
    if scopes == []:
        return []
    clock = now or datetime.now(UTC)
    scope_filter = (
        or_(
            *(
                and_(
                    JiraWorkerJob.instance == instance,
                    JiraWorkerJob.project_key == project_key,
                )
                for instance, project_key in scopes
            )
        )
        if scopes is not None
        else true()
    )
    due = (
        select(JiraWorkerJob.id)
        .where(
            JiraWorkerJob.status == "pending",
            JiraWorkerJob.due_at <= clock,
            scope_filter,
        )
        .order_by(JiraWorkerJob.due_at.asc())
        .limit(max(1, limit))
        .with_for_update(skip_locked=True)
        .cte("due")
    )
    stmt = (
        update(JiraWorkerJob)
        .where(
            JiraWorkerJob.id == due.c.id,
            JiraWorkerJob.status == "pending",
        )
        .values(
            status="claimed",
            claimed_at=clock,
            claimed_by=owner,
            attempts=JiraWorkerJob.attempts + 1,
        )
        .returning(JiraWorkerJob)
    )
    result = await session.execute(stmt)
    await session.flush()
    jobs = list(result.scalars().all())
    for job in jobs:
        if job.kind != POLL_JOB_KIND:
            continue
        payload = job.payload if isinstance(job.payload, dict) else {}
        watermark = payload.get("watermark")
        await schedule_next_poll(
            session,
            instance=job.instance,
            project_key=job.project_key,
            watermark=watermark if isinstance(watermark, str) else None,
            interval_seconds=interval_seconds,
            predecessor_id=job.id,
            now=clock,
        )
    return jobs


async def update_next_poll_watermark(
    session: AsyncSession,
    *,
    predecessor_id: str,
    watermark: str | None,
) -> None:
    result = await session.execute(
        select(JiraWorkerJob).where(
            JiraWorkerJob.kind == POLL_JOB_KIND,
            JiraWorkerJob.status == "pending",
            JiraWorkerJob.payload["predecessor_id"].astext == predecessor_id,
        )
    )
    job = result.scalar_one()
    job.payload = {"watermark": watermark, "predecessor_id": predecessor_id}
    await session.flush()


async def complete_job(
    session: AsyncSession,
    job_id: str,
    *,
    status: str,
    error: str | None = None,
    now: datetime | None = None,
) -> None:
    clock = now or datetime.now(UTC)
    job = await session.get(JiraWorkerJob, job_id)
    if job is None:
        return
    job.status = status
    job.error = error
    job.completed_at = clock  # type: ignore[assignment]
    await session.flush()


class JiraPollClient:
    """Read-only Jira REST client for the watermark poll (search only)."""

    def __init__(
        self,
        *,
        base_url: str,
        email: str,
        api_token: str,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._auth = (email, api_token)
        self._client = client or httpx.AsyncClient(timeout=30.0)
        self._owns_client = client is None

    async def search_updated(
        self, *, project_key: str, updated_since: str | None
    ) -> list[dict[str, Any]]:
        jql = f'project = "{project_key}"'
        if updated_since:
            jql += f' AND updated >= "{updated_since}"'
        jql += " ORDER BY updated ASC"
        issues: list[dict[str, Any]] = []
        next_page_token: str | None = None
        while True:
            params: dict[str, str | int] = {
                "jql": jql,
                "fields": "summary,status,updated,project,reporter",
                "maxResults": 50,
            }
            if next_page_token is not None:
                params["nextPageToken"] = next_page_token
            response = await self._client.get(
                f"{self._base_url}/rest/api/3/search/jql",
                params=params,
                auth=self._auth,
            )
            response.raise_for_status()
            body = response.json()
            page_issues = body.get("issues", [])
            issues.extend(issue for issue in page_issues if isinstance(issue, dict))
            page_token = body.get("nextPageToken")
            if not isinstance(page_token, str) or not page_token:
                if body.get("isLast") is False:
                    raise ValueError("Jira search response omitted nextPageToken")
                return issues
            next_page_token = page_token

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()


def build_poll_clients(config: SwitchConfig) -> dict[str, JiraPollClient]:
    """Build read-only poll clients for configured Jira instances."""
    return {
        instance: JiraPollClient(
            base_url=credentials.base_url,
            email=credentials.email,
            api_token=credentials.api_token,
        )
        for instance, credentials in config.jira_worker_credentials.items()
    }


class JiraWorkerScheduler:
    """Single-process scheduler loop: claims due rows, runs the watermark poll.

    Startup re-reads overdue rows on the first pass (they are just pending rows
    with ``due_at`` in the past). ``stop()`` cancels the loop cleanly.
    """

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        config: SwitchConfig,
        poll_clients: dict[str, JiraPollClient] | None = None,
        clock: ClockFn | None = None,
        sleep: SleepFn | None = None,
        owner: str = "scheduler",
        ticket_rooms: TicketRooms | None = None,
        card_updater: CardUpdater | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._config = config
        self._poll_clients = poll_clients or {}
        self._clock: ClockFn = clock or (lambda: datetime.now(UTC))
        self._sleep: SleepFn = sleep or asyncio.sleep
        self._owner = owner
        self._ticket_rooms = ticket_rooms
        self._card_updater = card_updater
        self._task: asyncio.Task[None] | None = None

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    def start(self) -> None:
        if self.running:
            return
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        task, self._task = self._task, None
        if task is None:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    async def _run(self) -> None:
        interval = max(1, self._config.jira_worker_poll_interval_seconds)
        while True:
            await self.run_once()
            await self._sleep(interval)

    async def run_once(self) -> None:
        """One scheduler pass: seed poll jobs, claim due rows, handle them."""
        now = self._clock()
        projects = self._config.jira_worker_enabled_projects
        scopes = [
            (instance, project_key)
            for instance, project_keys in projects.items()
            if instance in self._poll_clients
            for project_key in project_keys
        ]
        async with self._session_factory() as session:
            for instance, project_keys in projects.items():
                if instance not in self._poll_clients:
                    for project_key in project_keys:
                        logger.warning(
                            "Jira worker poll for instance %s project %s skipped: no Jira credentials",
                            instance,
                            project_key,
                        )
                    continue
                for project_key in project_keys:
                    await ensure_poll_job(
                        session,
                        instance=instance,
                        project_key=project_key,
                        interval_seconds=self._config.jira_worker_poll_interval_seconds,
                        now=now,
                    )
            jobs = await claim_due_jobs(
                session,
                now=now,
                limit=10,
                owner=self._owner,
                interval_seconds=self._config.jira_worker_poll_interval_seconds,
                scopes=scopes,
            )
            await session.commit()
        await self.sweep_ticket_rooms()
        for job in jobs:
            try:
                await self._handle_job(job)
            except Exception as exc:
                logger.exception(
                    "Jira worker job %s (%s) failed: %s",
                    job.id,
                    job.kind,
                    type(exc).__name__,
                )
                async with self._session_factory() as session:
                    await complete_job(
                        session,
                        job.id,
                        status="error",
                        error=f"{type(exc).__name__}",
                        now=self._clock(),
                    )
                    await session.commit()

    async def sweep_ticket_rooms(self, limit: int = 20) -> int:
        """Reconcile tickets missing room state or with a pending room sync."""
        if self._ticket_rooms is None:
            return 0
        projects = self._config.jira_worker_enabled_projects
        if not projects:
            return 0
        from switch_core.bridges.jira.rooms import sync_ticket_room

        follow_up_commands = ("invite_ticket_reporter", SYNC_TICKET_ROOM_COMMAND)
        async with self._session_factory() as session:
            scope = or_(
                *(
                    and_(
                        JiraWorkerTicket.instance == instance,
                        JiraWorkerTicket.project_key == project_key,
                    )
                    for instance, project_keys in projects.items()
                    for project_key in project_keys
                )
            )
            pending_follow_ups = select(
                JiraWorkerOutbox.payload["ticket_id"].astext
            ).where(
                JiraWorkerOutbox.channel == "switch",
                JiraWorkerOutbox.command.in_(follow_up_commands),
                JiraWorkerOutbox.status == "pending",
            )
            stmt = (
                select(JiraWorkerTicket.instance, JiraWorkerTicket.issue_key)
                .where(
                    or_(
                        JiraWorkerTicket.room_id.is_(None),
                        JiraWorkerTicket.card_event_id.is_(None),
                        JiraWorkerTicket.id.in_(pending_follow_ups),
                    ),
                    scope,
                )
                .order_by(JiraWorkerTicket.last_event_at.asc())
                .limit(max(1, limit))
            )
            pending = list((await session.execute(stmt)).all())
            await session.execute(
                update(JiraWorkerOutbox)
                .where(
                    JiraWorkerOutbox.channel == "switch",
                    JiraWorkerOutbox.command.in_(follow_up_commands),
                    JiraWorkerOutbox.status == "pending",
                    ~JiraWorkerOutbox.payload["ticket_id"].astext.in_(
                        select(JiraWorkerTicket.id)
                    ),
                )
                .values(status="done")
            )
            await session.commit()
        done = 0
        for instance, issue_key in pending:
            if await sync_ticket_room(
                self._session_factory,
                instance=instance,
                issue_key=issue_key,
                rooms=self._ticket_rooms,
                jira_agent_name=self._config.jira_agent_name,
                cards=self._card_updater,
            ):
                done += 1
        return done

    async def _handle_job(self, job: JiraWorkerJob) -> None:
        if job.kind != POLL_JOB_KIND:
            logger.warning("Jira worker ignoring unknown job kind %r", job.kind)
            async with self._session_factory() as session:
                await complete_job(session, job.id, status="done", now=self._clock())
                await session.commit()
            return
        await self._handle_poll_job(job)

    async def _handle_poll_job(self, job: JiraWorkerJob) -> None:
        raw_watermark = (
            job.payload.get("watermark") if isinstance(job.payload, dict) else None
        )
        watermark: str | None = (
            raw_watermark if isinstance(raw_watermark, str) else None
        )
        new_watermark: str | None = watermark
        client = self._poll_clients.get(job.instance)
        if client is None:
            logger.warning(
                "Jira worker poll for instance %s project %s skipped: no Jira credentials",
                job.instance,
                job.project_key,
            )
            new_watermark = watermark
        else:
            issues = await client.search_updated(
                project_key=job.project_key, updated_since=watermark
            )
            async with self._session_factory() as session:
                for issue in issues:
                    await record_polled_issue(session, issue, instance=job.instance)
                await session.commit()
            from switch_core.bridges.jira.rooms import sync_ticket_room

            for issue in issues:
                issue_key = str(issue.get("key") or "")
                if issue_key:
                    await sync_ticket_room(
                        self._session_factory,
                        instance=job.instance,
                        issue_key=issue_key,
                        rooms=self._ticket_rooms,
                        jira_agent_name=self._config.jira_agent_name,
                        cards=self._card_updater,
                    )
            stamps = [
                str(issue.get("fields", {}).get("updated") or "")
                for issue in issues
                if isinstance(issue.get("fields"), dict)
            ]
            stamps = [stamp for stamp in stamps if stamp]
            new_watermark = max(stamps) if stamps else watermark
            logger.info(
                "Jira worker poll for project %s recorded %d issue(s), watermark %r",
                job.project_key,
                len(issues),
                new_watermark,
            )
        async with self._session_factory() as session:
            await complete_job(session, job.id, status="done", now=self._clock())
            await update_next_poll_watermark(
                session,
                predecessor_id=job.id,
                watermark=new_watermark,
            )
            await session.commit()
