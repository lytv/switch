"""Jira ticket worker, step 1: intake only.

One module inside the Jira bridge (option C). It records webhook and poll
events into the worker tables and runs the scheduler skeleton. Jira writes,
rooms, the orchestrator, and the outbox sender are later steps.

With no project enabled (the default) the intake hook returns immediately and
the existing Jira bridge behaves exactly as today.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from switch_core.bridges.trigger_source import NormalizedTriggerEvent
from switch_core.config import SwitchConfig
from switch_core.db.models import JiraWorkerEvent, JiraWorkerJob, JiraWorkerTicket

logger = logging.getLogger(__name__)

POLL_JOB_KIND = "watermark_poll"

SleepFn = Callable[[float], Awaitable[None]]
ClockFn = Callable[[], datetime]

_PENDING_OR_CLAIMED = ("pending", "claimed")


def is_worker_enabled(config: SwitchConfig, project_key: str) -> bool:
    """Per-Jira-project on/off switch. Off by default (empty list)."""
    return project_key in (config.jira_worker_enabled_projects or [])


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


async def record_event(
    session: AsyncSession,
    *,
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
            idempotency_key=idempotency_key,
            issue_key=issue_key,
            project_key=project_key,
            event_kind=event_kind,
            webhook_event=webhook_event,
            payload=payload,
        )
        .on_conflict_do_nothing(index_elements=["idempotency_key"])
        .returning(JiraWorkerEvent.id)
    )
    result = await session.execute(stmt)
    await session.flush()
    return result.scalar_one_or_none()


async def upsert_ticket(
    session: AsyncSession,
    *,
    issue_key: str,
    issue_id: str = "",
    project_key: str = "",
    summary: str = "",
    status: str = "",
    now: datetime | None = None,
) -> None:
    clock = now or datetime.now(UTC)
    stmt = insert(JiraWorkerTicket).values(
        id=str(uuid.uuid4()),
        issue_key=issue_key,
        issue_id=issue_id,
        project_key=project_key,
        summary=summary,
        status=status,
        last_event_at=clock,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=["issue_key"],
        set_={
            "issue_id": stmt.excluded.issue_id,
            "project_key": stmt.excluded.project_key,
            "summary": stmt.excluded.summary,
            "status": stmt.excluded.status,
            "last_event_at": clock,
        },
    )
    await session.execute(stmt)
    await session.flush()


async def record_webhook_event(
    session: AsyncSession,
    event: NormalizedTriggerEvent,
    *,
    enabled_projects: Sequence[str],
) -> bool:
    """Record one parsed webhook event. Duplicate deliveries are no-ops.

    Returns True when the event was new, False for duplicates or when the
    event's project is not enabled.
    """
    if event.project not in enabled_projects:
        return False
    raw = event.raw if isinstance(event.raw, dict) else {}
    key = webhook_idempotency_key(raw, event.key)
    inserted = await record_event(
        session,
        idempotency_key=key,
        issue_key=event.key,
        project_key=event.project,
        event_kind=event.event_kind,
        webhook_event=event.webhook_event,
        payload=raw or None,
    )
    if inserted is None:
        return False
    issue_id = ""
    if isinstance(raw.get("issue"), dict):
        issue_id = str(raw["issue"].get("id") or "")
    await upsert_ticket(
        session,
        issue_key=event.key,
        issue_id=issue_id,
        project_key=event.project,
        summary=event.summary,
        status=event.status,
    )
    return True


async def record_polled_issue(
    session: AsyncSession,
    issue: dict[str, Any],
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
        idempotency_key=poll_idempotency_key(issue_key, updated),
        issue_key=issue_key,
        project_key=project_key,
        event_kind="updated",
        webhook_event="poll:issue_updated",
        payload=issue,
    )
    if inserted is None:
        return False
    await upsert_ticket(
        session,
        issue_key=issue_key,
        issue_id=str(issue.get("id") or ""),
        project_key=project_key,
        summary=str(fields.get("summary") or ""),
        status=status_name,
    )
    return True


async def ensure_poll_job(
    session: AsyncSession,
    *,
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
    project_key: str,
    watermark: str | None,
    interval_seconds: int,
    now: datetime | None = None,
) -> None:
    clock = now or datetime.now(UTC)
    due = clock + timedelta(seconds=interval_seconds)
    session.add(
        JiraWorkerJob(
            kind=POLL_JOB_KIND,
            project_key=project_key,
            due_at=due,
            status="pending",
            payload={"watermark": watermark},
        )
    )
    await session.flush()


async def claim_due_jobs(
    session: AsyncSession,
    *,
    now: datetime | None = None,
    limit: int = 10,
    owner: str = "scheduler",
) -> Sequence[JiraWorkerJob]:
    """Claim due rows transactionally: pending → claimed in one statement.

    The caller commits. Two concurrent claimers never win the same row: the
    CTE locks candidates with SKIP LOCKED and the UPDATE only touches rows
    still pending.
    """
    clock = now or datetime.now(UTC)
    due = (
        select(JiraWorkerJob.id)
        .where(
            JiraWorkerJob.status == "pending",
            JiraWorkerJob.due_at <= clock,
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
    return list(result.scalars().all())


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
        response = await self._client.get(
            f"{self._base_url}/rest/api/3/search/jql",
            params={
                "jql": jql,
                "fields": "summary,status,updated,project",
                "maxResults": 50,
            },
            auth=self._auth,
        )
        response.raise_for_status()
        body = response.json()
        issues = body.get("issues", [])
        return [issue for issue in issues if isinstance(issue, dict)]

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()


def build_poll_client(config: SwitchConfig) -> JiraPollClient | None:
    """Return a poll client, or None when credentials are not configured."""
    if not (
        config.jira_worker_base_url
        and config.jira_worker_email
        and config.jira_worker_api_token
    ):
        return None
    return JiraPollClient(
        base_url=config.jira_worker_base_url,
        email=config.jira_worker_email,
        api_token=config.jira_worker_api_token,
    )


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
        poll_client: JiraPollClient | None = None,
        clock: ClockFn | None = None,
        sleep: SleepFn | None = None,
        owner: str = "scheduler",
    ) -> None:
        self._session_factory = session_factory
        self._config = config
        self._poll_client = poll_client
        self._clock: ClockFn = clock or (lambda: datetime.now(UTC))
        self._sleep: SleepFn = sleep or asyncio.sleep
        self._owner = owner
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
        projects = list(self._config.jira_worker_enabled_projects or [])
        async with self._session_factory() as session:
            for project_key in projects:
                await ensure_poll_job(
                    session,
                    project_key=project_key,
                    interval_seconds=self._config.jira_worker_poll_interval_seconds,
                    now=now,
                )
            jobs = await claim_due_jobs(session, now=now, limit=10, owner=self._owner)
            await session.commit()
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
                    if job.kind == POLL_JOB_KIND:
                        watermark = (
                            job.payload.get("watermark")
                            if isinstance(job.payload, dict)
                            else None
                        )
                        await schedule_next_poll(
                            session,
                            project_key=job.project_key,
                            watermark=watermark,
                            interval_seconds=self._config.jira_worker_poll_interval_seconds,
                            now=self._clock(),
                        )
                    await session.commit()

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
        client = self._poll_client
        if client is None:
            logger.warning(
                "Jira worker poll for project %s skipped: no Jira credentials "
                "configured (set JIRA_WORKER_BASE_URL/EMAIL/API_TOKEN)",
                job.project_key,
            )
            new_watermark = watermark
        else:
            issues = await client.search_updated(
                project_key=job.project_key, updated_since=watermark
            )
            async with self._session_factory() as session:
                for issue in issues:
                    await record_polled_issue(session, issue)
                await session.commit()
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
            await schedule_next_poll(
                session,
                project_key=job.project_key,
                watermark=new_watermark,
                interval_seconds=self._config.jira_worker_poll_interval_seconds,
                now=self._clock(),
            )
            await session.commit()
