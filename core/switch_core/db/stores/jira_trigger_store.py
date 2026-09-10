from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from switch_core.db.models import JiraIssueThread, JiraTrigger, JiraTriggerFiring


class JiraTriggerStore:
    """CRUD for ``jira_triggers`` plus firing ledger helpers for dedupe/rate limit."""

    async def create(self, session: AsyncSession, trigger: JiraTrigger) -> JiraTrigger:
        session.add(trigger)
        await session.flush()
        return trigger

    async def get(self, session: AsyncSession, trigger_id: str) -> JiraTrigger | None:
        return await session.get(JiraTrigger, trigger_id)

    async def list(
        self,
        session: AsyncSession,
        *,
        instance: str | None = None,
        enabled_only: bool = False,
    ) -> list[JiraTrigger]:
        stmt = select(JiraTrigger).order_by(JiraTrigger.created_at.asc())
        if instance is not None:
            stmt = stmt.where(JiraTrigger.instance == instance)
        if enabled_only:
            stmt = stmt.where(JiraTrigger.enabled.is_(True))
        result = await session.execute(stmt)
        return list(result.scalars().all())

    async def update(
        self,
        session: AsyncSession,
        trigger_id: str,
        **fields: object,
    ) -> JiraTrigger | None:
        trigger = await session.get(JiraTrigger, trigger_id)
        if trigger is None:
            return None
        for key, value in fields.items():
            if not hasattr(trigger, key):
                raise ValueError(f"Unknown JiraTrigger field: {key}")
            setattr(trigger, key, value)
        await session.flush()
        return trigger

    async def delete(self, session: AsyncSession, trigger_id: str) -> bool:
        trigger = await session.get(JiraTrigger, trigger_id)
        if trigger is None:
            return False
        await session.delete(trigger)
        await session.flush()
        return True

    async def try_record_firing(
        self,
        session: AsyncSession,
        *,
        issue_key: str,
        rule_id: str,
        transition_key: str,
        dedupe_window: timedelta,
    ) -> bool:
        """Insert a firing row if the dedupe key is free inside the window.

        Returns True when this caller should proceed (row inserted or only
        stale duplicates exist and were replaced). Returns False when a recent
        duplicate already exists.
        """
        cutoff = datetime.now(UTC) - dedupe_window
        existing = await session.execute(
            select(JiraTriggerFiring).where(
                JiraTriggerFiring.issue_key == issue_key,
                JiraTriggerFiring.rule_id == rule_id,
                JiraTriggerFiring.transition_key == transition_key,
                JiraTriggerFiring.created_at >= cutoff,
            )
        )
        if existing.scalar_one_or_none() is not None:
            return False

        # Drop any stale row for the same key so the unique constraint allows
        # a fresh insert after the window.
        await session.execute(
            delete(JiraTriggerFiring).where(
                JiraTriggerFiring.issue_key == issue_key,
                JiraTriggerFiring.rule_id == rule_id,
                JiraTriggerFiring.transition_key == transition_key,
            )
        )
        stmt = (
            insert(JiraTriggerFiring)
            .values(
                issue_key=issue_key,
                rule_id=rule_id,
                transition_key=transition_key,
            )
            .on_conflict_do_nothing(
                constraint="uq_jira_trigger_firings_dedupe",
            )
            .returning(JiraTriggerFiring.id)
        )
        result = await session.execute(stmt)
        inserted = result.scalar_one_or_none()
        await session.flush()
        return inserted is not None

    async def count_firings_in_window(
        self,
        session: AsyncSession,
        *,
        rule_id: str,
        window: timedelta,
    ) -> int:
        cutoff = datetime.now(UTC) - window
        result = await session.execute(
            select(func.count())
            .select_from(JiraTriggerFiring)
            .where(
                JiraTriggerFiring.rule_id == rule_id,
                JiraTriggerFiring.created_at >= cutoff,
            )
        )
        return int(result.scalar_one())

    async def get_issue_thread_root(
        self,
        session: AsyncSession,
        *,
        room_id: str,
        issue_key: str,
    ) -> str | None:
        result = await session.execute(
            select(JiraIssueThread.thread_root_event_id).where(
                JiraIssueThread.room_id == room_id,
                JiraIssueThread.issue_key == issue_key,
            )
        )
        return result.scalar_one_or_none()

    async def upsert_issue_thread(
        self,
        session: AsyncSession,
        *,
        room_id: str,
        issue_key: str,
        thread_root_event_id: str,
    ) -> None:
        stmt = (
            insert(JiraIssueThread)
            .values(
                room_id=room_id,
                issue_key=issue_key,
                thread_root_event_id=thread_root_event_id,
            )
            .on_conflict_do_nothing(constraint="uq_jira_issue_threads_room_issue")
        )
        await session.execute(stmt)
        await session.flush()
