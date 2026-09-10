from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from switch_core.db.models import JiraIssueThread, JiraTrigger, JiraTriggerFiring

# Claim statuses block a repeat of the same dedupe key inside the window.
CLAIM_STATUSES = frozenset({"pending", "delivered", "error"})
# Rows that count toward burst / cool-down (actual fire attempts).
ATTEMPT_STATUSES = frozenset({"pending", "delivered", "error"})


class JiraTriggerStore:
    """CRUD for ``jira_triggers`` plus delivery-log / dedupe / burst helpers."""

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
    ) -> Sequence[JiraTrigger]:
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
        instance: str = "",
        rule_name: str = "",
        matched_rule_ids: Sequence[str] | None = None,
        now: datetime | None = None,
    ) -> str | None:
        """Insert a ``pending`` claim row when the dedupe key is free.

        Returns the new firing id when this caller should proceed, or None when
        a recent claim already exists.
        """
        if await self.has_recent_claim(
            session,
            issue_key=issue_key,
            rule_id=rule_id,
            transition_key=transition_key,
            dedupe_window=dedupe_window,
            now=now,
        ):
            return None

        clock = now or datetime.now(UTC)
        cutoff = clock - dedupe_window
        # Drop stale claim rows for the same key so history stays readable and
        # old pending/error rows outside the window do not confuse operators.
        await session.execute(
            delete(JiraTriggerFiring).where(
                JiraTriggerFiring.issue_key == issue_key,
                JiraTriggerFiring.rule_id == rule_id,
                JiraTriggerFiring.transition_key == transition_key,
                JiraTriggerFiring.status.in_(CLAIM_STATUSES),
                JiraTriggerFiring.created_at < cutoff,
            )
        )
        row = JiraTriggerFiring(
            issue_key=issue_key,
            rule_id=rule_id,
            transition_key=transition_key,
            instance=instance,
            rule_name=rule_name,
            status="pending",
            matched_rule_ids=list(matched_rule_ids) if matched_rule_ids else None,
            attempt_count=1,
            created_at=clock,
        )
        session.add(row)
        await session.flush()
        return row.id

    async def has_recent_claim(
        self,
        session: AsyncSession,
        *,
        issue_key: str,
        rule_id: str,
        transition_key: str,
        dedupe_window: timedelta,
        now: datetime | None = None,
    ) -> bool:
        clock = now or datetime.now(UTC)
        cutoff = clock - dedupe_window
        existing = await session.execute(
            select(JiraTriggerFiring.id).where(
                JiraTriggerFiring.issue_key == issue_key,
                JiraTriggerFiring.rule_id == rule_id,
                JiraTriggerFiring.transition_key == transition_key,
                JiraTriggerFiring.status.in_(CLAIM_STATUSES),
                JiraTriggerFiring.created_at >= cutoff,
            )
        )
        return existing.scalar_one_or_none() is not None

    async def record_suppressed_firing(
        self,
        session: AsyncSession,
        *,
        issue_key: str,
        rule_id: str,
        transition_key: str,
        status: str,
        instance: str,
        rule_name: str,
        matched_rule_ids: Sequence[str] | None = None,
        error: str | None = None,
        now: datetime | None = None,
    ) -> str:
        """Insert a suppression row (does not claim the dedupe key)."""
        clock = now or datetime.now(UTC)
        row = JiraTriggerFiring(
            issue_key=issue_key,
            rule_id=rule_id,
            transition_key=transition_key,
            instance=instance,
            rule_name=rule_name,
            status=status,
            matched_rule_ids=list(matched_rule_ids) if matched_rule_ids else None,
            error=error,
            attempt_count=0,
            created_at=clock,
        )
        session.add(row)
        await session.flush()
        return row.id

    async def finalize_firing(
        self,
        session: AsyncSession,
        firing_id: str,
        *,
        status: str,
        room_results: Sequence[dict[str, Any]] | None,
        error: str | None,
        attempt_count: int,
    ) -> None:
        row = await session.get(JiraTriggerFiring, firing_id)
        if row is None:
            return
        row.status = status
        row.room_results = list(room_results) if room_results is not None else None
        row.error = error
        row.attempt_count = attempt_count
        await session.flush()

    async def count_firings_in_window(
        self,
        session: AsyncSession,
        *,
        rule_id: str,
        window: timedelta,
        now: datetime | None = None,
    ) -> int:
        """Count claim/attempt rows for burst protection (excludes suppressions)."""
        clock = now or datetime.now(UTC)
        cutoff = clock - window
        result = await session.execute(
            select(func.count())
            .select_from(JiraTriggerFiring)
            .where(
                JiraTriggerFiring.rule_id == rule_id,
                JiraTriggerFiring.status.in_(ATTEMPT_STATUSES),
                JiraTriggerFiring.created_at >= cutoff,
            )
        )
        return int(result.scalar_one())

    async def seconds_since_last_attempt(
        self,
        session: AsyncSession,
        *,
        rule_id: str,
        now: datetime | None = None,
    ) -> float | None:
        """Age in seconds of the newest attempt row, or None when none exist."""
        clock = now or datetime.now(UTC)
        result = await session.execute(
            select(JiraTriggerFiring.created_at)
            .where(
                JiraTriggerFiring.rule_id == rule_id,
                JiraTriggerFiring.status.in_(ATTEMPT_STATUSES),
            )
            .order_by(JiraTriggerFiring.created_at.desc())
            .limit(1)
        )
        latest = result.scalar_one_or_none()
        if latest is None:
            return None
        if isinstance(latest, str):
            latest_dt = datetime.fromisoformat(latest)
        elif isinstance(latest, datetime):
            latest_dt = latest
        else:
            latest_dt = datetime.fromisoformat(str(latest))
        if latest_dt.tzinfo is None:
            latest_dt = latest_dt.replace(tzinfo=UTC)
        return (clock - latest_dt).total_seconds()

    async def list_deliveries(
        self,
        session: AsyncSession,
        *,
        instance: str | None = None,
        rule_id: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Sequence[JiraTriggerFiring]:
        stmt = select(JiraTriggerFiring).order_by(JiraTriggerFiring.created_at.desc())
        if instance is not None:
            stmt = stmt.where(JiraTriggerFiring.instance == instance)
        if rule_id is not None:
            stmt = stmt.where(JiraTriggerFiring.rule_id == rule_id)
        stmt = stmt.offset(max(offset, 0)).limit(max(1, min(limit, 200)))
        result = await session.execute(stmt)
        return list(result.scalars().all())

    async def prune_firings(
        self,
        session: AsyncSession,
        *,
        retain_seconds: int,
        max_rows: int,
        now: datetime | None = None,
    ) -> int:
        """Delete rows outside the retention window or beyond ``max_rows``.

        Returns the number of rows deleted.
        """
        clock = now or datetime.now(UTC)
        deleted = 0
        if retain_seconds > 0:
            cutoff = clock - timedelta(seconds=retain_seconds)
            result = await session.execute(
                delete(JiraTriggerFiring).where(JiraTriggerFiring.created_at < cutoff)
            )
            deleted += int(getattr(result, "rowcount", 0) or 0)

        if max_rows > 0:
            keep = (
                select(JiraTriggerFiring.id)
                .order_by(JiraTriggerFiring.created_at.desc())
                .limit(max_rows)
            )
            result = await session.execute(
                delete(JiraTriggerFiring).where(JiraTriggerFiring.id.not_in(keep))
            )
            deleted += int(getattr(result, "rowcount", 0) or 0)

        await session.flush()
        return deleted

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
        from sqlalchemy.dialects.postgresql import insert

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
