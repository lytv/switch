"""Shared admission locks and cumulative estimates for model sessions."""

from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy import Numeric, cast, func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from switch_core.db.models import JiraWorkerEvent, JiraWorkerJob

LIVE_ROLES = ("claimed", "running", "uncertain", "cancel_requested")


class CapacityUnavailable(Exception):
    """The durable ticket remains queued without a Jira status change."""


async def capacity_lock(session: AsyncSession) -> None:
    await session.execute(text("SELECT pg_advisory_xact_lock(73429105)"))


async def accounting_lock(session: AsyncSession) -> None:
    await capacity_lock(session)
    await session.execute(text("SELECT pg_advisory_xact_lock(73429106)"))


async def live_role_sessions(session: AsyncSession) -> int:
    return int(
        await session.scalar(
            select(func.count())
            .select_from(JiraWorkerJob)
            .where(
                JiraWorkerJob.kind == "role_job",
                JiraWorkerJob.status.in_(LIVE_ROLES),
            )
        )
        or 0
    )


async def live_sessions(session: AsyncSession) -> int:
    return int(
        await session.scalar(
            select(func.count())
            .select_from(JiraWorkerJob)
            .where(
                or_(
                    (JiraWorkerJob.kind == "orchestrator_turn")
                    & (JiraWorkerJob.status == "claimed"),
                    (JiraWorkerJob.kind == "role_job")
                    & JiraWorkerJob.status.in_(LIVE_ROLES),
                )
            )
        )
        or 0
    )


async def day_cost(session: AsyncSession, now: datetime) -> Decimal:
    payload = JiraWorkerJob.payload
    day = now.astimezone(UTC).date().isoformat()
    result = await session.scalar(
        select(
            func.coalesce(
                func.sum(
                    func.coalesce(cast(payload["held_usd"].astext, Numeric), 0)
                    + func.coalesce(
                        cast(payload["cost_by_day"][day].astext, Numeric), 0
                    )
                ),
                0,
            )
        ).where(JiraWorkerJob.kind.in_(("role_job", "orchestrator_turn")))
    )
    assert result is not None
    return result


async def day_limit(session: AsyncSession, now: datetime, base: Decimal) -> Decimal:
    delta = await session.scalar(
        select(
            func.coalesce(
                func.sum(
                    cast(
                        JiraWorkerEvent.payload["delta"]["day_estimated_usd"].astext,
                        Numeric,
                    )
                ),
                0,
            )
        ).where(
            JiraWorkerEvent.event_kind == "admin_budget",
            JiraWorkerEvent.payload["day"].astext
            == now.astimezone(UTC).date().isoformat(),
        )
    )
    assert delta is not None
    return base + delta
