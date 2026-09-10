from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from switch_core.bridges.agent.protocol.service import ProtocolService
from switch_core.bridges.agent.protocol.types import AgentStatus
from switch_core.bridges.jira.matching import matching_rules
from switch_core.bridges.jira.parse import ParsedJiraEvent
from switch_core.bridges.jira.template import render_template
from switch_core.config import SwitchConfig
from switch_core.db.models import JiraTrigger, Room
from switch_core.db.stores.agent_store import AgentStore
from switch_core.db.stores.jira_trigger_store import JiraTriggerStore
from switch_core.db.stores.room_store import RoomStore

logger = logging.getLogger(__name__)

SleepFn = Callable[[float], Awaitable[None]]
ClockFn = Callable[[], datetime]


def transition_key_for(event: ParsedJiraEvent) -> str:
    if event.transition is not None:
        return f"{event.transition.from_status}->{event.transition.to_status}"
    return event.event_kind


def is_permanent_delivery_error(exc: BaseException) -> bool:
    """Authorization / validation failures must not be retried.

    Transport send failures are often wrapped as ``ValueError("Failed to send
    message")`` (and similar client-not-ready messages); those stay retryable.
    """
    if isinstance(exc, PermissionError):
        return True
    if not isinstance(exc, ValueError):
        return False
    msg = str(exc)
    retryable_markers = (
        "Failed to send message",
        "Failed to send media message",
        "Agent client not running",
        "Agent client not connected",
    )
    return not any(marker in msg for marker in retryable_markers)


def safe_error_text(exc: BaseException) -> str:
    """Short error for the delivery log — no payload or secret content."""
    return f"{type(exc).__name__}: {exc}"[:500]


class JiraBridgeService:
    """Match rules and post addressed messages for one accepted webhook event."""

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker,
        trigger_store: JiraTriggerStore,
        agent_store: AgentStore,
        room_store: RoomStore,
        protocol: ProtocolService,
        config: SwitchConfig,
        sleep: SleepFn | None = None,
        clock: ClockFn | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._trigger_store = trigger_store
        self._agent_store = agent_store
        self._room_store = room_store
        self._protocol = protocol
        self._config = config
        self._sleep: SleepFn = sleep or asyncio.sleep
        self._clock: ClockFn = clock or (lambda: datetime.now(UTC))

    async def process_event(self, *, instance: str, event: ParsedJiraEvent) -> None:
        async with self._session_factory() as session:
            rules = await self._trigger_store.list(
                session, instance=instance, enabled_only=True
            )
        matches = matching_rules(rules, event)
        if not matches:
            logger.info(
                "Jira event %s (%s) matched no rules for instance %r",
                event.key,
                event.webhook_event,
                instance,
            )
            return

        jira_agent = await self._resolve_jira_agent()
        if jira_agent is None:
            logger.error(
                "Jira event %s matched %d rule(s) but the Jira system agent "
                "%r is not provisioned; skipping delivery",
                event.key,
                len(matches),
                self._config.jira_agent_name,
            )
            return

        matched_rule_ids = [rule.id for rule in matches]
        for rule in matches:
            await self._fire_rule(
                jira_agent_id=jira_agent,
                rule=rule,
                event=event,
                instance=instance,
                matched_rule_ids=matched_rule_ids,
            )

        await self._prune_delivery_log()

    async def _resolve_jira_agent(self) -> str | None:
        async with self._session_factory() as session:
            agent = await self._agent_store.get_by_name(
                session, self._config.jira_agent_name
            )
        return agent.id if agent is not None else None

    async def _resolve_target_rooms(self, rule: JiraTrigger) -> list[Room]:
        async with self._session_factory() as session:
            if rule.target_kind == "room":
                if not rule.target_room_id:
                    return []
                room = await self._room_store.get(session, rule.target_room_id)
                return [room] if room is not None else []
            if rule.target_kind == "group":
                if not rule.target_group_id:
                    return []
                result = await session.execute(
                    select(Room).where(
                        Room.group_id == rule.target_group_id,
                        Room.archived_at.is_(None),
                    )
                )
                return list(result.scalars().all())
        return []

    async def _claim_firing(
        self,
        *,
        rule: JiraTrigger,
        event: ParsedJiraEvent,
        instance: str,
        matched_rule_ids: list[str],
    ) -> str | None:
        """Return a pending firing id, or None after logging a suppression."""
        t_key = transition_key_for(event)
        now = self._clock()
        dedupe_window = timedelta(seconds=self._config.jira_dedupe_window_seconds)
        rate_window = timedelta(seconds=self._config.jira_rate_limit_window_seconds)
        cooldown = self._config.jira_rule_cooldown_seconds

        async with self._session_factory() as session:
            recent = await self._trigger_store.count_firings_in_window(
                session, rule_id=rule.id, window=rate_window, now=now
            )
            if recent >= self._config.jira_rate_limit_per_rule:
                await self._trigger_store.record_suppressed_firing(
                    session,
                    issue_key=event.key,
                    rule_id=rule.id,
                    transition_key=t_key,
                    status="suppressed_burst",
                    instance=instance,
                    rule_name=rule.name,
                    matched_rule_ids=matched_rule_ids,
                    error=(
                        f"burst cap: {recent} attempts in "
                        f"{self._config.jira_rate_limit_window_seconds}s"
                    ),
                    now=now,
                )
                await session.commit()
                logger.warning(
                    "Jira rule %s (%s) burst-capped for issue %s "
                    "(%d firings in window); suppressed",
                    rule.id,
                    rule.name,
                    event.key,
                    recent,
                )
                return None

            if await self._trigger_store.has_recent_claim(
                session,
                issue_key=event.key,
                rule_id=rule.id,
                transition_key=t_key,
                dedupe_window=dedupe_window,
                now=now,
            ):
                await self._trigger_store.record_suppressed_firing(
                    session,
                    issue_key=event.key,
                    rule_id=rule.id,
                    transition_key=t_key,
                    status="suppressed_dedupe",
                    instance=instance,
                    rule_name=rule.name,
                    matched_rule_ids=matched_rule_ids,
                    now=now,
                )
                await session.commit()
                logger.info(
                    "Jira rule %s (%s) deduped for issue %s transition %r; suppressed",
                    rule.id,
                    rule.name,
                    event.key,
                    t_key,
                )
                return None

            if cooldown > 0:
                age = await self._trigger_store.seconds_since_last_attempt(
                    session, rule_id=rule.id, now=now
                )
                if age is not None and age < cooldown:
                    await self._trigger_store.record_suppressed_firing(
                        session,
                        issue_key=event.key,
                        rule_id=rule.id,
                        transition_key=t_key,
                        status="suppressed_cooldown",
                        instance=instance,
                        rule_name=rule.name,
                        matched_rule_ids=matched_rule_ids,
                        error=f"cool-down: {age:.1f}s < {cooldown}s",
                        now=now,
                    )
                    await session.commit()
                    logger.info(
                        "Jira rule %s (%s) cool-down for issue %s "
                        "(%.1fs < %ds); suppressed",
                        rule.id,
                        rule.name,
                        event.key,
                        age,
                        cooldown,
                    )
                    return None

            claimed = await self._trigger_store.try_record_firing(
                session,
                issue_key=event.key,
                rule_id=rule.id,
                transition_key=t_key,
                dedupe_window=dedupe_window,
                instance=instance,
                rule_name=rule.name,
                matched_rule_ids=matched_rule_ids,
                now=now,
            )
            await session.commit()
            if claimed is None:
                # Race with another worker; treat as dedupe.
                async with self._session_factory() as session2:
                    await self._trigger_store.record_suppressed_firing(
                        session2,
                        issue_key=event.key,
                        rule_id=rule.id,
                        transition_key=t_key,
                        status="suppressed_dedupe",
                        instance=instance,
                        rule_name=rule.name,
                        matched_rule_ids=matched_rule_ids,
                        now=now,
                    )
                    await session2.commit()
                return None
        return claimed

    async def _fire_rule(
        self,
        *,
        jira_agent_id: str,
        rule: JiraTrigger,
        event: ParsedJiraEvent,
        instance: str,
        matched_rule_ids: list[str],
    ) -> None:
        rooms = await self._resolve_target_rooms(rule)
        if rule.target_kind not in ("room", "group"):
            logger.warning(
                "Jira rule %s (%s) skipped: unknown target_kind=%r",
                rule.id,
                rule.name,
                rule.target_kind,
            )
            return

        if rule.target_kind == "room" and not rooms:
            logger.warning(
                "Jira rule %s (%s) skipped: room target missing (target_room_id=%r)",
                rule.id,
                rule.name,
                rule.target_room_id,
            )
            return

        if rule.target_kind == "group" and not rooms:
            logger.info(
                "Jira rule %s (%s) group %r has no member rooms; nothing to post",
                rule.id,
                rule.name,
                rule.target_group_id,
            )
            return

        firing_id = await self._claim_firing(
            rule=rule,
            event=event,
            instance=instance,
            matched_rule_ids=matched_rule_ids,
        )
        if firing_id is None:
            return

        body = render_template(
            rule.message_template,
            event,
            max_chars=self._config.jira_message_max_chars,
        )

        room_results: list[dict[str, Any]] = []
        max_attempts = 1
        for room in rooms:
            outcome = await self._post_to_room(
                jira_agent_id=jira_agent_id,
                rule=rule,
                event=event,
                room=room,
                body=body,
            )
            room_results.append(outcome)
            max_attempts = max(max_attempts, int(outcome.get("attempts") or 1))

        any_error = any(r.get("status") == "error" for r in room_results)
        status = "error" if any_error else "delivered"
        error = None
        if any_error:
            parts = [
                f"{r.get('room_id')}: {r.get('error')}"
                for r in room_results
                if r.get("status") == "error"
            ]
            error = "; ".join(parts)[:500]

        async with self._session_factory() as session:
            await self._trigger_store.finalize_firing(
                session,
                firing_id,
                status=status,
                room_results=room_results,
                error=error,
                attempt_count=max_attempts,
            )
            await session.commit()

    async def _post_to_room(
        self,
        *,
        jira_agent_id: str,
        rule: JiraTrigger,
        event: ParsedJiraEvent,
        room: Room,
        body: str,
    ) -> dict[str, Any]:
        thread_id: str | None = None
        if rule.thread_by.strip().casefold() == "issue_key":
            async with self._session_factory() as session:
                thread_id = await self._trigger_store.get_issue_thread_root(
                    session, room_id=room.id, issue_key=event.key
                )

        max_attempts = max(1, self._config.jira_retry_max_attempts)
        backoff = max(0.0, float(self._config.jira_retry_backoff_seconds))
        last_error: BaseException | None = None

        for attempt in range(1, max_attempts + 1):
            try:
                result = await self._protocol.send_targeted_message(
                    agent_id=jira_agent_id,
                    room_id=room.id,
                    target_names=[rule.agent_name],
                    content=body,
                    thread_id=thread_id,
                )
            except Exception as exc:
                last_error = exc
                permanent = is_permanent_delivery_error(exc)
                if permanent or attempt >= max_attempts:
                    logger.exception(
                        "Jira rule %s (%s) failed to post for issue %s to room %s (%s) "
                        "attempt=%d/%d permanent=%s",
                        rule.id,
                        rule.name,
                        event.key,
                        room.id,
                        room.name,
                        attempt,
                        max_attempts,
                        permanent,
                    )
                    logger.info(
                        "Jira rule %s room_result room_id=%s room_name=%s status=error",
                        rule.id,
                        room.id,
                        room.name,
                    )
                    return {
                        "room_id": room.id,
                        "room_name": room.name,
                        "status": "error",
                        "error": safe_error_text(exc),
                        "attempts": attempt,
                    }
                delay = backoff * (2 ** (attempt - 1))
                logger.warning(
                    "Jira rule %s transient post failure for issue %s room %s "
                    "(%s); retry in %.2fs (attempt %d/%d)",
                    rule.id,
                    event.key,
                    room.id,
                    type(exc).__name__,
                    delay,
                    attempt,
                    max_attempts,
                )
                if delay > 0:
                    await self._sleep(delay)
                continue

            if rule.thread_by.strip().casefold() == "issue_key" and thread_id is None:
                async with self._session_factory() as session:
                    await self._trigger_store.upsert_issue_thread(
                        session,
                        room_id=room.id,
                        issue_key=event.key,
                        thread_root_event_id=result.event_id,
                    )
                    await session.commit()

            soft = (
                AgentStatus.NOT_PERMITTED,
                AgentStatus.NO_SESSION,
                AgentStatus.DORMANT,
                AgentStatus.AWAITING_MANUAL_POLL,
            )
            for name, status in result.target_statuses.items():
                if status in soft:
                    logger.warning(
                        "Jira rule %s addressed %s in room %s with status %s "
                        "(logged, not a hard failure)",
                        rule.id,
                        name,
                        room.name,
                        status.value,
                    )
                else:
                    logger.info(
                        "Jira rule %s addressed %s in room %s with status %s (event_id=%s)",
                        rule.id,
                        name,
                        room.name,
                        status.value,
                        result.event_id,
                    )
            logger.info(
                "Jira rule %s room_result room_id=%s room_name=%s status=ok event_id=%s",
                rule.id,
                room.id,
                room.name,
                result.event_id,
            )
            return {
                "room_id": room.id,
                "room_name": room.name,
                "status": "ok",
                "event_id": result.event_id,
                "attempts": attempt,
            }

        # Unreachable, but keep a safe fallback for type checkers.
        err = last_error or RuntimeError("post failed with no attempts")
        return {
            "room_id": room.id,
            "room_name": room.name,
            "status": "error",
            "error": safe_error_text(err),
            "attempts": max_attempts,
        }

    async def _prune_delivery_log(self) -> None:
        async with self._session_factory() as session:
            deleted = await self._trigger_store.prune_firings(
                session,
                retain_seconds=self._config.jira_delivery_log_retain_seconds,
                max_rows=self._config.jira_delivery_log_max_rows,
                now=self._clock(),
            )
            await session.commit()
        if deleted:
            logger.info("Jira delivery log pruned %d row(s)", deleted)
