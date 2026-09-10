from __future__ import annotations

import logging
from datetime import timedelta

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


def transition_key_for(event: ParsedJiraEvent) -> str:
    if event.transition is not None:
        return f"{event.transition.from_status}->{event.transition.to_status}"
    return event.event_kind


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
    ) -> None:
        self._session_factory = session_factory
        self._trigger_store = trigger_store
        self._agent_store = agent_store
        self._room_store = room_store
        self._protocol = protocol
        self._config = config

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

        for rule in matches:
            await self._fire_rule(jira_agent_id=jira_agent, rule=rule, event=event)

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

    async def _claim_firing(self, *, rule: JiraTrigger, event: ParsedJiraEvent) -> bool:
        t_key = transition_key_for(event)
        dedupe_window = timedelta(seconds=self._config.jira_dedupe_window_seconds)
        rate_window = timedelta(seconds=self._config.jira_rate_limit_window_seconds)

        async with self._session_factory() as session:
            recent = await self._trigger_store.count_firings_in_window(
                session, rule_id=rule.id, window=rate_window
            )
            if recent >= self._config.jira_rate_limit_per_rule:
                await session.commit()
                logger.warning(
                    "Jira rule %s (%s) rate-limited for issue %s "
                    "(%d firings in window); suppressed",
                    rule.id,
                    rule.name,
                    event.key,
                    recent,
                )
                return False

            claimed = await self._trigger_store.try_record_firing(
                session,
                issue_key=event.key,
                rule_id=rule.id,
                transition_key=t_key,
                dedupe_window=dedupe_window,
            )
            await session.commit()
            if not claimed:
                logger.info(
                    "Jira rule %s (%s) deduped for issue %s transition %r; suppressed",
                    rule.id,
                    rule.name,
                    event.key,
                    t_key,
                )
                return False
        return True

    async def _fire_rule(
        self,
        *,
        jira_agent_id: str,
        rule: JiraTrigger,
        event: ParsedJiraEvent,
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

        if not await self._claim_firing(rule=rule, event=event):
            return

        body = render_template(
            rule.message_template,
            event,
            max_chars=self._config.jira_message_max_chars,
        )

        for room in rooms:
            await self._post_to_room(
                jira_agent_id=jira_agent_id,
                rule=rule,
                event=event,
                room=room,
                body=body,
            )

    async def _post_to_room(
        self,
        *,
        jira_agent_id: str,
        rule: JiraTrigger,
        event: ParsedJiraEvent,
        room: Room,
        body: str,
    ) -> None:
        thread_id: str | None = None
        if rule.thread_by.strip().casefold() == "issue_key":
            async with self._session_factory() as session:
                thread_id = await self._trigger_store.get_issue_thread_root(
                    session, room_id=room.id, issue_key=event.key
                )

        try:
            result = await self._protocol.send_targeted_message(
                agent_id=jira_agent_id,
                room_id=room.id,
                target_names=[rule.agent_name],
                content=body,
                thread_id=thread_id,
            )
        except Exception:
            logger.exception(
                "Jira rule %s (%s) failed to post for issue %s to room %s (%s)",
                rule.id,
                rule.name,
                event.key,
                room.id,
                room.name,
            )
            logger.info(
                "Jira rule %s room_result room_id=%s room_name=%s status=error",
                rule.id,
                room.id,
                room.name,
            )
            return

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
