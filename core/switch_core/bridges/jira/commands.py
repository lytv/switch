"""Existing ticket outbox construction shared by intake and delivery waits."""

from datetime import datetime, timedelta
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from switch_core.bridges.jira.contracts import digest
from switch_core.bridges.jira.delivery_contracts import ExactTransition
from switch_core.bridges.jira.outbox import HumanChange, enqueue_jira_command
from switch_core.bridges.jira.target import delivery_state
from switch_core.config import SwitchConfig
from switch_core.db.models import JiraWorkerOutbox, JiraWorkerTicket


async def enqueue_ticket_command(
    session: AsyncSession,
    *,
    config: SwitchConfig,
    ticket: JiraWorkerTicket,
    command_key: str,
    channel: str,
    command: str,
    body: str,
    predecessor: str | None,
    now: datetime,
) -> str:
    if channel != "switch":
        assert (
            ticket.jira_read_updated is not None
            and ticket.jira_read_changelog_id is not None
        )
        target = delivery_state(ticket)
        data: Any = body
        if (
            command == "transition"
            and target is not None
            and (
                target.scope is not None
                or (
                    target.gate is not None
                    and target.gate.kind == "repository"
                    and target.repository_match is not None
                )
            )
        ):
            try:
                source = next(
                    item.jira_id
                    for item in target.statuses
                    if item.name == ticket.status
                )
                destination = next(
                    item.jira_id for item in target.statuses if item.name == body
                )
                transition = next(
                    item.jira_id
                    for item in target.transitions
                    if item.name == f"{source}->{destination}"
                )
            except StopIteration as exc:
                raise HumanChange(
                    "Delivery transition fence changed: frozen workflow mapping is missing"
                ) from exc
            data = ExactTransition(
                generation=ticket.agent_generation,
                pack_digest=target.pack.digest,
                step=target.step,
                gate_id=target.gate.id if target.gate else None,
                candidate_sha=target.candidate_head,
                transition_id=transition,
                source_status_id=source,
                target_status_id=destination,
                target_status_name=body,
            ).model_dump(mode="json")
            command = "transition_by_id"
        row_id = await enqueue_jira_command(
            session,
            config=config,
            ticket=ticket,
            command_key=command_key,
            command=command,
            data=data,
            based_updated=ticket.jira_read_updated,
            based_changelog_id=ticket.jira_read_changelog_id,
        )
        assert row_id is not None
        row = await session.get(JiraWorkerOutbox, row_id)
        assert row is not None
    else:
        row = JiraWorkerOutbox(
            channel="switch",
            instance=ticket.instance,
            issue_key=ticket.issue_key,
            command_key=command_key,
            command=command,
            payload={
                "data": body,
                "room_id": ticket.room_id,
                "thread_id": ticket.card_event_id,
                "based_updated": ticket.jira_read_updated,
                "based_changelog_id": ticket.jira_read_changelog_id,
                "based_comment_ids": ticket.jira_seen_comment_ids,
                "reporter_account_id": ticket.reporter_account_id,
            },
        )
        session.add(row)
        await session.flush()
    target = delivery_state(ticket)
    row.payload = {
        **(row.payload or {}),
        **(
            {"scope_digest": digest(target.scope)}
            if target is not None and target.scope is not None
            else {}
        ),
        **(
            {"repository_digest": target.gate.contract_digest}
            if target is not None
            and target.scope is None
            and target.gate is not None
            and target.gate.kind == "repository"
            and target.repository_match is not None
            else {}
        ),
        "intake_generation": ticket.agent_generation,
        "after_command": predecessor,
    }
    row.created_at = now
    if predecessor:
        previous = await session.get(JiraWorkerOutbox, predecessor)
        assert previous is not None
        row.created_at = previous.created_at + timedelta(microseconds=1)
    row.due_at = now
    return row.id
