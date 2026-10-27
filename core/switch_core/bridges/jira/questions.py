"""Language-only delivery questions on the existing ticket turn and outbox."""

from __future__ import annotations

import json
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from switch_core.bridges.jira.accounting import accounting_lock
from switch_core.bridges.jira.activity import TicketTurn, TurnResult, record_event
from switch_core.bridges.jira.admin import _cancel_wait_commands
from switch_core.bridges.jira.commands import enqueue_ticket_command
from switch_core.bridges.jira.contracts import Answer
from switch_core.bridges.jira.delivery import replace_state
from switch_core.bridges.jira.outbox import INTAKE_QUESTION_COMMAND, park_ticket
from switch_core.bridges.jira.parse import _plain_text
from switch_core.bridges.jira.target import delivery_state, reset_delivery_scope
from switch_core.config import SwitchConfig
from switch_core.db.models import JiraWorkerJob, JiraWorkerOutbox, JiraWorkerTicket

if TYPE_CHECKING:
    from switch_core.bridges.jira.intake import IntakeModel


async def relay_question(
    sessions: async_sessionmaker[AsyncSession],
    *,
    config: SwitchConfig,
    model: IntakeModel,
    turn: TicketTurn,
    now: datetime,
) -> TurnResult:
    async with sessions() as session:
        ticket = await session.get(JiraWorkerTicket, turn.ticket_id)
        assert ticket is not None
        state = delivery_state(ticket)
        if (
            state is None
            or state.question is None
            or state.answer is not None
            or ticket.worker_parked_reason
        ):
            return TurnResult(0, None)
        question = state.question
        generation = ticket.agent_generation
        initial = state.question_command is None
        if not initial:
            sent = await session.get(JiraWorkerOutbox, state.question_command)
            if sent is None or sent.status != "done":
                return TurnResult(0, None)
        replies = (
            [
                _plain_text(event.get("untrusted_data", {}).get("body")).strip()
                for event in turn.untrusted_data["events"]
                if event.get("reporter_input")
                and not event.get("wrong_place_type")
                and event.get("received_at", "") > (state.question_started_at or "")
            ]
            if not initial
            else []
        )
        replies = [reply for reply in replies if reply]
        if not initial and not replies:
            return TurnResult(0, None)
    decision = await model.relay(
        {"question": question.model_dump(mode="json"), "replies": replies},
        turn.token_budget,
    )
    async with sessions() as session:
        await accounting_lock(session)
        ticket = await session.get(
            JiraWorkerTicket, turn.ticket_id, with_for_update=True
        )
        assert ticket is not None
        current = delivery_state(ticket)
        if (
            ticket.worker_parked_reason
            or ticket.agent_generation != generation
            or current is None
            or current.question != question
            or current.answer is not None
        ):
            return TurnResult(decision.tokens_used, None)
        state = current
        key = f"delivery-question:{question.run.execution_key}:{question.id}"

        async def queue(
            suffix: str, *, body: str, transition: bool, predecessor: str | None
        ) -> str:
            command_id = await enqueue_ticket_command(
                session,
                config=config,
                ticket=ticket,
                command_key=f"{key}:{suffix}",
                channel="jira" if transition else ticket.wait_channel,
                command="transition"
                if transition
                else INTAKE_QUESTION_COMMAND
                if ticket.wait_channel == "switch"
                else "comment",
                body=body,
                predecessor=predecessor,
                now=now,
            )
            row = await session.get(JiraWorkerOutbox, command_id)
            assert row is not None
            row.payload = {
                **(row.payload or {}),
                "delivery_question": {
                    "id": question.id,
                    "execution_key": question.run.execution_key,
                },
                "requires_answer": suffix == "resume",
            }
            return command_id

        if decision.action == "budget":
            park_ticket(ticket, "Delivery question language budget exhausted")
        elif initial:
            if decision.action != "question":
                raise ValueError(
                    "A missing fact must produce exactly one reporter question"
                )
            predecessor = None
            if ticket.status != config.jira_worker_statuses.waiting_for_input:
                predecessor = await queue(
                    "input",
                    body=config.jira_worker_statuses.waiting_for_input,
                    transition=True,
                    predecessor=None,
                )
            command = await queue(
                "ask", body=decision.content, transition=False, predecessor=predecessor
            )
            replace_state(ticket, state, question_command=command)
        else:
            if decision.action not in {
                "answer",
                "clarify",
                "scope_change",
                "unrelated",
            }:
                raise ValueError("Delivery reply classification is invalid")
            revision = question.run.input_revision + 1
            answer = Answer.model_validate_json(
                json.dumps(
                    {
                        "question": question.model_dump(mode="json"),
                        "revision": revision,
                        "actor_account_id": ticket.reporter_account_id,
                        "classification": decision.action,
                        "text": "\n".join(replies),
                    }
                )
            )
            event_ids = [
                event["event_id"]
                for event in turn.untrusted_data["events"]
                if event.get("reporter_input")
            ]
            await record_event(
                session,
                instance=ticket.instance,
                issue_key=ticket.issue_key,
                project_key=ticket.project_key,
                idempotency_key=f"{key}:reply:{':'.join(event_ids)}",
                event_kind="delivery_answer",
                webhook_event="worker:delivery_answer",
                payload={
                    "answer": answer.model_dump(mode="json"),
                    "event_ids": event_ids,
                    **(
                        {"invalidated_workflow": state.model_dump(mode="json")}
                        if decision.action == "scope_change"
                        else {}
                    ),
                },
            )
            if decision.action == "answer":
                execution = await session.scalar(
                    select(JiraWorkerJob).where(
                        JiraWorkerJob.execution_key == question.run.execution_key
                    )
                )
                if (
                    execution is None
                    or execution.status != "waiting_input"
                    or execution.result is None
                ):
                    raise ValueError("Accepted answer has no exited question execution")
                execution.status = "done"
                revisions = dict(state.input_revisions)
                revisions[f"{question.run.step}/{question.run.attempt}"] = revision
                resume = await queue(
                    "resume",
                    body=config.jira_worker_statuses.in_progress,
                    transition=True,
                    predecessor=state.question_command,
                )
                replace_state(
                    ticket,
                    state,
                    answer=answer.model_dump(mode="json"),
                    input_revisions=revisions,
                    resume_command=resume,
                )
                ticket.wait_anchor = None
                ticket.wait_fired = None
                await _cancel_wait_commands(session, ticket)
            elif decision.action == "scope_change":
                reset_delivery_scope(
                    ticket,
                    state,
                    service=state.service,
                    supplied_url=state.repository.supplied_url
                    if state.repository
                    else "",
                )
                ticket.intake_state = {
                    **(ticket.intake_state or {}),
                    "pending_replies": replies,
                }
                await _cancel_wait_commands(session, ticket)
            elif decision.action == "clarify":
                await queue(
                    f"clarify:{':'.join(event_ids)}",
                    body=decision.content,
                    transition=False,
                    predecessor=state.question_command,
                )
        await session.commit()
    return TurnResult(decision.tokens_used, None)
