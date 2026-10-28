"""Worker-only Jira writes with durable claims and compare-read-write checks.

Jira has no atomic idempotency key or compare-and-set API. A committed
``sending`` row survives a crash; an unknown outcome is visible and is never
replayed automatically. The per-ticket session lock spans database commits
and the HTTP request. It also serializes commands from overlapping schedulers.
A Jira action can still race between the final read and the write.
"""

from __future__ import annotations

import json
import logging
import uuid
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import quote

import httpx
from sqlalchemy import and_, literal, or_, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from switch_core.bridges.jira.contracts import digest
from switch_core.bridges.jira.delivery_contracts import ExactTransition
from switch_core.bridges.jira.observe import _stamp, observe_jira
from switch_core.bridges.jira.state import DeliveryWorkflowState, workflow_state_adapter
from switch_core.config import SwitchConfig
from switch_core.db.models import JiraWorkerJob, JiraWorkerOutbox, JiraWorkerTicket

if TYPE_CHECKING:
    from switch_core.bridges.jira.rooms import TicketRooms
    from switch_core.bridges.jira.worker import ClockFn, JiraPollClient

logger = logging.getLogger(__name__)
_ACTIVE = ("pending", "claimed", "sending")
WAIT_COMMAND_PREFIXES = ("wait-reminder:", "wait-blocked:")
INTAKE_QUESTION_COMMAND = "intake_question"
WAIT_REMINDER_COMMAND = "wait_reminder"


def _validate_command(command: str, data: Any) -> None:
    if command == "transition_by_id":
        ExactTransition.model_validate_json(json.dumps(data))
        return
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
        "Expected a comment, exact transition, target status name, or nonempty fields object"
    )


def delivery_complete(state: DeliveryWorkflowState) -> bool:
    return (
        state.step == "close"
        and state.scope is not None
        and state.scope.verification is not None
        and state.candidate_head is not None
        and state.accepted_head
        == state.review_head
        == state.qa_head
        == state.candidate_head
        and state.merge_receipt is not None
        and state.merge_receipt.outcome == "merged"
        and state.merge_receipt.human
        and state.merge_receipt.actor_id is not None
        and state.merge_receipt.candidate_sha == state.candidate_head
        and state.merge_receipt.repository_url
        == state.scope.job.repository.canonical_url
        and state.merge_receipt.merged_sha == state.merge_sha
        and state.smoke is not None
        and state.smoke.verdict == "PASS"
        and state.smoke.head == state.merge_sha
        and not state.unknown_spend
        and any(
            approval.gate.kind == "acceptance"
            and approval.gate.candidate_sha == state.candidate_head
            for approval in state.approvals
        )
    )


def check_transition_fence(ticket: JiraWorkerTicket, data: Any) -> ExactTransition:
    command = ExactTransition.model_validate_json(json.dumps(data))
    state = workflow_state_adapter.validate_json(json.dumps(ticket.workflow_state))
    if (
        not isinstance(state, DeliveryWorkflowState)
        or ticket.worker_parked_reason
        or ticket.agent_generation != command.generation
        or state.pack.digest != command.pack_digest
        or state.step != command.step
        or (state.gate.id if state.gate else None) != command.gate_id
        or state.candidate_head != command.candidate_sha
        or not any(
            item.jira_id == command.transition_id
            and item.name == f"{command.source_status_id}->{command.target_status_id}"
            for item in state.transitions
        )
        or (
            state.gate is not None and command.transition_id == state.gate.transition_id
        )
        or command.source_status_id not in {item.jira_id for item in state.statuses}
        or not any(
            item.jira_id == command.target_status_id
            and item.name == command.target_status_name
            for item in state.statuses
        )
        or (
            command.target_status_name.strip().casefold()
            in {"done", "cancelled", "canceled"}
            and not (
                command.target_status_name.strip().casefold() == "done"
                and delivery_complete(state)
                and state.scope is not None
                and state.scope.verification is not None
                and command.transition_id
                == state.scope.verification.complete_transition_id
            )
        )
    ):
        raise HumanChange(
            "Delivery transition fence changed or completion is not authorized"
        )
    return command


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
    based_comment_ids: list[str] | None = None,
) -> str | None:
    """Enqueue once per instance/key, in the caller's transaction."""
    if ticket.project_key not in config.jira_worker_enabled_projects.get(
        ticket.instance, []
    ):
        return None
    if not command_key.strip():
        raise ValueError("Jira command key is required")
    _validate_command(command, data)
    if command == "transition_by_id":
        check_transition_fence(ticket, data)
    _stamp(based_updated)
    if based_comment_ids is None:
        based_comment_ids = sorted(set(ticket.jira_seen_comment_ids or []))
    payload = {
        "data": data,
        "based_updated": based_updated,
        "based_changelog_id": based_changelog_id,
        "based_comment_ids": based_comment_ids,
        "reporter_account_id": ticket.reporter_account_id,
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
    fields = (
        await client.request(
            "GET",
            path,
            params={"fields": client.issue_fields},
        )
    ).json()["fields"]
    before = fields["updated"]
    histories = await _pages(client, path + "/changelog", "values")
    comments = await _pages(client, path + "/comment", "comments")
    updated = (await client.request("GET", path, params={"fields": "updated"})).json()[
        "fields"
    ]["updated"]
    if _stamp(before) != _stamp(updated):
        raise HumanChange(ISSUE_CHANGED_DURING_READ)
    histories.sort(key=lambda item: (_stamp(item["created"]), int(item["id"])))
    return {
        "updated": updated,
        "fields": fields,
        "changelog_id": str(histories[-1]["id"]) if histories else "",
        "histories": histories,
        "comments": comments,
    }


class HumanChange(Exception):
    """A meaningful non-worker or unattributed change must park the ticket."""


HUMAN_JIRA_ACTION = "Human Jira action"
UNATTRIBUTED_JIRA_ACTION = "Unattributed Jira action"
TOKEN_LIMIT = "Ticket token limit"
ISSUE_CHANGED_DURING_READ = "Issue changed while the version check read its history"
STAMP_MISMATCH = "Jira updated stamp has no attributable worker change"
NON_WORKER_SUPERSEDED = "A non-worker Jira action superseded the command"
INTERRUPTED_TURN = "Interrupted orchestrator turn; outcome and token usage unknown"
TURN_FAILED = "Orchestrator turn failed; outcome and token usage unknown"
UNCERTAIN_REPLAY = "automatic replay is unsafe"


def needs_person(reason: str | None) -> bool:
    return bool(reason) and reason != "Jira ticket completed or cancelled"


def check_jira_version(
    version: dict[str, Any], payload: dict[str, Any], account_id: str
) -> None:
    reporter = payload.get("reporter_account_id")
    if not isinstance(reporter, str):
        reporter = ""
    known = payload.get("based_comment_ids")
    seen = {str(item) for item in known} if isinstance(known, list) else set()
    observation = observe_jira(
        version,
        seen_ids=seen,
        changelog_id=payload["based_changelog_id"],
        base_stamp=_stamp(payload["based_updated"]),
        worker_account=account_id,
        reporter_account=reporter,
    )
    if observation.park is None:
        return
    if observation.park == "changelog_missing":
        raise HumanChange("The command's changelog baseline is missing")
    if observation.park == "regressed":
        raise HumanChange("Jira version is older than the command baseline")
    if observation.park == "stamp_mismatch":
        raise HumanChange(STAMP_MISMATCH)
    raise HumanChange(NON_WORKER_SUPERSEDED)


def park_ticket(ticket: JiraWorkerTicket, reason: str) -> None:
    ticket.agent_state = "parked"
    ticket.agent_generation += 1
    ticket.wake_at = None
    ticket.worker_parked_reason = reason


def _failure_reason(command_id: str) -> str:
    return f"Jira command failed: {command_id}"


async def _still_claimed(session: AsyncSession, row: JiraWorkerOutbox) -> bool:
    fresh = await session.get(
        JiraWorkerOutbox,
        row.id,
        with_for_update=True,
        populate_existing=True,
    )
    return fresh is not None and fresh.status == "claimed"


class JiraOutboxSender:
    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        config: SwitchConfig,
        clients: dict[str, JiraPollClient],
        clock: ClockFn,
        rooms: TicketRooms | None = None,
        before_wait_send: Callable[[str], Awaitable[bool]] | None = None,
        authorize_delivery: Callable[[JiraWorkerTicket], Awaitable[bool]] | None = None,
    ) -> None:
        self._factory = session_factory
        self._engine = cast(AsyncEngine, session_factory.kw["bind"])
        self._config = config
        self._clients = clients
        self._clock = clock
        self._accounts: dict[str, str] = {}
        self._rooms = rooms
        self._before_wait_send = before_wait_send
        self._authorize_delivery = authorize_delivery

    async def run_once(self) -> None:
        scopes = [
            and_(
                JiraWorkerTicket.instance == instance,
                JiraWorkerTicket.project_key.in_(projects),
            )
            for instance, projects in self._config.jira_worker_enabled_projects.items()
            if projects
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
                            or_(
                                and_(
                                    JiraWorkerOutbox.channel == "jira",
                                    JiraWorkerOutbox.instance.in_(self._clients),
                                ),
                                and_(
                                    JiraWorkerOutbox.channel == "switch",
                                    JiraWorkerOutbox.command.in_(
                                        (
                                            "wrong_place_notice",
                                            INTAKE_QUESTION_COMMAND,
                                            WAIT_REMINDER_COMMAND,
                                        )
                                    ),
                                    literal(self._rooms is not None),
                                ),
                            ),
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
        payload = row.payload or {}
        if row.status != "sending" and (
            "delivery_question" in payload or "delivery_progress_execution" in payload
        ):
            state = workflow_state_adapter.validate_json(
                json.dumps(ticket.workflow_state)
            )
            valid = False
            if (
                isinstance(state, DeliveryWorkflowState)
                and not ticket.worker_parked_reason
            ):
                valid = True
                if "delivery_question" in payload:
                    valid = bool(
                        state.question is not None
                        and {
                            "id": state.question.id,
                            "execution_key": state.question.run.execution_key,
                        }
                        == payload["delivery_question"]
                        and (state.answer is not None) == payload["requires_answer"]
                    )
                if "delivery_progress_execution" in payload:
                    execution = await session.scalar(
                        select(JiraWorkerJob).where(
                            JiraWorkerJob.execution_key
                            == payload["delivery_progress_execution"]
                        )
                    )
                    valid = bool(
                        execution is not None
                        and execution.status == "running"
                        and state.question is None
                    )
            if not valid:
                row.status = "cancelled"
                row.error = "Delivery question or progress phase ended before send"
                await session.commit()
                return
        if row.status != "sending" and row.command == "transition_by_id":
            try:
                check_transition_fence(ticket, payload["data"])
            except HumanChange as exc:
                row.status = "cancelled"
                row.error = str(exc)
                await session.commit()
                return
        if row.status != "sending" and "scope_digest" in payload:
            state = workflow_state_adapter.validate_json(
                json.dumps(ticket.workflow_state)
            )
            if (
                not isinstance(state, DeliveryWorkflowState)
                or state.scope is None
                or digest(state.scope) != payload["scope_digest"]
            ):
                row.status = "cancelled"
                row.error = "Frozen delivery scope changed"
                await session.commit()
                return
        if row.status != "sending" and "repository_digest" in payload:
            state = workflow_state_adapter.validate_json(
                json.dumps(ticket.workflow_state)
            )
            if (
                not isinstance(state, DeliveryWorkflowState)
                or state.repository_match is None
                or state.gate is None
                or state.gate.kind != "repository"
                or state.gate.contract_digest != payload["repository_digest"]
            ):
                row.status = "cancelled"
                row.error = "Frozen repository confirmation changed"
                await session.commit()
                return
        if row.status != "sending" and "intake_generation" in payload:
            if (
                ticket.worker_parked_reason
                or payload["intake_generation"] != ticket.agent_generation
            ):
                row.status = "cancelled"
                row.error = ticket.worker_parked_reason or "Intake generation changed"
                await session.commit()
                return
            predecessor = payload.get("after_command")
            if predecessor:
                previous = await session.get(JiraWorkerOutbox, predecessor)
                if previous is None:
                    raise ValueError("Intake predecessor command is missing")
                if previous.status != "done":
                    return
        if row.channel == "switch":
            if row.command == INTAKE_QUESTION_COMMAND:
                await self._send_switch_question(session, row, ticket)
            elif row.command == WAIT_REMINDER_COMMAND:
                await self._send_switch_wait_reminder(session, row, ticket)
            else:
                await self._send_switch_notice(session, row)
            return
        if row.status == "sending":
            row.status = "uncertain"
            row.error = (
                f"Restart found a write with an unknown outcome; {UNCERTAIN_REPLAY}"
            )
            park_ticket(ticket, row.error)
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
        if self._before_wait_send is not None and (row.command_key or "").startswith(
            WAIT_COMMAND_PREFIXES
        ):
            cancelled = await self._before_wait_send(ticket.id)
            await session.refresh(ticket)
            if cancelled:
                if await _still_claimed(session, row):
                    row.status = "cancelled"
                    row.error = "Wait ended before send"
                    await session.commit()
                return
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
            ticket.jira_worker_account_id = account_id
            path = f"/rest/api/3/issue/{quote(row.issue_key, safe='')}"
            method = "POST"
            if row.command in {"transition", "transition_by_id"}:
                available = (await client.request("GET", path + "/transitions")).json()[
                    "transitions"
                ]
                if row.command == "transition_by_id":
                    exact = check_transition_fence(ticket, payload["data"])
                    matches = [
                        item["id"]
                        for item in available
                        if item.get("id") == exact.transition_id
                        and item.get("to", {}).get("id") == exact.target_status_id
                        and (
                            item.get("to", {}).get("statusCategory", {}).get("key")
                            != "done"
                            or exact.step == "close"
                        )
                    ]
                else:
                    state = workflow_state_adapter.validate_json(
                        json.dumps(ticket.workflow_state)
                    )
                    if (
                        isinstance(state, DeliveryWorkflowState)
                        and state.scope is not None
                    ):
                        raise ValueError(
                            "A frozen delivery scope requires transition_by_id"
                        )
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
            if not await _still_claimed(session, row):
                return
            if row.command == "transition_by_id":
                await session.refresh(ticket, with_for_update=True)
                exact = check_transition_fence(ticket, payload["data"])
                if (
                    exact.step in {"accept", "close"}
                    and payload.get("delivery_step") != exact.step
                ):
                    raise HumanChange(
                        "Candidate commands require fresh forge authorization"
                    )
                if (
                    version["fields"].get("status", {}).get("id")
                    != exact.source_status_id
                ):
                    raise HumanChange("Exact transition source status changed")
            if "delivery_step" in payload:
                await session.refresh(ticket, with_for_update=True)
                state = workflow_state_adapter.validate_json(
                    json.dumps(ticket.workflow_state)
                )
                if (
                    not isinstance(state, DeliveryWorkflowState)
                    or state.step != payload["delivery_step"]
                    or state.candidate_head != payload["candidate_sha"]
                    or ticket.agent_generation != payload["intake_generation"]
                    or ticket.worker_parked_reason
                    or self._authorize_delivery is None
                    or not await self._authorize_delivery(ticket)
                ):
                    raise HumanChange(
                        "Delivery candidate authorization changed or is unavailable"
                    )
            row.status = "sending"
            await session.commit()
            sending = True
            await client.request(method, path, json=body)
        except HumanChange as exc:
            if not await _still_claimed(session, row):
                return
            row.status = "parked"
            row.error = str(exc)
            park_ticket(ticket, str(exc))
        except Exception as exc:
            if not sending and not await _still_claimed(session, row):
                return
            if sending and (
                isinstance(exc, httpx.TransportError)
                or (
                    isinstance(exc, httpx.HTTPStatusError)
                    and exc.response.status_code >= 500
                )
            ):
                row.status = "uncertain"
                row.error = f"{type(exc).__name__}: Jira write outcome is unknown; {UNCERTAIN_REPLAY}"
                park_ticket(ticket, row.error)
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
                        park_ticket(ticket, _failure_reason(row.id))
                    if (
                        not payload.get("failure_of")
                        and payload.get("based_updated")
                        and "based_changelog_id" in payload
                    ):
                        await self._enqueue_failure(session, ticket, row)
        else:
            row.status = "done"
            row.error = None
            if row.command == "transition_by_id":
                if payload["data"]["step"] != "close":
                    ticket.status = payload["data"]["target_status_name"]
                state = workflow_state_adapter.validate_json(
                    json.dumps(ticket.workflow_state)
                )
                predecessor = (
                    await session.get(JiraWorkerOutbox, payload["after_command"])
                    if payload.get("after_command")
                    else None
                )
                if (
                    isinstance(state, DeliveryWorkflowState)
                    and state.scope is not None
                    and state.gate is not None
                    and payload["data"]["target_status_id"]
                    == state.scope.approval_route.source_status_id
                    and predecessor is not None
                    and predecessor.status == "done"
                    and predecessor.command == "comment"
                    and (predecessor.payload or {}).get(
                        "gate_digest"
                        if state.gate.kind == "acceptance"
                        else "scope_digest"
                    )
                    == state.gate.contract_digest
                ):
                    ticket.workflow_state = {
                        **state.model_dump(mode="json"),
                        "gate_ready": True,
                    }
                if (
                    isinstance(state, DeliveryWorkflowState)
                    and state.scope is None
                    and state.gate is not None
                    and state.gate.kind == "repository"
                    and state.repository_match is not None
                    and payload["data"]["target_status_id"]
                    == state.repository_match.approval_route.source_status_id
                    and predecessor is not None
                    and predecessor.status == "done"
                    and predecessor.command == "comment"
                    and (predecessor.payload or {}).get("repository_digest")
                    == state.gate.contract_digest
                ):
                    ticket.workflow_state = {
                        **state.model_dump(mode="json"),
                        "gate_ready": True,
                    }
                if (
                    isinstance(state, DeliveryWorkflowState)
                    and payload["data"]["step"] == "close"
                ):
                    ticket.workflow_state = {
                        **state.model_dump(mode="json"),
                        "outcome": "delivered",
                    }
            elif row.command == "transition" and "intake_generation" in payload:
                ticket.status = payload["data"]
        await session.commit()

    async def _send_switch_question(
        self, session: AsyncSession, row: JiraWorkerOutbox, ticket: JiraWorkerTicket
    ) -> None:
        if self._rooms is None:
            raise ValueError("Intake question sender requires ticket rooms")
        if row.status == "sending":
            row.status = "uncertain"
            row.error = "Restart found a question with an unknown outcome; automatic replay is unsafe"
            park_ticket(ticket, row.error)
            await session.commit()
            return
        payload = row.payload or {}
        row.attempts += 1
        try:
            client = self._clients[row.instance]
            version = await read_jira_version(client, row.issue_key)
            account = ticket.jira_worker_account_id
            if not account:
                raise ValueError("Intake question requires the Jira worker identity")
            check_jira_version(version, payload, account)
        except HumanChange as exc:
            row.status = "parked"
            row.error = str(exc)
            park_ticket(ticket, row.error)
            await session.commit()
            return
        except Exception as exc:
            row.error = f"{type(exc).__name__}: Question version check failed"
            logger.exception("Intake question %s version check failed", row.id)
            if row.attempts >= 3:
                row.status = "failed"
                park_ticket(ticket, row.error)
            else:
                row.due_at = self._clock() + timedelta(
                    seconds=self._config.jira_worker_write_backoff_seconds
                )
            await session.commit()
            return
        row.status = "sending"
        await session.commit()
        try:
            await self._rooms.post_question(
                payload["room_id"], thread_id=payload["thread_id"], body=payload["data"]
            )
        except Exception as exc:
            row.status = "uncertain"
            row.error = f"{type(exc).__name__}: Question outcome is unknown; automatic replay is unsafe"
            park_ticket(ticket, row.error)
            logger.exception("Intake question %s delivery failed", row.id)
        else:
            row.status = "done"
            row.error = None
        await session.commit()

    async def _send_switch_notice(
        self, session: AsyncSession, row: JiraWorkerOutbox
    ) -> None:
        if row.command != "wrong_place_notice" or self._rooms is None:
            raise ValueError("Switch notice sender requires ticket rooms")
        if row.status == "sending":
            row.status = "uncertain"
            row.error = "Restart found a room notice with an unknown outcome; automatic replay is unsafe"
            logger.error("Switch notice %s: %s", row.id, row.error)
            await session.commit()
            return
        payload = row.payload or {}
        row.status = "sending"
        row.attempts += 1
        await session.commit()
        try:
            await self._rooms.post_notice(payload["room_id"], body=payload["data"])
        except Exception as exc:
            row.status = "uncertain"
            row.error = f"{type(exc).__name__}: Room notice outcome is unknown; automatic replay is unsafe"
            logger.exception("Switch notice %s failed", row.id)
        else:
            row.status = "done"
            row.error = None
        await session.commit()

    async def _send_switch_wait_reminder(
        self, session: AsyncSession, row: JiraWorkerOutbox, ticket: JiraWorkerTicket
    ) -> None:
        from switch_core.bridges.jira.admin import wait_send_cancelled

        if self._rooms is None:
            raise ValueError("Wait reminder sender requires ticket rooms")
        if row.status == "sending":
            row.status = "uncertain"
            row.error = "Restart found a wait reminder with an unknown outcome; automatic replay is unsafe"
            logger.error("Wait reminder %s: %s", row.id, row.error)
            await session.commit()
            return
        cancelled = False
        if self._before_wait_send is not None:
            await session.commit()
            cancelled = await self._before_wait_send(ticket.id)
            await session.refresh(row, with_for_update=True)
            if row.status not in ("pending", "claimed"):
                return
            await session.refresh(ticket, with_for_update=True)
        payload = row.payload or {}
        anchor = ticket.wait_anchor
        anchor_text = anchor.isoformat() if anchor is not None else None
        if anchor_text is None or payload.get("wait_anchor") != anchor_text:
            row.status = "cancelled"
            row.error = "Wait anchor changed before send"
            await session.commit()
            return
        if (
            cancelled
            or ticket.project_key
            not in self._config.jira_worker_enabled_projects.get(ticket.instance, [])
            or ticket.worker_parked_reason
            or await wait_send_cancelled(session, ticket, self._config)
        ):
            row.status = "cancelled"
            row.error = ticket.worker_parked_reason or "Wait ended before send"
            await session.commit()
            return
        row.status = "sending"
        row.attempts += 1
        await session.commit()
        try:
            await self._rooms.post_notice(payload["room_id"], body=payload["data"])
        except Exception as exc:
            row.status = "uncertain"
            row.error = f"{type(exc).__name__}: Wait reminder outcome is unknown; automatic replay is unsafe"
            logger.exception("Wait reminder %s failed", row.id)
        else:
            row.status = "done"
            row.error = None
        await session.commit()

    async def _enqueue_failure(
        self, session: AsyncSession, ticket: JiraWorkerTicket, row: JiraWorkerOutbox
    ) -> None:
        payload = row.payload or {}
        state = workflow_state_adapter.validate_json(json.dumps(ticket.workflow_state))
        if row.command == "transition_by_id" or (
            isinstance(state, DeliveryWorkflowState) and state.scope is not None
        ):
            return
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
                based_comment_ids=(
                    payload["based_comment_ids"]
                    if isinstance(payload.get("based_comment_ids"), list)
                    else None
                ),
            )
            child = await session.get(JiraWorkerOutbox, command_id)
            assert child is not None
            child.payload = {**(child.payload or {}), "failure_of": row.id}
