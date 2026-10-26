"""Trusted, branch-scoped publication. Forge access never enters a worker home."""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Protocol, Self
from urllib.parse import urlsplit

from pydantic import model_validator
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from switch_core.bridges.jira.contracts import Commit, Contract, Text
from switch_core.bridges.jira.delivery_contracts import (
    Handoff,
    Publication,
    ScopeContract,
)
from switch_core.bridges.jira.target import delivery_state
from switch_core.db.models import JiraWorkerTicket


class DraftRequest(Contract):
    url: Text
    branch: Text
    base_branch: Text
    head: Commit
    draft: bool
    open: bool

    @model_validator(mode="after")
    def link_has_no_credentials(self) -> Self:
        url = urlsplit(self.url)
        if (
            url.scheme != "https"
            or not url.hostname
            or url.username is not None
            or url.password is not None
            or url.query
            or url.fragment
        ):
            raise ValueError("Draft link requires credential-free HTTPS")
        return self


class Forge(Protocol):
    """Trusted transport must use a sealed object store, not worker git config.

    push_new_branch refuses a discovered existing ref and never forces a push.
    It targets only the approved generation branch. Credentials live in this port,
    outside the worker's filesystem, process environment, and repository hooks.
    """

    async def branch_head(self, repository_url: str, branch: str) -> str | None: ...

    async def push_new_branch(
        self, repository_url: str, branch: str, head: str
    ) -> None: ...

    async def find_requests(
        self, repository_url: str, branch: str
    ) -> tuple[DraftRequest, ...]: ...

    async def create_draft(
        self,
        repository_url: str,
        branch: str,
        base_branch: str,
        head: str,
        execution_key: str,
    ) -> DraftRequest: ...

    async def verify_candidate(
        self, scope: ScopeContract, handoff: Handoff
    ) -> None: ...


class GitCandidateObjects:
    """Inspect a trusted, sealed bare object store; never execute checkout code."""

    def __init__(self, bare_repository: Path) -> None:
        if (
            not bare_repository.is_absolute()
            or bare_repository.resolve() != bare_repository
        ):
            raise ValueError(
                "Candidate object store must be absolute, without symlinks"
            )
        self._path = bare_repository

    async def _git(self, *args: str, authorization: str | None = None) -> bytes:
        environment = {
            "PATH": os.defpath,
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_NO_REPLACE_OBJECTS": "1",
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_ASKPASS": "/bin/false",
        }
        if authorization is not None:
            if any(char in authorization for char in "\r\n\0"):
                raise ValueError("Invalid forge authorization header")
            environment |= {
                "GIT_CONFIG_COUNT": "1",
                "GIT_CONFIG_KEY_0": "http.extraHeader",
                "GIT_CONFIG_VALUE_0": "Authorization: " + authorization,
            }
        process = await asyncio.create_subprocess_exec(
            "git",
            "--no-replace-objects",
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.fsmonitor=false",
            "-c",
            "credential.helper=",
            "-c",
            "http.followRedirects=false",
            "--git-dir",
            str(self._path),
            *args,
            env=environment,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            async with asyncio.timeout(30):
                output, _ = await process.communicate()
            if process.returncode:
                raise ValueError("Candidate object verification failed")
            return output
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()

    async def verify(self, scope: ScopeContract, handoff: Handoff) -> None:
        if (await self._git("rev-parse", "--is-bare-repository")).strip() != b"true":
            raise ValueError("Publisher requires a sealed bare object store")
        base, test, fix = (
            scope.job.repository.base_sha,
            handoff.regression_commit,
            handoff.fix_commit,
        )
        if (
            base is None
            or test is None
            or fix is None
            or fix != handoff.head
            or len({base, test, fix}) != 3
        ):
            raise ValueError("Candidate requires separate regression and fix commits")
        for commit in (base, test, fix):
            if (await self._git("cat-file", "-t", commit)).strip() != b"commit":
                raise ValueError("Candidate evidence must identify commit objects")
        await self._git("merge-base", "--is-ancestor", base, test)
        await self._git("merge-base", "--is-ancestor", test, fix)
        paths = (
            (
                await self._git(
                    "diff",
                    "--no-ext-diff",
                    "--no-textconv",
                    "--name-only",
                    "-z",
                    base,
                    fix,
                )
            )
            .decode()
            .rstrip("\0")
            .split("\0")
        )
        if not paths[0] or set(paths) != set(handoff.changed_paths):
            raise ValueError("Candidate changed paths do not match the sealed objects")
        if not (
            await self._git(
                "diff", "--no-ext-diff", "--no-textconv", "--name-only", test, fix
            )
        ).strip():
            raise ValueError(
                "Fix commit contains no change after the regression commit"
            )


@asynccontextmanager
async def delivery_lock(
    sessions: async_sessionmaker[AsyncSession], ticket_id: str, operation: str
) -> AsyncIterator[bool]:
    async with sessions() as session:
        key = f"jira-delivery:{operation}:{ticket_id}"
        acquired = await session.scalar(
            text("SELECT pg_try_advisory_lock(hashtextextended(:key, 0))"), {"key": key}
        )
        try:
            yield bool(acquired)
        finally:
            if acquired:
                await session.execute(
                    text("SELECT pg_advisory_unlock(hashtextextended(:key, 0))"),
                    {"key": key},
                )


class PublicationUncertain(RuntimeError):
    """A remote write needs discovery before another publication decision."""


class DraftPublisher:
    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        forge: Forge,
        authorize: Callable[[str], Awaitable[bool]],
    ) -> None:
        self._sessions = session_factory
        self._forge = forge
        self._authorize = authorize

    async def _current(
        self, ticket_id: str
    ) -> tuple[ScopeContract, Handoff, Publication | None]:
        async with self._sessions() as session:
            ticket = await session.get(JiraWorkerTicket, ticket_id)
            if ticket is None:
                raise ValueError("Publication ticket is missing")
            state = delivery_state(ticket)
            if (
                ticket.worker_parked_reason
                or state is None
                or state.scope is None
                or state.candidate is None
                or state.step != "fix"
                or state.unknown_spend
                or state.scope.job.run.generation != ticket.agent_generation
                or state.candidate_head != state.candidate.head
                or not state.approvals
            ):
                raise ValueError("Current worker authorization forbids publication")
            return state.scope, state.candidate, state.publication

    async def _save(self, ticket_id: str, publication: Publication) -> None:
        async with self._sessions() as session:
            ticket = await session.get(
                JiraWorkerTicket, ticket_id, with_for_update=True
            )
            assert ticket is not None
            state = delivery_state(ticket)
            if (
                state is None
                or state.scope is None
                or state.candidate is None
                or ticket.worker_parked_reason
                or ticket.agent_generation != state.scope.job.run.generation
                or state.candidate.head != publication.head
            ):
                raise ValueError("Publication authorization changed")
            data = state.model_dump(mode="json")
            data["publication"] = publication.model_dump(mode="json")
            ticket.workflow_state = data
            await session.commit()

    async def _authorized(
        self, ticket_id: str, scope: ScopeContract, head: str
    ) -> None:
        if not await self._authorize(ticket_id):
            raise RuntimeError(
                "Fresh worker authorization is unavailable; publication is deferred"
            )
        current, candidate, _ = await self._current(ticket_id)
        if current != scope or candidate.head != head:
            raise ValueError("Frozen publication scope changed")

    async def publish(self, ticket_id: str) -> None:
        async with delivery_lock(self._sessions, ticket_id, "publish") as acquired:
            if not acquired:
                return
            scope, candidate, previous = await self._current(ticket_id)
            await self._authorized(ticket_id, scope, candidate.head)
            await self._forge.verify_candidate(scope, candidate)
            repository = scope.job.repository
            assert (
                repository.canonical_url is not None
                and repository.base_branch is not None
            )
            url, branch, head = repository.canonical_url, scope.branch, candidate.head
            key = f"{scope.job.run.ticket_id}/{scope.job.run.generation}/fix/1"
            if previous is not None and (
                previous.head != head or previous.execution_key != key
            ):
                raise ValueError(
                    "Publication intent does not match the frozen candidate"
                )
            remote = await self._forge.branch_head(url, branch)
            requests = await self._forge.find_requests(url, branch)
            if remote is not None and remote != head:
                raise ValueError("Approved branch already exists with another head")
            if len(requests) > 1:
                raise ValueError("Multiple requests exist for the approved branch")
            if requests:
                request = requests[0]
                if (
                    remote != head
                    or request.branch != branch
                    or request.base_branch != repository.base_branch
                    or request.head != head
                    or not request.draft
                    or not request.open
                ):
                    raise ValueError("Existing request differs from the approved draft")
                await self._save(
                    ticket_id,
                    Publication(
                        execution_key=key,
                        head=head,
                        status="published",
                        pr_url=request.url,
                    ),
                )
                return
            # A timeout or process crash is not evidence that a remote write failed.
            if previous is not None and previous.status in {
                "pushing",
                "creating",
                "uncertain",
                "published",
            }:
                operation = previous.uncertain_operation or (
                    "push" if previous.status == "pushing" else "create"
                )
                if operation != "push":
                    await self._save(
                        ticket_id,
                        Publication(
                            execution_key=key,
                            head=head,
                            status="uncertain",
                            uncertain_operation=operation,
                        ),
                    )
                    return
            if remote is None:
                await self._authorized(ticket_id, scope, head)
                await self._save(
                    ticket_id,
                    Publication(execution_key=key, head=head, status="pushing"),
                )
                try:
                    await self._forge.push_new_branch(url, branch, head)
                except Exception as exc:
                    await self._save(
                        ticket_id,
                        Publication(
                            execution_key=key,
                            head=head,
                            status="uncertain",
                            uncertain_operation="push",
                        ),
                    )
                    raise PublicationUncertain(
                        "Branch push requires reconciliation"
                    ) from exc
                if await self._forge.branch_head(url, branch) != head:
                    raise ValueError("Branch publication requires reconciliation")
            await self._authorized(ticket_id, scope, head)
            await self._save(
                ticket_id, Publication(execution_key=key, head=head, status="creating")
            )
            try:
                request = await self._forge.create_draft(
                    url, branch, repository.base_branch, head, key
                )
            except Exception as exc:
                await self._save(
                    ticket_id,
                    Publication(
                        execution_key=key,
                        head=head,
                        status="uncertain",
                        uncertain_operation="create",
                    ),
                )
                raise PublicationUncertain(
                    "Draft creation requires reconciliation"
                ) from exc
            if (
                request.branch != branch
                or request.base_branch != repository.base_branch
                or request.head != head
                or not request.draft
                or not request.open
            ):
                raise ValueError(
                    "Forge returned a request outside the approved draft contract"
                )
            await self._authorized(ticket_id, scope, head)
            await self._save(
                ticket_id,
                Publication(
                    execution_key=key, head=head, status="published", pr_url=request.url
                ),
            )
