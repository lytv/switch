"""Local Selected-D2 integration. Switch owns admission, metering and publication."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import subprocess
import tempfile
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote, urlsplit

import httpx
from pydantic import Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from switch_core.bridges.jira.activity import TicketActivity
from switch_core.bridges.jira.contracts import (
    Contract,
    Gate,
    Job,
    Meter,
    ModelPrices,
    Pack,
    Repository,
    Reservation,
    Result,
    Run,
    Text,
    digest,
    repository_identity,
)
from switch_core.bridges.jira.delivery import (
    DeliverySupervisor,
    replace_state,
    stage_scope,
)
from switch_core.bridges.jira.delivery_contracts import (
    ExecutionEnvelope,
    Handoff,
    ScopeContract,
    ScopePolicy,
)
from switch_core.bridges.jira.firstmate_copy import (
    FirstmateCopy,
    SecretGuard,
    TaskObservation,
    _create,
    _directory,
    _read,
)
from switch_core.bridges.jira.firstmate_jobs import Execution, FirstmateJobs
from switch_core.bridges.jira.forge import GitHubDraftForge
from switch_core.bridges.jira.model_gateway import model_capability, model_identity
from switch_core.bridges.jira.packs import load_pack
from switch_core.bridges.jira.publisher import (
    DraftPublisher,
    DraftRequest,
    GitCandidateObjects,
)
from switch_core.bridges.jira.state import WorkflowMapping
from switch_core.bridges.jira.target import delivery_state, record_repository_triage
from switch_core.bridges.jira.verification import MergeReceipt
from switch_core.config import JiraWorkerDeliveryRuntime, SwitchConfig
from switch_core.db.models import JiraWorkerEvent, JiraWorkerJob, JiraWorkerTicket


def model_prices(runtime: JiraWorkerDeliveryRuntime) -> ModelPrices:
    return ModelPrices.model_validate_json(runtime.prices.model_dump_json())


class AdmissionPolicy(Contract):
    scope: ScopePolicy
    statuses: tuple[WorkflowMapping, ...]
    transitions: tuple[WorkflowMapping, ...]
    approver_account_ids: tuple[Text, ...] = Field(min_length=1)


def runtime_config(config: SwitchConfig) -> JiraWorkerDeliveryRuntime:
    runtime = config.jira_worker_delivery_runtime
    if runtime is None:
        raise ValueError("Configure JIRA_WORKER_DELIVERY_RUNTIME before live execution")
    if not runtime.forge_authorization.get_secret_value().strip():
        raise ValueError(
            "Live publication requires supervisor-only forge authorization"
        )
    pack = load_pack(config).snapshot
    if pack.fixture_only:
        raise ValueError(
            "Live execution requires the immutable non-fixture delivery pack"
        )
    if not config.jira_worker_llm_base_url or not config.jira_worker_llm_api_key:
        raise ValueError("Live model access requires the configured LLM upstream")
    for endpoint in (
        runtime.forge_api_url,
        config.jira_worker_llm_base_url,
        runtime.model_url,
    ):
        url = urlsplit(endpoint)
        if (
            url.scheme not in {"http", "https"}
            or not url.hostname
            or url.username
            or url.password
            or url.query
            or url.fragment
            or (
                url.scheme != "https"
                and url.hostname not in {"localhost", "127.0.0.1", "::1"}
            )
        ):
            raise ValueError(
                "Delivery endpoints require credential-free HTTPS, or loopback HTTP"
            )
    for root in (runtime.checkout, runtime.homes, runtime.repositories):
        if not root.is_absolute() or root.resolve() != root or not root.is_dir():
            raise ValueError("Provision absolute, symlink-free delivery roots")
    roots = (runtime.checkout, runtime.homes, runtime.repositories)
    if any(
        left == right or left in right.parents or right in left.parents
        for index, left in enumerate(roots)
        for right in roots[index + 1 :]
    ):
        raise ValueError(
            "Firstmate, worker homes and trusted repositories require separate roots"
        )
    if (
        _read(runtime.checkout, ".switch-delivery-copy")
        != b"switch-firstmate-delivery-v1\n"
        or _read(runtime.homes, ".switch-delivery-workers")
        != b"switch-firstmate-delivery-v1\n"
    ):
        raise ValueError("Provision the dedicated Firstmate and worker-root markers")
    if (
        _read(runtime.repositories, ".switch-delivery-repositories")
        != b"switch-firstmate-delivery-v1\n"
    ):
        raise ValueError("Provision the trusted repository-root marker")
    probe_home = tempfile.TemporaryDirectory()
    environment = {
        "PATH": runtime.executable_path,
        "HOME": probe_home.name,
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": os.devnull,
    }
    try:
        revision = subprocess.check_output(
            ["git", "-C", str(runtime.checkout), "rev-parse", "HEAD"],
            env=environment,
            stderr=subprocess.DEVNULL,
            timeout=10,
            text=True,
        ).strip()
        version = subprocess.check_output(
            ["pi", "--version"],
            env=environment,
            stderr=subprocess.DEVNULL,
            timeout=10,
            text=True,
        ).strip()
    except (subprocess.SubprocessError, OSError) as exc:
        raise ValueError("The pinned Firstmate and pi runtime are unavailable") from exc
    finally:
        probe_home.cleanup()
    if revision != pack.firstmate_commit or version != pack.harness_version:
        raise ValueError("Firstmate or pi differs from the immutable live pack pin")
    return runtime


def load_policies(
    config: SwitchConfig, runtime: JiraWorkerDeliveryRuntime
) -> dict[tuple[str, str], AdmissionPolicy]:
    policies = {}
    prices = model_prices(runtime)
    for instance, projects in config.jira_worker_delivery_projects.items():
        for project in projects:
            path = runtime.policies.get(instance, {}).get(project)
            if path is None or not path.is_absolute() or path.resolve() != path:
                raise ValueError(
                    "Each delivery project requires an absolute trusted policy file"
                )
            content = _read(path.parent, path.name)
            if content is None:
                raise ValueError("Delivery policy file is missing")
            policy = AdmissionPolicy.model_validate_json(content)
            verification = policy.scope.verification
            if verification is None:
                raise ValueError(
                    "Live policy requires independent review, QA and smoke"
                )
            roles = (
                policy.scope.reproduction_reservation,
                policy.scope.fix_reservation,
                verification.review.reservation,
                verification.qa.reservation,
                verification.smoke.reservation,
            )
            if any(
                item.price_source != prices.source
                or item.estimated_usd < item.tokens * prices.maximum
                for item in roles
            ):
                raise ValueError(
                    "Role reservations must cover the configured worst-case price"
                )
            policies[instance, project] = policy
    return policies


class LocalRepositories:
    def __init__(self, runtime: JiraWorkerDeliveryRuntime) -> None:
        self.runtime = runtime

    def root(self, job: Job) -> Path:
        identity = f"{job.run.instance}/{job.run.ticket_id}/{job.run.generation}"
        return self.runtime.repositories / hashlib.sha256(identity.encode()).hexdigest()

    def objects(self, job: Job) -> GitCandidateObjects:
        return GitCandidateObjects(self.root(job) / "objects.git")

    def project(self, job: Job) -> Path:
        return self.root(job) / model_identity(job) / "project"

    def base(self, job: Job) -> Path:
        return self.root(job) / model_identity(job) / "base"

    async def _git(self, *args: str) -> str:
        process = await asyncio.create_subprocess_exec(
            "git",
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.fsmonitor=false",
            "-c",
            "credential.helper=",
            *args,
            env={
                "PATH": self.runtime.executable_path,
                "GIT_CONFIG_NOSYSTEM": "1",
                "GIT_CONFIG_GLOBAL": os.devnull,
                "GIT_TERMINAL_PROMPT": "0",
                "GIT_ASKPASS": "/bin/false",
            },
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            async with asyncio.timeout(120):
                output, _ = await process.communicate()
            if process.returncode:
                raise ValueError("Trusted repository preparation failed")
            return output.decode().strip()
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()

    async def prepare(self, job: Job, envelope: ExecutionEnvelope) -> None:
        _directory(self.root(job))
        objects = self.objects(job)
        if not objects._path.exists():
            await self._git("init", "--bare", str(objects._path))
        target = envelope.target_sha or job.repository.base_sha
        assert target is not None and job.repository.canonical_url is not None
        host, port, path = repository_identity(job.repository.canonical_url)
        if host != self.runtime.forge_repository_host or port is not None:
            raise ValueError("Repository host differs from the trusted forge")
        remote = f"https://{host}{path}.git"
        for sha in dict.fromkeys((job.repository.base_sha, target)):
            assert sha is not None
            await objects._git(
                "fetch",
                "--no-tags",
                "--",
                remote,
                f"{sha}:refs/heads/pin-{sha}",
                authorization=self.runtime.forge_authorization.get_secret_value(),
            )
        _directory(self.project(job).parent)
        for project, sha in (
            (self.project(job), target),
            *(
                [(self.base(job), job.repository.base_sha)]
                if job.run.step == "qa"
                else []
            ),
        ):
            if project.exists():
                if project.resolve() != project:
                    raise ValueError("A prepared role checkout contains a symlink")
                continue
            await self._git(
                "clone",
                "--no-local",
                "--no-hardlinks",
                "--no-checkout",
                "--",
                str(objects._path),
                str(project),
            )
            await self._git(
                "-C",
                str(project),
                "remote",
                "set-url",
                "origin",
                job.repository.canonical_url,
            )
            await self._git(
                "-C", str(project), "config", "user.name", "Switch delivery"
            )
            await self._git(
                "-C", str(project), "config", "user.email", "delivery@example.invalid"
            )
            if project == self.base(job) or job.run.step in {"review", "qa", "smoke"}:
                await self._git("-C", str(project), "checkout", "--detach", sha or "")
            else:
                await self._git(
                    "-C",
                    str(project),
                    "checkout",
                    "-b",
                    envelope.scope.branch,
                    sha or "",
                )

    async def import_candidate(
        self, scope: ScopeContract, handoff: Handoff, job: Job
    ) -> None:
        project = self.project(job)
        if project.resolve() != project:
            raise ValueError("Candidate checkout contains a symlink")
        # Git ignores pack-objects hooks in repository config, so this fetch runs no worker hook.
        await self.objects(job)._git(
            "fetch",
            "--no-tags",
            "--",
            str(project),
            f"{handoff.head}:refs/heads/pin-{handoff.head}",
        )
        await self.objects(job).verify(scope, handoff)


class MeteredFirstmateCopy(FirstmateCopy):
    def __init__(
        self,
        *,
        sessions: async_sessionmaker[AsyncSession],
        config: SwitchConfig,
        repositories: LocalRepositories,
        job: Job,
        secrets: SecretGuard,
    ) -> None:
        runtime = repositories.runtime
        self._sessions, self._config, self._repositories = (
            sessions,
            config,
            repositories,
        )
        super().__init__(
            checkout=runtime.checkout,
            homes=runtime.homes,
            project=repositories.root(job),
            executable_path=runtime.executable_path,
            secrets=secrets,
            projects=repositories.project,
            base_project=repositories.base(job),
        )

    def _prepare(
        self,
        job: Job,
        pack: Pack,
        reservation: Reservation,
        estimate_seconds: int,
        envelope: ExecutionEnvelope | None,
    ) -> bool:
        fresh = super()._prepare(job, pack, reservation, estimate_seconds, envelope)
        root = self.location(job) / "os-home/.pi/agent"
        _directory(root)
        identity = model_identity(job)
        runtime = self._repositories.runtime
        models = {
            "providers": {
                pack.provider: {
                    "baseUrl": runtime.model_url.rstrip("/")
                    + f"/integrations/jira/delivery/model/{identity}/v1",
                    "api": "openai-completions",
                    "apiKey": model_capability(self._config, identity),
                    "models": [
                        {
                            "id": pack.model,
                            "contextWindow": 1047576,
                            "maxTokens": 16384,
                            "compat": {
                                "maxTokensField": "max_tokens",
                                "supportsStore": False,
                                "supportsDeveloperRole": False,
                                "supportsReasoningEffort": False,
                            },
                        }
                    ],
                }
            }
        }
        for name, value in (
            ("models.json", models),
            ("settings.json", {"defaultProjectTrust": "never"}),
        ):
            content = json.dumps(value, sort_keys=True).encode()
            if not _create(root / name, content) and _read(root, name) != content:
                raise ValueError("Frozen worker model configuration changed")
        return fresh

    async def get(self, job: Job) -> TaskObservation:
        observed = await super().get(job)
        async with self._sessions() as session:
            row = await session.scalar(
                select(JiraWorkerJob).where(
                    JiraWorkerJob.execution_key == job.run.execution_key
                )
            )
            if row is None:
                raise ValueError("Metered role execution is missing")
            execution = Execution.model_validate_json(json.dumps(row.payload))
            if execution.meter is None:
                raise ValueError("Trusted model meter is missing")
            meter = execution.meter
            usage = meter.usage(execution.pack)
            expired = (
                meter.pending_since is not None
                and (
                    datetime.now(UTC) - datetime.fromisoformat(meter.pending_since)
                ).total_seconds()
                > 130
            )
            if meter.uncertain or (meter.pending and expired):
                observed = observed.model_copy(
                    update={
                        "result": Result(
                            run=job.run,
                            input_digest=digest(job),
                            status="uncertain",
                            question=None,
                            usage=usage,
                        )
                    }
                )
        return observed.model_copy(
            update={
                "usage": usage,
                "result": observed.result.model_copy(update={"usage": usage})
                if observed.result is not None
                else None,
            }
        )

    async def cancel(self, job: Job) -> None:
        async with self._sessions() as session:
            row = await session.scalar(
                select(JiraWorkerJob)
                .where(JiraWorkerJob.execution_key == job.run.execution_key)
                .with_for_update()
            )
            if row is None:
                raise ValueError("Metered role execution is missing")
            execution = Execution.model_validate_json(json.dumps(row.payload))
            if execution.meter is not None:
                row.payload = execution.model_copy(
                    update={
                        "meter": execution.meter.model_copy(update={"stopped": True})
                    }
                ).model_dump(mode="json")
            await session.commit()
        await super().cancel(job)


class LiveCopy:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        config: SwitchConfig,
        repositories: LocalRepositories,
        secrets: SecretGuard,
    ) -> None:
        self.sessions, self.config, self.repositories, self.secrets = (
            sessions,
            config,
            repositories,
            secrets,
        )

    def copy(self, job: Job) -> MeteredFirstmateCopy:
        return MeteredFirstmateCopy(
            sessions=self.sessions,
            config=self.config,
            repositories=self.repositories,
            job=job,
            secrets=self.secrets,
        )

    async def start(
        self,
        job: Job,
        pack: Pack,
        reservation: Reservation,
        estimate_seconds: int,
        *,
        envelope: ExecutionEnvelope | None = None,
    ) -> TaskObservation:
        if envelope is None or pack.fixture_only:
            raise ValueError(
                "Live execution requires a non-fixture pack and approved envelope"
            )
        # Preparation precedes the native dispatch marker; it never uses ticket shell text.
        home = self.repositories.runtime.homes / model_identity(job)
        task = f"data/jira-{model_identity(job)[:32]}/dispatch"
        if not home.exists() or _read(home, task) is None:
            await self.repositories.prepare(job, envelope)
        copy = self.copy(job)
        if copy.location(job) != home or f"data/{copy.task_id(job)}/dispatch" != task:
            raise ValueError("Native task identity differs from the metered identity")
        async with self.sessions() as session:
            row = await session.scalar(
                select(JiraWorkerJob)
                .where(JiraWorkerJob.execution_key == job.run.execution_key)
                .with_for_update()
            )
            assert row is not None
            execution = Execution.model_validate_json(json.dumps(row.payload))
            if execution.meter is None:
                row.payload = execution.model_copy(
                    update={
                        "meter": Meter(prices=model_prices(self.repositories.runtime)),
                        "model_identity": model_identity(job),
                    }
                ).model_dump(mode="json")
            await session.commit()
        return await copy.start(
            job, pack, reservation, estimate_seconds, envelope=envelope
        )

    async def get(self, job: Job) -> TaskObservation:
        return await self.copy(job).get(job)

    async def cancel(self, job: Job) -> None:
        await self.copy(job).cancel(job)


class LiveForge:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        repositories: LocalRepositories,
    ) -> None:
        self.sessions, self.repositories = sessions, repositories

    async def call[T](
        self,
        repository: str,
        identity: str,
        operation: Callable[[GitHubDraftForge], Awaitable[T]],
    ) -> T:
        async with self.sessions() as session:
            tickets = await session.scalars(
                select(JiraWorkerTicket).where(
                    JiraWorkerTicket.workflow_state["policy"].astext == "delivery"
                )
            )
            matches = [
                (ticket, state)
                for ticket in tickets
                if (state := delivery_state(ticket)) is not None
                and state.scope is not None
                and state.scope.job.repository.canonical_url == repository
                and (
                    state.scope.branch == identity
                    or (
                        state.publication is not None
                        and state.publication.pr_url == identity
                    )
                )
            ]
            if len(matches) != 1:
                raise ValueError("Forge operation requires one frozen delivery scope")
            _, state = matches[0]
            assert state.scope is not None
        runtime = self.repositories.runtime
        async with httpx.AsyncClient(
            base_url=runtime.forge_api_url, timeout=30, trust_env=False
        ) as client:
            forge = GitHubDraftForge(
                scope=state.scope,
                objects=self.repositories.objects(state.scope.job),
                client=client,
                authorization=runtime.forge_authorization.get_secret_value(),
            )
            return await operation(forge)

    async def branch_head(self, repository_url: str, branch: str) -> str | None:
        return await self.call(
            repository_url,
            branch,
            lambda forge: forge.branch_head(repository_url, branch),
        )

    async def push_new_branch(
        self, repository_url: str, branch: str, head: str
    ) -> None:
        await self.call(
            repository_url,
            branch,
            lambda forge: forge.push_new_branch(repository_url, branch, head),
        )

    async def find_requests(
        self, repository_url: str, branch: str
    ) -> tuple[DraftRequest, ...]:
        return await self.call(
            repository_url,
            branch,
            lambda forge: forge.find_requests(repository_url, branch),
        )

    async def create_draft(
        self,
        repository_url: str,
        branch: str,
        base_branch: str,
        head: str,
        execution_key: str,
    ) -> DraftRequest:
        return await self.call(
            repository_url,
            branch,
            lambda forge: forge.create_draft(
                repository_url, branch, base_branch, head, execution_key
            ),
        )

    async def verify_candidate(self, scope: ScopeContract, handoff: Handoff) -> None:
        async with self.sessions() as session:
            ticket = await session.get(JiraWorkerTicket, scope.job.run.ticket_id)
            state = delivery_state(ticket) if ticket else None
            if state is None or state.candidate_job is None or state.scope != scope:
                raise ValueError("Candidate requires its accepted fix execution")
            job = state.candidate_job
        await self.repositories.import_candidate(scope, handoff, job)
        await self.call(
            scope.job.repository.canonical_url or "",
            scope.branch,
            lambda forge: forge.verify_candidate(scope, handoff),
        )

    async def update_branch(
        self, repository_url: str, branch: str, previous: str, head: str
    ) -> None:
        await self.call(
            repository_url,
            branch,
            lambda forge: forge.update_branch(repository_url, branch, previous, head),
        )

    async def ready_request(
        self, repository_url: str, request_url: str, head: str
    ) -> None:
        await self.call(
            repository_url,
            request_url,
            lambda forge: forge.ready_request(repository_url, request_url, head),
        )

    async def merge_receipt(
        self, repository_url: str, request_url: str
    ) -> MergeReceipt:
        return await self.call(
            repository_url,
            request_url,
            lambda forge: forge.merge_receipt(repository_url, request_url),
        )


class LiveAdmission:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        runtime: JiraWorkerDeliveryRuntime,
        policies: dict[tuple[str, str], AdmissionPolicy],
    ) -> None:
        self.sessions, self.runtime, self.policies = sessions, runtime, policies

    async def triage(self, supplied: str) -> Repository:
        host, port, path = repository_identity(supplied)
        parts = path.lstrip("/").split("/")
        if (
            host != self.runtime.forge_repository_host
            or port is not None
            or len(parts) != 2
        ):
            raise ValueError(
                "This live pack supports repositories on the configured GitHub-compatible host"
            )
        api = "/repos/" + "/".join(quote(part, safe="") for part in parts)
        async with httpx.AsyncClient(
            base_url=self.runtime.forge_api_url,
            headers={
                "Authorization": self.runtime.forge_authorization.get_secret_value()
            },
            timeout=30,
            trust_env=False,
            follow_redirects=False,
        ) as client:

            async def read(path: str) -> dict[str, object]:
                response = await client.get(path)
                if response.is_redirect or 400 <= response.status_code < 500:
                    # Transient 5xx and transport errors retry; a refusal parks the ticket.
                    raise ValueError(
                        f"Trusted forge refused repository triage ({response.status_code})"
                    )
                response.raise_for_status()
                body = response.json()
                if not isinstance(body, dict):
                    raise ValueError("Trusted forge returned an invalid triage body")
                return body

            body = await read(api)
            canonical, branch = body.get("clone_url"), body.get("default_branch")
            if not isinstance(canonical, str) or not isinstance(branch, str):
                raise ValueError("Trusted forge omitted the canonical repository")
            if repository_identity(canonical) != repository_identity(supplied):
                raise ValueError("Forge canonical identity differs from the ticket")
            sha = (await read(api + "/commits/" + quote(branch, safe=""))).get("sha")
            if not isinstance(sha, str):
                raise ValueError("Trusted forge omitted the base commit")
        return Repository(
            supplied_url=supplied,
            canonical_url=canonical,
            base_branch=branch,
            base_sha=sha,
            outcome="verified",
            evidence=f"Trusted forge repository and default-branch commit reads: {api}, {branch}, {sha}",
        )

    async def __call__(self, ticket_id: str) -> None:
        async with self.sessions() as session:
            ticket = await session.get(JiraWorkerTicket, ticket_id)
            state = delivery_state(ticket) if ticket else None
            if (
                ticket is None
                or state is None
                or state.service != "Bug"
                or state.step != "intake"
                or ticket.worker_parked_reason
            ):
                return
            if state.pack.snapshot.fixture_only:
                raise ValueError("A fixture ticket cannot enter the live supervisor")
            if state.repository is None:
                raise ValueError("Bug delivery requires exactly one repository URL")
            repository, generation = state.repository, ticket.agent_generation
        verified = (
            await self.triage(repository.supplied_url)
            if repository.outcome != "verified"
            else None
        )
        # One transaction: intake never observes a verified target without its scope.
        async with self.sessions() as session:
            if verified is not None:
                await record_repository_triage(
                    session,
                    ticket_id=ticket_id,
                    generation=generation,
                    repository=verified,
                )
            ticket = await session.get(
                JiraWorkerTicket,
                ticket_id,
                with_for_update=True,
                populate_existing=True,
            )
            state = delivery_state(ticket) if ticket else None
            if (
                ticket is None
                or state is None
                or state.step != "intake"
                or ticket.worker_parked_reason
                or ticket.agent_generation != generation
            ):
                return
            intake = ticket.intake_state or {}
            summary = next(
                (
                    item["complete"]
                    for item in reversed(intake.get("history", []))
                    if "complete" in item
                ),
                None,
            )
            if not summary or intake.get("phase") not in {"triage", "scope_ready"}:
                await session.commit()
                return
            assert state.repository is not None
            policy = self.policies[ticket.instance, ticket.project_key]
            run = Run(
                instance=ticket.instance,
                ticket_id=ticket.id,
                generation=generation,
                step="reproduce",
                attempt=1,
                execution_key=f"{ticket.id}/{generation}/reproduce/1",
            )
            job = Job(
                schema_version=1,
                run=run,
                pack_digest=state.pack.digest,
                service="Bug",
                repository=state.repository,
                acceptance=(summary,),
            )
            scope = ScopeContract(
                **policy.scope.model_dump(),
                job=job,
                branch=f"switch/{ticket.id}/{generation}",
            )
            replace_state(
                ticket,
                state,
                statuses=[item.model_dump(mode="json") for item in policy.statuses],
                transitions=[
                    item.model_dump(mode="json") for item in policy.transitions
                ],
            )
            gate = Gate(
                run=run,
                id=f"scope:{ticket.id}:{generation}",
                kind="scope",
                contract_digest=digest(scope),
                transition_id=scope.approval_route.transition_id,
                candidate_sha=None,
                approver_account_ids=policy.approver_account_ids,
            )
            stage_scope(ticket, scope, gate)
            ticket.intake_state = {**intake, "phase": "scope_ready"}
            ticket.agent_state = "wake_requested"
            session.add(
                JiraWorkerEvent(
                    instance=ticket.instance,
                    issue_key=ticket.issue_key,
                    project_key=ticket.project_key,
                    idempotency_key=f"live-scope:{ticket.id}:{generation}",
                    event_kind="explicit_input",
                    webhook_event="worker:scope_ready",
                    payload={"reporter_input": True},
                )
            )
            await session.commit()


def build_delivery_factory(
    *, session_factory: async_sessionmaker[AsyncSession], config: SwitchConfig
) -> Callable[[TicketActivity], DeliverySupervisor] | None:
    if not config.jira_worker_delivery_execution_enabled:
        return None
    runtime = runtime_config(config)
    policies = load_policies(config, runtime)
    if not any(
        project in config.jira_worker_enabled_projects.get(instance, [])
        and instance in config.jira_worker_credentials
        for instance, project in policies
    ):
        raise ValueError(
            "Live execution requires an enabled delivery project with Jira credentials"
        )
    values = [
        config.db_password,
        config.jwt_secret_key,
        config.agent_registration_token,
        config.gateway_admin_password,
        runtime.forge_authorization.get_secret_value(),
        *config.jira_webhook_secrets.values(),
        *(item.api_token for item in config.jira_worker_credentials.values()),
    ]
    if config.jira_worker_llm_api_key:
        values.append(config.jira_worker_llm_api_key.get_secret_value())
    secrets = SecretGuard(forbidden_values=tuple(value for value in values if value))
    for policy in policies.values():
        secrets.check(policy.model_dump_json())
    repositories = LocalRepositories(runtime)
    jobs = FirstmateJobs(
        session_factory=session_factory,
        delivery_copy=LiveCopy(session_factory, config, repositories, secrets),
        secrets=secrets,
        clock=lambda: datetime.now(UTC),
    )
    admission = LiveAdmission(session_factory, runtime, policies)

    def factory(activity: TicketActivity) -> DeliverySupervisor:
        async def authorize(ticket_id: str) -> bool:
            if not await activity.read_ticket(ticket_id, force_jira=True):
                return False
            async with session_factory() as session:
                ticket = await session.get(JiraWorkerTicket, ticket_id)
                return bool(
                    ticket is not None
                    and not ticket.worker_parked_reason
                    and config.jira_worker_delivery_execution_enabled
                    and ticket.project_key
                    in config.jira_worker_enabled_projects.get(ticket.instance, [])
                    and ticket.project_key
                    in config.jira_worker_delivery_projects.get(ticket.instance, {})
                )

        publisher = DraftPublisher(
            session_factory=session_factory,
            forge=LiveForge(session_factory, repositories),
            authorize=authorize,
        )
        return DeliverySupervisor(
            session_factory=session_factory,
            config=config,
            activity=activity,
            jobs=jobs,
            publisher=publisher,
            clock=lambda: datetime.now(UTC),
            admission=admission,
        )

    return factory
