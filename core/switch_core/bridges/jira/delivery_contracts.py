"""Bounded execution envelopes and separate evidence handoffs for the Switch profile."""

from __future__ import annotations

import hashlib
import re
from pathlib import PurePosixPath
from typing import Any, Literal, Self

from pydantic import (
    Field,
    SerializerFunctionWrapHandler,
    model_serializer,
    model_validator,
)

from switch_core.bridges.jira.contracts import (
    Answer,
    Budget,
    Commit,
    Contract,
    Digest,
    Job,
    Positive,
    Reservation,
    Text,
    digest,
)


class ApprovedCheck(Contract):
    id: Text
    argv: tuple[Text, ...] = Field(min_length=1)
    expected: Text
    kind: Literal["command", "behavior"] = "command"

    @model_serializer(mode="wrap")
    def legacy_shape(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        data: dict[str, Any] = handler(self)
        if self.kind == "command":
            data.pop("kind")
        return data


class ApprovalRoute(Contract):
    source_status_id: Text
    target_status_id: Text
    transition_id: Text
    unique_edge_verified: Literal[True]
    evidence: Text


class RoleProfile(Contract):
    commit: Commit
    content: Text
    sha256: Digest

    @model_validator(mode="after")
    def content_matches_pin(self) -> Self:
        import_digest = hashlib.sha256(self.content.encode()).hexdigest()
        if self.commit == "0" * 40:
            raise ValueError("Role profile requires an exact reviewed commit")
        if self.sha256 != import_digest:
            raise ValueError("Role profile content does not match its digest")
        return self


class VerificationRole(Contract):
    profile: RoleProfile
    reservation: Reservation
    estimate_seconds: Positive


class VerificationContract(Contract):
    review: VerificationRole
    qa: VerificationRole
    smoke: VerificationRole
    behavior_check_ids: tuple[Text, ...] = Field(min_length=1)
    smoke_check_ids: tuple[Text, ...] = Field(min_length=1)
    complete_transition_id: Text


class ScopeContract(Contract):
    job: Job
    branch: Text
    allowed_paths: tuple[Text, ...] = Field(min_length=1)
    forbidden_paths: tuple[Text, ...]
    checks: tuple[ApprovedCheck, ...] = Field(min_length=1)
    approval_route: ApprovalRoute
    profile: RoleProfile
    reproduction_reservation: Reservation
    fix_reservation: Reservation
    reproduction_estimate_seconds: Positive
    fix_estimate_seconds: Positive
    verification: VerificationContract | None = None

    @model_serializer(mode="wrap")
    def legacy_shape(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        data: dict[str, Any] = handler(self)
        if self.verification is None:
            data.pop("verification")
        return data

    @model_validator(mode="after")
    def scope_is_bounded(self) -> Self:
        run = self.job.run
        if run.step != "reproduce" or run.attempt != 1:
            raise ValueError("Scope must start with one reproduction attempt")
        if (
            self.branch != f"switch/{run.ticket_id}/{run.generation}"
            or re.fullmatch(
                r"switch/[A-Za-z0-9][A-Za-z0-9_-]*/[1-9][0-9]*", self.branch
            )
            is None
        ):
            raise ValueError("Scope requires the generation's dedicated branch")
        if len({check.id for check in self.checks}) != len(self.checks):
            raise ValueError("Approved check IDs must be unique")
        if self.verification is not None:
            approved = {check.id: check for check in self.checks}
            verification = self.verification
            if not set(verification.smoke_check_ids).issubset(approved) or any(
                key not in approved or approved[key].kind != "behavior"
                for key in verification.behavior_check_ids
            ):
                raise ValueError(
                    "Verification requires approved behavioral and smoke checks"
                )
        for path in (*self.allowed_paths, *self.forbidden_paths):
            if path.startswith("/") or ".." in PurePosixPath(path).parts:
                raise ValueError("Scope paths must stay inside the repository")
        return self


class CheckEvidence(Contract):
    check_id: Text
    commit: Commit
    exit_code: int
    expected: Text
    actual: Text
    environment: Text
    artifact_digest: Digest


class Handoff(Contract):
    input_digest: Digest
    verdict: Literal["PASS", "FAIL", "BLOCKED"]
    head: Commit
    findings: tuple[Text, ...]
    changed_paths: tuple[Text, ...]
    checkpoints: tuple[Text, ...] = ()
    checks: tuple[CheckEvidence, ...] = Field(min_length=1)
    regression_checks: tuple[CheckEvidence, ...] = ()
    regression_commit: Commit | None = None
    fix_commit: Commit | None = None


class ExecutionEnvelope(Contract):
    scope: ScopeContract
    reproduction: Handoff | None
    candidate: Handoff | None = None
    target_sha: Commit | None = None
    feedback: Handoff | None = None
    resume_answer: Answer | None = None
    reproduction_job: Job | None = None

    def role_profile(self, job: Job) -> RoleProfile:
        if job.run.step in {"review", "qa", "smoke"}:
            if self.scope.verification is None:
                raise ValueError("The frozen scope lacks verification role profiles")
            return {
                "review": self.scope.verification.review,
                "qa": self.scope.verification.qa,
                "smoke": self.scope.verification.smoke,
            }[job.run.step].profile
        return self.scope.profile

    def validate_job(self, job: Job) -> None:
        original = self.scope.job
        if (
            job.repository != original.repository
            or job.pack_digest != original.pack_digest
            or job.acceptance != original.acceptance
            or job.run.instance != original.run.instance
            or job.run.ticket_id != original.run.ticket_id
            or job.run.generation != original.run.generation
            or job.run.attempt > 3
        ):
            raise ValueError("Execution envelope differs from the approved scope")
        if job.run.input_revision:
            answer = self.resume_answer
            if (
                answer is None
                or answer.classification != "answer"
                or answer.question.run.ticket_id != job.run.ticket_id
                or answer.question.run.generation != job.run.generation
                or answer.question.run.step != job.run.step
                or answer.question.run.attempt != job.run.attempt
                or answer.question.run.input_revision + 1 != job.run.input_revision
            ):
                raise ValueError("Checkpoint resume requires its accepted answer")
        elif self.resume_answer is not None:
            raise ValueError("An initial execution cannot consume a checkpoint answer")
        if job.run.step == "reproduce":
            if (
                role_identity(job) != role_identity(original)
                or self.reproduction is not None
            ):
                raise ValueError("Reproduction must start at the frozen base")
        elif job.run.step == "fix":
            if self.reproduction is None:
                raise ValueError("A fix requires reproduction evidence")
            validate_reproduction(self.scope, self.reproduction, self.reproduction_job)
        elif job.run.step in {"review", "qa", "smoke"}:
            if (
                self.scope.verification is None
                or self.candidate is None
                or self.reproduction is None
                or self.target_sha is None
                or (job.run.step != "smoke" and self.target_sha != self.candidate.head)
            ):
                raise ValueError(
                    "Verification requires a pinned candidate and target SHA"
                )
            validate_reproduction(self.scope, self.reproduction, self.reproduction_job)
        else:
            raise ValueError("Unsupported delivery role")


class PreparedExecutionEnvelope(ExecutionEnvelope):
    base_worktree_path: Text | None = None
    worktree_path: Text
    branch: Text
    artifact_directory: Text
    report_path: Text
    status_path: Text
    pack_limits: Budget


def role_identity(job: Job) -> dict[str, Any]:
    data = job.model_dump(mode="json")
    data["run"].pop("input_revision", None)
    data["run"]["execution_key"] = (
        f"{job.run.ticket_id}/{job.run.generation}/{job.run.step}/{job.run.attempt}"
    )
    return data


def validate_reproduction(
    scope: ScopeContract, handoff: Handoff, job: Job | None = None
) -> None:
    base = scope.job.repository.base_sha
    checks = {check.id: check for check in scope.checks}
    if (
        (job is not None and role_identity(job) != role_identity(scope.job))
        or handoff.input_digest != digest(job or scope.job)
        or handoff.verdict != "PASS"
        or handoff.head != base
        or not any(
            check.commit == base
            and check.exit_code != 0
            and check.check_id in checks
            and check.expected == checks[check.check_id].expected
            for check in handoff.checks
        )
    ):
        raise ValueError(
            "Reproduction requires an approved failing check at the base SHA"
        )


def validate_candidate(scope: ScopeContract, job: Job, handoff: Handoff) -> None:
    approved = {check.id: check for check in scope.checks}
    recorded = {check.check_id for check in handoff.checks}
    if (
        handoff.input_digest != digest(job)
        or handoff.verdict != "PASS"
        or handoff.head != handoff.fix_commit
        or handoff.regression_commit is None
        or len({handoff.regression_commit, handoff.fix_commit, job.repository.base_sha})
        != 3
        or recorded != set(approved)
        or not any(
            check.commit == handoff.regression_commit
            and check.exit_code != 0
            and check.check_id in approved
            and check.expected == approved[check.check_id].expected
            for check in handoff.regression_checks
        )
        or any(
            check.commit != handoff.head
            or check.exit_code != 0
            or check.expected != approved[check.check_id].expected
            for check in handoff.checks
        )
    ):
        raise ValueError(
            "Candidate requires distinct test/fix commits and passing approved checks"
        )
    for path in handoff.changed_paths:
        value = PurePosixPath(path)
        if (
            value.is_absolute()
            or ".." in value.parts
            or not any(value.is_relative_to(root) for root in scope.allowed_paths)
            or any(value.is_relative_to(root) for root in scope.forbidden_paths)
        ):
            raise ValueError("Candidate changed a path outside its approved scope")


class Publication(Contract):
    execution_key: Text
    head: Commit
    status: Literal["pending", "pushing", "creating", "uncertain", "published"]
    pr_url: Text | None = None
    uncertain_operation: Literal["push", "create"] | None = None
    previous_head: Commit | None = None
    readiness: Literal["draft", "requesting", "uncertain", "ready"] = "draft"


class ExactTransition(Contract):
    generation: Positive
    pack_digest: Digest
    step: Text
    gate_id: Text | None
    candidate_sha: Commit | None
    transition_id: Text
    source_status_id: Text
    target_status_id: Text
    target_status_name: Text
