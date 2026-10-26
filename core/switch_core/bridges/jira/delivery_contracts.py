"""Bounded execution envelopes and separate evidence handoffs for the Switch profile."""

from __future__ import annotations

import hashlib
import re
from pathlib import PurePosixPath
from typing import Literal, Self

from pydantic import Field, model_validator

from switch_core.bridges.jira.contracts import (
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

    def validate_job(self, job: Job) -> None:
        original = self.scope.job
        if (
            job.repository != original.repository
            or job.pack_digest != original.pack_digest
            or job.acceptance != original.acceptance
            or job.run.instance != original.run.instance
            or job.run.ticket_id != original.run.ticket_id
            or job.run.generation != original.run.generation
            or job.run.attempt != 1
        ):
            raise ValueError("Execution envelope differs from the approved scope")
        if job.run.step == "reproduce":
            if job != original or self.reproduction is not None:
                raise ValueError("Reproduction must start at the frozen base")
        elif job.run.step == "fix":
            if self.reproduction is None:
                raise ValueError("A fix requires reproduction evidence")
            validate_reproduction(self.scope, self.reproduction)
        else:
            raise ValueError("This envelope only authorizes reproduction and fix")


class PreparedExecutionEnvelope(ExecutionEnvelope):
    worktree_path: Text
    branch: Text
    artifact_directory: Text
    report_path: Text
    status_path: Text
    pack_limits: Budget


def validate_reproduction(scope: ScopeContract, handoff: Handoff) -> None:
    base = scope.job.repository.base_sha
    checks = {check.id: check for check in scope.checks}
    if (
        handoff.input_digest != digest(scope.job)
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
