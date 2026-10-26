"""Evidence checks for immutable candidates and the merged-code smoke check."""

from __future__ import annotations

from typing import Literal

from switch_core.bridges.jira.contracts import Commit, Contract, Job, Text, digest
from switch_core.bridges.jira.delivery_contracts import Handoff, ScopeContract


class MergeReceipt(Contract):
    repository_url: Text
    request_url: Text
    candidate_sha: Commit
    base_branch: Text
    outcome: Literal["open", "closed_unmerged", "unknown", "merged"]
    merged_sha: Commit | None
    actor_id: Text | None
    human: bool


def validate_verification(
    scope: ScopeContract, job: Job, handoff: Handoff, target: str
) -> None:
    policy = scope.verification
    if policy is None:
        raise ValueError("The approved scope lacks a verification contract")
    approved = {check.id: check for check in scope.checks}
    required = set(policy.smoke_check_ids) if job.run.step == "smoke" else set(approved)
    if (
        handoff.input_digest != digest(job)
        or handoff.head != target
        or handoff.changed_paths
        or {check.check_id for check in handoff.checks} != required
        or len(handoff.checks) != len(required)
        or any(
            check.commit != target
            or check.expected != approved[check.check_id].expected
            for check in handoff.checks
        )
    ):
        raise ValueError(
            "Verification evidence differs from the immutable approved checks"
        )
    if handoff.verdict == "PASS" and any(
        check.exit_code != 0 for check in handoff.checks
    ):
        raise ValueError("Passing verification contains a failed check")
    if job.run.step == "qa" and (
        not set(policy.behavior_check_ids).issubset(
            check.check_id
            for check in handoff.regression_checks
            if check.commit == scope.job.repository.base_sha
            and check.exit_code != 0
            and check.check_id in approved
            and check.expected == approved[check.check_id].expected
        )
    ):
        raise ValueError("QA must repeat the approved original behavioral failure")


def validate_merge(
    scope: ScopeContract, receipt: MergeReceipt, request: str, head: str
) -> None:
    if (
        receipt.repository_url != scope.job.repository.canonical_url
        or receipt.request_url != request
        or receipt.candidate_sha != head
        or receipt.base_branch != scope.job.repository.base_branch
    ):
        raise ValueError(
            "Forge receipt differs from the accepted repository and candidate"
        )
    if receipt.outcome == "merged" and (
        receipt.merged_sha is None or not receipt.human or receipt.actor_id is None
    ):
        raise ValueError("Delivery requires a confirmed human merge receipt")
