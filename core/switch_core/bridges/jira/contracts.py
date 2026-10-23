"""Frozen Jira delivery wire contracts. Validation does not authenticate receipts."""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

Text = Annotated[str, Field(min_length=1, pattern=r"\S")]
Version = Annotated[str, Field(pattern=r"^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$")]
Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
Commit = Annotated[str, Field(pattern=r"^[0-9a-f]{40}$")]
Count = Annotated[int, Field(ge=0)]
Positive = Annotated[int, Field(gt=0)]
USD = Annotated[Decimal, Field(ge=0)]


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class Budget(Contract):
    ticket_tokens: Positive
    ticket_estimated_usd: USD
    day_estimated_usd: USD


class Reservation(Contract):
    tokens: Positive
    estimated_usd: Annotated[Decimal, Field(gt=0)]
    price_source: Text


class Pack(Contract):
    schema_version: Literal[1]
    fixture_only: Literal[True]
    id: Literal["delivery"]
    version: Version
    firstmate_commit: Commit
    harness: Literal["pi"]
    harness_version: Version
    provider: Text
    model: Text
    bug_path: tuple[str, ...]
    non_bug_path: tuple[Literal["intake"], Literal["manual_handoff"]]
    human_merge: Literal[True]
    deploy: Literal[False]
    overrun_action: Literal["alert_and_continue_within_budget"]
    budget: Budget

    @model_validator(mode="after")
    def pins_and_path_are_explicit(self) -> Self:
        if self.firstmate_commit == "0" * 40:
            raise ValueError("Firstmate requires an exact commit pin")
        if self.provider in {"latest", "default"} or self.model in {
            "latest",
            "default",
        }:
            raise ValueError("Provider and model require explicit pins")
        if self.bug_path != (
            "intake",
            "approve_scope",
            "reproduce",
            "fix",
            "review",
            "qa",
            "accept",
            "merge",
            "smoke",
            "close",
        ):
            raise ValueError("Delivery requires the ordered Bug safety gates")
        return self


class Run(Contract):
    instance: Text
    ticket_id: Text
    generation: Positive
    step: Literal["reproduce", "fix", "review", "qa", "smoke"]
    attempt: Positive
    execution_key: Text

    @model_validator(mode="after")
    def key_matches_identity(self) -> Self:
        expected = f"{self.ticket_id}/{self.generation}/{self.step}/{self.attempt}"
        if self.execution_key != expected:
            raise ValueError("Execution key does not match the frozen run")
        return self


class Repository(Contract):
    supplied_url: Text
    outcome: Literal["verified", "unknown", "invalid"]
    canonical_url: Text | None
    base_branch: Text | None
    base_sha: Commit | None
    evidence: Text

    @model_validator(mode="after")
    def checkout_requires_verification(self) -> Self:
        pins = (self.canonical_url, self.base_branch, self.base_sha)
        if self.outcome == "verified" and any(pin is None for pin in pins):
            raise ValueError("Verified repository requires all checkout pins")
        if self.outcome != "verified" and any(pin is not None for pin in pins):
            raise ValueError("Unknown or invalid repository cannot authorize checkout")
        return self


class Job(Contract):
    schema_version: Literal[1]
    run: Run
    pack_digest: Digest
    service: Literal["Bug"]
    repository: Repository
    acceptance: tuple[Text, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def repository_is_verified(self) -> Self:
        if self.repository.outcome != "verified":
            raise ValueError("Development requires a triage-verified repository")
        return self


class Gate(Contract):
    run: Run
    id: Text
    kind: Literal["scope", "acceptance"]
    contract_digest: Digest
    transition_id: Text
    candidate_sha: Commit | None
    approver_account_ids: tuple[Text, ...] = Field(min_length=1)


class Approval(Contract):
    gate: Gate
    actor_account_id: Text
    actor_kind: Literal["human"]
    history_id: Text

    @model_validator(mode="after")
    def actor_is_authorized(self) -> Self:
        if self.actor_account_id not in self.gate.approver_account_ids:
            raise ValueError("Approval actor is not an authorized approver")
        if (self.gate.kind == "acceptance") != (self.gate.candidate_sha is not None):
            raise ValueError("Candidate acceptance must bind the tested head")
        return self


class Question(Contract):
    run: Run
    input_digest: Digest
    id: Text
    checkpoint_digest: Digest
    text: Text


class Answer(Contract):
    question: Question
    revision: Positive
    actor_account_id: Text
    classification: Literal["answer", "clarify", "scope_change", "unrelated"]
    text: Text


class Usage(Contract):
    measurement: Literal["complete", "partial", "unknown"]
    provider: Text | None
    model: Text | None
    input_tokens: Count | None
    output_tokens: Count | None
    cache_read_tokens: Count | None
    cache_write_tokens: Count | None
    estimated_usd: USD | None
    price_source: Text | None
    billing: Literal["estimate_not_subscription_invoice"]

    @model_validator(mode="after")
    def unknown_is_not_zero(self) -> Self:
        counts = (
            self.input_tokens,
            self.output_tokens,
            self.cache_read_tokens,
            self.cache_write_tokens,
        )
        if self.measurement == "complete" and (
            any(count is None for count in counts)
            or self.provider is None
            or self.model is None
        ):
            raise ValueError("Complete usage requires attribution and all token counts")
        if self.measurement == "unknown" and any(count is not None for count in counts):
            raise ValueError("Unknown usage cannot contain fabricated zero counts")
        if (self.estimated_usd is None) != (self.price_source is None):
            raise ValueError("An estimate requires an explicit price source")
        return self


class Result(Contract):
    run: Run
    input_digest: Digest
    status: Literal["completed", "needs_input", "cancelled", "uncertain"]
    question: Question | None
    usage: Usage

    @model_validator(mode="after")
    def question_matches_execution(self) -> Self:
        if (self.status == "needs_input") != (self.question is not None):
            raise ValueError("Only needs_input carries an open question")
        if self.question is not None and (
            self.question.run != self.run
            or self.question.input_digest != self.input_digest
        ):
            raise ValueError("Question belongs to another execution or input")
        return self


class Receipt(Contract):
    pack: Pack
    job: Job
    gate: Gate
    approval: Approval
    result: Result
    answer: Answer | None

    @model_validator(mode="after")
    def records_match_frozen_contract(self) -> Self:
        if self.job.pack_digest != digest(self.pack):
            raise ValueError("Pack snapshot changed")
        if (
            self.gate.run != self.job.run
            or self.gate.contract_digest != digest(self.job)
            or self.approval.gate != self.gate
            or self.result.run != self.job.run
            or self.result.input_digest != digest(self.job)
        ):
            raise ValueError("Receipt does not match the frozen job and gate")
        if self.answer is not None and self.answer.question != self.result.question:
            raise ValueError("Answer does not match the open question and checkpoint")
        return self


def digest(value: Contract) -> str:
    canonical = json.dumps(
        value.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(canonical.encode()).hexdigest()
