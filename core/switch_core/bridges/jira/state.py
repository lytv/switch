"""Durable workflow state; agent_generation remains the ticket's only fence."""

from __future__ import annotations

from decimal import Decimal
from typing import Annotated, Literal, Self

from pydantic import Field, TypeAdapter, model_validator

from switch_core.bridges.jira.contracts import (
    USD,
    Answer,
    Approval,
    Commit,
    Contract,
    Count,
    Gate,
    Question,
    Repository,
    Text,
)
from switch_core.bridges.jira.delivery_contracts import (
    Handoff,
    Publication,
    ScopeContract,
)
from switch_core.bridges.jira.packs import FrozenPack


class LegacyWorkflowState(Contract):
    schema_version: Literal[1] = 1
    policy: Literal["legacy_intake"] = "legacy_intake"
    pack_id: Literal["delivery"] = "delivery"
    pack_version: Literal["1"] = "1"


class WorkflowMapping(Contract):
    name: Text
    jira_id: Text


class DeliveryWorkflowState(Contract):
    schema_version: Literal[1] = 1
    policy: Literal["delivery"] = "delivery"
    pack: FrozenPack
    service: Literal["Bug", "Feature", "Docs", "Other"]
    repository: Repository | None = None
    service_field: Text | None = None
    repository_url_field: Text = "description"
    step: Text = "intake"
    gate: Gate | None = None
    gate_ready: bool = False
    checkpoints: tuple[Text, ...] = ()
    statuses: tuple[WorkflowMapping, ...]
    transitions: tuple[WorkflowMapping, ...]
    scope: ScopeContract | None = None
    reproduction: Handoff | None = None
    candidate: Handoff | None = None
    publication: Publication | None = None
    candidate_head: Commit | None = None
    review_head: Commit | None = None
    qa_head: Commit | None = None
    accepted_head: Commit | None = None
    merge_sha: Commit | None = None
    final_evidence: tuple[Text, ...] = ()
    approvals: tuple[Approval, ...] = ()
    question: Question | None = None
    answer: Answer | None = None
    rework_count: Count = 0
    reserved_tokens: Count = 0
    reserved_estimated_usd: USD = Decimal("0")
    known_estimated_usd: USD = Decimal("0")
    unknown_spend: bool = False

    @model_validator(mode="after")
    def state_matches_contract(self) -> Self:
        path = (
            self.pack.snapshot.bug_path
            if self.service == "Bug"
            else self.pack.snapshot.non_bug_path
        )
        if self.step not in path:
            raise ValueError("Step does not belong to the selected Service path")
        if self.answer is not None and self.answer.question != self.question:
            raise ValueError("Answer does not match the open question")
        for mapping in (self.statuses, self.transitions):
            if len({entry.name for entry in mapping}) != len(mapping):
                raise ValueError("Workflow mapping names must be unique")
        return self


WorkflowState = Annotated[
    LegacyWorkflowState | DeliveryWorkflowState, Field(discriminator="policy")
]
workflow_state_adapter: TypeAdapter[WorkflowState] = TypeAdapter(WorkflowState)
