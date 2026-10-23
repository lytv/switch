"""Draft v2 wire fixtures for the shared contracts, not an enabled delivery pack.

Jira observations must come from trusted reads. Validating a receipt does not
authenticate its actor or repository.
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Literal

import pytest
from pydantic import ValidationError

from switch_core.bridges.jira.contracts import (
    Answer,
    Approval,
    Budget,
    Contract,
    Gate,
    Job,
    Pack,
    Question,
    Receipt,
    Repository,
    Result,
    Run,
    Text,
    Usage,
    digest,
)
from switch_core.bridges.jira.outbox import _validate_command


class ProbeRecord(Contract):
    """Wire shape from the bounded synthetic Firstmate probe, not an adapter."""

    schema_version: Literal[1]
    execution_key: Literal["fixture-ticket/1/reproduce/1"]
    question_id: Literal["q1"]
    checkpoint: Literal["checkpoint-1"]
    fm_home: Text
    code_common_dir: Text
    usage: Usage


class ProbeQuestion(ProbeRecord):
    status: Literal["needs_input"]
    question: Text


class ProbeCompleted(ProbeRecord):
    status: Literal["completed"]
    answer: Literal["blue"]


@pytest.fixture
def pack() -> Pack:
    return Pack.model_validate_json(
        json.dumps(
            {
                "schema_version": 1,
                "fixture_only": True,
                "id": "delivery",
                "version": "2.0.0-draft",
                "firstmate_commit": "6eb8b7f8277dc37637e3ba58c2fc461d88dc4ff3",
                "harness": "pi",
                "harness_version": "1.4.0",
                "provider": "openai-codex",
                "model": "gpt-6.1-sol",
                "bug_path": [
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
                ],
                "non_bug_path": ["intake", "manual_handoff"],
                "human_merge": True,
                "deploy": False,
                "overrun_action": "alert_and_continue_within_budget",
                "budget": {
                    "ticket_tokens": 200000,
                    "ticket_estimated_usd": "5",
                    "day_estimated_usd": "25",
                },
            }
        )
    )


@pytest.fixture
def job(pack: Pack) -> Job:
    return Job.model_validate_json(
        json.dumps(
            {
                "schema_version": 1,
                "run": {
                    "instance": "fixture",
                    "ticket_id": "fixture-ticket",
                    "generation": 1,
                    "step": "reproduce",
                    "attempt": 1,
                    "execution_key": "fixture-ticket/1/reproduce/1",
                },
                "pack_digest": digest(pack),
                "service": "Bug",
                "repository": {
                    "supplied_url": "https://example.com/team/bug.git",
                    "outcome": "verified",
                    "canonical_url": "https://example.com/team/bug.git",
                    "base_branch": "main",
                    "base_sha": "1" * 40,
                    "evidence": "Synthetic triage receipt: reachable, project and scope match",
                },
                "acceptance": ["The original failure no longer occurs"],
            }
        )
    )


@pytest.fixture
def question(job: Job) -> Question:
    return Question(
        run=job.run,
        input_digest=digest(job),
        id="q1",
        checkpoint_digest="2" * 64,
        text="Which color?",
    )


@pytest.fixture
def unknown_usage() -> Usage:
    return Usage(
        measurement="unknown",
        provider="openai-codex",
        model="gpt-6.1-sol",
        input_tokens=None,
        output_tokens=None,
        cache_read_tokens=None,
        cache_write_tokens=None,
        estimated_usd=None,
        price_source=None,
        billing="estimate_not_subscription_invoice",
    )


def test_pack_snapshot_is_frozen(pack: Pack, job: Job) -> None:
    assert (
        digest(pack)
        == "965793653f39572b8b0a85134ef1ebeb5c0a77c19cbe81cc689354166d428fbf"
    )
    assert job.pack_digest == digest(pack)
    with pytest.raises(ValidationError, match="frozen"):
        pack.version = "changed"
    with pytest.raises(ValidationError, match="frozen"):
        pack.budget.ticket_tokens = 1
    assert Pack.model_validate_json(pack.model_dump_json()) == pack


@pytest.mark.parametrize(
    "field,value",
    [("generation", 2), ("attempt", 2), ("step", "fix"), ("ticket_id", "other-ticket")],
)
def test_execution_key_cannot_change_identity(
    job: Job, field: str, value: object
) -> None:
    data = job.run.model_dump()
    data[field] = value
    with pytest.raises(ValidationError, match="frozen run"):
        Run.model_validate(data)


@pytest.mark.parametrize("outcome", ["unknown", "invalid"])
def test_unverified_repository_has_no_checkout(job: Job, outcome: str) -> None:
    data = job.model_dump(mode="json")
    data["repository"].update(
        outcome=outcome,
        canonical_url=None,
        base_branch=None,
        base_sha=None,
        evidence="Reporter clarification required",
    )
    repository = Repository.model_validate_json(json.dumps(data["repository"]))
    assert repository.outcome == outcome
    with pytest.raises(ValidationError, match="triage-verified"):
        Job.model_validate_json(json.dumps(data))
    data["repository"]["base_sha"] = "1" * 40
    with pytest.raises(ValidationError, match="cannot authorize checkout"):
        Repository.model_validate_json(json.dumps(data["repository"]))


@pytest.mark.parametrize(
    "url",
    [
        "https://example.org/new/repository.git",
        "ssh://git@example.org/team/repo",
        "git@example.org:team/repo.git",
    ],
)
def test_repository_identity_has_no_registration_allowlist(job: Job, url: str) -> None:
    data = job.model_dump(mode="json")
    data["repository"].update(supplied_url=url, canonical_url=url)
    changed = Job.model_validate_json(json.dumps(data))
    assert changed.repository.supplied_url == url
    assert digest(changed) != digest(job)


def test_scope_approval_binds_actor_and_frozen_input(job: Job) -> None:
    gate = Gate(
        run=job.run,
        id="scope-1",
        kind="scope",
        contract_digest=digest(job),
        transition_id="approve-scope",
        candidate_sha=None,
        approver_account_ids=("human-approver",),
    )
    approval = Approval(
        gate=gate,
        actor_account_id="human-approver",
        actor_kind="human",
        history_id="history-1",
    )
    assert approval.gate.contract_digest == digest(job)
    for actor in ("bot-nonapprover", "reporter-nonapprover"):
        with pytest.raises(ValidationError, match="authorized approver"):
            Approval(
                gate=gate,
                actor_account_id=actor,
                actor_kind="human",
                history_id="history-2",
            )
    with pytest.raises(ValidationError):
        Approval(
            gate=gate,
            actor_account_id="human-approver",
            actor_kind="bot",
            history_id="history-3",
        )


def test_acceptance_is_separate_and_binds_candidate(job: Job) -> None:
    gate = Gate(
        run=job.run,
        id="accept-1",
        kind="acceptance",
        contract_digest=digest(job),
        transition_id="accept-candidate",
        candidate_sha="3" * 40,
        approver_account_ids=("human-approver",),
    )
    accepted = Approval(
        gate=gate,
        actor_account_id="human-approver",
        actor_kind="human",
        history_id="history-accept",
    )
    assert accepted.gate.candidate_sha == "3" * 40
    assert accepted.gate.kind != "scope"
    data = accepted.model_dump(mode="json")
    data["gate"]["candidate_sha"] = None
    with pytest.raises(ValidationError, match="tested head"):
        Approval.model_validate_json(json.dumps(data))


def test_question_answer_and_result_round_trip(
    job: Job, question: Question, unknown_usage: Usage
) -> None:
    result = Result(
        run=job.run,
        input_digest=digest(job),
        status="needs_input",
        question=question,
        usage=unknown_usage,
    )
    answer = Answer(
        question=question,
        revision=1,
        actor_account_id="reporter",
        classification="answer",
        text="blue",
    )
    assert Result.model_validate_json(result.model_dump_json()) == result
    assert (
        Answer.model_validate_json(answer.model_dump_json()).question == result.question
    )
    assert answer.question.checkpoint_digest == question.checkpoint_digest
    for field, value in (
        ("input_digest", "4" * 64),
        ("run", job.run.model_dump(mode="json") | {"instance": "other-instance"}),
    ):
        data = result.model_dump(mode="json")
        data["question"][field] = value
        with pytest.raises(ValidationError, match="another execution or input"):
            Result.model_validate_json(json.dumps(data))


@pytest.mark.parametrize("status", ["completed", "cancelled", "uncertain"])
def test_terminal_results_keep_unknown_usage_explicit(
    job: Job, unknown_usage: Usage, status: str
) -> None:
    data = {
        "run": job.run.model_dump(mode="json"),
        "input_digest": digest(job),
        "status": status,
        "question": None,
        "usage": unknown_usage.model_dump(mode="json"),
    }
    result = Result.model_validate_json(json.dumps(data))
    assert result.usage.input_tokens is None
    assert result.usage.estimated_usd is None
    data["usage"]["measurement"] = "complete"
    with pytest.raises(ValidationError, match="all token counts"):
        Result.model_validate_json(json.dumps(data))


def test_budgets_are_configurable_not_enforced_caps(pack: Pack) -> None:
    changed = Budget(
        ticket_tokens=100000,
        ticket_estimated_usd=Decimal("2.5"),
        day_estimated_usd=Decimal("10"),
    )
    assert changed != pack.budget
    assert pack.budget.ticket_tokens == 200000
    assert pack.budget.ticket_estimated_usd == Decimal("5")
    assert pack.budget.day_estimated_usd == Decimal("25")
    assert (
        pack.bug_path.index("accept")
        < pack.bug_path.index("merge")
        < pack.bug_path.index("smoke")
        < pack.bug_path.index("close")
    )
    assert pack.non_bug_path == ("intake", "manual_handoff")


@pytest.fixture
def receipt(pack: Pack, job: Job, question: Question, unknown_usage: Usage) -> Receipt:
    gate = Gate(
        run=job.run,
        id="scope-1",
        kind="scope",
        contract_digest=digest(job),
        transition_id="approve-scope",
        candidate_sha=None,
        approver_account_ids=("human-approver",),
    )
    return Receipt(
        pack=pack,
        job=job,
        gate=gate,
        approval=Approval(
            gate=gate,
            actor_account_id="human-approver",
            actor_kind="human",
            history_id="history-1",
        ),
        result=Result(
            run=job.run,
            input_digest=digest(job),
            status="needs_input",
            question=question,
            usage=unknown_usage,
        ),
        answer=Answer(
            question=question,
            revision=1,
            actor_account_id="reporter",
            classification="answer",
            text="blue",
        ),
    )


@pytest.mark.parametrize(
    "path,value",
    [
        (("pack", "version"), "2.0.1-draft"),
        (("approval", "gate", "id"), "different-gate"),
        (("approval", "gate", "transition_id"), "generic-status-move"),
        (("approval", "gate", "contract_digest"), "5" * 64),
        (("approval", "gate", "run", "instance"), "other-instance"),
        (("result", "input_digest"), "5" * 64),
        (("answer", "question", "id"), "other-question"),
        (("answer", "question", "checkpoint_digest"), "5" * 64),
        (("answer", "question", "run", "instance"), "other-instance"),
    ],
)
def test_mismatched_receipts_fail(
    receipt: Receipt, path: tuple[str, ...], value: str
) -> None:
    data = receipt.model_dump(mode="json")
    target = data
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    with pytest.raises(ValidationError):
        Receipt.model_validate_json(json.dumps(data))


@pytest.mark.parametrize(
    "field,value",
    [
        ("input_tokens", -1),
        ("output_tokens", True),
        ("cache_read_tokens", "0"),
        ("estimated_usd", "-1"),
    ],
)
def test_invalid_usage_fails(unknown_usage: Usage, field: str, value: object) -> None:
    data = unknown_usage.model_dump(mode="json")
    data[field] = value
    with pytest.raises(ValidationError):
        Usage.model_validate_json(json.dumps(data))


def test_partial_usage_keeps_missing_counts(unknown_usage: Usage) -> None:
    data = unknown_usage.model_dump(mode="json")
    data.update(measurement="partial", input_tokens=123, output_tokens=5)
    usage = Usage.model_validate_json(json.dumps(data))
    assert usage.cache_read_tokens is None
    assert usage.estimated_usd is None


def test_real_probe_wire_shapes_keep_unknown_usage(unknown_usage: Usage) -> None:
    identity = {
        "schema_version": 1,
        "execution_key": "fixture-ticket/1/reproduce/1",
        "question_id": "q1",
        "checkpoint": "checkpoint-1",
        "fm_home": "/lab/worker-a",
        "code_common_dir": "/lab/firstmate-source/.git",
        "usage": unknown_usage.model_dump(mode="json"),
    }
    question = ProbeQuestion.model_validate_json(
        json.dumps(identity | {"status": "needs_input", "question": "Which color?"})
    )
    result = ProbeCompleted.model_validate_json(
        json.dumps(identity | {"status": "completed", "answer": "blue"})
    )
    assert result.usage.measurement == "unknown"
    assert result.usage.input_tokens is None
    assert result.question_id == question.question_id
    assert result.checkpoint == question.checkpoint
    data = result.model_dump(mode="json")
    del data["usage"]
    with pytest.raises(ValidationError, match="usage"):
        ProbeCompleted.model_validate_json(json.dumps(data))


def test_recorded_pi_counts_include_cached_context() -> None:
    usage = Usage(
        measurement="complete",
        provider="openai-codex",
        model="gpt-6.1-sol",
        input_tokens=301,
        output_tokens=69,
        cache_read_tokens=29952,
        cache_write_tokens=0,
        estimated_usd=Decimal("0.0042872"),
        price_source="Pi 1.4.0 recorded estimate; tariff unverified",
        billing="estimate_not_subscription_invoice",
    )
    assert (
        sum(
            (
                usage.input_tokens,
                usage.output_tokens,
                usage.cache_read_tokens,
                usage.cache_write_tokens,
            )
        )
        == 30322
    )
    assert usage.estimated_usd != Decimal("0")


def test_legacy_outbox_does_not_accept_delivery_commands() -> None:
    _validate_command("transition", "Waiting for approval")
    with pytest.raises(ValueError):
        _validate_command("transition_by_id", {"id": "approve-scope"})
