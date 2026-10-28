"""Unit proof for trusted product corroboration: untrusted facts only veto."""

import pytest

from switch_core.bridges.jira.contracts import Repository
from switch_core.bridges.jira.delivery_contracts import ApprovalRoute
from switch_core.bridges.jira.repository_match import (
    confirmation_request_body,
    evaluate_repository_match,
)

REPOSITORY = Repository(
    supplied_url="https://forge.example/team/booking-sandbox",
    canonical_url="https://forge.example/team/booking-sandbox",
    base_branch="main",
    base_sha="a" * 40,
    outcome="verified",
    evidence="fixture",
)

ROUTE = ApprovalRoute(
    source_status_id="approval",
    target_status_id="progress",
    transition_id="approve-scope",
    unique_edge_verified=True,
    evidence="fixture",
)

TICKET = (
    "Repository URL: https://forge.example/team/booking-sandbox\n\n"
    "Intended product: the disposable booking sandbox.\n"
    "The booking preview returns 7 for input 7."
)

FACTS = ("team/booking-sandbox", "Disposable booking sandbox", "booking.py")


def check(**overrides):
    arguments = {
        "generation": 1,
        "repository": REPOSITORY,
        "markers": ("booking", "sandbox"),
        "approval_route": ROUTE,
        "facts": FACTS,
        "facts_available": True,
        "ticket_text": TICKET,
    }
    arguments.update(overrides)
    return evaluate_repository_match(**arguments)


def test_match_when_ticket_and_facts_share_markers():
    assert check().outcome == "match"


@pytest.mark.parametrize(
    ("ticket_marker", "facts_marker", "outcome"),
    [
        ("booking", "booking", "match"),
        ("sandbox", "sandbox", "match"),
        ("booking", "sandbox", "mismatch"),
        ("sandbox", "booking", "mismatch"),
    ],
)
@pytest.mark.parametrize("fact_template", ["Agent {} service", "{}_utils.py"])
@pytest.mark.parametrize("structured_ticket", [False, True])
def test_match_requires_the_same_marker(
    ticket_marker, facts_marker, outcome, fact_template, structured_ticket
):
    text = f"{ticket_marker} preview returns 7"
    ticket = (
        {"type": "doc", "content": [{"type": "text", "text": text}]}
        if structured_ticket
        else text
    )
    match = check(
        ticket_text=ticket,
        facts=(fact_template.format(facts_marker.upper()),),
        markers=("BOOKING", "SANDBOX"),
    )
    assert match.outcome == outcome


def test_mismatch_when_facts_lack_ticket_markers():
    match = check(facts=("team/orchestration-service", "Agent orchestration service"))
    assert match.outcome == "mismatch"
    assert "sandbox" in match.reason or "marker" in match.reason


def test_unclear_when_ticket_asserts_no_marker():
    match = check(ticket_text="Fix the crash on login.")
    assert match.outcome == "unclear"


def test_unclear_when_facts_are_unavailable():
    match = check(facts_available=False)
    assert match.outcome == "unclear"


def test_repository_content_cannot_authorize_by_itself():
    # Facts packed with markers but a silent ticket still needs a human.
    match = check(ticket_text="Repository URL: https://forge.example/team/x")
    assert match.outcome == "unclear"


def test_confirmation_is_bound_outside_the_contract_digest():
    match = check()
    assert match.confirmation is None
    changed = match.model_copy(
        update={"confirmation": None, "outcome": "mismatch", "reason": "changed"}
    )
    assert changed.contract_digest != match.contract_digest
    body = confirmation_request_body(match)
    assert "not scope approval" in body
    assert REPOSITORY.canonical_url in body
    assert "ticket prose never count as confirmation" in body
