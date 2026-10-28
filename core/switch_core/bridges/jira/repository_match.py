"""Trusted product corroboration for repository triage.

A verified repository proves the forge holds that repository at a pinned
commit. It does not prove the repository belongs to the ticket's product.
`evaluate_repository_match` compares operator-pinned product markers against
the ticket text and bounded forge facts. Marker corroboration is a
fail-closed signal: repository facts can only veto automatic admission,
never authorize it. A mismatch or an unclear result stages a repository
gate for an authorized human confirmation; it never stages scope.
"""

from __future__ import annotations

from typing import Literal

from pydantic import Field

from switch_core.bridges.jira.contracts import (
    Approval,
    Contract,
    Positive,
    Repository,
    Text,
    digest,
)
from switch_core.bridges.jira.delivery_contracts import ApprovalRoute
from switch_core.bridges.jira.parse import _plain_text

MAX_FACTS = 64
MAX_TREE_NAMES = 50


def normalize_marker(value: str) -> str:
    return value.casefold()


class RepositoryMatch(Contract):
    generation: Positive
    repository: Repository
    markers: tuple[Text, ...] = ()
    approval_route: ApprovalRoute
    facts: tuple[Text, ...] = Field(max_length=MAX_FACTS)
    facts_available: bool
    outcome: Literal["pending", "match", "mismatch", "unclear"]
    reason: Text
    confirmation: Approval | None = None

    @property
    def contract_digest(self) -> str:
        return digest(self.model_copy(update={"confirmation": None}))


def confirmation_request_body(match: RepositoryMatch) -> str:
    """Reporter- and approver-visible explanation of a pending confirmation."""
    repository = match.repository
    lines = [
        "Repository confirmation required (not scope approval):",
        f"Verified repository: {repository.canonical_url}",
        f"Base branch: {repository.base_branch}",
        f"Base commit: {repository.base_sha}",
        f"Product check: {match.outcome} - {match.reason}",
    ]
    if match.facts:
        shown = ", ".join(match.facts[:12])
        rest = len(match.facts) - 12
        lines.append(
            f"Repository facts: {shown}" + (f" (+{rest} more)" if rest > 0 else "")
        )
    lines.append(
        "An authorized approver must Approve to confirm this repository for "
        "this ticket, or correct the Repository URL and release for fresh "
        "triage. Chat text and ticket prose never count as confirmation."
    )
    return "\n".join(lines)


def evaluate_repository_match(
    *,
    generation: int,
    repository: Repository,
    markers: tuple[str, ...],
    approval_route: ApprovalRoute,
    facts: tuple[str, ...],
    facts_available: bool,
    ticket_text: object,
) -> RepositoryMatch:
    """Corroborate the ticket's product against bounded repository facts."""
    text = _plain_text(ticket_text)
    ticket = text.casefold()
    haystack = "\n".join(facts).casefold()
    pinned = tuple(marker for marker in map(normalize_marker, markers) if marker)
    ticket_hit = any(marker in ticket for marker in pinned)
    shared_marker = any(marker in ticket and marker in haystack for marker in pinned)
    outcome: Literal["match", "mismatch", "unclear"]
    if not facts_available:
        outcome = "unclear"
        reason = (
            "Trusted forge reads did not supply bounded product facts; "
            "an authorized human must confirm the repository before scope."
        )
    elif shared_marker:
        outcome = "match"
        reason = "Ticket product markers corroborate the verified repository facts."
    elif ticket_hit:
        outcome = "mismatch"
        reason = (
            "Ticket product markers do not corroborate the verified repository "
            "facts; an authorized human must confirm the repository before scope."
        )
    else:
        outcome = "unclear"
        reason = (
            "Ticket text asserts no pinned product marker; an authorized human "
            "must confirm the repository before scope."
        )
    return RepositoryMatch(
        generation=generation,
        repository=repository,
        markers=tuple(pinned),
        approval_route=approval_route,
        facts=tuple(facts),
        facts_available=facts_available,
        outcome=outcome,
        reason=reason,
    )
