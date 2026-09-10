"""Source-neutral trigger event contract.

Matching, template rendering, and delivery consume this shape. Source-specific
parsers (Jira today) are responsible for mapping raw payloads into it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class StatusTransition:
    from_status: str
    to_status: str


@dataclass(frozen=True)
class NormalizedTriggerEvent:
    """Fields every trigger source must provide for rule matching and posting.

    ``webhook_event`` keeps the source-native event name (e.g. Jira's
    ``jira:issue_updated``) for logging and diagnostics. ``event_kind`` is the
    coarse category the rule engine matches on (``created`` / ``updated``).
    """

    event_kind: str
    webhook_event: str
    key: str
    summary: str
    issue_type: str
    project: str
    status: str
    assignee: str
    priority: str
    reporter: str
    url: str
    labels: tuple[str, ...] = ()
    transition: StatusTransition | None = None
    raw: dict[str, Any] = field(default_factory=dict, repr=False, compare=False)
