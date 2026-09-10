from __future__ import annotations

import re
from collections.abc import Sequence

from switch_core.bridges.jira.parse import ParsedJiraEvent
from switch_core.db.models import JiraTrigger

# Minimal JQL-style filter: `field = value` and `field in (a, b)` joined by AND.
_EQ_RE = re.compile(r"^\s*([A-Za-z_]+)\s*=\s*(?:\"([^\"]*)\"|'([^']*)'|(\S+))\s*$")
_IN_RE = re.compile(
    r"^\s*([A-Za-z_]+)\s+in\s*\((.*)\)\s*$",
    re.IGNORECASE,
)


def _blank_is_any(rule_value: str, actual: str) -> bool:
    if rule_value.strip() == "":
        return True
    return rule_value.strip().casefold() == actual.strip().casefold()


def _field_value(event: ParsedJiraEvent, field: str) -> str | set[str]:
    key = field.casefold()
    if key == "labels":
        return {label.casefold() for label in event.labels}
    if key == "priority":
        return event.priority
    if key == "assignee":
        return event.assignee
    if key == "reporter":
        return event.reporter
    if key == "status":
        return event.status
    if key in ("type", "issuetype", "issue_type"):
        return event.issue_type
    if key in ("project", "project_key"):
        return event.project
    if key == "key":
        return event.key
    return ""


def _match_clause(clause: str, event: ParsedJiraEvent) -> bool:
    clause = clause.strip()
    if not clause:
        return True

    in_match = _IN_RE.match(clause)
    if in_match:
        field, raw_list = in_match.group(1), in_match.group(2)
        items = [
            p.strip().strip("\"'").casefold() for p in raw_list.split(",") if p.strip()
        ]
        actual = _field_value(event, field)
        if isinstance(actual, set):
            return bool(actual.intersection(items))
        return actual.casefold() in items

    eq_match = _EQ_RE.match(clause)
    if eq_match:
        field = eq_match.group(1)
        value = eq_match.group(2) or eq_match.group(3) or eq_match.group(4) or ""
        actual = _field_value(event, field)
        if isinstance(actual, set):
            return value.casefold() in actual
        return actual.casefold() == value.casefold()

    # Unrecognised clause: do not match (fail closed for the filter only).
    return False


def match_jql(jql: str, event: ParsedJiraEvent) -> bool:
    """Apply a blank-or-simple JQL-style filter. Blank jql always matches."""
    text = jql.strip()
    if not text:
        return True
    # Split on AND only — Phase 1 does not need OR precedence.
    clauses = re.split(r"\s+AND\s+", text, flags=re.IGNORECASE)
    return all(_match_clause(clause, event) for clause in clauses)


def rule_matches(rule: JiraTrigger, event: ParsedJiraEvent) -> bool:
    """Deterministic match of one enabled rule against a parsed event."""
    if not rule.enabled:
        return False
    if not _blank_is_any(rule.project_key, event.project):
        return False
    if not _blank_is_any(rule.issue_type, event.issue_type):
        return False

    fire_on = rule.fire_on.strip().casefold()
    if fire_on == "created":
        if event.event_kind != "created":
            return False
    elif fire_on == "updated":
        if event.event_kind != "updated":
            return False
    elif fire_on == "transition":
        if event.transition is None:
            return False
        if rule.target_status.strip() and not _blank_is_any(
            rule.target_status, event.transition.to_status
        ):
            return False
    else:
        return False

    if rule.target_status.strip() and fire_on != "transition":
        # For created/updated, target_status means "current status is …".
        if not _blank_is_any(rule.target_status, event.status):
            return False

    return match_jql(rule.jql, event)


def matching_rules(
    rules: Sequence[JiraTrigger], event: ParsedJiraEvent
) -> list[JiraTrigger]:
    """Return every matching rule independently (one event may fire several)."""
    return [rule for rule in rules if rule_matches(rule, event)]
