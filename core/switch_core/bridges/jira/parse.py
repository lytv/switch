from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


class JiraPayloadError(ValueError):
    """The webhook body is not a usable Jira issue event."""


@dataclass(frozen=True)
class StatusTransition:
    from_status: str
    to_status: str


@dataclass(frozen=True)
class ParsedJiraEvent:
    event_kind: str  # created | updated
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


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        for key in ("displayName", "name", "value", "key"):
            if key in value and value[key] is not None:
                return str(value[key])
        return ""
    return str(value)


def _field(fields: dict[str, Any], name: str) -> Any:
    return fields.get(name)


def _issue_url(payload: dict[str, Any], issue: dict[str, Any], key: str) -> str:
    self_url = issue.get("self")
    if isinstance(self_url, str) and "/rest/api/" in self_url:
        # Cloud/Server REST self → browse URL.
        base = self_url.split("/rest/api/", 1)[0]
        return f"{base}/browse/{key}"
    linked = payload.get("issue_url") or payload.get("url")
    if isinstance(linked, str) and linked:
        return linked
    return ""


def _status_transition(payload: dict[str, Any]) -> StatusTransition | None:
    changelog = payload.get("changelog")
    if not isinstance(changelog, dict):
        return None
    items = changelog.get("items")
    if not isinstance(items, list):
        return None
    for item in items:
        if not isinstance(item, dict):
            continue
        if str(item.get("field") or "").lower() != "status":
            continue
        return StatusTransition(
            from_status=_text(item.get("fromString") or item.get("from")),
            to_status=_text(item.get("toString") or item.get("to")),
        )
    return None


def _event_kind(webhook_event: str, payload: dict[str, Any]) -> str:
    lowered = webhook_event.lower()
    if "issue_created" in lowered or lowered.endswith(":created"):
        return "created"
    if "issue_updated" in lowered or lowered.endswith(":updated"):
        return "updated"
    # Automation "Send web request" often omits webhookEvent; infer from body.
    if payload.get("changelog"):
        return "updated"
    return "created"


def parse_jira_payload(payload: Any) -> ParsedJiraEvent:
    """Extract the issue fields a trigger rule needs.

    Tolerates unknown/extra fields. Raises ``JiraPayloadError`` when the body
    is not a dict or has no usable issue key.
    """
    if not isinstance(payload, dict):
        raise JiraPayloadError("Jira payload must be a JSON object")

    issue = payload.get("issue")
    if not isinstance(issue, dict):
        # Some Automation payloads flatten the issue to the top level.
        if "key" in payload and isinstance(payload.get("fields"), dict):
            issue = payload
        else:
            raise JiraPayloadError("Jira payload missing issue object")

    key = _text(issue.get("key"))
    if not key:
        raise JiraPayloadError("Jira issue missing key")

    fields = issue.get("fields")
    if not isinstance(fields, dict):
        fields = {}

    project_raw = _field(fields, "project")
    project = _text(
        project_raw.get("key") if isinstance(project_raw, dict) else project_raw
    )

    issue_type_raw = _field(fields, "issuetype") or _field(fields, "issueType")
    issue_type = _text(
        issue_type_raw.get("name")
        if isinstance(issue_type_raw, dict)
        else issue_type_raw
    )

    status_raw = _field(fields, "status")
    status = _text(
        status_raw.get("name") if isinstance(status_raw, dict) else status_raw
    )

    labels_raw = _field(fields, "labels")
    labels: tuple[str, ...] = ()
    if isinstance(labels_raw, list):
        labels = tuple(str(x) for x in labels_raw if x is not None)

    webhook_event = _text(payload.get("webhookEvent") or payload.get("event") or "")
    kind = _event_kind(webhook_event, payload)
    transition = _status_transition(payload)

    return ParsedJiraEvent(
        event_kind=kind,
        webhook_event=webhook_event or f"inferred:{kind}",
        key=key,
        summary=_text(_field(fields, "summary")),
        issue_type=issue_type,
        project=project,
        status=status,
        assignee=_text(_field(fields, "assignee")),
        priority=_text(_field(fields, "priority")),
        reporter=_text(_field(fields, "reporter")),
        url=_issue_url(payload, issue, key),
        labels=labels,
        transition=transition,
        raw=payload,
    )
