"""Prove the generic trigger-source seam and Jira as its first parser."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from switch_core.bridges.jira.matching import rule_matches
from switch_core.bridges.jira.parse import (
    JiraPayloadError,
    JiraPayloadParser,
    ParsedJiraEvent,
    parse_jira_payload,
)
from switch_core.bridges.jira.template import render_template
from switch_core.bridges.trigger_source import (
    NormalizedTriggerEvent,
    PayloadParseError,
    StatusTransition,
    TriggerSourceParser,
)


def _jira_created_payload() -> dict[str, Any]:
    return {
        "webhookEvent": "jira:issue_created",
        "issue": {
            "key": "PROJ-1",
            "self": "https://example.atlassian.net/rest/api/2/issue/10001",
            "fields": {
                "summary": "Add export",
                "issuetype": {"name": "Story"},
                "project": {"key": "PROJ"},
                "status": {"name": "To Do"},
                "assignee": {"displayName": "Ada"},
                "priority": {"name": "High"},
                "reporter": {"displayName": "Bob"},
                "labels": ["agentic"],
            },
        },
    }


def _rule(**overrides: object) -> SimpleNamespace:
    base: dict[str, object] = {
        "enabled": True,
        "project_key": "PROJ",
        "issue_type": "",
        "fire_on": "created",
        "target_status": "",
        "jql": "",
    }
    base.update(overrides)
    return SimpleNamespace(**base)


class _SyntheticParser:
    """Minimal non-Jira TriggerSourceParser for seam tests only."""

    def parse(self, payload: Any) -> NormalizedTriggerEvent:
        if not isinstance(payload, dict) or "key" not in payload:
            raise PayloadParseError("synthetic payload needs key")
        return NormalizedTriggerEvent(
            event_kind=str(payload.get("event_kind") or "created"),
            webhook_event=str(payload.get("webhook_event") or "synthetic:created"),
            key=str(payload["key"]),
            summary=str(payload.get("summary") or ""),
            issue_type=str(payload.get("issue_type") or ""),
            project=str(payload.get("project") or ""),
            status=str(payload.get("status") or ""),
            assignee=str(payload.get("assignee") or ""),
            priority=str(payload.get("priority") or ""),
            reporter=str(payload.get("reporter") or ""),
            url=str(payload.get("url") or ""),
            labels=tuple(payload.get("labels") or ()),
            transition=None,
            raw=payload,
        )


class TestNormalizedEventContract:
    def test_parsed_jira_event_is_normalized_alias(self) -> None:
        assert ParsedJiraEvent is NormalizedTriggerEvent

    def test_jira_payload_error_is_payload_parse_error(self) -> None:
        assert issubclass(JiraPayloadError, PayloadParseError)

    def test_jira_parser_satisfies_protocol(self) -> None:
        parser: TriggerSourceParser = JiraPayloadParser()
        event = parser.parse(_jira_created_payload())
        assert isinstance(event, NormalizedTriggerEvent)
        assert event.key == "PROJ-1"
        assert event.event_kind == "created"

    def test_parse_jira_payload_returns_normalized_event(self) -> None:
        event = parse_jira_payload(_jira_created_payload())
        assert type(event) is NormalizedTriggerEvent
        assert event.summary == "Add export"
        assert event.project == "PROJ"
        assert event.labels == ("agentic",)
        assert event.url == "https://example.atlassian.net/browse/PROJ-1"

    def test_jira_transition_uses_shared_status_transition(self) -> None:
        body = _jira_created_payload()
        body["webhookEvent"] = "jira:issue_updated"
        body["issue"]["fields"]["status"] = {"name": "In Progress"}
        body["changelog"] = {
            "items": [
                {
                    "field": "status",
                    "fromString": "To Do",
                    "toString": "In Progress",
                }
            ]
        }
        event = parse_jira_payload(body)
        assert isinstance(event.transition, StatusTransition)
        assert event.transition.from_status == "To Do"
        assert event.transition.to_status == "In Progress"


class TestSourceAgnosticMatchingAndRender:
    def test_synthetic_parser_event_matches_and_renders(self) -> None:
        parser: TriggerSourceParser = _SyntheticParser()
        event = parser.parse(
            {
                "key": "SRC-9",
                "summary": "From elsewhere",
                "project": "PROJ",
                "event_kind": "created",
            }
        )
        assert rule_matches(_rule(), event) is True
        rendered = render_template(
            "{{issue.key}}: {{issue.summary}}",
            event,
            max_chars=4000,
        )
        assert rendered == "SRC-9: From elsewhere"

    def test_jira_payload_same_match_and_render_as_before(self) -> None:
        event = parse_jira_payload(_jira_created_payload())
        assert rule_matches(_rule(), event) is True
        assert rule_matches(_rule(project_key="OTHER"), event) is False
        rendered = render_template(
            "{{issue.key}} {{issue.summary}} @{{issue.assignee}}",
            event,
            max_chars=4000,
        )
        assert rendered == "PROJ-1 Add export @Ada"
