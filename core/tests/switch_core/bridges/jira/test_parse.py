from __future__ import annotations

import pytest

from switch_core.bridges.jira.parse import JiraPayloadError, parse_jira_payload


def _issue_created() -> dict:
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


def _issue_updated_status() -> dict:
    body = _issue_created()
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
    return body


class TestParseJiraPayload:
    def test_issue_created(self) -> None:
        event = parse_jira_payload(_issue_created())
        assert event.event_kind == "created"
        assert event.key == "PROJ-1"
        assert event.summary == "Add export"
        assert event.issue_type == "Story"
        assert event.project == "PROJ"
        assert event.status == "To Do"
        assert event.assignee == "Ada"
        assert event.priority == "High"
        assert event.reporter == "Bob"
        assert event.url == "https://example.atlassian.net/browse/PROJ-1"
        assert event.labels == ("agentic",)
        assert event.transition is None

    def test_issue_updated_reads_transition(self) -> None:
        event = parse_jira_payload(_issue_updated_status())
        assert event.event_kind == "updated"
        assert event.transition is not None
        assert event.transition.from_status == "To Do"
        assert event.transition.to_status == "In Progress"

    def test_extra_fields_tolerated(self) -> None:
        body = _issue_created()
        body["weird"] = {"nested": True}
        body["issue"]["fields"]["customfield_10000"] = "x"
        event = parse_jira_payload(body)
        assert event.key == "PROJ-1"

    def test_malformed_missing_issue(self) -> None:
        with pytest.raises(JiraPayloadError, match="missing issue"):
            parse_jira_payload({"webhookEvent": "jira:issue_created"})

    def test_malformed_not_object(self) -> None:
        with pytest.raises(JiraPayloadError, match="JSON object"):
            parse_jira_payload(["not", "a", "dict"])

    def test_flattened_automation_shape(self) -> None:
        event = parse_jira_payload(
            {
                "key": "SUP-9",
                "fields": {
                    "summary": "Ping",
                    "issuetype": {"name": "Bug"},
                    "project": {"key": "SUP"},
                    "status": {"name": "Open"},
                },
            }
        )
        assert event.key == "SUP-9"
        assert event.event_kind == "created"
