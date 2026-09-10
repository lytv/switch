from __future__ import annotations

from types import SimpleNamespace

from switch_core.bridges.jira.matching import matching_rules, rule_matches
from switch_core.bridges.jira.parse import ParsedJiraEvent, StatusTransition


def _event(**overrides: object) -> ParsedJiraEvent:
    base = dict(
        event_kind="updated",
        webhook_event="jira:issue_updated",
        key="PROJ-1",
        summary="Add export",
        issue_type="Story",
        project="PROJ",
        status="In Progress",
        assignee="Ada",
        priority="High",
        reporter="Bob",
        url="https://example/browse/PROJ-1",
        labels=("agentic", "urgent"),
        transition=StatusTransition("To Do", "In Progress"),
    )
    base.update(overrides)
    return ParsedJiraEvent(**base)  # type: ignore[arg-type]


def _rule(**overrides: object) -> SimpleNamespace:
    base = dict(
        enabled=True,
        project_key="PROJ",
        issue_type="Story",
        fire_on="transition",
        target_status="In Progress",
        jql="",
    )
    base.update(overrides)
    return SimpleNamespace(**base)


class TestRuleMatching:
    def test_transition_match(self) -> None:
        assert rule_matches(_rule(), _event()) is True

    def test_blank_project_is_wildcard(self) -> None:
        assert rule_matches(_rule(project_key=""), _event(project="OTHER")) is True

    def test_blank_issue_type_is_wildcard(self) -> None:
        assert rule_matches(_rule(issue_type=""), _event(issue_type="Bug")) is True

    def test_wrong_project(self) -> None:
        assert rule_matches(_rule(project_key="OTHER"), _event()) is False

    def test_created_fire_on(self) -> None:
        assert (
            rule_matches(
                _rule(fire_on="created", target_status=""),
                _event(event_kind="created", transition=None),
            )
            is True
        )
        assert (
            rule_matches(
                _rule(fire_on="created", target_status=""),
                _event(),
            )
            is False
        )

    def test_transition_without_changelog_does_not_match(self) -> None:
        assert rule_matches(_rule(), _event(transition=None)) is False

    def test_jql_labels(self) -> None:
        assert rule_matches(_rule(jql='labels = "agentic"'), _event()) is True
        assert rule_matches(_rule(jql="labels = missing"), _event()) is False

    def test_jql_and(self) -> None:
        assert (
            rule_matches(
                _rule(jql="priority = High AND labels = agentic"),
                _event(),
            )
            is True
        )
        assert (
            rule_matches(
                _rule(jql="priority = Low AND labels = agentic"),
                _event(),
            )
            is False
        )

    def test_disabled_never_matches(self) -> None:
        assert rule_matches(_rule(enabled=False), _event()) is False

    def test_zero_matches_ok(self) -> None:
        assert matching_rules([_rule(project_key="ZZ")], _event()) == []

    def test_multiple_rules_independent(self) -> None:
        a = _rule(name="a")
        b = _rule(name="b", project_key="")
        c = _rule(name="c", project_key="NO")
        matches = matching_rules([a, b, c], _event())
        assert matches == [a, b]
