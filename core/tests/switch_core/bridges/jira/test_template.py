from __future__ import annotations

from switch_core.bridges.jira.parse import ParsedJiraEvent, StatusTransition
from switch_core.bridges.jira.template import render_template, strip_html


def _event(**overrides: object) -> ParsedJiraEvent:
    base = dict(
        event_kind="updated",
        webhook_event="jira:issue_updated",
        key="PROJ-1",
        summary="Add <b>export</b>",
        issue_type="Story",
        project="PROJ",
        status="In Progress",
        assignee="Ada",
        priority="High",
        reporter="Bob",
        url="https://example.atlassian.net/browse/PROJ-1",
        labels=("agentic",),
        transition=StatusTransition("To Do", "In Progress"),
    )
    base.update(overrides)
    return ParsedJiraEvent(**base)  # type: ignore[arg-type]


class TestTemplateRender:
    def test_documented_tokens(self) -> None:
        rendered = render_template(
            "{{issue.key}} {{issue.summary}} {{issue.status}} "
            "{{issue.assignee}} {{issue.priority}} {{issue.url}} "
            "{{issue.reporter}} {{transition.from}}->{{transition.to}} {{transition}}",
            _event(),
            max_chars=4000,
        )
        assert "PROJ-1" in rendered
        assert "export" in rendered
        assert "<b>" not in rendered
        assert "In Progress" in rendered
        assert "Ada" in rendered
        assert "High" in rendered
        assert "https://example.atlassian.net/browse/PROJ-1" in rendered
        assert "Bob" in rendered
        assert "To Do->In Progress" in rendered
        assert "→" in rendered

    def test_missing_token_empty(self) -> None:
        rendered = render_template(
            "before {{issue.missing}} after",
            _event(),
            max_chars=4000,
        )
        assert rendered == "before  after"

    def test_strips_html(self) -> None:
        assert strip_html("<script>x</script>hi") == "hi"
        rendered = render_template(
            "{{issue.summary}}",
            _event(summary="<img src=x onerror=alert(1)>Boom"),
            max_chars=4000,
        )
        assert "<img" not in rendered
        assert "Boom" in rendered

    def test_length_capped(self) -> None:
        rendered = render_template(
            "{{issue.summary}}",
            _event(summary="abcdefghij"),
            max_chars=5,
        )
        assert len(rendered) <= 5
        assert rendered.endswith("…")
