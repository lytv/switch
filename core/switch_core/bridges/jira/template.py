from __future__ import annotations

import html
import re
from html.parser import HTMLParser

from switch_core.bridges.jira.parse import ParsedJiraEvent

# Proposal tokens: {{issue.key}}, {{transition.from}}, …
_TOKEN_RE = re.compile(r"\{\{\s*([a-zA-Z0-9_.]+)\s*\}\}")


class _StripTags(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._parts: list[str] = []
        self._skip_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.casefold() in {"script", "style"}:
            self._skip_depth += 1

    def handle_endtag(self, tag: str) -> None:
        if tag.casefold() in {"script", "style"} and self._skip_depth:
            self._skip_depth -= 1

    def handle_data(self, data: str) -> None:
        if self._skip_depth == 0:
            self._parts.append(data)

    def get_text(self) -> str:
        return "".join(self._parts)


def strip_html(value: str) -> str:
    """Remove HTML tags and unescape entities so Jira markup cannot enter a room."""
    if not value:
        return ""
    parser = _StripTags()
    try:
        parser.feed(value)
        parser.close()
        text = parser.get_text()
    except Exception:
        text = re.sub(r"<[^>]+>", "", value)
    return html.unescape(text)


def _token_values(event: ParsedJiraEvent) -> dict[str, str]:
    transition_from = event.transition.from_status if event.transition else ""
    transition_to = event.transition.to_status if event.transition else ""
    transition = f"{transition_from} → {transition_to}" if event.transition else ""
    return {
        "issue.key": event.key,
        "issue.summary": event.summary,
        "issue.status": event.status,
        "issue.assignee": event.assignee,
        "issue.priority": event.priority,
        "issue.url": event.url,
        "issue.reporter": event.reporter,
        "issue.type": event.issue_type,
        "issue.project": event.project,
        "key": event.key,
        "summary": event.summary,
        "status": event.status,
        "assignee": event.assignee,
        "priority": event.priority,
        "url": event.url,
        "reporter": event.reporter,
        "transition": transition,
        "transition.from": transition_from,
        "transition.to": transition_to,
    }


def render_template(
    template: str,
    event: ParsedJiraEvent,
    *,
    max_chars: int,
) -> str:
    """Fill ``{{…}}`` tokens from the parsed issue; missing tokens become empty.

    HTML from Jira field values is stripped. The result is truncated to
    ``max_chars`` so a room message stays bounded.
    """
    values = {k: strip_html(v) for k, v in _token_values(event).items()}

    def _replace(match: re.Match[str]) -> str:
        return values.get(match.group(1), "")

    rendered = _TOKEN_RE.sub(_replace, template)
    rendered = strip_html(rendered)
    if max_chars > 0 and len(rendered) > max_chars:
        rendered = rendered[: max(0, max_chars - 1)] + "…"
    return rendered
