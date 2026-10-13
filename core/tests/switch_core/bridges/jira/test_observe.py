"""Shared observer boundaries use returned evidence, not source snapshots."""

from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime, timedelta

import pytest

from switch_core.bridges.jira.observe import _stamp, observe_jira
from switch_core.bridges.jira.outbox import (
    ISSUE_CHANGED_DURING_READ,
    HumanChange,
    read_jira_version,
)
from tests.switch_core.bridges.jira.test_outbox import JiraMock, client_for

BASE = datetime(2026, 10, 2, tzinfo=UTC)


def stamp(milliseconds):
    return (BASE + timedelta(milliseconds=milliseconds)).isoformat()


def comment_version(actor, gap_ms):
    return {
        "updated": stamp(10_000 + gap_ms),
        "changelog_id": "",
        "histories": [],
        "comments": [
            {
                "id": "1",
                "created": stamp(10_000),
                "updated": stamp(10_000),
                "author": {"accountId": actor},
                "updateAuthor": {"accountId": actor},
            }
        ],
    }


def observe(version, seen_ids):
    return observe_jira(
        version,
        seen_ids=seen_ids,
        changelog_id="",
        base_stamp=BASE,
        worker_account="worker-account",
        reporter_account="reporter-account",
    )


@pytest.mark.parametrize("edited", [False, True])
@pytest.mark.parametrize("gap_ms", [0, 14, 33, 47, 52, 1000, 1001, -1])
def test_worker_comment_gap_boundary(edited, gap_ms):
    version = comment_version("worker-account", gap_ms)
    seen = {"1"} if edited else set()
    if edited:
        version["comments"][0]["created"] = stamp(0)
    result = observe(version, seen)
    assert result.park == (None if 0 <= gap_ms <= 1000 else "stamp_mismatch")
    assert not result.reporter_new and not result.reporter_edits


@pytest.mark.parametrize(
    "case",
    [
        "transition",
        "field",
        "reporter_comment",
        "reporter_edit",
        "unattributed",
        "later_transition",
        "later_reporter",
        "tied_transition",
        "tied_reporter",
        "new_reporter_comment_edited_by_worker",
        "old_worker_comment_newly_visible",
    ],
)
def test_non_worker_comment_latest_items_stay_exact(case):
    version, seen = comment_version("worker-account", 52), set()
    if case in ("transition", "field", "later_transition", "tied_transition"):
        entry_stamp = 10_001 if case == "later_transition" else 10_000
        version["histories"] = [
            {
                "id": "1",
                "created": stamp(entry_stamp),
                "author": {"accountId": "worker-account"},
                "items": [{"field": "summary" if case == "field" else "status"}],
            }
        ]
        version["changelog_id"] = "1"
        if case in ("transition", "field"):
            version["comments"] = []
    elif case in ("reporter_comment", "reporter_edit"):
        version = comment_version("reporter-account", 52)
        if case == "reporter_edit":
            seen = {"1"}
            version["comments"][0]["created"] = stamp(0)
    elif case == "unattributed":
        version["comments"] = []
    elif case in ("later_reporter", "tied_reporter"):
        other = comment_version("reporter-account", 52)["comments"][0]
        other["id"] = "2"
        other["created"] = other["updated"] = stamp(
            10_001 if case == "later_reporter" else 10_000
        )
        version["comments"].append(other)
    elif case == "old_worker_comment_newly_visible":
        version["comments"][0]["created"] = stamp(0)
        version["comments"][0]["updated"] = stamp(0)
        version["updated"] = stamp(52)
    else:
        version["comments"][0]["author"] = {"accountId": "reporter-account"}
    assert observe(version, seen).park == (
        "unattributed" if case == "unattributed" else "stamp_mismatch"
    )


@pytest.mark.parametrize("case", ["comment", "edit", "changelog", "automation"])
@pytest.mark.parametrize("milliseconds", [9999, 10_000, 10_026, 10_052])
def test_visible_human_items_still_park_before_or_inside_worker_gap(case, milliseconds):
    version, seen = comment_version("worker-account", 52), set()
    if case in ("changelog", "automation"):
        version["histories"] = [
            {
                "id": "1",
                "created": stamp(milliseconds),
                "author": {
                    "accountId": "automation-account"
                    if case == "automation"
                    else "other-account"
                },
            }
        ]
        version["changelog_id"] = "1"
    else:
        other = comment_version("other-account", 0)["comments"][0]
        other["id"] = "2"
        other["updated"] = stamp(milliseconds)
        if case == "edit":
            seen = {"2"}
            other["created"] = stamp(0)
            other["author"] = {"accountId": "worker-account"}
        else:
            other["created"] = stamp(milliseconds)
        version["comments"].append(other)
    assert observe(version, seen).park == "human"


@pytest.mark.parametrize("gap_ms", [0, 52])
def test_deleted_known_comment_never_disappears_into_worker_attribution(gap_ms):
    assert (
        observe(comment_version("worker-account", gap_ms), {"deleted"}).park == "human"
    )


def test_regression_and_missing_history_still_park():
    version = comment_version("worker-account", 52)
    missing = observe_jira(
        version,
        seen_ids=set(),
        changelog_id="missing",
        base_stamp=BASE,
        worker_account="worker-account",
        reporter_account="reporter-account",
    )
    assert missing.park == "changelog_missing"
    version["updated"] = stamp(-1)
    assert observe(version, set()).park == "regressed"


def test_repeated_accepted_read_does_not_repeat_reporter_input():
    version = comment_version("worker-account", 52)
    first = observe(version, set())
    repeated = observe_jira(
        version,
        seen_ids=set(first.seen_ids),
        changelog_id=first.changelog_id,
        base_stamp=_stamp(version["updated"]),
        worker_account="worker-account",
        reporter_account="reporter-account",
    )
    assert first.park is repeated.park is None
    assert not repeated.new_ids and not repeated.reporter_edits


def test_hidden_change_is_an_explicit_accepted_evidence_limit():
    drift = comment_version("worker-account", 52)
    hidden_change = deepcopy(drift)
    assert drift == hidden_change
    assert observe(drift, set()).park is None
    assert observe(hidden_change, set()).park is None


class MillisecondReadChange(JiraMock):
    def __init__(self):
        super().__init__()
        self.issue_reads = 0

    async def handle(self, request):
        if (
            request.method == "GET"
            and "/issue/" in request.url.path
            and not request.url.path.endswith(("/comment", "/changelog"))
        ):
            self.issue_reads += 1
            if self.issue_reads == 2:
                self.updated = (
                    _stamp(self.updated) + timedelta(milliseconds=1)
                ).isoformat()
        return await super().handle(request)


async def test_one_ms_change_while_reading_history_still_stops():
    client = client_for(MillisecondReadChange())
    try:
        with pytest.raises(HumanChange, match=ISSUE_CHANGED_DURING_READ):
            await read_jira_version(client, "KAN-1")
    finally:
        await client.aclose()
