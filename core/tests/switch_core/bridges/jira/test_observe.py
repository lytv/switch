"""Shared observer boundaries use returned evidence, not source snapshots."""

from __future__ import annotations

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


@pytest.mark.parametrize("gap_ms", [0, 52])
def test_deleted_known_comment_preserves_same_read_reporter_input(gap_ms):
    version = comment_version("worker-account", gap_ms)
    reporter = comment_version("reporter-account", 0)["comments"][0]
    reporter["id"] = "7"
    version["comments"].append(reporter)
    result = observe(version, {"3"})
    assert result.park == "human"
    assert result.new_ids == frozenset({"1", "7"})
    assert result.reporter_new == (reporter,)
    assert result.seen_ids == ("1", "3", "7")


@pytest.mark.parametrize("other_id", ["1", "8"])
@pytest.mark.parametrize(
    "case",
    [
        "deleted",
        "new_comment",
        "missing_history",
        "changelog",
        "seen_edit",
        "stamp_mismatch",
        "regressed",
        "initial_comment",
        "initial_history",
    ],
)
def test_park_returns_preserve_all_reporter_evidence(case, other_id):
    version = comment_version("reporter-account", 0)
    reporter_edit = version["comments"][0]
    reporter_edit["id"] = "4"
    reporter_edit["created"] = stamp(0)
    reporter_new = comment_version("reporter-account", 0)["comments"][0]
    reporter_new["id"] = "7"
    version["comments"].append(reporter_new)
    seen, changelog_id = {"4"}, ""
    if case == "deleted":
        seen.add("3")
    elif case in ("new_comment", "seen_edit", "initial_comment"):
        other = comment_version("other-account", 0)["comments"][0]
        other["id"] = other_id
        if case == "seen_edit":
            seen.add(other_id)
            other["created"] = stamp(0)
        version["comments"].append(other)
    elif case == "missing_history":
        changelog_id = "missing"
    elif case in ("changelog", "initial_history"):
        version["histories"] = [
            {"id": "1", "created": stamp(10_000), "author": {"accountId": "other-account"}}
        ]
        version["changelog_id"] = "1"
    elif case == "stamp_mismatch":
        version["updated"] = stamp(10_052)
    elif case == "regressed":
        version["updated"] = stamp(-1)
    if case.startswith("initial"):
        seen = None
    result = observe_jira(
        version,
        seen_ids=seen,
        changelog_id=changelog_id,
        base_stamp=BASE,
        worker_account="worker-account",
        reporter_account="reporter-account",
    )
    expected_park = {
        "missing_history": "changelog_missing",
        "stamp_mismatch": "stamp_mismatch",
        "regressed": "regressed",
    }.get(case, "human")
    assert result.park == expected_park
    assert result.reporter_new == (reporter_new,)
    assert result.reporter_edits == (reporter_edit,)
    assert "7" in result.new_ids


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
