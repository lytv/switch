"""Shared observer boundaries use returned evidence, not source snapshots."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from switch_core.bridges.jira.observe import _stamp, observe_jira
from switch_core.bridges.jira.outbox import (
    ISSUE_CHANGED_DURING_READ,
    HumanChange,
    check_jira_version,
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


@pytest.mark.parametrize("gap_ms", [-1, 0, 50, 1000, 1001])
@pytest.mark.parametrize("worker_comment", [False, True])
def test_new_reporter_owned_comment_gap_boundary(gap_ms, worker_comment):
    version = comment_version("reporter-account", gap_ms)
    if worker_comment:
        other = comment_version("worker-account", gap_ms)["comments"][0]
        other["id"] = "2"
        version["comments"].append(other)
    result = observe(version, set())
    assert result.park == (None if 0 <= gap_ms <= 1000 else "stamp_mismatch")
    assert result.reporter_new == (version["comments"][0],)
    payload = {
        "based_updated": stamp(0),
        "based_changelog_id": "",
        "reporter_account_id": "reporter-account",
    }
    if result.park is None:
        check_jira_version(version, payload, "worker-account")
    else:
        with pytest.raises(HumanChange):
            check_jira_version(version, payload, "worker-account")


@pytest.mark.parametrize("case", ["newly_visible_edit", "other_author", "other_editor"])
def test_reporter_gap_requires_new_unedited_reporter_owned_comment(case):
    version = comment_version("reporter-account", 50)
    comment = version["comments"][0]
    if case == "newly_visible_edit":
        comment["created"] = stamp(0)
    elif case == "other_author":
        comment["author"] = {"accountId": "other-account"}
    else:
        comment["updateAuthor"] = {"accountId": "other-account"}
    assert observe(version, set()).park == (
        "human" if case == "other_editor" else "stamp_mismatch"
    )


@pytest.mark.parametrize(
    "case",
    [
        "transition",
        "field",
        "reporter_edit",
        "unattributed",
        "later_transition",
        "tied_transition",
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
    elif case == "reporter_edit":
        version = comment_version("reporter-account", 52)
        seen = {"1"}
        version["comments"][0]["created"] = stamp(0)
    elif case == "unattributed":
        version["comments"] = []
    elif case == "old_worker_comment_newly_visible":
        version["comments"][0]["created"] = stamp(0)
        version["comments"][0]["updated"] = stamp(0)
        version["updated"] = stamp(52)
    else:
        version["comments"][0]["author"] = {"accountId": "reporter-account"}
    assert observe(version, seen).park == (
        "unattributed" if case == "unattributed" else "stamp_mismatch"
    )


@pytest.mark.parametrize("latest_actor", ["worker-account", "reporter-account"])
@pytest.mark.parametrize("case", ["comment", "edit", "changelog", "automation"])
@pytest.mark.parametrize("milliseconds", [9999, 10_000, 10_026, 10_052])
def test_visible_human_items_still_park_before_or_inside_comment_gap(
    latest_actor, case, milliseconds
):
    version, seen = comment_version(latest_actor, 52), set()
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
            {
                "id": "1",
                "created": stamp(10_000),
                "author": {"accountId": "other-account"},
            }
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


def rank_version(actor, gap_ms):
    return {
        "updated": stamp(10_000 + gap_ms),
        "changelog_id": "1",
        "histories": [
            {
                "id": "1",
                "created": stamp(10_000),
                "author": {"accountId": actor},
                "items": [{"field": "Rank", "fieldtype": "custom"}],
            }
        ],
        "comments": [],
    }


@pytest.mark.parametrize("seen", [None, set()])
@pytest.mark.parametrize(
    "actor", ["reporter-account", "other-account", "worker-account"]
)
@pytest.mark.parametrize("gap_ms", [0, 168, 1000, 1001, -1])
def test_rank_only_history_and_bounded_final_updated_gap(seen, actor, gap_ms):
    result = observe(rank_version(actor, gap_ms), seen)
    assert result.park == (
        None if seen is None or 0 <= gap_ms <= 1000 else "stamp_mismatch"
    )
    assert result.first is (seen is None)
    assert result.changelog_id == "1"
    assert not result.new_ids and not result.reporter_new and not result.reporter_edits


@pytest.mark.parametrize("seen", [None, set()])
@pytest.mark.parametrize("actor", ["reporter-account", "other-account", ""])
@pytest.mark.parametrize(
    "items",
    [
        None,
        [],
        [{"field": "Rank"}],
        [{"field": "Rank", "fieldtype": "jira"}],
        [{"field": "Service", "fieldtype": "custom"}],
        [{"field": "description"}],
        [{"field": "status"}],
        [{"field": "Rank", "fieldtype": "custom"}, {"field": "description"}],
        [{"field": "Rank", "fieldtype": "custom"}, {"field": "status"}],
        [{"field": "Rank", "fieldtype": "custom"}, {}],
        [{"field": "Rank", "fieldtype": "custom"}, None],
    ],
)
def test_rank_exemption_requires_known_rank_only_items(seen, actor, items):
    version = rank_version(actor, 168)
    if items is None:
        del version["histories"][0]["items"]
    else:
        version["histories"][0]["items"] = items
    assert observe(version, seen).park == "human"


@pytest.mark.parametrize("seen", [None, set()])
def test_rank_with_unknown_author_still_parks(seen):
    assert observe(rank_version("", 168), seen).park == "human"


@pytest.mark.parametrize("seen", [None, set()])
@pytest.mark.parametrize(
    "case", ["tied_status", "later_status", "tied_reporter", "later_reporter"]
)
def test_rank_gap_does_not_hide_latest_meaningful_items(seen, case):
    version = rank_version("reporter-account", 168)
    latest = 10_001 if case.startswith("later") else 10_000
    if case.endswith("status"):
        version["histories"].append(
            {
                "id": "2",
                "created": stamp(latest),
                "author": {"accountId": "worker-account"},
                "items": [{"field": "status"}],
            }
        )
        version["changelog_id"] = "2"
    else:
        comment = comment_version("reporter-account", 0)["comments"][0]
        comment["created"] = comment["updated"] = stamp(latest)
        version["comments"].append(comment)
    assert observe(version, seen).park == (
        None if seen is None or case.endswith("reporter") else "stamp_mismatch"
    )
    version["updated"] = stamp(latest)
    assert observe(version, seen).park is None


@pytest.mark.parametrize("seen", [None, set()])
def test_timestamp_only_change_is_not_rank_evidence(seen):
    version = rank_version("reporter-account", 168)
    version["histories"] = []
    version["changelog_id"] = ""
    assert observe(version, seen).park == (None if seen is None else "unattributed")


def test_known_rank_does_not_explain_a_later_timestamp_only_change():
    version = rank_version("reporter-account", 168)
    result = observe_jira(
        version,
        seen_ids=set(),
        changelog_id="1",
        base_stamp=_stamp(stamp(10_000)),
        worker_account="worker-account",
        reporter_account="reporter-account",
    )
    assert result.park == "unattributed"


@pytest.mark.parametrize("seen", [None, set()])
def test_same_timestamp_rank_history_is_safe_without_a_timestamp_exemption(seen):
    version = rank_version("reporter-account", 0)
    version["histories"][0]["created"] = version["updated"] = stamp(0)
    assert observe(version, seen).park is None
    version["updated"] = stamp(168)
    assert observe(version, seen).park == (None if seen is None else "stamp_mismatch")


def test_rank_read_advances_changelog_baseline_without_repeating_input():
    version = rank_version("reporter-account", 168)
    previous = {**version["histories"][0], "id": "0", "created": stamp(0)}
    version["histories"].insert(0, previous)
    first = observe_jira(
        version,
        seen_ids=set(),
        changelog_id="0",
        base_stamp=BASE,
        worker_account="worker-account",
        reporter_account="reporter-account",
    )
    assert first.park is None and first.changelog_id == "1"
    repeated = observe_jira(
        version,
        seen_ids=set(first.seen_ids),
        changelog_id=first.changelog_id,
        base_stamp=_stamp(version["updated"]),
        worker_account="worker-account",
        reporter_account="reporter-account",
    )
    assert repeated.park is None and not repeated.new_ids


async def test_rank_history_from_jira_allows_the_shared_command_version_check():
    jira = JiraMock()
    version = rank_version("reporter-account", 168)
    jira.updated, jira.histories = version["updated"], version["histories"]
    client = client_for(jira)
    try:
        read = await read_jira_version(client, "KAN-1")
        payload = {"based_updated": stamp(0), "based_changelog_id": ""}
        check_jira_version(read, payload, "worker-account")
        read["updated"] = stamp(11_001)
        with pytest.raises(HumanChange):
            check_jira_version(read, payload, "worker-account")
    finally:
        await client.aclose()


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
