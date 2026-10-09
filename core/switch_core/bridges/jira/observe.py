"""One Jira observer shared by every read path.

Activity reads, the outbox version check, and the webhook intake all classify
the same way: comment newness only by ID, worker items ignored, new reporter
IDs recorded, other authors or non-worker changelog rows park. A stamp move
with no new ID and no new changelog row is attributed through the comments
already returned in the same read (updated/updateAuthor, no new state), even alongside new IDs
or changelog rows; only a still-unexplained move parks.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


def _stamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Jira updated stamp must include a timezone")
    return parsed


def comment_actor(comment: dict[str, Any]) -> str:
    source = comment.get("updateAuthor", comment.get("author", {}))
    if not isinstance(source, dict):
        return ""
    account = source.get("accountId", "")
    return account if isinstance(account, str) else ""


def _author(entry: dict[str, Any]) -> str:
    author = entry.get("author", {})
    if not isinstance(author, dict):
        return ""
    account = author.get("accountId", "")
    return account if isinstance(account, str) else ""


@dataclass(frozen=True)
class Observation:
    first: bool
    new_ids: frozenset[str] = frozenset()
    reporter_new: tuple[dict[str, Any], ...] = ()
    reporter_edits: tuple[dict[str, Any], ...] = ()
    park: str | None = None
    seen_ids: tuple[str, ...] = ()
    changelog_id: str = ""


def observe_jira(
    version: dict[str, Any],
    *,
    seen_ids: set[str] | None,
    changelog_id: str | None,
    base_stamp: datetime,
    worker_account: str,
    reporter_account: str,
) -> Observation:
    comments = version["comments"]
    histories = version["histories"]
    current_ids = {str(comment.get("id", "")) for comment in comments}
    by_id = {str(comment.get("id", "")): comment for comment in comments}
    current_changelog = str(version["changelog_id"])
    current_stamp = _stamp(version["updated"])
    if seen_ids is None:
        pre_new = sorted(
            str(comment.get("id", ""))
            for comment in comments
            if _stamp(comment["created"]) > base_stamp
        )
        pre_by_id = {str(comment.get("id", "")): comment for comment in comments}
        initial_reporter_new = []
        for comment_id in pre_new:
            actor = comment_actor(pre_by_id[comment_id])
            if actor == worker_account:
                continue
            if actor and actor == reporter_account:
                initial_reporter_new.append(pre_by_id[comment_id])
                continue
            return Observation(
                first=True,
                new_ids=frozenset(pre_new),
                reporter_new=tuple(initial_reporter_new),
                park="human",
                seen_ids=tuple(sorted(current_ids)),
                changelog_id=current_changelog,
            )
        for entry in histories:
            if (
                _stamp(entry["created"]) > base_stamp
                and _author(entry) != worker_account
            ):
                return Observation(
                    first=True,
                    new_ids=frozenset(pre_new),
                    reporter_new=tuple(initial_reporter_new),
                    park="human",
                    seen_ids=tuple(sorted(current_ids)),
                    changelog_id=current_changelog,
                )
        return Observation(
            first=True,
            new_ids=frozenset(pre_new),
            reporter_new=tuple(initial_reporter_new),
            seen_ids=tuple(sorted(current_ids)),
            changelog_id=current_changelog,
        )
    if current_stamp < base_stamp:
        return Observation(
            first=False,
            park="regressed",
            seen_ids=tuple(sorted(seen_ids | current_ids)),
            changelog_id=current_changelog,
        )
    new_ids = current_ids - seen_ids
    reporter_new: list[dict[str, Any]] = []
    for comment_id in sorted(new_ids):
        actor = comment_actor(by_id[comment_id])
        if actor == worker_account:
            continue
        if actor and actor == reporter_account:
            reporter_new.append(by_id[comment_id])
            continue
        return Observation(
            first=False,
            new_ids=frozenset(new_ids),
            reporter_new=tuple(reporter_new),
            park="human",
            seen_ids=tuple(sorted(seen_ids | current_ids)),
            changelog_id=current_changelog,
        )
    base_id = changelog_id if isinstance(changelog_id, str) else ""
    if base_id:
        index = next(
            (i for i, entry in enumerate(histories) if str(entry["id"]) == base_id),
            None,
        )
        if index is None:
            return Observation(
                first=False,
                new_ids=frozenset(new_ids),
                reporter_new=tuple(reporter_new),
                park="changelog_missing",
                seen_ids=tuple(sorted(seen_ids | current_ids)),
                changelog_id=current_changelog,
            )
        new_entries = histories[index + 1 :]
    else:
        new_entries = list(histories)
    for entry in new_entries:
        if _author(entry) != worker_account:
            return Observation(
                first=False,
                new_ids=frozenset(new_ids),
                reporter_new=tuple(reporter_new),
                park="human",
                seen_ids=tuple(sorted(seen_ids | current_ids)),
                changelog_id=current_changelog,
            )
    attributable: list[datetime] = [_stamp(entry["created"]) for entry in new_entries]
    for comment_id in sorted(new_ids):
        actor = comment_actor(by_id[comment_id])
        if actor == worker_account or (actor and actor == reporter_account):
            attributable.append(_stamp(by_id[comment_id]["created"]))
            attributable.append(_stamp(by_id[comment_id]["updated"]))
    seen_comments = current_ids & seen_ids
    edits: list[dict[str, Any]] = []
    for comment_id in sorted(seen_comments):
        comment = by_id[comment_id]
        if _stamp(comment["updated"]) <= base_stamp:
            continue
        actor = comment_actor(comment)
        if actor == worker_account:
            attributable.append(_stamp(comment["updated"]))
            continue
        if actor and actor == reporter_account:
            attributable.append(_stamp(comment["updated"]))
            edits.append(comment)
            continue
        return Observation(
            first=False,
            new_ids=frozenset(new_ids),
            reporter_new=tuple(reporter_new),
            park="human",
            seen_ids=tuple(sorted(seen_ids | current_ids)),
            changelog_id=current_changelog,
        )
    if current_stamp > base_stamp and not attributable:
        return Observation(
            first=False,
            park="unattributed",
            seen_ids=tuple(sorted(seen_ids | current_ids)),
            changelog_id=current_changelog,
        )
    if (
        current_stamp > base_stamp
        and attributable
        and max(attributable) != current_stamp
    ):
        return Observation(
            first=False,
            new_ids=frozenset(new_ids),
            reporter_new=tuple(reporter_new),
            park="stamp_mismatch",
            seen_ids=tuple(sorted(seen_ids | current_ids)),
            changelog_id=current_changelog,
        )
    return Observation(
        first=False,
        new_ids=frozenset(new_ids),
        reporter_new=tuple(reporter_new),
        reporter_edits=tuple(edits),
        seen_ids=tuple(sorted(seen_ids | current_ids)),
        changelog_id=current_changelog,
    )
