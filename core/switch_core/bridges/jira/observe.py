"""One Jira observer shared by every read path.

Comment IDs identify new input. Meaningful non-worker history and unknown
changes park intake. Verified Rank-only history does not. A forward stamp gap
up to one second requires that every latest attributable item is a worker-owned
comment or Rank-only history; other items require exact stamp attribution.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
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


def _rank_only(entry: dict[str, Any]) -> bool:
    items = entry.get("items")
    return (
        bool(_author(entry))
        and isinstance(items, list)
        and bool(items)
        and all(
            isinstance(item, dict)
            and item.get("field") == "Rank"
            and item.get("fieldtype") == "custom"
            for item in items
        )
    )


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
    first = seen_ids is None
    comments = version["comments"]
    histories = version["histories"]
    current_ids = {str(comment.get("id", "")) for comment in comments}
    by_id = {str(comment.get("id", "")): comment for comment in comments}
    current_changelog = str(version["changelog_id"])
    current_stamp = _stamp(version["updated"])
    reporter_ids = {
        comment_id
        for comment_id in current_ids
        if reporter_account
        and reporter_account != worker_account
        and comment_actor(by_id[comment_id]) == reporter_account
    }
    if seen_ids is None:
        pre_new = sorted(
            str(comment.get("id", ""))
            for comment in comments
            if _stamp(comment["created"]) > base_stamp
        )
        pre_edits = {
            str(comment.get("id", ""))
            for comment in comments
            if _stamp(comment["created"]) <= base_stamp
            and _stamp(comment["updated"]) > base_stamp
        }
        initial_reporter_new = [
            by_id[comment_id] for comment_id in pre_new if comment_id in reporter_ids
        ]
        initial_reporter_edits = [
            by_id[comment_id] for comment_id in sorted(pre_edits & reporter_ids)
        ]
        for comment_id in sorted(set(pre_new) | pre_edits):
            actor = comment_actor(by_id[comment_id])
            if actor == worker_account or comment_id in reporter_ids:
                continue
            return Observation(
                first=True,
                new_ids=frozenset(pre_new),
                reporter_new=tuple(initial_reporter_new),
                reporter_edits=tuple(initial_reporter_edits),
                park="human",
                seen_ids=tuple(sorted(current_ids)),
                changelog_id=current_changelog,
            )
        new_entries = [
            entry for entry in histories if _stamp(entry["created"]) > base_stamp
        ]
        new_ids = set(pre_new)
        reporter_new = initial_reporter_new
        edits = initial_reporter_edits
        seen_comments = current_ids - new_ids
        seen_ids = set()
    else:
        new_ids = current_ids - seen_ids
        seen_comments = current_ids & seen_ids
        reporter_new = [
            by_id[comment_id] for comment_id in sorted(new_ids & reporter_ids)
        ]
        edits = [
            by_id[comment_id]
            for comment_id in sorted(seen_comments & reporter_ids)
            if _stamp(by_id[comment_id]["updated"]) > base_stamp
        ]
        if current_stamp < base_stamp:
            return Observation(
                first=False,
                new_ids=frozenset(new_ids),
                reporter_new=tuple(reporter_new),
                reporter_edits=tuple(edits),
                park="regressed",
                seen_ids=tuple(sorted(seen_ids | current_ids)),
                changelog_id=current_changelog,
            )
        for comment_id in sorted(new_ids):
            actor = comment_actor(by_id[comment_id])
            if actor == worker_account or comment_id in reporter_ids:
                continue
            return Observation(
                first=False,
                new_ids=frozenset(new_ids),
                reporter_new=tuple(reporter_new),
                reporter_edits=tuple(edits),
                park="human",
                seen_ids=tuple(sorted(seen_ids | current_ids)),
                changelog_id=current_changelog,
            )
        if seen_ids - current_ids:
            return Observation(
                first=False,
                new_ids=frozenset(new_ids),
                reporter_new=tuple(reporter_new),
                reporter_edits=tuple(edits),
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
                    reporter_edits=tuple(edits),
                    park="changelog_missing",
                    seen_ids=tuple(sorted(seen_ids | current_ids)),
                    changelog_id=current_changelog,
                )
            new_entries = histories[index + 1 :]
        else:
            new_entries = list(histories)
    for entry in new_entries:
        if _author(entry) != worker_account and not _rank_only(entry):
            return Observation(
                first=first,
                new_ids=frozenset(new_ids),
                reporter_new=tuple(reporter_new),
                reporter_edits=tuple(edits),
                park="human",
                seen_ids=tuple(sorted(seen_ids | current_ids)),
                changelog_id=current_changelog,
            )
    attributable: list[tuple[datetime, bool]] = [
        (_stamp(entry["created"]), _rank_only(entry)) for entry in new_entries
    ]
    for comment_id in sorted(new_ids):
        comment = by_id[comment_id]
        actor = comment_actor(comment)
        if actor == worker_account or (actor and actor == reporter_account):
            own_comment = actor == worker_account and _author(comment) == worker_account
            attributable.append((_stamp(comment["created"]), own_comment))
            attributable.append((_stamp(comment["updated"]), own_comment))
    for comment_id in sorted(seen_comments):
        comment = by_id[comment_id]
        if _stamp(comment["updated"]) <= base_stamp:
            continue
        actor = comment_actor(comment)
        if actor == worker_account:
            attributable.append((_stamp(comment["updated"]), True))
            continue
        if actor and actor == reporter_account:
            attributable.append((_stamp(comment["updated"]), False))
            continue
        return Observation(
            first=first,
            new_ids=frozenset(new_ids),
            reporter_new=tuple(reporter_new),
            reporter_edits=tuple(edits),
            park="human",
            seen_ids=tuple(sorted(seen_ids | current_ids)),
            changelog_id=current_changelog,
        )
    if current_stamp > base_stamp and not attributable:
        return Observation(
            first=first,
            new_ids=frozenset(new_ids),
            reporter_new=tuple(reporter_new),
            reporter_edits=tuple(edits),
            park="unattributed",
            seen_ids=tuple(sorted(seen_ids | current_ids)),
            changelog_id=current_changelog,
        )
    if current_stamp > base_stamp and attributable:
        latest = max(stamp for stamp, _ in attributable)
        gap_allowed = all(allowed for stamp, allowed in attributable if stamp == latest)
        # Accepted risk: an unseen human change inside this second is undetectable.
        gap = current_stamp - latest
        attributed_gap = (
            gap_allowed
            and latest > base_stamp
            and timedelta(0) < gap <= timedelta(seconds=1)
        )
        if latest != current_stamp and not attributed_gap:
            return Observation(
                first=first,
                new_ids=frozenset(new_ids),
                reporter_new=tuple(reporter_new),
                reporter_edits=tuple(edits),
                park="stamp_mismatch",
                seen_ids=tuple(sorted(seen_ids | current_ids)),
                changelog_id=current_changelog,
            )
    return Observation(
        first=first,
        new_ids=frozenset(new_ids),
        reporter_new=tuple(reporter_new),
        reporter_edits=tuple(edits),
        seen_ids=tuple(sorted(seen_ids | current_ids)),
        changelog_id=current_changelog,
    )
