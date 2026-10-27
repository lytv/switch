"""Immutable draft pack snapshots. This module does not enable delivery."""

from __future__ import annotations

import json
from importlib.resources import files
from typing import TYPE_CHECKING, Self

from pydantic import model_validator

from switch_core.bridges.jira.contracts import Budget, Contract, Digest, Pack, digest

if TYPE_CHECKING:
    from switch_core.config import SwitchConfig


class FrozenPack(Contract):
    snapshot: Pack
    digest: Digest

    @model_validator(mode="after")
    def snapshot_matches_digest(self) -> Self:
        if digest(self.snapshot) != self.digest:
            raise ValueError("Pack snapshot changed")
        return self


def load_pack(config: SwitchConfig) -> FrozenPack:
    """Check the immutable release before freezing user-configured budgets."""
    version = config.jira_worker_delivery_pack
    source = files("switch_core.bridges.jira").joinpath(
        f"packs/delivery/{version}.json"
    )
    pack = Pack.model_validate_json(source.read_text(encoding="utf-8"))
    pins = {
        "2.0.0-draft": "965793653f39572b8b0a85134ef1ebeb5c0a77c19cbe81cc689354166d428fbf",
        "2.0.0": "aa1e6a26ec686cd0097d6fe416258cac5989029511dca798fbb34e94314a07b6",
    }
    if digest(pack) != pins[version]:
        raise ValueError(f"Historical immutable delivery@{version} pack changed")
    budget = Budget(
        ticket_tokens=config.jira_worker_delivery_ticket_tokens,
        ticket_estimated_usd=config.jira_worker_delivery_ticket_estimated_usd,
        day_estimated_usd=config.jira_worker_delivery_day_estimated_usd,
    )
    data = pack.model_dump(mode="json")
    data["budget"] = budget.model_dump(mode="json")
    snapshot = Pack.model_validate_json(json.dumps(data))
    return FrozenPack(snapshot=snapshot, digest=digest(snapshot))
