"""Parser seam: raw source payload → NormalizedTriggerEvent."""

from __future__ import annotations

from typing import Any, Protocol

from switch_core.bridges.trigger_source.event import NormalizedTriggerEvent


class PayloadParseError(ValueError):
    """The webhook body cannot be normalized into a trigger event."""


class TriggerSourceParser(Protocol):
    """One implementation per inbound source. No registry — call sites pick it."""

    def parse(self, payload: Any) -> NormalizedTriggerEvent:
        """Map a source-specific body into the shared event contract.

        Raises ``PayloadParseError`` (or a subclass) when the body is unusable.
        """
        ...
