"""Generic trigger-source seam for inbound webhook bridges.

Isolates source-specific payload parsing from rule matching, rendering, and
delivery. Jira is the first (and currently only) parser implementation.
"""

from switch_core.bridges.trigger_source.event import (
    NormalizedTriggerEvent,
    StatusTransition,
)
from switch_core.bridges.trigger_source.parser import (
    PayloadParseError,
    TriggerSourceParser,
)

__all__ = [
    "NormalizedTriggerEvent",
    "PayloadParseError",
    "StatusTransition",
    "TriggerSourceParser",
]
