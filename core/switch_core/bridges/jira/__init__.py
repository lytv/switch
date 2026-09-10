"""Jira inbound webhook bridge.

Posts addressed room messages from signed Jira events via stored trigger
rules. Payload parsing implements ``bridges.trigger_source.TriggerSourceParser``;
matching, rendering, and delivery consume ``NormalizedTriggerEvent``.
"""

from switch_core.bridges.jira.routes import router

__all__ = ["router"]

__all__ = ["router"]
