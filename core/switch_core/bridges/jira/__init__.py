"""Jira inbound webhook bridge (Phase 1).

Posts addressed room messages from signed Jira events via stored trigger rules.
"""

from switch_core.bridges.jira.routes import router

__all__ = ["router"]
