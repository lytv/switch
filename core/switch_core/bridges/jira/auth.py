from __future__ import annotations

import hmac
import logging

logger = logging.getLogger(__name__)

SECRET_HEADER = "X-Switch-Secret"


def verify_instance_secret(
    *,
    instance: str,
    provided: str | None,
    secrets_by_instance: dict[str, str],
) -> bool:
    """Return True when ``provided`` matches the configured secret for ``instance``.

    Uses constant-time compare. Missing instance or missing/wrong secret is False.
    Never logs the secret value.
    """
    expected = secrets_by_instance.get(instance)
    if expected is None or expected == "":
        logger.warning(
            "Jira webhook rejected: no secret configured for instance %r", instance
        )
        return False
    if provided is None or provided == "":
        logger.warning(
            "Jira webhook rejected: missing %s for instance %r",
            SECRET_HEADER,
            instance,
        )
        return False
    if not hmac.compare_digest(provided, expected):
        logger.warning("Jira webhook rejected: wrong secret for instance %r", instance)
        return False
    return True
