from __future__ import annotations

import logging

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from switch_core.bridges.agent.protocol.service import ProtocolService
from switch_core.bridges.agent.protocol.types import (
    IntegrationProfile,
    TaskProtocolConfig,
)
from switch_core.config import SwitchConfig
from switch_core.db.stores.agent_store import AgentStore
from switch_core.db.stores.user_store import UserStore

logger = logging.getLogger(__name__)

JIRA_AGENT_METADATA_KEY = "switch_system_role"
JIRA_AGENT_METADATA_VALUE = "jira_bridge"

_JIRA_PROFILE = IntegrationProfile(
    connection_model="always_on",
    message_exchange=True,
    pre_invocation_mediation=[],
    post_invocation_mediation=[],
    event_reporting=[],
    task_protocol=TaskProtocolConfig(can_delegate=False, can_accept=False),
)


async def ensure_jira_system_agent(
    *,
    protocol: ProtocolService,
    session_factory: async_sessionmaker[AsyncSession],
    agent_store: AgentStore,
    user_store: UserStore,
    config: SwitchConfig,
) -> str | None:
    """Provision the shared Jira system agent when webhook secrets are configured.

    Returns the agent id, or None when Jira ingress is not configured. The API
    key is stored encrypted via the normal ApiKey path and is never logged.
    The agent must still be added as a member of each target room — same rule
    as any other agent — before it can post there.
    """
    if not config.jira_webhook_secrets:
        return None

    name = config.jira_agent_name
    async with session_factory() as session:
        existing = await agent_store.get_by_name(session, name)
        if existing is not None:
            logger.info(
                "Jira system agent already present: %s (%s). "
                "Add it to each target room before rules can post.",
                name,
                existing.id,
            )
            return existing.id

        admin = await user_store.get_by_email(session, config.gateway_admin_email)
        if admin is None:
            raise RuntimeError(
                "Cannot provision Jira system agent: admin user "
                f"{config.gateway_admin_email!r} is missing"
            )
        owner_id = admin.id

    result = await protocol.register_agent(
        name=name,
        description=(
            "System agent that posts Jira trigger messages into rooms. "
            "Add this agent to each room a Jira trigger targets."
        ),
        display_name="Jira",
        connector_type="external",
        integration_profile=_JIRA_PROFILE,
        owner_id=owner_id,
        owner_only=False,
        metadata={JIRA_AGENT_METADATA_KEY: JIRA_AGENT_METADATA_VALUE},
    )
    logger.info(
        "Provisioned Jira system agent %s (%s). "
        "Add it to each target room before rules can post. "
        "API key stored encrypted; not logged.",
        name,
        result.agent_id,
    )
    return result.agent_id
