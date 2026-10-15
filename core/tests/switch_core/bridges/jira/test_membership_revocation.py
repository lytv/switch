"""Mapping and claim changes reconcile existing private ticket channels."""

from __future__ import annotations

import pytest
from sqlalchemy import select

from switch_core.bridges.jira.rooms import REPORTER_ACCESS_REASON
from switch_core.bridges.jira.worker import (
    JiraWorkerScheduler,
    remove_identity_mapping,
    set_identity_mapping,
)
from switch_core.db.models import ExternalUserClaim, JiraWorkerTicket, User
from switch_core.db.stores.external_user_store import ExternalUserStore
from switch_core.db.stores.user_store import UserStore
from switch_core.gateway.collaborations import release_bridge_identity
from tests.switch_core.bridges.collaboration.test_mattermost_membership_revocation import (
    revocation_adapter,
)
from tests.switch_core.bridges.jira.test_rooms import _switch_config
from tests.switch_core.bridges.jira.test_ticket_bridge import (
    sync,
)
from tests.switch_core.bridges.jira.test_ticket_bridge import (
    ticket_bridge as _ticket_bridge,
)

ticket_bridge = _ticket_bridge


@pytest.mark.parametrize("terminal", [False, True])
@pytest.mark.parametrize("remove_mapping", [False, True])
async def test_mapping_changes_revoke_old_reporter_accounts(
    session_factory, ticket_bridge, terminal, remove_mapping
):
    rooms, _, adapter, _ = ticket_bridge
    room_id = await sync(session_factory, rooms)
    async with session_factory() as session:
        session.add(
            User(
                id="replacement",
                name="Replacement",
                email="replacement@example.invalid",
                role="user",
            )
        )
        session.add(
            User(id="admin", name="Admin", email="admin@example.invalid", role="admin")
        )
        await session.flush()
        session.add_all(
            [
                ExternalUserClaim(external_user_id="other", user_id="replacement"),
                ExternalUserClaim(external_user_id="second", user_id="admin"),
            ]
        )
        ticket = await session.scalar(select(JiraWorkerTicket))
        if terminal:
            ticket.status = "Done"
        await session.commit()
    if terminal:
        await sync(session_factory, rooms)
    platform = revocation_adapter()
    channels = platform._admin_driver.channels
    channels.get_channel_members.return_value = [
        {"user_id": uid, "roles": "channel_user"}
        for uid in ("ext-first", "ext-second", "worker", "bridge")
    ]
    adapter.retain_channel_members.reset_mock()
    adapter.retain_channel_members.side_effect = platform.retain_channel_members
    config = _switch_config(jira_worker_enabled_projects={"acme": ["KAN"]})
    async with session_factory() as session:
        if remove_mapping:
            await remove_identity_mapping(
                session,
                instance="acme",
                jira_account_id="acct",
                enabled_projects=["KAN"],
            )
        else:
            await set_identity_mapping(
                session,
                instance="acme",
                jira_account_id="acct",
                switch_user_id="replacement",
                enabled_projects=["KAN"],
            )
        await session.commit()
    scheduler = JiraWorkerScheduler(
        session_factory=session_factory, config=config, ticket_rooms=rooms
    )
    assert await scheduler.sweep_ticket_rooms() == 1
    expected = {"ext-second"} if remove_mapping else {"ext-second", "ext-other"}
    adapter.retain_channel_members.assert_awaited_with("private-channel", expected)
    channels.remove_channel_member.assert_called_once_with(
        "private-channel", "ext-first"
    )
    assert await sync(session_factory, rooms) == room_id
    adapter.create_channel.assert_awaited_once()


@pytest.mark.parametrize("terminal", [False, True])
async def test_shared_claim_release_keeps_reporter_and_admin_access(
    session_factory, ticket_bridge, terminal
):
    rooms, _, adapter, _ = ticket_bridge
    await sync(session_factory, rooms)
    config = _switch_config(jira_worker_enabled_projects={"acme": ["KAN"]})
    async with session_factory() as session:
        session.add(
            User(id="admin", name="Admin", email="admin@example.invalid", role="admin")
        )
        await session.flush()
        session.add(ExternalUserClaim(external_user_id="first", user_id="admin"))
        ticket = await session.scalar(select(JiraWorkerTicket))
        if terminal:
            ticket.status = "Done"
        await session.commit()
    if terminal:
        await sync(session_factory, rooms)
    adapter.retain_channel_members.reset_mock()
    async with session_factory() as session:
        user = await session.get(User, "reporter")
        await release_bridge_identity(
            bridge_id="mm",
            external_user_row_id="first",
            session=session,
            external_user_store=ExternalUserStore(),
            user_store=UserStore(),
            user=user,
            config=config,
        )
    scheduler = JiraWorkerScheduler(
        session_factory=session_factory, config=config, ticket_rooms=rooms
    )
    assert await scheduler.sweep_ticket_rooms() == 1
    adapter.retain_channel_members.assert_awaited_with(
        "private-channel", {"ext-first", "ext-second"}
    )


async def test_unsupported_revocation_keeps_invitations_and_reports_degraded_access(
    session_factory, ticket_bridge
):
    rooms, _, adapter, _ = ticket_bridge
    adapter.retain_channel_members.side_effect = NotImplementedError(
        "Unsupported revocation"
    )
    await sync(session_factory, rooms)
    assert set(adapter.add_users_to_channel.await_args.args[2]) == {
        "ext-first",
        "ext-second",
    }
    async with session_factory() as session:
        ticket = await session.scalar(select(JiraWorkerTicket))
        assert "Unsupported revocation" in ticket.admin_reasons[REPORTER_ACCESS_REASON]


async def test_revocation_failure_is_visible_and_retried(
    session_factory, ticket_bridge
):
    rooms, _, adapter, _ = ticket_bridge
    await sync(session_factory, rooms)
    adapter.retain_channel_members.side_effect = RuntimeError("Cannot revoke member")
    await sync(session_factory, rooms)
    async with session_factory() as session:
        ticket = await session.scalar(select(JiraWorkerTicket))
        assert "Cannot revoke member" in ticket.admin_reasons[REPORTER_ACCESS_REASON]
    adapter.retain_channel_members.side_effect = None
    await sync(session_factory, rooms)
    async with session_factory() as session:
        ticket = await session.scalar(select(JiraWorkerTicket))
        assert REPORTER_ACCESS_REASON not in (ticket.admin_reasons or {})
