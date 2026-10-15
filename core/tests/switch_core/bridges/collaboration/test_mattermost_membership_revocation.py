"""Private ticket membership revocation preserves people with current access."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import Mock, call

import pytest

from tests.switch_core.bridges.collaboration.test_mattermost_existing_channel_adoption import (
    _adapter,
)


def revocation_adapter():
    adapter = _adapter()
    adapter._main_loop = asyncio.get_running_loop()
    adapter._admin_user_id = "bridge"
    adapter._bridge_bot_ids = {"worker"}
    adapter._admin_driver = SimpleNamespace(
        channels=SimpleNamespace(
            get_channel=Mock(return_value={"type": "P", "team_id": "team"}),
            get_channel_members=Mock(return_value=[]),
            remove_channel_member=Mock(),
        ),
        users=SimpleNamespace(
            get_user=Mock(return_value={"roles": "system_user", "is_bot": False})
        ),
        teams=SimpleNamespace(
            get_team_member=Mock(return_value={"roles": "team_user"})
        ),
    )
    return adapter


async def test_revocation_preserves_reporter_admins_and_bots():
    adapter = revocation_adapter()
    driver = adapter._admin_driver
    ids = [
        "reporter",
        "switch-admin",
        "bridge",
        "worker",
        "channel-admin",
        "system-admin",
        "team-admin",
        "bot",
        "old-reporter",
    ]
    driver.channels.get_channel_members.return_value = [
        {
            "user_id": uid,
            "roles": "channel_admin" if uid == "channel-admin" else "channel_user",
        }
        for uid in ids
    ]
    driver.users.get_user.side_effect = lambda uid: {
        "roles": "system_admin" if uid == "system-admin" else "system_user",
        "is_bot": uid == "bot",
    }
    driver.teams.get_team_member.side_effect = lambda team, uid: {
        "roles": "team_admin" if uid == "team-admin" else "team_user"
    }
    await adapter.retain_channel_members("channel", {"reporter", "switch-admin"})
    driver.channels.remove_channel_member.assert_called_once_with(
        "channel", "old-reporter"
    )


async def test_revocation_snapshots_all_pages_before_removing_members():
    adapter = revocation_adapter()
    channels = adapter._admin_driver.channels
    first_page = [{"user_id": str(i), "roles": "channel_user"} for i in range(200)]
    channels.get_channel_members.side_effect = [
        first_page,
        [{"user_id": "last", "roles": "channel_user"}],
    ]
    await adapter.retain_channel_members("channel", {str(i) for i in range(200)})
    assert channels.get_channel_members.call_args_list == [
        call("channel", {"page": 0, "per_page": 200}),
        call("channel", {"page": 1, "per_page": 200}),
    ]
    channels.remove_channel_member.assert_called_once_with("channel", "last")


@pytest.mark.parametrize("failure", ["get_channel_members", "remove_channel_member"])
async def test_revocation_surfaces_platform_failures(failure):
    adapter = revocation_adapter()
    channels = adapter._admin_driver.channels
    channels.get_channel_members.return_value = [
        {"user_id": "old", "roles": "channel_user"}
    ]
    getattr(channels, failure).side_effect = RuntimeError("platform unavailable")
    with pytest.raises(RuntimeError, match="platform unavailable"):
        await adapter.retain_channel_members("channel", set())


async def test_revocation_refuses_public_channels():
    adapter = revocation_adapter()
    channels = adapter._admin_driver.channels
    channels.get_channel.return_value = {"type": "O"}
    with pytest.raises(ValueError, match="private channel"):
        await adapter.retain_channel_members("channel", set())
    channels.remove_channel_member.assert_not_called()
