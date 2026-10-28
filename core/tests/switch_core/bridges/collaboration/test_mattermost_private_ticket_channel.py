"""Private ticket channels exclude default members and invite stable account ids."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from tests.switch_core.bridges.collaboration.test_mattermost_existing_channel_adoption import (
    _adapter,
)


@pytest.mark.parametrize("invite_default_member", [False, True])
async def test_channel_default_member_is_explicit(invite_default_member):
    adapter = _adapter()
    adapter._config.default_member = "operator"
    channels = SimpleNamespace(
        create_channel=Mock(return_value={"id": "private-channel"}),
        add_user=Mock(),
    )
    users = SimpleNamespace(
        get_user_by_username=Mock(return_value={"id": "operator-id"})
    )
    adapter._admin_driver = SimpleNamespace(channels=channels, users=users)
    adapter._main_loop = asyncio.get_running_loop()
    adapter._team_id = "test-team"
    assert (
        await adapter.create_channel(
            "Ticket",
            "Private details",
            channel_type="channel_private",
            invite_default_member=invite_default_member,
        )
        == "private-channel"
    )
    assert channels.create_channel.call_args.args[0]["type"] == "P"
    if invite_default_member:
        channels.add_user.assert_called_once_with(
            "private-channel", {"user_id": "operator-id"}
        )
    else:
        channels.add_user.assert_not_called()
        users.get_user_by_username.assert_not_called()


@pytest.mark.parametrize("topic", ["", "Short ticket topic", "a" * 250, "界" * 251])
@pytest.mark.parametrize("retry", [False, True])
async def test_channel_purpose_respects_character_limit(topic, retry, caplog):
    adapter = _adapter()
    purposes = []

    def create_channel(payload):
        purpose = payload["purpose"]
        assert len(purpose) <= 250
        purposes.append(purpose)
        if retry and len(purposes) == 1:
            raise ValueError("Channel name already exists")
        return {"id": "ticket-channel"}

    channels = SimpleNamespace(create_channel=Mock(side_effect=create_channel))
    adapter._admin_driver = SimpleNamespace(channels=channels)
    adapter._main_loop = asyncio.get_running_loop()
    adapter._team_id = "test-team"

    assert (
        await adapter.create_channel("Ticket", topic, invite_default_member=False)
        == "ticket-channel"
    )
    assert purposes == [topic[:250]] * (2 if retry else 1)
    assert ("Truncating Mattermost channel purpose" in caplog.text) == (
        len(topic) > 250
    )


async def test_claimed_account_invite_does_not_follow_reassigned_username():
    adapter = _adapter()
    channels = SimpleNamespace(add_user=Mock())
    users = SimpleNamespace(
        get_user_by_username=Mock(return_value={"id": "another-person"})
    )
    adapter._admin_driver = SimpleNamespace(channels=channels, users=users)
    adapter._main_loop = asyncio.get_running_loop()
    assert (
        await adapter.add_users_to_channel(
            "private-channel", ["old-name"], ["claimed-account"]
        )
        == []
    )
    channels.add_user.assert_called_once_with(
        "private-channel", {"user_id": "claimed-account"}
    )
    users.get_user_by_username.assert_not_called()
