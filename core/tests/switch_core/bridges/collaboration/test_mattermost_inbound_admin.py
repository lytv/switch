"""The bridge delivers posts a human writes from the admin account.

The admin user is the account the bridge logs in as, and it is also a human
account. Dropping every post by that user id silently discarded the human's
messages, so the bridge now skips only the posts it created itself.
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

from switch_core.bridges.collaboration.mattermost.adapter import (
    MattermostAdapter,
    MattermostConnectionConfig,
)
from switch_core.bridges.collaboration.models import InboundMessage


def _adapter() -> MattermostAdapter:
    return MattermostAdapter(
        config=MattermostConnectionConfig(
            url="http://mm",
            admin_user="admin",
            admin_password="pw",
            team_name="team",
        )
    )


def _posted_event(post: dict[str, object]) -> str:
    return json.dumps(
        {
            "event": "posted",
            "data": {
                "post": json.dumps(post),
                "channel_type": "O",
                "channel_display_name": "Agentic SDLC",
            },
        }
    )


def _wire(adapter: MattermostAdapter, received: list[InboundMessage]) -> None:
    adapter._main_loop = asyncio.get_running_loop()
    adapter._admin_user_id = "admin-user"
    adapter._admin_driver = SimpleNamespace(
        users=SimpleNamespace(get_user=lambda _id: {"username": "admin"})
    )

    async def on_message(message: InboundMessage) -> None:
        received.append(message)

    adapter._on_message = on_message


async def test_human_admin_post_reaches_on_message() -> None:
    adapter = _adapter()
    received: list[InboundMessage] = []
    _wire(adapter, received)

    await adapter._ws_handler(
        _posted_event(
            {
                "id": "human-post",
                "user_id": "admin-user",
                "channel_id": "chan-1",
                "message": "hi @orchestrator",
            }
        ),
        "orchestrator",
    )
    await asyncio.sleep(0.05)

    assert [m.content for m in received] == ["hi @orchestrator"]


async def test_bridge_created_admin_post_is_ignored() -> None:
    adapter = _adapter()
    received: list[InboundMessage] = []
    _wire(adapter, received)
    adapter._remember_own_post("own-post")

    await adapter._ws_handler(
        _posted_event(
            {
                "id": "own-post",
                "user_id": "admin-user",
                "channel_id": "chan-1",
                "message": "posted by the bridge",
            }
        ),
        "orchestrator",
    )
    await asyncio.sleep(0.05)

    assert received == []


async def test_bot_post_is_ignored() -> None:
    adapter = _adapter()
    received: list[InboundMessage] = []
    _wire(adapter, received)
    adapter._bridge_bot_ids = {"bot-user"}

    await adapter._ws_handler(
        _posted_event(
            {
                "id": "bot-post",
                "user_id": "bot-user",
                "channel_id": "chan-1",
                "message": "posted by a bot",
            }
        ),
        "orchestrator",
    )
    await asyncio.sleep(0.05)

    assert received == []


async def test_create_post_records_own_post() -> None:
    adapter = _adapter()
    adapter._main_loop = asyncio.get_running_loop()
    driver = SimpleNamespace(
        posts=SimpleNamespace(create_post=lambda _post: {"id": "post-1"})
    )

    ref = await adapter._create_post(driver, "chan-1", "hello", None)  # type: ignore[arg-type]

    assert ref == "post-1"
    assert "post-1" in adapter._own_post_ids
