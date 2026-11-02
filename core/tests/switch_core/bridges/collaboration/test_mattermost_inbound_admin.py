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


async def test_bridge_pending_admin_post_is_ignored() -> None:
    adapter = _adapter()
    received: list[InboundMessage] = []
    _wire(adapter, received)
    adapter._remember_own_post("pending-1")

    await adapter._ws_handler(
        _posted_event(
            {
                "id": "server-post",
                "pending_post_id": "pending-1",
                "user_id": "admin-user",
                "channel_id": "chan-1",
                "message": "posted by the bridge",
            }
        ),
        "orchestrator",
    )
    await asyncio.sleep(0.05)

    assert received == []


async def test_attachment_post_carries_pending_id() -> None:
    adapter = _adapter()
    adapter._main_loop = asyncio.get_running_loop()
    seen: dict[str, object] = {}

    def create_post(post: dict[str, object]) -> dict[str, str]:
        seen["own"] = adapter._is_own_post(
            {"id": "server-post", "pending_post_id": post["pending_post_id"]}
        )
        return {"id": "server-post"}

    adapter._admin_driver = SimpleNamespace(
        files=SimpleNamespace(
            upload_file=lambda *_a, **_k: {"file_infos": [{"id": "file-1"}]}
        ),
        posts=SimpleNamespace(create_post=create_post),
    )

    ref = await adapter.send_attachment("chan-1", "agent", "a.txt", "text/plain", b"hi")

    assert ref == "server-post"
    assert seen["own"] is True


async def test_pending_id_marks_own_before_http_response() -> None:
    """The posted event can arrive before create_post returns, so the marker
    must be live while the HTTP call is still in flight."""
    adapter = _adapter()
    adapter._main_loop = asyncio.get_running_loop()
    seen: dict[str, object] = {}

    def create_post(post: dict[str, str]) -> dict[str, str]:
        seen["own"] = adapter._is_own_post(
            {
                "id": "server-post",
                "pending_post_id": post["pending_post_id"],
                "user_id": "admin-user",
            }
        )
        return {"id": "server-post"}

    adapter._admin_driver = SimpleNamespace(
        posts=SimpleNamespace(create_post=create_post)
    )

    await adapter._create_post(adapter._admin_driver, "chan-1", "hello", None)  # type: ignore[arg-type]

    assert seen["own"] is True
