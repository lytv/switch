"""Pinned model selection and accounting, without provider requests."""

from __future__ import annotations

import json
from importlib.resources import files

import pytest
from pydantic import SecretStr, ValidationError

from switch_core.bridges.jira import packs
from switch_core.bridges.jira.model_gateway import _counts
from switch_core.bridges.jira.model_upstream import model_prices, model_upstream
from switch_core.bridges.jira.packs import load_pack
from switch_core.bridges.jira.target import (
    delivery_state,
    initialize_target,
    reset_delivery_scope,
)
from switch_core.config import JiraWorkerDeliveryUpstream
from switch_core.db.models import JiraWorkerTicket
from tests.switch_core.bridges.jira.test_delivery import URL, config

PINS = [
    (
        "2.0.0",
        "switch-delivery",
        "gpt-4.1-2025-04-14",
        "aa1e6a26ec686cd0097d6fe416258cac5989029511dca798fbb34e94314a07b6",
    ),
    (
        "2.0.0-deepseek",
        "switch-delivery-deepseek",
        "deepseek-v4-pro",
        "b60133794230b234a3ad67b3db9aff6d195d6ba3a2e58660e28305701c2e9242",
    ),
]


@pytest.fixture
def settings():
    values = config(False).model_dump()
    values["jira_worker_delivery_upstreams"] = {
        provider: {
            "base_url": f"https://{provider}.example.invalid/v1",
            "api_key": f"PLACEHOLDER_{provider}_VALUE",
            "prices": {
                "input": "0.000002",
                "output": "0.000008",
                "cache_read": "0.0000005",
                "cache_write": "0.000002",
                "source": f"{provider}-reviewed-prices",
            },
        }
        for _, provider, _, _ in PINS
    }
    return type(config(False)).model_validate(values)


@pytest.mark.parametrize("version,provider,model,pin", PINS)
def test_pack_pin_and_frozen_upstream(settings, version, provider, model, pin):
    settings.jira_worker_delivery_pack = version
    frozen = load_pack(settings)
    assert frozen.digest == pin
    assert frozen.snapshot.provider == provider and frozen.snapshot.model == model
    assert not frozen.snapshot.fixture_only
    assert frozen.snapshot.human_merge and not frozen.snapshot.deploy
    settings.jira_worker_delivery_pack = (
        "2.0.0" if version == "2.0.0-deepseek" else "2.0.0-deepseek"
    )
    upstream = model_upstream(settings, frozen.snapshot)
    assert upstream.base_url == f"https://{provider}.example.invalid/v1"
    assert upstream.api_key.get_secret_value() == f"PLACEHOLDER_{provider}_VALUE"
    assert isinstance(upstream.api_key, SecretStr)
    assert upstream.api_key.get_secret_value() not in repr(upstream)
    assert (
        model_prices(settings, frozen.snapshot).source == f"{provider}-reviewed-prices"
    )
    assert frozen.digest == pin


@pytest.mark.parametrize("version,provider,model,pin", PINS)
def test_changed_release_refuses(
    settings, tmp_path, monkeypatch, version, provider, model, pin
):
    settings.jira_worker_delivery_pack = version
    original = files("switch_core.bridges.jira").joinpath(
        f"packs/delivery/{version}.json"
    )
    data = json.loads(original.read_text())
    data["model"] = "changed-model"
    target = tmp_path / "packs/delivery" / f"{version}.json"
    target.parent.mkdir(parents=True)
    target.write_text(json.dumps(data))
    monkeypatch.setattr(packs, "files", lambda package: tmp_path)
    with pytest.raises(ValueError, match="immutable.*changed"):
        load_pack(settings)


@pytest.mark.parametrize("version,provider,model,pin", PINS)
def test_missing_provider_does_not_fall_back_to_legacy(
    settings, version, provider, model, pin
):
    settings.jira_worker_delivery_pack = version
    settings.jira_worker_llm_base_url = "https://legacy.example.invalid/v1"
    settings.jira_worker_llm_api_key = SecretStr("PLACEHOLDER_LEGACY_VALUE")
    del settings.jira_worker_delivery_upstreams[provider]
    with pytest.raises(ValueError, match="no delivery upstream"):
        model_upstream(settings, load_pack(settings).snapshot)


@pytest.mark.parametrize(
    "endpoint,key",
    [
        ("http://remote.example.invalid/v1", "PLACEHOLDER_VALUE"),
        ("https://user:password@example.invalid/v1", "PLACEHOLDER_VALUE"),
        ("https://model.example.invalid/v1?key=value", "PLACEHOLDER_VALUE"),
        ("https://model.example.invalid/v1#fragment", "PLACEHOLDER_VALUE"),
        ("https://model.example.invalid/v1", " "),
    ],
)
def test_invalid_upstream_refuses(settings, endpoint, key):
    settings.jira_worker_delivery_pack = "2.0.0-deepseek"
    settings.jira_worker_delivery_upstreams["switch-delivery-deepseek"] = (
        settings.jira_worker_delivery_upstreams["switch-delivery-deepseek"].model_copy(
            update={"base_url": endpoint, "api_key": SecretStr(key)}
        )
    )
    with pytest.raises(ValueError, match="Model upstream requires"):
        model_upstream(settings, load_pack(settings).snapshot)


def test_upstream_requires_complete_secret_configuration(settings):
    data = settings.jira_worker_delivery_upstreams["switch-delivery"].model_dump()
    del data["api_key"]
    with pytest.raises(ValidationError):
        JiraWorkerDeliveryUpstream.model_validate(data)


@pytest.mark.parametrize(
    "cache,expected",
    [
        ({}, (30, 10, 0, 0)),
        ({"prompt_tokens_details": None}, (30, 10, 0, 0)),
        ({"prompt_tokens_details": {"cached_tokens": 10}}, (20, 10, 10, 0)),
        (
            {"prompt_cache_hit_tokens": 10, "prompt_cache_miss_tokens": 20},
            (20, 10, 10, 0),
        ),
        ({"prompt_tokens_details": {}, "prompt_cache_hit_tokens": 10}, (20, 10, 10, 0)),
        (
            {
                "prompt_tokens_details": {"cached_tokens": 10},
                "prompt_cache_hit_tokens": 10,
            },
            (20, 10, 10, 0),
        ),
        ({"prompt_cache_hit_tokens": 0}, (30, 10, 0, 0)),
    ],
)
def test_usage_counts_each_prompt_token_once(cache, expected):
    assert (
        _counts(
            {
                "usage": {
                    "prompt_tokens": 30,
                    "completion_tokens": 10,
                    "total_tokens": 40,
                    **cache,
                }
            }
        )
        == expected
    )


@pytest.mark.parametrize(
    "cache",
    [{"prompt_cache_hit_tokens": value} for value in (-1, 31, True, "10", 1.5, None)]
    + [
        {"prompt_tokens_details": {"cached_tokens": value}}
        for value in (-1, 31, True, "10", 1.5, None)
    ]
    + [
        {"prompt_tokens_details": {"cached_tokens": 10}, "prompt_cache_hit_tokens": 20},
        {
            "prompt_tokens_details": {"cached_tokens": 10},
            "prompt_cache_hit_tokens": True,
        },
    ],
)
def test_invalid_cache_usage_refuses(cache):
    with pytest.raises(ValueError, match="usage is missing or invalid"):
        _counts(
            {
                "usage": {
                    "prompt_tokens": 30,
                    "completion_tokens": 10,
                    "total_tokens": 40,
                    **cache,
                }
            }
        )


@pytest.mark.parametrize(
    "usage",
    [
        {},
        {"prompt_tokens": 30, "completion_tokens": 10},
        {"prompt_tokens": 30, "completion_tokens": 10, "total_tokens": 41},
        {"prompt_tokens": True, "completion_tokens": 10, "total_tokens": 11},
        {"prompt_tokens": 30, "completion_tokens": -1, "total_tokens": 29},
    ],
)
def test_incomplete_or_invalid_usage_still_refuses(usage):
    with pytest.raises((ValueError, KeyError)):
        _counts({"usage": usage})


@pytest.mark.parametrize("generation", [0, 1, 7])
@pytest.mark.parametrize("parked", [False, True])
def test_target_initialization_starts_only_zero_generation(
    settings, generation, parked
):
    reason = "Administrator fence" if parked else None
    ticket = JiraWorkerTicket(
        instance="acme",
        project_key="TEST",
        agent_generation=generation,
        agent_state="parked" if parked else "wake_requested",
        worker_parked_reason=reason,
    )
    initialize_target(
        ticket,
        {"customfield_12345": "Bug", "description": f"Repository URL: {URL}"},
        settings,
    )
    assert ticket.agent_generation == (1 if generation == 0 else generation)
    assert ticket.worker_parked_reason == reason
    assert ticket.agent_state == ("parked" if parked else "wake_requested")


def test_ticket_freezes_pack_and_prices_across_switch_and_retriage(settings):
    settings.jira_worker_delivery_pack = "2.0.0"
    ticket = JiraWorkerTicket(instance="acme", project_key="TEST", agent_generation=1)
    initialize_target(
        ticket,
        {"customfield_12345": "Bug", "description": f"Repository URL: {URL}"},
        settings,
    )
    frozen = delivery_state(ticket)
    assert frozen.model_prices == model_prices(settings, frozen.pack.snapshot)
    settings.jira_worker_delivery_pack = "2.0.0-deepseek"
    settings.jira_worker_delivery_upstreams["switch-delivery"] = (
        settings.jira_worker_delivery_upstreams["switch-delivery"].model_copy(
            update={
                "prices": settings.jira_worker_delivery_upstreams[
                    "switch-delivery-deepseek"
                ].prices,
            }
        )
    )
    reset_delivery_scope(ticket, frozen, service="Bug", supplied_url=URL)
    retained = delivery_state(ticket)
    assert retained.pack == frozen.pack
    assert retained.model_prices == frozen.model_prices
    assert retained.model_prices.source == "switch-delivery-reviewed-prices"
