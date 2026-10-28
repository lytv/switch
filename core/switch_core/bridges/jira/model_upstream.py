"""Supervisor-only delivery upstreams, selected by the frozen pack provider."""

from __future__ import annotations

from urllib.parse import urlsplit

from switch_core.bridges.jira.contracts import ModelPrices, Pack
from switch_core.config import JiraWorkerDeliveryUpstream, SwitchConfig


def model_upstream(config: SwitchConfig, pack: Pack) -> JiraWorkerDeliveryUpstream:
    if config.jira_worker_delivery_upstreams:
        upstream = config.jira_worker_delivery_upstreams.get(pack.provider)
        if upstream is None:
            raise ValueError("The frozen pack provider has no delivery upstream")
    else:
        if (
            not config.jira_worker_llm_base_url
            or not config.jira_worker_llm_api_key
            or config.jira_worker_delivery_runtime is None
        ):
            raise ValueError("Model upstream is not configured")
        upstream = JiraWorkerDeliveryUpstream(
            base_url=config.jira_worker_llm_base_url,
            api_key=config.jira_worker_llm_api_key,
            prices=config.jira_worker_delivery_runtime.prices,
        )
    url = urlsplit(upstream.base_url)
    if (
        url.scheme not in {"http", "https"}
        or not url.hostname
        or url.username
        or url.password
        or url.query
        or url.fragment
        or (
            url.scheme != "https"
            and url.hostname not in {"localhost", "127.0.0.1", "::1"}
        )
        or not upstream.api_key.get_secret_value().strip()
    ):
        raise ValueError(
            "Model upstream requires credential-free HTTPS or loopback HTTP, and an API key"
        )
    return upstream


def model_prices(config: SwitchConfig, pack: Pack) -> ModelPrices:
    return ModelPrices.model_validate_json(
        model_upstream(config, pack).prices.model_dump_json()
    )
