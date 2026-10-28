"""Read-only local preflight. This module never connects to Jira or starts work."""

from __future__ import annotations

import argparse
from pathlib import Path

from switch_core.bridges.jira.delivery_contracts import ScopeContract, ScopePolicy
from switch_core.bridges.jira.firstmate_copy import tmux_directory
from switch_core.bridges.jira.live import load_policies, runtime_config
from switch_core.bridges.jira.model_upstream import model_upstream
from switch_core.bridges.jira.packs import load_pack
from switch_core.config import SwitchConfig


def check_readiness(
    config: SwitchConfig, scope: ScopeContract, instance: str, project: str
) -> dict[str, str | None]:
    """Return a refusal reason per failed item; None means the local check passed."""
    checks: dict[str, str | None] = {
        "admission_disabled": None
        if not config.jira_worker_delivery_execution_enabled
        else "Disable delivery admission before preflight.",
        "project_enabled": None
        if project in config.jira_worker_enabled_projects.get(instance, [])
        else "The project has no worker admission mapping.",
        "delivery_fields": None
        if project in config.jira_worker_delivery_projects.get(instance, {})
        else "The project has no delivery field mapping.",
        "jira_credentials": None
        if instance in config.jira_worker_credentials
        else "The instance has no Jira credentials configuration.",
        "webhook_secret": None
        if config.jira_webhook_secrets.get(instance)
        else "The instance has no webhook secret configuration.",
        "language_prices": None
        if config.jira_worker_delivery_language_pricing is not None
        else "Configure the bounded language price and its source.",
        "scope_instance": None
        if scope.job.run.instance == instance
        else "The scope belongs to another Jira instance.",
        "scope_approval_route": None
        if scope.approval_route.unique_edge_verified
        else "Supply the verified unique approval route.",
        "verification": None
        if scope.verification is not None
        else "The scope lacks review, QA, smoke, and completion configuration.",
        "live_prerequisites": (
            "Not checked: authorization, live refusal/bypass tests, automation, "
            "OS sandbox, provider limit behavior, permissions, channels, and waits. "
            "Complete pilot P0 separately."
        ),
    }
    try:
        pack = load_pack(config)
    except (OSError, ValueError):
        checks["pack_pin"] = "The immutable delivery pack failed validation."
        checks["scope_pack"] = "Cannot check the scope against an invalid pack."
        checks["live_pack"] = "No validated live pack is available."
        checks["model_upstream"] = "Cannot select an upstream without a valid pack."
    else:
        checks["pack_pin"] = None
        checks["scope_pack"] = (
            None
            if scope.job.pack_digest == pack.digest
            else "The scope does not match the configured frozen pack."
        )
        checks["live_pack"] = (
            "The pack is fixture-only; live delivery is not supported by this pack."
            if pack.snapshot.fixture_only
            else None
        )
        try:
            model_upstream(config, pack.snapshot)
        except ValueError:
            checks["model_upstream"] = (
                "The selected pack requires a valid upstream URL and API key."
            )
        else:
            checks["model_upstream"] = None
    runtime = config.jira_worker_delivery_runtime
    if runtime is None:
        checks["delivery_tmux"] = "Configure the live delivery runtime first."
    else:
        try:
            tmux_directory(runtime.homes / ("0" * 64), runtime.tmux_sockets)
        except ValueError as exc:
            checks["delivery_tmux"] = str(exc)
        else:
            checks["delivery_tmux"] = None
    try:
        runtime = runtime_config(config)
        policies = load_policies(config, runtime)
    except (ValueError, OSError):
        checks["delivery_runtime"] = (
            "The live runtime, trusted policies, roots, or role prices failed validation."
        )
        checks["trusted_scope"] = "Cannot compare the scope to an invalid live policy."
    else:
        checks["delivery_runtime"] = None
        policy = policies.get((instance, project))
        proposed = scope.model_dump_json(exclude={"job", "branch"})
        checks["trusted_scope"] = (
            None
            if policy is not None
            and policy.scope == ScopePolicy.model_validate_json(proposed)
            else "The scope differs from the configured trusted policy."
        )
    return checks


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scope", type=Path, required=True)
    parser.add_argument("--instance", required=True)
    parser.add_argument("--project", required=True)
    args = parser.parse_args()
    try:
        scope = ScopeContract.model_validate_json(args.scope.read_text())
        config = SwitchConfig()
    except (ValueError, OSError):
        print(
            "REFUSE: Invalid or missing local configuration or scope. No action occurred."
        )
        return 1
    checks = check_readiness(config, scope, args.instance, args.project)
    for name, reason in checks.items():
        print(
            f"{'BLOCK' if reason else 'PASS'} {name}: {reason or 'Local check passed.'}"
        )
    if any(checks.values()):
        print("REFUSE: Readiness is incomplete. No setting changed and no job started.")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
