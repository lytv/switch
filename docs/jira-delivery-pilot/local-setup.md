# Local version 2 delivery

This procedure configures the existing Switch server, not another worker service.
It does not authorize a live ticket run.
Keep delivery disabled until the separate P0 authorization and isolation checks pass.
Keep existing legacy project settings when adding AGD.

## Prepare the runtime

Use a separate Firstmate checkout at commit
`6eb8b7f8277dc37637e3ba58c2fc461d88dc4ff3` and pi `1.4.0`.
Install `git`, `tmux`, pi, and the dependencies of the approved repository checks.
Do not use a standing Firstmate home or persistent SDLC sessions.

Provision three separate, absolute, symlink-free directories:

- `/srv/switch-delivery/firstmate`: the pinned Firstmate checkout.
- `/srv/switch-delivery/homes`: private per-execution homes, initially empty.
- `/srv/switch-delivery/repositories`: supervisor-owned bare stores and separate role checkouts.

Create these regular marker files, each containing exactly
`switch-firstmate-delivery-v1` followed by a newline:

- `firstmate/.switch-delivery-copy`
- `homes/.switch-delivery-workers`
- `repositories/.switch-delivery-repositories`

Use private directory permissions. The server checks both runtime pins at startup.
The native adapter also checks the Firstmate revision before dispatch.

OS isolation remains a deployment requirement, not a marker-file assertion.
Workers must not access the server environment, credentials, trusted policies, bare stores, or other homes.
Expose only each role's project and home to that role.
Mount review, QA, and smoke projects read-only. QA also needs its separate read-only base project.
Deny workers forge write access. Only the supervisor publishes the generation branch.
The generated paths are deterministic hashes under the configured roots.
The server uses Firstmate's existing task start/get/cancel mechanics.
It does not create another daemon or queue.

Do not treat a same-user, unrestricted local launch as an OS sandbox.
P0 must prove the actual filesystem, process, network, and cancellation boundaries before a live run.

## Prepare the trusted policy

Create `/srv/switch-delivery/agd-policy.json` outside every worker mount.
Its format is `live.AdmissionPolicy`, not a Jira field or model response.
Export its complete schema without starting work:

```sh
uv run --project core python - <<'PY'
import json
from switch_core.bridges.jira.live import AdmissionPolicy
print(json.dumps(AdmissionPolicy.model_json_schema(), indent=2))
PY
```

The policy contains:

- `scope`: allowed/forbidden paths, approved command argv and expected results,
  the unique human approval route, rendered dev profile, reservations, estimates,
  and the independent review/QA/smoke verification contract.
- `statuses`: Jira status names and exact IDs.
- `transitions`: human approval ID plus worker `source-id->target-id` mappings.
  Include initial scope request, question/wait/resume, candidate request, and completion edges.
- `approver_account_ids`: the authorized human Jira account IDs, never the worker account.

Render the four role profiles from reviewed, exact skills-library commits.
Retain their content checksums and dependency-validation evidence.
Use real workflow IDs and verified unique-edge evidence from the target Jira workflow.
Do not copy fixture workflow evidence or invent the `unique_edge_verified` assertion.
Both human gates use this verified approval route.

Every reservation's `price_source` must equal the configured price table's `source`.
Its USD hold must cover `tokens * maximum configured token price`.
For example, reserve 20000 tokens for reproduction, 40000 for fix,
20000 each for review and QA, and 10000 for smoke.
Retries still share the ticket and daily budgets; a reservation does not extend either budget.
The server freezes the intake summary as acceptance criteria before presenting the scope.
No ticket text can add a command or broaden a path permission.
Repository URLs need no registration list. This release supports the configured GitHub-compatible host only.

## Local environment

Add these keys to the local server's private `.env` after configuration approval.
Replace every placeholder. Field IDs and credentials below are examples, not usable values.
Preserve unrelated existing keys and legacy mappings.

```dotenv
JIRA_WORKER_ENABLED_PROJECTS='{"local":["AGD"]}'
JIRA_WORKER_DELIVERY_PROJECTS='{"local":{"AGD":{"service_field":"customfield_12345","repository_url_field":"description"}}}'
JIRA_WORKER_DELIVERY_PACK=2.0.0
JIRA_WORKER_DELIVERY_EXECUTION_ENABLED=false
JIRA_WORKER_DELIVERY_TICKET_TOKENS=200000
JIRA_WORKER_DELIVERY_TICKET_ESTIMATED_USD=5
JIRA_WORKER_DELIVERY_DAY_ESTIMATED_USD=25
JIRA_WORKER_CREDENTIALS='{"local":{"base_url":"https://jira.example.invalid","email":"automation@example.invalid","api_token":"REPLACE_FROM_SECRET_MANAGER"}}'
JIRA_WEBHOOK_SECRETS='{"local":"REPLACE_FROM_SECRET_MANAGER"}'
JIRA_WORKER_DELIVERY_LANGUAGE_PRICING='{"max_usd_per_token":"0.000008","price_source":"operator-reviewed-flat-ceiling-YYYY-MM-DD"}'
JIRA_WORKER_DELIVERY_RUNTIME='{"checkout":"/srv/switch-delivery/firstmate","homes":"/srv/switch-delivery/homes","repositories":"/srv/switch-delivery/repositories","executable_path":"/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin","model_url":"http://127.0.0.1:8000","forge_api_url":"https://api.github.com","forge_repository_host":"github.com","forge_authorization":"REPLACE_WITH_HTTP_AUTHORIZATION_FROM_SECRET_MANAGER","prices":{"input":"0.000002","output":"0.000008","cache_read":"0.0000005","cache_write":"0.000002","source":"operator-reviewed-flat-ceiling-YYYY-MM-DD"},"policies":{"local":{"AGD":"/srv/switch-delivery/agd-policy.json"}}}'
```

Review current provider prices before using these example rates.
Prices are USD per token, not per million tokens.
Configure conservative flat ceilings if provider prices depend on context length or other tiers.
Do not enable a provider that charges unbounded extra fees or ignores token limits.
### Choose GPT or DeepSeek

Two immutable live packs remain available:

| `JIRA_WORKER_DELIVERY_PACK` | Private provider | Pinned model | Release digest |
| --- | --- | --- | --- |
| `2.0.0` | `switch-delivery` | `gpt-4.1-2025-04-14` | `aa1e6a26ec686cd0097d6fe416258cac5989029511dca798fbb34e94314a07b6` |
| `2.0.0-deepseek` | `switch-delivery-deepseek` | `deepseek-v4-pro` | `b60133794230b234a3ad67b3db9aff6d195d6ba3a2e58660e28305701c2e9242` |

Configure both delivery upstreams once in the server's private environment.
Use this JSON shape for `JIRA_WORKER_DELIVERY_UPSTREAMS`:

```json
{
  "switch-delivery": {
    "base_url": "https://api.openai.com/v1",
    "api_key": "REPLACE_WITH_GPT_KEY_FROM_SECRET_MANAGER",
    "prices": {
      "input": "0.000002",
      "output": "0.000008",
      "cache_read": "0.0000005",
      "cache_write": "0.000002",
      "source": "operator-reviewed-both-provider-ceilings-YYYY-MM-DD"
    }
  },
  "switch-delivery-deepseek": {
    "base_url": "https://deepseek.example.invalid/v1",
    "api_key": "REPLACE_WITH_DEEPSEEK_KEY_FROM_SECRET_MANAGER",
    "prices": {
      "input": "0.000002",
      "output": "0.000008",
      "cache_read": "0.0000005",
      "cache_write": "0.000002",
      "source": "operator-reviewed-both-provider-ceilings-YYYY-MM-DD"
    }
  }
}
```

Replace the DeepSeek URL with the approved OpenAI-compatible endpoint that serves `deepseek-v4-pro`.
These example rates are not a DeepSeek price quote.
Review both providers' rates before configuration.
For `.env`, encode this object as one JSON line inside single quotes.
The GPT entry needs its GPT key. The DeepSeek entry needs its DeepSeek key.
The server keeps these keys as secrets. Workers receive only execution-scoped capabilities.
A nonempty mapping must contain every provider needed by new or frozen tickets.
A missing provider refuses access; it never falls back to legacy credentials.

For a one-setting switch, prepare both entries and one trusted policy in advance.
Use a reviewed price source that covers both tables.
Use the same source in every policy reservation.
Each reservation must cover its tokens at the larger maximum price of the two tables.
Set the language price ceiling to cover the separate intake model.
Change only `JIRA_WORKER_DELIVERY_PACK` to `2.0.0` or `2.0.0-deepseek`, then restart the existing server.
Keep execution disabled during preparation. Changing a pack does not enable execution.

Without `JIRA_WORKER_DELIVERY_UPSTREAMS`, delivery retains the previous upstream behavior:
`JIRA_WORKER_LLM_BASE_URL`, `JIRA_WORKER_LLM_API_KEY`, and `JIRA_WORKER_DELIVERY_RUNTIME.prices` supply model access and rates.
Those LLM keys remain the legacy KAN/intake configuration.
`JIRA_WORKER_LLM_MODEL` selects only the legacy/intake model, never the delivery model.
Do not edit those keys to switch delivery providers.
With a delivery mapping, its provider entry supplies the upstream and rates instead of the shared keys and runtime rates.
Retain old provider entries while frozen tickets need them.

The server sends each execution's frozen model ID to its provider's upstream.
The upstream must support byte-level tokenization, text/function-tool Chat Completions,
non-streaming responses, `max_tokens`, and complete prompt, completion, and total usage.
The gateway accepts OpenAI's `prompt_tokens_details.cached_tokens` or DeepSeek's `prompt_cache_hit_tokens`.
It counts a cache hit once when both fields agree. Conflicting or invalid cache counts refuse the result.
If neither cache field exists, it conservatively prices all prompt tokens as uncached.
The gateway converts the buffered response to the SSE format that pi requests.
Images and provider-hosted tools are not supported by this pack.

`model_url` is the existing Switch origin reachable by the isolated worker, not the upstream provider.
Use HTTPS for a remote worker origin. HTTP is accepted only on loopback.
The generated capability permits only one execution's model access within its frozen reservation.
Workers receive no provider key, Jira key, forge credential, or existing provider subscription.
Forge authorization must work for API reads, draft creation/readiness, and non-forced HTTPS git push.
Use narrowly scoped repository permissions. Deny approval, merge, and deployment permissions.
For GitHub, an HTTP Basic header for a dedicated token identity can serve both REST and git authentication.
Keep its value only in the server's private configuration.

## Readiness and enablement

Prepare the reviewed `ScopeContract` JSON with the verified repository/base pins,
the selected pack digest, and the same trusted policy.
Use `load_pack(config).digest`, not a copied release digest, when budgets differ from the release defaults.
The scope's pack digest must match the selection. Its reservation price sources must match the selected upstream's reviewed source.
The trusted policy must contain those same reservation sources and sufficient price ceilings.
The server stages the real ticket's scope later; the preflight uses a placeholder ticket identity.
Replace the repository values with the triage evidence you reviewed:

```sh
umask 077
uv run --env-file .env --project core python - > /absolute/private/evidence/scope.json <<'PY'
from pathlib import Path
from switch_core.bridges.jira.contracts import Job, Repository, Run
from switch_core.bridges.jira.delivery_contracts import ScopeContract
from switch_core.bridges.jira.live import AdmissionPolicy
from switch_core.bridges.jira.packs import load_pack
from switch_core.config import SwitchConfig

policy = AdmissionPolicy.model_validate_json(Path("/srv/switch-delivery/agd-policy.json").read_text())
repository = Repository(
    supplied_url="https://github.com/example/project",
    canonical_url="https://github.com/example/project.git",
    outcome="verified",
    base_branch="main",
    base_sha="0123456789abcdef0123456789abcdef01234567",
    evidence="Reviewed forge repository and default-branch reads",
)
run = Run(instance="local", ticket_id="preflight", generation=1, step="reproduce", attempt=1, execution_key="preflight/1/reproduce/1")
job = Job(schema_version=1, run=run, pack_digest=load_pack(SwitchConfig()).digest, service="Bug", repository=repository, acceptance=("The approved behavior succeeds",))
print(ScopeContract(**policy.scope.model_dump(), job=job, branch="switch/preflight/1").model_dump_json())
PY
```

Run this command with delivery still disabled:

```sh
uv run --env-file .env --project core python -m switch_core.bridges.jira.readiness \
  --instance local --project AGD --scope /absolute/private/evidence/scope.json
```

A configured live setup can pass every local item.
`live_prerequisites` still blocks until a human completes P0 evidence separately.
That intentional refusal is not a request to remove the check.
The command starts no task and changes no setting.
It validates the immutable live pack, selected upstream URL and key, roots and pins, prices, and scope/policy equality.
It makes no provider request, so it cannot prove that an endpoint serves the pinned model.
It does not prove live permissions, OS confinement, provider limit behavior, channels, or wait windows.

After separate live-run approval, set `JIRA_WORKER_DELIVERY_EXECUTION_ENABLED=true` and restart the existing server.
A new AGD ticket with Service `Bug` and exactly one `Repository URL: https://github.com/example/project` line enters delivery intake.
The supervisor verifies canonical identity and the default branch SHA through the trusted forge API before checkout.
It stages the bounded scope only after intake completes.
The Jira scope gate must succeed before any native role starts.
Review and QA use separate native tasks at the tested candidate; QA repeats the original failure at the base.
The candidate gate grants no merge authority. A human merges through the forge.
Smoke uses the observed merged SHA before the exact Done transition.
No deployment occurs.

Keep the default fixture pack for tests. Existing fixture tickets cannot enter the live supervisor.
Switching packs does not upgrade a frozen ticket or reprice an existing execution.
New live tickets freeze their provider's price table with their pack.
Historical tickets without a price snapshot recover their earlier meter prices when available.
Conflicting historical prices refuse a new role. With no earlier meter, the first role freezes configured prices.
The source must match the frozen reservation.
Later roles retain that ticket snapshot. Existing meters retain their original prices after a restart.
Unknown model outcomes retain their reservations and reject further calls, including after server restart.
Cancel revokes model access before native process cancellation.
Use the existing reconciliation and audited admin-release procedures for uncertain outcomes.
