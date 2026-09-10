# Jira inbound triggers setup

Switch can accept **inbound Jira webhooks**, match them against stored trigger
rules, and post an **addressed** message into a Switch room (or every room in a
room group). This is not a collaboration bridge: traffic is one-way from Jira
into Switch. The operator UI lives under **Jira** in the gateway dashboard.

For a full operator guideline (where to click, daily use, rule fields,
troubleshooting), open [`jira-operator-guide.html`](jira-operator-guide.html).

For collaboration platforms (Slack, Teams, …), see [`README.md`](README.md).

## Prerequisites

1. A running Switch API with a reachable public URL (or a tunnel) so Jira can
   `POST` to it.
2. `GATEWAY_PUBLIC_URL` set to that origin (scheme + host, no path) so the
   dashboard shows absolute webhook URLs.
3. At least one webhook secret configured:

   ```bash
   JIRA_WEBHOOK_SECRETS={"acme":"<long-random-secret>"}
   ```

   Keys are instance path segments (`/integrations/jira/acme`). Values are the
   shared secrets Jira must send as the `X-Switch-Secret` header.

Optional knobs (defaults are safe for most installs):

| Variable | Default | Purpose |
| --- | --- | --- |
| `JIRA_AGENT_NAME` | `jira` | Shared system agent that posts trigger messages |
| `JIRA_MESSAGE_MAX_CHARS` | `4000` | Truncate rendered bodies |
| `JIRA_DEDUPE_WINDOW_SECONDS` | `300` | Suppress the same issue+rule+transition |
| `JIRA_RATE_LIMIT_PER_RULE` | `10` | Burst cap per rule |
| `JIRA_RATE_LIMIT_WINDOW_SECONDS` | `60` | Burst window |
| `JIRA_RULE_COOLDOWN_SECONDS` | `5` | Minimum gap between fires for one rule |
| `JIRA_RETRY_MAX_ATTEMPTS` | `3` | Transient post retries |
| `JIRA_RETRY_BACKOFF_SECONDS` | `0.5` | Base backoff (exponential) |
| `JIRA_DELIVERY_LOG_RETAIN_SECONDS` | `604800` | Delivery history age bound (7d) |
| `JIRA_DELIVERY_LOG_MAX_ROWS` | `5000` | Delivery history size bound |

On boot, Switch provisions the system agent named by `JIRA_AGENT_NAME` when any
secret is configured. **You must still add that agent as a member of every
target room** (and every room in a target group) or posts will fail.

## Jira Cloud

### Classic webhook

1. In Jira Cloud: **Settings → System → WebHooks → Create a WebHook**.
2. URL: the instance URL from the Switch **Jira → Setup** panel
   (`https://<switch-api>/integrations/jira/<instance>`).
3. Add a custom header `X-Switch-Secret` with the instance secret (Reveal / Copy
   in the dashboard, or the value from `JIRA_WEBHOOK_SECRETS`).
4. Subscribe to **Issue created** and/or **Issue updated** (updated events carry
   changelog items used for transition matching).
5. Save. Use a test transition and watch **Delivery history** on the Jira page.

### Automation rule

1. Create a Jira Automation rule (project or global).
2. Trigger on issue created / field changed / transitioned as needed.
3. Action: **Send web request**
   - Method: `POST`
   - URL: same as above
   - Headers: `X-Switch-Secret: <secret>`, `Content-Type: application/json`
   - Body: include at least the `issue` object (key + fields). For transitions,
     include changelog `items` with `field` / `fieldId` of `status` and
     `fromString` / `toString`.

Cloud and Automation payloads both parse through the same path; Automation
bodies that omit fields Switch needs will 400 at the webhook.

## Jira Server / Data Center

The same webhook path and `X-Switch-Secret` header apply. Server/Data Center
classic webhooks typically POST a Cloud-like `webhookEvent` + `issue` body;
Switch accepts that shape.

Verified differences to watch for:

- Absolute issue URLs may use your Server base URL; templates still render
  `{{issue.url}}` from the payload when present.
- Some Server plugins send slightly different changelog shapes; if transition
  rules never fire, inspect the raw payload and prefer Automation with an
  explicit JSON body that includes status `fromString` / `toString`.
- TLS and egress: the Jira host must reach Switch. On-prem installs often need
  an allow-listed ingress or reverse proxy in front of Switch.

If a Server/DC payload fails parse, the webhook returns **400** with a short
reason — check Switch logs for `malformed payload` (issue keys only; secrets
and full bodies are not logged).

## Secrets

- Never put the secret in the webhook URL query string.
- Rotate from the dashboard (**Rotate secret**): the live process updates
  immediately; you must also update `JIRA_WEBHOOK_SECRETS` (or your secret
  store) before the next restart, and update the Jira webhook/Automation header
  to match.
- Reveal is admin-only and copies the current live secret.

## System agent membership

The poster is the shared agent (`jira` by default), not the addressed coding
agent. For each rule:

1. Ensure `jira` (or `JIRA_AGENT_NAME`) is a **room member**.
2. Ensure the **target agent** (`agent_name` on the rule) is also a room member
   — addressing uses `@agent_name`.
3. For `target_kind=group`, add the system agent to **every** non-archived room
   in that group.

## Rule fields

| Field | Meaning |
| --- | --- |
| `name` | Operator label |
| `enabled` | Disabled rules never fire |
| `instance` | Must match the webhook path segment / secret map key |
| `project_key` | Blank = any project; otherwise exact project key |
| `issue_type` | Blank = any; otherwise exact issue type name |
| `fire_on` | `created` \| `updated` \| `transition` |
| `target_status` | For `transition`: fire when status becomes this (blank = any) |
| `jql` | Optional simple filters (`labels = x`, `priority = High`, …) |
| `target_kind` | `room` or `group` |
| `target_room_id` / `target_group_id` | Where to post |
| `agent_name` | Switch agent to `@address` in the room |
| `message_template` | Body with `{{issue.*}}` tokens (see Message tokens in the UI) |
| `thread_by` | `new` (top-level) or `issue_key` (reuse a thread per issue in that room) |

## Dry-run

On the Jira rules table, **Test rule** runs a dry-run: match explanation,
rendered message, and resolved targets with **`would_post=false`**. Nothing is
posted and nothing is written to the delivery log. Use it after editing filters
or templates.

## Delivery history, burst, cool-down, retry

- Every match attempt records a delivery row: `delivered`, `error`, or
  `suppressed_dedupe` / `suppressed_burst` / `suppressed_cooldown`.
- Per-room send results (including retry attempt counts) are stored on the row.
- Transient post failures retry with exponential backoff; permanent validation /
  authorization errors are not retried.
- History is pruned by age and max rows (see env table above).

## Troubleshooting

| Symptom | Check |
| --- | --- |
| Webhook **401** | `X-Switch-Secret` missing/wrong; secret rotated but Jira not updated |
| Webhook **400** | Body not JSON, or missing `issue` / unrecognised shape |
| **202** but no room message | Rule disabled / wrong `instance` / filters; system agent not in room; see Delivery history |
| `suppressed_dedupe` | Same issue+rule+transition inside the dedupe window |
| `suppressed_burst` | Rule hit the per-window burst cap |
| `suppressed_cooldown` | Rule fired again inside the cool-down gap |
| `error` / “Targets not in room” | Add `agent_name` (and the Jira system agent) to the room |
| Soft agent status in logs | Message was posted; the target has no live session — expected |

Keep secrets and raw issue payloads out of tickets and chat logs; the delivery
UI shows issue keys and sanitized error types only.
