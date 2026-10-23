# Jira ticket activity

The ticket worker remains off by default. `JIRA_WORKER_ENABLED_PROJECTS` enables
projects by Jira instance. `JIRA_WORKER_LIMITS` accepts a JSON object with these
positive integer values:

| Key | Default | Scope |
| --- | ---: | --- |
| `open_rooms` | 50 | Open rooms of enabled projects. Done, cancelled, and archived rooms do not count, and a project that is off does not hold a slot |
| `live_sessions` | 5 | Claimed orchestrator turns of enabled projects. A project that is off does not hold a session |
| `room_creates_per_hour` | 20 | Create attempts in the previous rolling hour |
| `tokens_per_ticket` | 50000 | Cumulative reported usage for one ticket |

Room reservations and create attempts survive a restart. An uncertain room
create keeps its reservation. Reconcile adopts the marked room before it attempts
another create. Archive releases the room reservation after the archive succeeds.
Tickets that exceed room or session capacity remain in the worker's To Do queue.
Admission selects the oldest unclaimed ticket in an enabled project first, including one that has no queue reason yet. The queue does not overwrite
Jira status or a human decision.

## Frozen workflow state

Existing tickets retain the `delivery@1` legacy intake policy. New intake tickets
also use that policy. The additive workflow schema does not enable delivery,
start coding jobs, or change approval behavior.

`packs.py` loads the immutable `delivery@2.0.0-draft` fixture. It checks the
historical digest before it creates a snapshot with configured budgets.
A ticket stores its snapshot and digest together. Later configuration or pack
file changes do not change that stored snapshot. The draft remains fixture-only.
Typed state and result contracts validate JSON at the workflow boundary.

| Environment variable | Default |
| --- | ---: |
| `JIRA_WORKER_DELIVERY_TICKET_TOKENS` | 200000 |
| `JIRA_WORKER_DELIVERY_TICKET_ESTIMATED_USD` | 5 |
| `JIRA_WORKER_DELIVERY_DAY_ESTIMATED_USD` | 25 |

Costs are USD estimates, not subscription invoices. These settings do not change
the existing intake limit of 50000 tokens. The delivery lifecycle reserves these budgets before a task starts. Delivery
execution remains disconnected from the live approval path.

The scheduler claims only explicit job kinds. The intake scheduler claims
watermark polls, not role jobs or unknown kinds. Execution keys remain unique
after completion. Only one active role execution may hold a ticket generation.
Downgrade refuses delivery workflow or execution records, including completed
executions, to preserve their snapshots and audit history.

## Firstmate task lifecycle

`firstmate_jobs.py` exposes `FirstmateJobs.start`, `get`, and `cancel`.
No live Approve handler or scheduler calls this service. It uses the existing
role-job records and the ticket's `agent_generation`. It adds no daemon, queue,
or schema migration.

Start commits the frozen input and usage reservation before it calls Firstmate.
A repeated execution key returns the same execution. Different input, reservation,
or duration estimates for that key fail. One database lock serializes each
execution's process operations. Concurrent calls wait for the lock without a
separate wait timeout. Native commands retain their individual timeouts, so a
start can span several command timeouts. Cancel commits its intent before it
waits. A separate transaction lock serializes budget admission and usage
accounting across tickets.

`firstmate_copy.py` calls the delivery checkout's `fm-spawn.sh --scout` and
`fm-control.sh <task-id> exit`. It uses the native task metadata, brief, status,
and steering inbox paths. Each execution receives its own Firstmate home,
operating-system HOME directory, and private tmux server. The operator must
prepare a separate delivery checkout and a separate worker-homes root. Both
paths must be absolute and contain no symlinks. To opt these roots in, the
operator creates these regular marker files with exactly
`switch-firstmate-delivery-v1` followed by a newline:

- `<delivery-checkout>/.switch-delivery-copy`
- `<worker-homes>/.switch-delivery-workers`

The caller supplies a prepared project checkout and an explicit executable PATH.
The adapter checks the frozen Firstmate and repository commits. It does not
clone a ticket URL, discover a standing home, or configure provider credentials.
Its subprocess environment contains only explicit operational values. Inject
all configured credential values into `SecretGuard.forbidden_values`; never
forward them into task data or the process environment. The guard also rejects
credential-bearing URLs and common credential forms. Pattern checks cannot
identify every possible secret supplied as arbitrary prose.

The task writes `result.json` and then its hex SHA-256 digest in `result.sha256`
inside its artifact directory. Results use the frozen `Result` contract.
Optional `usage.json` contains cumulative `Usage` observations. Reads reject
oversized files, path traversal, symlinks, hardlinks, non-regular files, digest
mismatches, mismatched inputs, and forbidden credential values. A result does
not prove process exit. The service confirms native cancellation before it
accepts the result or releases unused reservations. Cancellation and generation
changes reject late results, but still account for their known usage.
Terminal status and accepted results remain unchanged after later cancellation,
generation, park, or budget changes. Terminal executions still account for late
usage, but they never accept a previously unaccepted result.

A durable dispatch marker precedes native spawn. If a crash leaves that marker
without native task metadata, the execution becomes `uncertain`. It retains its
reservation and never repeats the ambiguous spawn. Recovery needs evidence from
the operator; this adapter deliberately provides no automatic marker reset.
A crash before that handoff can reuse the durable database reservation.

Reservations include cached tokens and estimated USD. Partial or unknown usage
retains the unaccounted reservation, including across UTC day boundaries.
Known usage never resets on retry. Budget admission includes previous intake
usage. Reported spend at the reserved limit triggers cancellation and an admin
alert. At twice the duration estimate, the service queues an admin alert but
continues within the existing budget. Polls and reminders run independently of
these short lifecycle calls.

These primitives do not prove an OS sandbox, trusted live metering, or real
provider process-tree cancellation. A worker can exceed a limit between usage
observations. Prompts and reservation accounting cannot prevent that spend.
Before live enablement, deployment must supply isolation, credential-free model
access, and trusted metering that prevents additional spend at the limit.
The draft fixture pack is not a production runtime. Tests use real PostgreSQL
and fake Firstmate processes, never paid agents or standing homes.

## Ticket rooms

Each ticket room uses a private channel on the instance's default collaboration
bridge. Switch read and write visibility also stays private. The mapped reporter
owns the room; admins retain access through Switch authorization. The worker
invites every account that the reporter claims on that bridge, and no other
people. It does not invite Mattermost's configured default member. Mapping and
claim changes automatically remove unauthorized people from private Mattermost
ticket channels, including archived channels. The current reporter's claimed
accounts, Switch admins' claimed accounts, platform admins, and bots retain
membership. Other bridges report unsupported membership revocation on the admin
card.

Room sync attaches a private channel to an existing unbridged ticket room.
Repeated sync and worker restarts reuse that channel. The worker reposts the
ticket card and current open intake question once after attachment. The new
card becomes the thread root for later questions and card edits.
Done and cancelled tickets archive the Switch room and close the linked private
channel when the bridge supports it. A channel-close failure stays on the admin card and
keeps room sync pending until it succeeds. Reopening the ticket restores the
existing channel before editing its card when the bridge supports it. A restore
failure keeps the room archived and room sync pending for retry. A human reopen
does not resume intake or permit bot Jira writes. Archived rooms receive no reporter
invitation until that reopen path runs. Missing, stopped, channel-disabled, or unreachable default bridges
leave new ticket rooms internal-only. Access failures log a warning and add a reason to the
admin card without blocking ticket sync. Mapping changes, claim changes, and
ordinary ticket sync retry access and clear the reason after success.
A mapped reporter needs a claimed account on the bridge to answer there.

## Input and waits

The worker reads every human message in the ticket room through `read_context`:
top-level messages and replies in any thread. It skips its own messages and the
ticket card. Its internal sequence cursor follows database commit order. It does
not use a timestamp cursor or require a mention. Event keys and cursor updates
commit together.

An unmapped reporter uses Jira comments. A failed thread read records a fallback
and selects Jira comments for that ticket. Ordinary Jira reads follow the existing
webhook and watermark intake. While a ticket waits on Jira comments or waits
for input in its room, the worker also reads comments on the poll interval when
the issue timestamp has not moved. Room-based waiting tickets use this read to
send wrong-place notices, not to accept Jira comments as answers.
Comment event ids keep that repeat read from recording the same comment twice.
The backup search converts its watermark to an unquoted JQL epoch-millisecond
bound. This preserves its timezone and includes the boundary again for deduplication.
After a completed read, a new comment from anyone except the worker and the reporter parks the ticket even when the issue timestamp has not moved. Comments stored on the first read do not.
The worker also checks Jira immediately before each orchestrator turn. It uses
the existing paginated history and comment read path. A Done or cancelled status
discovered on that read parks the ticket and enqueues the same room and admin-card
follow-ups as webhook and poll intake.

The shared observer permits an issue updated stamp up to one second after the
latest attributable worker-owned comment, Rank-only history, or new unedited
comment owned by the ticket's actual Jira reporter. Reporter comments still use
the ticket's selected wait channel: Jira fallback accepts answers; room-based
intake sends wrong-place guidance. Seen reporter edits, transitions, field changes,
and unrelated changes keep exact stamp attribution. A tied item that needs exact
attribution also keeps that requirement.
The first read establishes its baseline from visible history and comments only, so stamp drift alone never parks it.
Missing known comments, missing history, visible other-author actions, and
regressed stamps still stop the worker. History reads require exact before/after stamps.
Accepted limitation: an unseen human change inside that one-second window cannot
be distinguished from Jira's comment or Rank timestamp drift.

Reporter input and due timers request a wake. They do not change Jira wait status.
A human Jira action, Done, cancel, or an exhausted token budget parks the agent.
New input cannot clear a human park. An interrupted turn also parks the ticket,
because its outcome and token usage are unknown.

## Wrong-place notices

Jira owns status changes and the Approve/Reject transitions. Switch owns answers
when the ticket's wait channel is `switch`. Milestone summaries remain Jira
comments. No keyword matching or model infers approval from a room message.

A new or edited reporter Jira comment on a Switch-channel ticket creates an
`ignored_input` event, without a wake. The orchestrator never consumes that event.
The worker enqueues a Jira comment notice through the compare-read-write outbox.
The notice links to the ticket room using `FRONTEND_BASE_URL`; missing link
configuration fails the read without advancing its cursor. A ticket that uses
`jira_comments` still accepts reporter comments as input.

While Jira status equals `JIRA_WORKER_STATUSES.waiting_for_approval`, each human
room message remains normal input with `wrong_place_type: switch_approval`.
The worker queues a room notice with the Jira issue link. Agents do not trigger
that notice. The notice never queues a Jira status change.

`JIRA_WORKER_NOTICE_WINDOW_SECONDS` defaults to 86400 (24 hours) and must be
positive. The durable outbox records each person's notice type and ticket.
The ticket row lock serializes the rolling-window check and enqueue across
restarts and concurrent readers. Notice claims count even when delivery fails.
A Switch notice with an unknown send outcome becomes `uncertain` and logs an
error; the worker does not replay it automatically. Jira notices retain the
existing outbox version check, retry, and uncertain-write rules.

## Admin cards

One admin room holds every alert. The worker finds it by the
`jira_worker_admin_room` marker, not by name. The configured admin user owns
the room. The room uses the default bridge channel. The Jira system agent
posts there.

One card per ticket is posted once, then edited in place. The message keeps
its id and position. The card shows the current reasons, ticket key, summary,
status, reporter mapping state, Jira link, and ticket room link. Clearing a
reason edits the card. The card stays. A new reason also posts one short
alert line after the card commit. A clear does not. Follow-up: propagate
message edits to bridge channels.

Reasons come from an unmapped reporter, a Jira write that failed 3 times or
parked the worker, any park except a completed or cancelled ticket,
and the wait rules. `set_admin_card_reason` is the raise/clear call for an
intake question count. The intake orchestrator raises this reason when it needs
a sixth question, without sending that question. Completion clears the reason.

Wait timers count work hours on `JIRA_WORKER_CALENDAR` (default Monday-Friday
09:00-17:00 UTC). The reminder is 2 work hours and goes to the reporter on the
ticket wait channel. The admin alert is 4 work hours. Blocked follows one
work day, using the calendar's daily window. These thresholds are fixed.
A disabled project is left unchanged. A Switch reminder is a durable outbox
command: reconciliation queues it atomically with its wait milestone, and the
sender posts it after rechecking the wait. A reminder with an unknown send
outcome becomes `uncertain` and is never replayed automatically.

An admin accepts a stored `ignored_input` event with
`POST /jira-triggers/worker/inputs/{event_id}/accept`. That writes a new
`explicit_input` event. The old event is not rewritten.

An admin releases a parked ticket with
`POST /jira-triggers/worker/tickets/{issue_key}/release?instance={instance}`.
The JSON body requires `request_id`, a UUID generated for this release request.
Repeat the same UUID after a connection failure or a retryable conflict.
A repeated request returns its stored release receipt without another release.
The receipt records the admin, time, and previous park reason.

Release clears the park reason and accepts a fresh observation baseline.
It replaces remembered comment IDs with the IDs currently visible in Jira.
A deleted comment therefore cannot immediately park the ticket again.
Release queues an update to the existing admin card and keeps other alert reasons.
It preserves the Jira status, intake state, token usage, generation, and wait timers.
It does not retry, recreate, cancel, or otherwise change Jira commands.
Unread reporter input remains subject to the ticket's existing channel rules.

A worker BLOCKED transition after a failed Jira write is eligible when the stored
failure command and Jira history prove that the worker made that transition.
An approved In Progress ticket is eligible when the intake approval command
completed and Jira history proves a human transition from Waiting for approval.
Release changes only the park reason and observation baseline in these cases.
Unclear phase/status combinations require separate reconciliation.

Release refuses a ticket that is not parked.
Release refuses completed, cancelled, and token-limit tickets.
A current Jira status in the Done category also prevents release.
Pending or claimed commands require separate reconciliation before release.
Unknown send outcomes and interrupted or failed turns with unknown token usage
also require reconciliation, not replay.
Active sends, turns, or admin card updates return a retryable conflict.
An issue stamp change during the history read also returns a conflict.
If Jira's project or reporter differs from stored routing, release fails.
Refresh the ticket metadata before retrying that release.

No UI is part of this boundary.

## One-turn integration

The worker loop calls `scheduler.activity.run_one_turn(callback)` when intake
model configuration is complete. This method claims one ticket and holds a
database session claim until the callback returns.

The callback receives a `TicketTurn`. Ticket text and event payloads appear under
`untrusted_data`; they are data, not instructions. The callback receives no Jira
credentials. It must enforce `token_budget` during its model calls.

The callback returns `TurnResult(tokens_used, wake_at)`. Usage must be a
nonnegative integer. A wake time must include a timezone; `None` means no timer.
Completion records usage and consumes the turn's input events. Input that arrives
during the turn requests another wake. A concurrent human park invalidates the
turn's completion; it cannot clear that park or consume the pending input.

## Intake orchestrator

Configure these environment variables to enable model calls for enabled projects:

| Variable | Value |
| --- | --- |
| `JIRA_WORKER_LLM_BASE_URL` | OpenAI-compatible API root, including `/v1` when the provider requires it |
| `JIRA_WORKER_LLM_API_KEY` | Deployment secret for that API |
| `JIRA_WORKER_LLM_MODEL` | Model identifier selected by the operator |

No provider or model is selected by default. If any variable is missing or empty,
intake turns stay disabled and the worker logs the missing variable names. The
server still starts. Polling, room reconciliation, notices, and existing outbox
work remain available. The per-project switch still defaults to off.

The API must support non-streaming Chat Completions, JSON-object responses,
`max_tokens`, and explicit `prompt_tokens`, `completion_tokens`, and
`total_tokens` usage. It must use a byte-level tokenizer and honor the output
limit. The worker reserves a conservative UTF-8 byte bound for prompt tokens,
plus 256 tokens for framing, before allocating the remaining output budget.
It makes no request when the prompt cannot fit. It records actual response usage,
not the reservation. Missing usage, an invalid decision, or a violated budget
parks the ticket with a visible failure. HTTP 429 and 5xx responses, transport
failures, and unfinished decisions retry after 5 minutes before the second
attempt and 30 minutes before the third attempt. A valid `Retry-After` header
replaces that delay, up to 1 hour. The worker accepts delay seconds or an HTTP
date; a past date permits an immediate retry. An invalid header logs a warning
and uses the fixed delay. The retry deadline uses the durable wake timer.
New reporter input does not shorten that delay. The worker
stores consecutive failures and pending reporter text across restarts, records
known usage (zero when unknown), and parks after three consecutive failures.
A successful turn resets the failure count. Model errors never select a fallback
provider. This interface does not support APIs that require `max_completion_tokens`
instead of `max_tokens`.

A turn makes at most one model call. It asks one question on the ticket wait
channel through the outbox. Switch questions reply to the ticket card thread.
An unmapped reporter receives Jira comments. A question starts a Waiting for
input milestone when needed. The transcript, question count, and approval cycle
remain in the ticket row across restarts. The model receives only plain-text
summary, description, and reply bodies, marked as untrusted data. ADF descriptions
and comments become text; author and profile metadata do not enter the model
prompt or intake history. A pending intake delivery prevents a
new turn from consuming replies before delivery completes. An unknown question
send outcome parks the worker and does not replay automatically.

After five questions, a request for another question raises `intake_questions`
on the admin card and sends no further question. The transcript does not record
an unsent over-limit question. Reporter or admin-accepted
input can resolve the missing requirements. A completed intake clears that
reason. The question count resets to zero when a human Reject starts a new
approval cycle; each cycle has its own budget of five questions.

Completion queues one Jira summary comment per approval cycle, then a dependent
Waiting for approval transition. Both use the existing compare-read-write
outbox. Room text never approves a ticket. A Jira move to Done archives the room
through the existing reconciliation path. Approval reads use the poll-interval
refresh schedule. Jira enforces the approvers-group
restriction on Approve and Reject; the worker does not grant approval rights.

A clean Jira status change from Waiting for approval to Waiting for input
resumes intake with a question about the required corrections, before another
summary can be queued. That question is the first question of the new cycle's
five-question budget.
Any accompanying human field change, unrelated comment, or
existing human park still stops the worker. The worker never clears that park.
Role agents, code work, routing, attachments, and linked tickets remain out of scope.
