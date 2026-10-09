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

## Input and waits

The worker reads every human message in the ticket room through `read_context`:
top-level messages and replies in any thread. It skips its own messages and the
ticket card. Its internal sequence cursor follows database commit order. It does
not use a timestamp cursor or require a mention. Event keys and cursor updates
commit together.

An unmapped reporter uses Jira comments. A failed thread read records a fallback
and selects Jira comments for that ticket. Ordinary Jira reads follow the existing
webhook and watermark intake. While a ticket waits on Jira comments, the worker
also reads comments on the poll interval when the issue timestamp has not moved.
Comment event ids keep that repeat read from recording the same comment twice.
After a completed read, a new comment from anyone except the worker and the reporter parks the ticket even when the issue timestamp has not moved. Comments stored on the first read do not.
The worker also checks Jira immediately before each orchestrator turn. It uses
the existing paginated history and comment read path.

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

Admin override and admin cards remain step 7 work. Intake decisions remain step 8
work. No UI is part of this boundary.

## One-turn integration

Step 8 can call `scheduler.activity.run_one_turn(callback)`. This method claims
one ticket and holds a database session claim until the callback returns.

The callback receives a `TicketTurn`. Ticket text and event payloads appear under
`untrusted_data`; they are data, not instructions. The callback receives no Jira
credentials. It must enforce `token_budget` during its model calls.

The callback returns `TurnResult(tokens_used, wake_at)`. Usage must be a
nonnegative integer. A wake time must include a timezone; `None` means no timer.
Completion records usage and consumes the turn's input events. Input that arrives
during the turn requests another wake. A concurrent human park invalidates the
turn's completion; it cannot clear that park or consume the pending input.

This boundary does not implement intake prompts, Jira approval decisions,
or admin cards.
