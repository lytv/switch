# Jira ticket activity

The ticket worker remains off by default. `JIRA_WORKER_ENABLED_PROJECTS` enables
projects by Jira instance. `JIRA_WORKER_LIMITS` accepts a JSON object with these
positive integer values:

| Key | Default | Scope |
| --- | ---: | --- |
| `open_rooms` | 50 | All reserved or open ticket rooms |
| `live_sessions` | 5 | All claimed orchestrator turns |
| `room_creates_per_hour` | 20 | Create attempts in the previous rolling hour |
| `tokens_per_ticket` | 50000 | Cumulative reported usage for one ticket |

Room reservations and create attempts survive a restart. An uncertain room
create keeps its reservation. Reconcile adopts the marked room before it attempts
another create. Archive releases the room reservation after the archive succeeds.
Tickets that exceed room or session capacity remain in the worker's To Do queue.
Admission selects the oldest queued tickets first. The queue does not overwrite
Jira status or a human decision.

## Input and waits

The worker reads the ticket card's thread through `read_context`. Its internal
sequence cursor follows database commit order. It does not use a timestamp cursor
or require a mention. Event keys and cursor updates commit together.

An unmapped reporter uses Jira comments. A failed thread read records a fallback
and selects Jira comments for that ticket. Ordinary Jira reads follow the existing
webhook and watermark intake. The worker also checks Jira immediately before each
orchestrator turn. It uses the existing paginated history and comment read path.

Reporter input and due timers request a wake. They do not change Jira wait status.
A human Jira action, Done, cancel, or an exhausted token budget parks the agent.
New input cannot clear a human park. An interrupted turn also parks the ticket,
because its outcome and token usage are unknown.

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
wrong-place notices, or admin cards.
