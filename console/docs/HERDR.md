# Herdr session host

Switch Console can run a location's agent sessions inside [Herdr](https://github.com/sandboxaq/herdr)
instead of a plain PTY or tmux. This page is for an operator turning it on, not
for the implementation — see `agents/architecture/remote-execution.md` for that.

## Enabling it

Open the location → **Settings** → **Session host**, and set it to **Herdr**.
The setting is per-location and DB-backed (not written to `.switchdash.json`),
so it does not follow the repo if you share config files with a teammate.

The section also exposes the Herdr-specific options once Herdr is selected:

| Setting | Default | What it does |
| --- | --- | --- |
| Session name | `switchdash` | Prefix for the Herdr workspace(s) this location creates. |
| Protocol floor | `14` | Minimum Herdr protocol version required to launch a session here. Switch Console refuses to launch (rather than degrade silently) below it. |
| Prefer agent prompt injection | on | Route the initial prompt through `herdr agent prompt` when possible, falling back to `herdr pane send-text`. |
| Workspace mode | `flat` | How sessions are grouped into Herdr workspaces — see below. |

Leaving **Session host** on its default (unset) keeps the existing behaviour:
`pty` on a local location, `tmux` on a remote one (or whatever the location's
own **Enable tmux** setting already says).

## Requirements

- `herdr` must be on `PATH` — on the local machine for a local location, or on
  the remote host for an SSH one. Switch Console preflights this and fails the
  launch with a clear error rather than silently falling back to another host.
- The running Herdr's protocol must be at least the **protocol floor** above.
  Protocol 16+ additionally carries live agent-status events over Herdr's own
  socket; below that, Switch Console polls instead — both paths work, the
  event stream is just less chatty.

## Local vs. SSH

The mechanics are the same either way — Switch Console creates a Herdr
workspace/tab/pane and attaches its terminal to that pane, exactly as it does
for a local or remote tmux pane. The difference is only which host the `herdr`
binary and the pane itself live on:

- **Local**: the pane is created and attached to on this machine.
- **SSH**: the pane is created and attached to on the remote host; a
  Switch Console-deployed sidecar (`docs/old/LOCAL_DEVELOPMENT.md` /
  `agents/architecture/remote-execution.md`) keeps the session's room
  connection alive from that same host while Switch Console is closed, the
  same way it does for tmux.

## Workspace modes

Workspace mode controls how sessions are grouped into Herdr workspaces, not
whether they get one — every session that runs on Herdr gets its own tab and
pane regardless of this setting:

- **`flat`** — every session in the location shares one Herdr workspace
  (named after **Session name**). Simplest; fine for a location with a
  handful of agents.
- **`per-agent`** — one workspace per Switch agent in the location.
- **`per-room`** — one workspace per Switch room a session is connected to.
- **`per-task`** — one workspace per session, so nothing is ever shared.

Pick the mode that matches how you want to browse panes from Herdr's own UI —
Switch Console's own terminal view does not change with this setting.

## Finding a session's exact pane from outside Switch Console

Right-click a session (or open its row actions menu) and use **Copy Herdr
attach command** to copy the exact `herdr pane attach --pane <id>` for that
session's pane — the same subcommand Switch Console itself runs, on the
pane's literal id rather than a name that could match more than one pane.
Useful for reattaching from a terminal Switch Console isn't running in, or for
confirming which pane a session actually landed on.

## If a session's terminal won't attach

A remote session's terminal shows a Herdr-specific hint on attach failure
(daemon down, or the pane closed outside Switch Console) rather than the
generic "could not open the terminal" message tmux/pty failures get. **Try
again** retries the same attach; the agent itself keeps running on the host
either way — only the terminal view failed.
