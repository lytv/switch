# switch-connector-pi

The pi ([pi.dev](https://pi.dev), `@earendil-works/pi-coding-agent`) build of
the Switch connector. It ships the **Switch room-workflow skill**
(`skills/switch/SKILL.md`) and a pi **extension** (`extensions/switch/`) that
wires `@sandboxaq/switch-agent-runtime` into a pi session so it can join a
Switch room and take part like the existing Claude Code, Codex and OpenCode
agents.

This is the sibling of `connectors/claude-code-plugin/`,
`connectors/codex-plugin/` and `connectors/opencode-plugin/`, and it exists
for the same reason: give one more coding-agent harness the Switch tool
surface without re-implementing the Agent Protocol against it. One thing
about it is genuinely different from all three, and it is worth
understanding before changing anything here.

## pi has no built-in MCP client

Claude Code, Codex and OpenCode all register `@sandboxaq/switch-agent-runtime`
as an MCP server through host-native config (`.mcp.json`, `opencode.json`).
pi's own docs are explicit that this is not available: "It intentionally does
not include built-in MCP, sub-agents, permission popups, plan mode, to-dos, or
background bash. You can build or install those workflows as extensions or
packages" (`docs/usage.md` in the pi package).

So this connector's extension **is** the MCP client. `extensions/switch/mcp-bridge.ts`
spawns the runtime's `./bin` entry over stdio using the MCP TypeScript SDK's
own `Client` / `StdioClientTransport`, fetches its tool catalog
(`client.listTools()`), and re-registers every tool pi's own way via
`pi.registerTool()`, proxying each call to `client.callTool()`. Inbound room
events arrive as the runtime's `notifications/claude/channel` MCP notification
(the same one Claude Code's `claude/channel` experimental capability
consumes); the MCP client SDK routes any notification with no more specific
handler to `fallbackNotificationHandler`, which this connector uses to turn
each event into a pi turn via `pi.sendUserMessage()`.

**Nothing here re-implements the Agent Protocol, its SSE stream, credential
resolution, or the operation catalog.** All of that stays inside the spawned
runtime process, exactly as it does for the other three connectors. This
extension only bridges MCP tool calls and MCP notifications to pi's own tool
and message APIs, plus a small amount of plumbing (below) to reach parity
with what the other connectors get from host-native hooks.

## What's in this pass

1. **`skills/switch/SKILL.md`** - the room-workflow skill, copied verbatim
   from `connectors/opencode-plugin/skills/switch/SKILL.md`. It is identical
   across every connector; do not rewrite it here.
2. **`extensions/switch/`** - the pi extension for manual installs:
   - `mcp-bridge.ts` spawns the runtime and bridges its MCP tool surface and
     `claude/channel` notifications.
   - `event-format.ts` renders a channel notification as the `[Switch] …`
     line the skill promises, and decides whether to deliver it as a fresh
     turn or steer it into one already running.
   - `hooks.ts` talks to the runtime's local hook listener (see "Feature
     parity" below).
   - `env.ts` passes this process's environment through to the spawned
     runtime - `StdioClientTransport` does **not** inherit the parent
     environment by default (see the source comment); without this, the
     runtime would start with neither `SWITCH_*` credentials nor `PATH`
     for `npx` to resolve itself.
   - `index.ts` is the extension entry: it connects on `session_start`,
     registers a `/switch` status command, and cleans up on
     `session_shutdown`.
3. **`console/switch-connector.ts`** - the single-file, dependency-free port
   of the same extension that Switch Console writes into
   `~/.pi/agent/extensions/` for its own managed sessions (plus the skill
   above into `~/.pi/agent/skills/`). It cannot import the MCP SDK — nothing
   runs `npm install` in the home directory — so it hand-rolls the MCP stdio
   framing over `node:child_process` instead. It is the source of truth for
   what Console installs; `console/packages/plugins/src/agents/impl/pi/`
   embeds it verbatim and fails its drift guard if the two disagree. In a
   Console-managed session it registers tools only and leaves event delivery
   to Console's own `[Switch]` injection; standalone, it bridges channel
   notifications into pi turns like the manual extension does.
4. **`README.md`** (this file).

Pinned runtime version: `@sandboxaq/switch-agent-runtime@0.4.1`, the same pin
`connectors/{claude-code,codex,opencode}-plugin` use. Bump all four together.

### Feature parity with the other connectors

The runtime's own MCP tool-call handling already tracks the connected room
for *any* host - `connect_to_room` needs no extra plumbing here, because
`bin.ts`'s `CallToolRequestSchema` handler updates its internal room state
whenever that tool succeeds, regardless of which client called it.

A few things are different: they are tracked by the runtime's *local hook
listener* (`startHookListener` in `bin.ts`), which exists for hosts that
cannot special-case a tool call in-process - Claude Code's separate
`PostToolUse` hook script, and here, this extension (which talks to the
runtime only through a generic MCP client). `extensions/switch/hooks.ts`
calls the same listener:

- after `read_context` succeeds, so the runtime clears its unaddressed-message
  tally instead of telling you you're behind on messages you just read;
- after `assume_role` / `release_role`, so the runtime's exclusive-role
  lease-renewal loop starts and stops with a live session holding the role;
- after each pi turn triggered by a Switch event ends (`agent_settled`), so
  the runtime clears the "typing" indicator it sets on the room's bridged
  channel when the agent doesn't reply.

### Explicitly deferred

- **Publishing to any marketplace or package registry.** pi has its own
  package mechanism (`pi install`, see below) but this connector is not
  published anywhere yet - install it from a local path.
- **Changing the shared runtime package** (`console/packages/switch-agent-runtime/`).
- **A standalone `configure` skill** (the kind `codex-plugin` and
  `claude-code-plugin` ship for registering a fresh agent from a bare
  terminal). OpenCode doesn't have one either - env vars or a
  `.switch/agents/*.json` written by hand are the standalone path for both.
  If pi grows a Switch Console `switchSetup` integration later, that
  integration point is the natural place to add one.
- **A Switch Console reporting integration** like OpenCode's
  `plugin/switch-notifications.js` (which reports session state - working,
  idle, tool activity - to Switch Console over a local hook port). pi's
  extension events (`agent_start`, `tool_execution_start/end`,
  `agent_settled`, …) could support the same thing, but there is no Switch
  Console `switchSetup` wiring in this pass to consume it, so it's left for
  the Switch Console integration follow-up above rather than built and left
  unused.
- **Tool call mediation and local-tool-call event reporting**, the way
  Claude Code's `hooks/switch_hook.py` does (`pre-tool-call` /
  `post-tool-result` mediation, `events/report`). The room-workflow skill
  itself says this is Claude Code-specific ("Pre-execution mediation is a
  Claude Code connector feature; an OpenCode session has no such hook") -
  this connector matches OpenCode's scope, not Claude Code's.

## Installing

Switch Console installs this connector itself: pi appears in Settings →
Agent providers with install/status controls like the other CLI agents, and
as an agent type wherever Console creates a new agent. That install writes
`console/switch-connector.ts` to `~/.pi/agent/extensions/` and the skill to
`~/.pi/agent/skills/`, stamped with the `switch-connector-pi` artifact
version, so "update available" means the connector actually changed.

For a standalone pi session with no Switch Console involved, install the
manual connector below instead. It is a [pi
package](https://pi.dev) - a directory with a `pi` manifest in its
`package.json` (`extensions/`, `skills/`). Install it from a local checkout
of this repository:

```bash
cd connectors/pi-plugin
npm install   # installs @modelcontextprotocol/sdk, the one runtime dependency

pi install /absolute/path/to/switch/connectors/pi-plugin
# or, to try it for one run without installing:
pi -e /absolute/path/to/switch/connectors/pi-plugin
```

`pi install` writes the package into your `~/.pi/agent/settings.json`
(project-local with `-l`), which loads `extensions/switch/index.ts` as a
global extension and `skills/switch/SKILL.md` as a skill pi discovers from
`~/.pi/agent/skills/switch/` - the directory name matching the skill's
frontmatter `name` is load-bearing, the same rule OpenCode's connector
documents for its own skill location.

You do not need to build anything: pi loads extensions with
[jiti](https://github.com/unjs/jiti), so the TypeScript sources run directly.
`npm install` in this directory is still required once, so
`@modelcontextprotocol/sdk` is present in `node_modules` for jiti to resolve.

### Credentials

The extension spawns the runtime with this process's environment, so set the
same variables Switch Console would inject for a managed session, or the
ones the runtime's local agent store resolves from
(`.switch/agents/*.json` in pi's working directory - see the other
connectors' `configure` skills for the shape of that file, or write it by
hand):

- `SWITCH_API_ENDPOINT`, `SWITCH_API_TOKEN`, `SWITCH_AGENT_ID` - a complete
  environment wins outright.
- `SWITCH_CONNECTION_ID` - only set this if something else already opened a
  connection to share (this connector always owns its own, since nothing
  supervises it the way Switch Console supervises the other hosts).
- `SWITCH_CHANNEL_DISABLE_POLL` - leave unset; it exists for a supervisor
  reading the same event stream separately, which doesn't apply here.

If several agents are provisioned in the working directory and no complete
environment is set, the runtime serves a `select_agent` tool - call it once
with the agent name pi should act as before using any other Switch tool.

### `/switch` command

Reports whether the connector is connected and how many Switch tools are
registered. It does not do setup - with no Switch Console and no `configure`
skill for pi yet, setup is exporting the environment variables above (or
writing the agent store file another connector's `configure` skill would
write) before starting pi.

## Testing

```bash
cd connectors/pi-plugin
npm install
npm test        # vitest, colocated *.test.ts files
```

Tests cover the non-trivial logic this connector adds: the MCP-result-to-pi-
tool-result mapping (`tool-result.test.ts`), the channel-notification-to-pi-
turn formatting and delivery-mode decision (`event-format.test.ts`), the
runtime hook plumbing (`hooks.test.ts`), and environment passthrough
(`env.test.ts`), plus the same coverage for the dependency-free Console port
(`console/switch-connector.test.ts`). The verbatim-copied skill and the thin `index.ts` wiring
(which only calls the tested functions above) are not separately tested, per
the repo's existing test conventions.
