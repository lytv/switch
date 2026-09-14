import * as os from 'node:os';
import * as path from 'node:path';
import { deriveAgentStatus, deriveErrorDetail } from '@main/core/agent-hooks/derive-agent-status';
import type { ContextResolver, ParsedHookEvent } from '@main/core/agent-hooks/event-enricher';
import { parseHookEvent } from '@main/core/agent-hooks/event-enricher';
import type { RawHookRequest } from '@main/core/agent-hooks/hook-server';
import type { SessionStartupWatch } from '@main/core/agent-runtime/session-startup-watch';
import {
  HerdrInjectionSink,
  type HerdrPromptTarget,
  type HerdrRun,
} from '@main/core/switch-rooms/herdr-injection-sink';
import { PluginPromptInjector } from '@main/core/switch-rooms/plugin-prompt-injector';
import {
  RoomConnection,
  type RoomConnectionDeps,
  type RoomConnectionLogger,
  type SwitchCredentials,
} from '@main/core/switch-rooms/room-connection';
import { sessionConnectionId } from '@main/core/switch-rooms/session-connection-id';
import { resolveSessionControl } from '@main/core/switch-rooms/session-control';
import { type TmuxRun, TmuxInjectionSink } from '@main/core/switch-rooms/tmux-injection-sink';
import { asPtyProviderId, makePtyId, parsePtyId } from '@shared/core/pty/ptyId';
import type { SessionHostTarget } from './session-host-backend';
import { makeAgentTmuxSessionName } from './vm-tmux';

/** A live per-room connection — the slice of RoomConnection the runtime drives. */
export interface ManagedConnection {
  start(): void;
  stop(): void;
  onAgentStatusChange(
    status: Parameters<RoomConnection['onAgentStatusChange']>[0],
    notificationType?: Parameters<RoomConnection['onAgentStatusChange']>[1],
    detail?: Parameters<RoomConnection['onAgentStatusChange']>[2]
  ): void;
  reportActivity(detail: string): void;
  /** The connection id this session's tool calls are expected to arrive on. */
  readonly connection: string;
}

export type RoomConnectionFactory = (deps: RoomConnectionDeps) => ManagedConnection;

export const defaultRoomConnectionFactory: RoomConnectionFactory = (deps) =>
  new RoomConnection(deps);

export interface SidecarRuntimeDeps {
  creds: SwitchCredentials;
  deeplinkScheme: string;
  tmuxRun: TmuxRun;
  herdrRun: HerdrRun;
  /** Whether a given agent tmux target is currently live (poller-backed cache). */
  isTmuxPaneLive: (tmuxTarget: string) => boolean;
  /** Whether a given herdr pane is currently live (poller-backed cache). */
  isHerdrPaneLive: (paneId: string) => boolean;
  /** Prompt target for a herdr pane when the agent is recognized and promptable. */
  herdrPromptTarget: (paneId: string) => HerdrPromptTarget | null;
  /** Whether a herdr agent pane is currently blocked on human input. */
  isHerdrPaneBlocked: (paneId: string) => boolean;
  /** Whether this sidecar should prefer herdr agent-prompt injection over pane send-text. */
  preferHerdrAgentPrompt: boolean;
  log: RoomConnectionLogger;
  createConnection: RoomConnectionFactory;
  /** Durable registry of the sessions this sidecar owns. Backed by the state
   * file so ownership survives a restart, rather than being rebuilt only as
   * each session happens to post its next hook. */
  registry: SessionRegistry;
  /**
   * Tracks which spawned sessions have reported that they are really running.
   *
   * Shared with the spawner, which arms it: the spawner knows a session is new,
   * and this runtime is where the report arrives and where the pane is written
   * to, so both halves need the same view.
   */
  startupWatch: SessionStartupWatch;
}

/** The slice of the durable state store the runtime needs. */
export interface SessionRegistry {
  has(sessionId: string): boolean;
  record(entry: {
    sessionId: string;
    roomId: string | null;
    providerId: string;
    target: SessionHostTarget;
  }): void;
  /** Drop the session's room while keeping the session. Distinct from `record`
   * with a null room, which means "no room information" and preserves what is
   * already known. */
  clearRoom(sessionId: string): void;
  forget(sessionId: string): void;
}

interface SessionConnection {
  connection: ManagedConnection;
  providerId: string;
  ptyId: string | null;
  /** Null while the session holds no room — the server has not named one yet,
   * or it named one and then took it away. `connectRoom` branches on this. */
  roomId: string | null;
  /** True once the server has taken this session's room away, as opposed to
   * never having named one. The two are both `roomId: null` but must be
   * reported differently: a session that lost its room has to be published as
   * roomless so clients stop showing it under a room it no longer attends,
   * whereas one that has yet to be told its room must not overwrite the room
   * the durable registry restored for it. */
  lostRoom: boolean;
  target: SessionHostTarget | null;
}

/**
 * The remote sidecar's manager: receives every session's agent-CLI hook
 * callbacks over one local HTTP server and drives, per session, a tmux-backed
 * RoomConnection injecting into that session's own pane. Multi-session — the
 * single agent-scoped sidecar serves every session on the VM (the one Switch Console
 * started over SSH, and any the notification watcher auto-starts), each keyed by
 * its session id, so there is exactly one sidecar per agent rather than one
 * per session.
 *
 * Runs entirely on the VM with no database or Electron — the agent's Switch
 * credentials come from its `.claude/settings.local.json`.
 */
export class SidecarRuntime {
  /** sessionId → its live room connection. */
  private readonly sessions = new Map<string, SessionConnection>();
  private readonly herdrStatuses = new Map<
    string,
    { status: Parameters<ManagedConnection['onAgentStatusChange']>[0]; detail?: string }
  >();
  private readonly resolveContext: ContextResolver;
  /** Notified when a session connects to a room, so the notification watcher can
   * hand its per-room in-flight guard off to the live-room check (mirrors the
   * local AutoSessionWatcher's room-connection subscription). */
  private roomConnectedListener: ((roomId: string, sessionId: string) => void) | null = null;

  constructor(private readonly deps: SidecarRuntimeDeps) {
    this.resolveContext = async (ptyId) => {
      const parsed = parsePtyId(ptyId);
      if (!parsed) return null;
      return {
        sessionId: parsed.sessionId,
        providerId: parsed.providerId,
        ptyId,
      };
    };
  }

  /**
   * Register the room-connected listener (the notification watcher's guard
   * hand-off).
   *
   * Carries the session id as well as the room because the spawn guards are
   * keyed differently: the watcher's in-flight guard by room, the spawner's
   * launched-session entry by session. Both end at the moment a session
   * connects, and after that the runtime's own room map is the only thing that
   * should decide whether a room is covered.
   */
  onRoomConnected(listener: (roomId: string, sessionId: string) => void): void {
    this.roomConnectedListener = listener;
  }

  /** Handle one raw hook callback from an agent CLI. Never throws. */
  async handleHook(raw: RawHookRequest): Promise<void> {
    // Any hook at all proves the CLI is past its startup prompts and running,
    // so this is not narrowed to the session-start event: it is also the point
    // where the pane stops being a place a security prompt might be showing,
    // and room messages held back for that reason are released.
    this.deps.startupWatch.markStarted(raw.ptyId);

    // Every hook posted to THIS sidecar comes from a session it owns (the
    // session's hook env points here), so record its session id. This is
    // how `/sessions` scopes the VM-wide tmux enumeration to this agent's own
    // panes — tmux session names carry no repo/agent, so without this a sidecar
    // would report other agents' sessions on the same host.
    const pid = parsePtyId(raw.ptyId);
    if (pid) {
      const target = this.sessions.get(pid.sessionId)?.target ?? {
        kind: 'tmux',
        tmuxTarget: makeAgentTmuxSessionName(pid.sessionId),
      };
      this.deps.registry.record({
        sessionId: pid.sessionId,
        roomId: null,
        providerId: pid.providerId,
        target,
      });
    }

    let parsed: ParsedHookEvent;
    try {
      parsed = await parseHookEvent(raw, this.resolveContext, this.deps.log);
    } catch (error) {
      this.deps.log.warn('SidecarRuntime: failed to parse hook event', {
        type: raw.type,
        error: String(error),
      });
      return;
    }

    if (parsed.kind === 'switch-room') {
      this.connectRoom(parsed.ctx.sessionId, parsed.ctx.providerId, parsed.roomId, parsed.roomName);
      return;
    }

    if (parsed.kind === 'status') {
      const status = deriveAgentStatus(parsed.event);
      if (!status) return;
      const notificationType =
        parsed.event.type === 'notification' ? parsed.event.payload.notificationType : undefined;
      // Route to the session the event came from — not every connection.
      const session = this.sessions.get(parsed.event.sessionId);
      session?.connection.onAgentStatusChange(
        status,
        notificationType,
        deriveErrorDetail(parsed.event)
      );
      return;
    }

    if (parsed.kind === 'activity') {
      const session = this.sessions.get(parsed.ctx.sessionId);
      session?.connection.reportActivity(parsed.detail);
      return;
    }
    // 'session' | 'ignore' → no-op: the VM persists no provider-session id and
    // has no database to update.
  }

  /**
   * Open a session's connection before it launches, and return the id to hand
   * it in its environment. Mirrors Switch Console's `ensureForSession`.
   *
   * The connection must exist before the session's first `connect_to_room`,
   * which arrives tagged with this id. It also makes the room server-driven
   * here too: the claim lands on this connection and comes back as
   * `subscription_changed`, so the sidecar stops depending on parsing the hook.
   *
   * `roomId` is the room the session is being launched for, when it is being
   * launched for one — an auto-started session always is. Declaring it opens
   * the connection already claiming that room, so the session belongs to it
   * from the start instead of from whenever the agent calls connect_to_room.
   * Null for a session opened without a room in mind.
   */
  ensureForSession(
    sessionId: string,
    providerId: string,
    roomId: string | null,
    startCursor?: number,
    target: SessionHostTarget | null | undefined = undefined
  ): string {
    const initialTarget: SessionHostTarget | null =
      target === undefined
        ? ({ kind: 'tmux', tmuxTarget: makeAgentTmuxSessionName(sessionId) } as const)
        : target;
    const existing = this.sessions.get(sessionId);
    if (existing) {
      if (initialTarget && !existing.target) {
        existing.target = initialTarget;
        this.recordSessionTarget(sessionId, providerId, initialTarget, existing.roomId);
      }
      return existing.connection.connection;
    }
    return this.openConnection(sessionId, providerId, roomId, null, startCursor, initialTarget);
  }

  private connectRoom(
    sessionId: string,
    providerId: string,
    roomId: string,
    roomName: string | null
  ): void {
    const existing = this.sessions.get(sessionId);
    // A repeat connect to the same room by the same session is a no-op so the
    // in-flight queue and renew loop are preserved — but still hand off the
    // watcher's spawn guard (idempotent), since a session is attending the room.
    if (existing && existing.roomId === roomId) {
      this.roomConnectedListener?.(roomId, sessionId);
      return;
    }
    if (existing && existing.roomId === null) {
      // We opened this connection at launch and the server has not named a
      // room yet. The claim is in flight; trust it rather than rebuilding.
      this.deps.log.debug('SidecarRuntime: hook named a room before the server did', {
        sessionId,
        roomId,
      });
      this.roomConnectedListener?.(roomId, sessionId);
      return;
    }
    // A session re-targeting to a new room supersedes ITS OWN prior room only —
    // other sessions' connections are untouched (mirrors each session's
    // connect_to_room re-targeting independently).
    if (existing) existing.connection.stop();

    this.openConnection(sessionId, providerId, roomId, roomName, undefined, existing?.target);
    this.roomConnectedListener?.(roomId, sessionId);
  }

  private openConnection(
    sessionId: string,
    providerId: string,
    roomId: string | null,
    roomName: string | null,
    startCursor?: number,
    target: SessionHostTarget | null | undefined = undefined
  ): string {
    const resolvedTarget: SessionHostTarget | null =
      target === undefined
        ? ({ kind: 'tmux', tmuxTarget: makeAgentTmuxSessionName(sessionId) } as const)
        : target;
    // A connection can be opened from persisted or test data before the
    // provider has been validated. Only a known provider can have a startup
    // watch; preserve the connection path for unknown values instead of
    // making liveness cleanup introduce a new validation failure.
    let ptyId: string | null = null;
    try {
      ptyId = makePtyId(asPtyProviderId(providerId), sessionId);
    } catch {
      // The provider-specific injector remains the authority for that input.
    }
    // Derived, not random: the session's pane outlives this process and keeps
    // stamping the id it was launched with, so a restarted sidecar has to
    // recompute that id rather than mint one the agent will never hear about.
    const connectionId = sessionConnectionId(sessionId);
    const connection = this.deps.createConnection({
      creds: this.deps.creds,
      roomId,
      roomName,
      connectionId,
      startCursor,
      sessionId,
      sink: this.sinkForSession(sessionId, ptyId),
      injector: new PluginPromptInjector(providerId),
      control: resolveSessionControl(providerId),
      deeplinkScheme: this.deps.deeplinkScheme,
      // The sidecar has no in-process signal for an attached operator's
      // keystrokes (those go through Switch Console's main process over SSH), so it
      // can't gate on human typing yet — a known follow-up for the attached case.
      isHumanTyping: () => false,
      mediaDir: path.join(os.tmpdir(), 'switch-console-switch-media', sessionId),
      // The server naming this connection's room is what records it here, so a
      // session that moves rooms is followed without re-reading a hook.
      onRoomChanged: (room) => {
        const entry = this.sessions.get(sessionId);
        if (entry) {
          entry.roomId = room;
          if (!room) entry.lostRoom = true;
        }
        if (room) {
          const sessionTarget = this.sessions.get(sessionId)?.target;
          if (sessionTarget) this.recordSessionTarget(sessionId, providerId, sessionTarget, room);
          this.roomConnectedListener?.(room, sessionId);
          return;
        }
        // Losing the room has to reach the durable registry too. It is what
        // `/sessions` reports for a pane with no live connection and what a
        // restart restores from, so leaving the old room there would put the
        // session back under a room it no longer attends — and reconnect it,
        // taking the room off whoever holds it now.
        this.deps.registry.clearRoom(sessionId);
      },
      log: this.deps.log,
    });
    this.sessions.set(sessionId, {
      connection,
      providerId,
      ptyId,
      roomId,
      lostRoom: false,
      target: resolvedTarget,
    });
    if (roomId && resolvedTarget) {
      this.recordSessionTarget(sessionId, providerId, resolvedTarget, roomId);
    }
    // The other end of the watcher's hand-off. If a spawned session comes up
    // without the message that triggered it, this says whether a cursor was
    // handed over and honoured, or whether it opened at head and read past it.
    this.deps.log.info('SidecarRuntime: connection opened', {
      event: 'switch_session_connection_open',
      sessionId,
      roomId,
      roomName,
      connectionId,
      startFrom: startCursor ?? 'head',
    });
    connection.start();
    return connectionId;
  }

  private sinkForSession(sessionId: string, ptyId: string | null): RoomConnectionDeps['sink'] {
    return {
      acquire: () => {
        const current = this.sessions.get(sessionId);
        if (!current?.target) return null;
        const target = current.target;
        const canType = ptyId === null || !this.deps.startupWatch.blocksInjection(ptyId);
        if (target.kind === 'tmux') {
          return new TmuxInjectionSink(
            target.tmuxTarget,
            this.deps.tmuxRun,
            () => this.deps.isTmuxPaneLive(target.tmuxTarget) && canType
          ).acquire();
        }
        return new HerdrInjectionSink(
          target.paneId,
          this.deps.herdrRun,
          () => {
            return (
              this.deps.isHerdrPaneLive(target.paneId) &&
              canType &&
              !this.deps.isHerdrPaneBlocked(target.paneId)
            );
          },
          {
            preferAgentPrompt: this.deps.preferHerdrAgentPrompt,
            promptTarget: () => this.deps.herdrPromptTarget(target.paneId),
          }
        ).acquire();
      },
    };
  }

  private recordSessionTarget(
    sessionId: string,
    providerId: string,
    target: SessionHostTarget,
    roomId: string | null
  ): void {
    this.deps.registry.record({ sessionId, roomId, providerId, target });
  }

  /**
   * Re-establish a room connection for a session restored from durable state.
   *
   * Without this a restarted sidecar would sit idle for every session it just
   * restored, resuming poll and injection only when that agent next posted a
   * hook — which an agent waiting on a room message never does. The room
   * membership itself is server-side and outlived the restart; only our
   * connection to it did not.
   */
  restoreSession(entry: {
    sessionId: string;
    roomId: string;
    providerId: string;
    target: SessionHostTarget;
  }): void {
    this.openConnection(
      entry.sessionId,
      entry.providerId,
      entry.roomId,
      null,
      undefined,
      entry.target
    );
  }

  setSessionTarget(sessionId: string, target: SessionHostTarget): void {
    const session = this.sessions.get(sessionId);
    if (!session) return;
    session.target = target;
    this.herdrStatuses.delete(sessionId);
    this.recordSessionTarget(sessionId, session.providerId, target, session.roomId);
  }

  onHerdrStatusChange(
    paneId: string,
    status: Parameters<ManagedConnection['onAgentStatusChange']>[0],
    detail?: string
  ): void {
    for (const [sessionId, session] of this.sessions) {
      if (session.target?.kind !== 'herdr' || session.target.paneId !== paneId) continue;
      const previous = this.herdrStatuses.get(sessionId);
      if (previous?.status === status && previous.detail === detail) continue;
      this.herdrStatuses.set(sessionId, { status, detail });
      session.connection.onAgentStatusChange(status, undefined, detail);
    }
  }

  /** The room a session is currently attending, or null if it has none. */
  roomIdForSession(sessionId: string): string | null {
    return this.sessions.get(sessionId)?.roomId ?? null;
  }

  /**
   * Drop one session's room connection: stop its RoomConnection (which ends the
   * poll + renew heartbeat that keeps the agent marked live) and forget it, so
   * `/sessions` no longer reports it. Called when Switch Console deletes the session.
   */
  /**
   * Report a spawned session as waiting on a human because it never started.
   *
   * Goes through the session's runtime state rather than a posted message: the
   * state report carries the session deeplink, and switch-core turns that into
   * the clickable "Open in Switch Console" line addressed to the agent's owner.
   * A `switchdash://` URL written into a message body gets no such treatment —
   * only `deeplink_url` on the report is rewritten — so it arrives as dead text.
   *
   * No-op for a session with no connection yet, which is the common case when a
   * pane dies before its first `connect_to_room`.
   */
  reportStartupStalled(sessionId: string): void {
    this.sessions
      .get(sessionId)
      ?.connection.onAgentStatusChange('awaiting-input', 'startup_prompt');
  }

  stopSession(sessionId: string): void {
    const session = this.sessions.get(sessionId);
    if (!session) return;
    session.connection.stop();
    this.herdrStatuses.delete(sessionId);
    if (session.ptyId) this.deps.startupWatch.end(session.ptyId);
    this.sessions.delete(sessionId);
    this.deps.registry.forget(sessionId);
    this.deps.log.debug('SidecarRuntime: session stopped', { sessionId });
  }

  /**
   * Reap connections whose backing host target disappeared since the last liveness poll.
   *
   * A dead pane must not remain in this map: RoomConnection would otherwise
   * keep its no-target retry timer alive forever, and the durable registry plus
   * watcher would continue to say that the room has a session. Return the
   * rooms before stopSession forgets the connection so the entrypoint can
   * release its corresponding spawn guard as well.
   */
  reapDeadSessions(): Array<{ sessionId: string; roomId: string | null }> {
    const dead: Array<{ sessionId: string; roomId: string | null }> = [];
    for (const [sessionId, session] of this.sessions) {
      if (this.isSessionLive(session)) continue;
      dead.push({ sessionId, roomId: session.roomId });
      this.stopSession(sessionId);
    }
    return dead;
  }

  /** Agent tmux targets the runtime is currently injecting into (for pane-liveness polling). */
  activeTmuxTargets(): string[] {
    return [...this.sessions.values()]
      .map((s) => (s.target?.kind === 'tmux' ? s.target.tmuxTarget : null))
      .filter((target): target is string => target !== null);
  }

  activeHerdrPaneIds(): string[] {
    return [...this.sessions.values()]
      .map((s) => (s.target?.kind === 'herdr' ? s.target.paneId : null))
      .filter((paneId): paneId is string => paneId !== null);
  }

  /**
   * Sessions the runtime has connected to a room, for Switch Console to reconcile
   * into its UI. Only panes that are still live are reported so a session whose
   * agent has exited does not surface as a ghost row — and only sessions whose
   * room the server has actually named, since a room-less one has nothing for
   * Switch Console to reconcile against.
   */
  connectedSessions(): Array<{ sessionId: string; roomId: string | null }> {
    const out: Array<{ sessionId: string; roomId: string | null }> = [];
    for (const [sessionId, session] of this.sessions) {
      if (!this.isSessionLive(session)) continue;
      if (session.roomId === null && !session.lostRoom) continue;
      out.push({ sessionId, roomId: session.roomId });
    }
    return out;
  }

  /** Whether this sidecar owns the session — i.e. it is one of this agent's own
   * sessions rather than another agent's pane on the same host. Backed by the
   * durable registry, so it is true from boot for a restored session instead of
   * only once that session next posts a hook. */
  hasSeen(sessionId: string): boolean {
    return this.deps.registry.has(sessionId);
  }

  /** True when a live session is attending the room and its pane is up (watcher gate). */
  hasLiveRoom(roomId: string): boolean {
    for (const session of this.sessions.values()) {
      if (session.roomId === roomId && this.isSessionLive(session)) return true;
    }
    return false;
  }

  stop(): void {
    for (const session of this.sessions.values()) {
      session.connection.stop();
      if (session.ptyId) this.deps.startupWatch.end(session.ptyId);
    }
    this.sessions.clear();
  }

  private isSessionLive(session: SessionConnection): boolean {
    if (!session.target) return false;
    if (session.target.kind === 'tmux') return this.deps.isTmuxPaneLive(session.target.tmuxTarget);
    return this.deps.isHerdrPaneLive(session.target.paneId);
  }
}
