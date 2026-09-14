import type { SessionHerdrTarget } from '@shared/core/sessions/herdr-target';
import { type Session } from '@shared/core/sessions/sessions';

/**
 * Runtime handle for the single agent process of one session (one session =
 * one agent run). Implementations own the PTY, the respawn
 * supervisor, and — for SSH — the sidecar relay for that one agent.
 */
export interface AgentRuntimeProvider {
  start(
    session: Session,
    initialSize?: { cols: number; rows: number },
    isResuming?: boolean,
    initialPrompt?: string
  ): Promise<void>;
  /**
   * Close the local PTY view of the agent (the `dehydrateSession` RPC).
   * On tmux the agent keeps running and stays re-attachable; otherwise the
   * agent is gone and respawn tracking is cleared.
   */
  dehydrate(): Promise<void>;
  /**
   * Detach at session teardown: kill the local PTY but keep re-attach
   * bookkeeping (tmux pane, sidecar, reconnect listener) intact.
   */
  detach(): Promise<void>;
  /** Stop the agent for good: kill the PTY and tmux pane, disconnect the sidecar. */
  stop(): Promise<void>;
  /** Terminate teardown: stop everything and release agent-scoped listeners. */
  destroy(): Promise<void>;
  /**
   * The exact Herdr workspace/tab/pane this session is running in, or `null`
   * when the session is not on the Herdr host, or the pane has not been
   * created yet. A read of already-known state — never creates a pane or
   * shells out as a side effect.
   */
  getHerdrTarget(): SessionHerdrTarget | null;
}
