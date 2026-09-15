/**
 * The exact Herdr workspace/tab/pane a session is running in. Always the
 * literal ids Herdr returned when the pane was created — never a label or
 * prefix. On Herdr 0.9+, attach with `herdr agent attach <agentName>`.
 */
export type SessionHerdrTarget = {
  workspaceId: string;
  tabId: string;
  paneId: string;
  /** Herdr agent name for attach/prompt. Required for new launches. */
  agentName?: string;
};
