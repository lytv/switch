/**
 * The exact Herdr workspace/tab/pane a session is running in. Always the
 * literal ids Herdr returned when the pane was created — never a label or
 * prefix — so a caller can reattach with `herdr pane attach --pane <paneId>`
 * without guessing which pane among several matches a name.
 */
export type SessionHerdrTarget = {
  workspaceId: string;
  tabId: string;
  paneId: string;
};
