import { useQuery } from '@tanstack/react-query';
import { rpc } from '@renderer/lib/ipc';

/**
 * A session's exact Herdr workspace/tab/pane, or `null` when it isn't one.
 * Read-only and cheap (no shell-out on the main side), but a session's target
 * is only known once its agent has actually started, so this can flip from
 * `null` to a target after the session's terminal first opens — polled at a
 * human scale rather than fetched once, for that reason.
 */
export function useHerdrTarget(sessionId: string) {
  return useQuery({
    queryKey: ['herdr-target', sessionId],
    queryFn: () => rpc.sessions.getHerdrTarget(sessionId),
    refetchInterval: 15_000,
  });
}
