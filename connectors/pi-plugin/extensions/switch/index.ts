/**
 * The Switch connector for pi (pi.dev).
 *
 * Spawns `@sandboxaq/switch-agent-runtime` as an MCP server, registers its
 * whole tool surface as native pi tools, and turns the runtime's inbound
 * room events into pi turns - the same role Switch Console (or, standalone,
 * nothing) plays for the other connectors' `[Switch] …` delivery. See
 * `mcp-bridge.ts` for why pi needs this instead of host-native MCP config.
 */

import type { ExtensionAPI } from '@earendil-works/pi-coding-agent';
import { chooseDeliveryMode, formatChannelEvent } from './event-format';
import { notifyRuntimeHook } from './hooks';
import { connectSwitchRuntime, registerSwitchTools } from './mcp-bridge';
import type { SwitchBridge } from './mcp-bridge';

export default function switchExtension(pi: ExtensionAPI) {
  let bridge: SwitchBridge | null = null;
  // Tracked from pi's own turn lifecycle rather than read from a captured
  // `ExtensionContext`: the channel notification handler fires outside any
  // event dispatch, so there is no ctx in scope there to call isIdle() on.
  let busy = false;

  pi.on('agent_start', () => {
    busy = true;
  });
  pi.on('agent_settled', () => {
    busy = false;
    if (bridge) void notifyRuntimeHook(bridge.sessionPid, '/turn-end');
  });

  pi.on('session_start', async (_event, ctx) => {
    try {
      bridge = await connectSwitchRuntime((content, meta) => {
        const text = formatChannelEvent({ content, meta });
        if (chooseDeliveryMode(!busy) === 'immediate') {
          pi.sendUserMessage(text);
        } else {
          pi.sendUserMessage(text, { deliverAs: 'steer' });
        }
      });
      await registerSwitchTools(pi, bridge);
    } catch (err) {
      bridge = null;
      ctx.ui.notify(`Switch connector failed to start: ${err instanceof Error ? err.message : err}`, 'error');
    }
  });

  pi.on('session_shutdown', async () => {
    if (!bridge) return;
    const current = bridge;
    bridge = null;
    await notifyRuntimeHook(current.sessionPid, '/disconnect');
    await current.close();
  });

  pi.registerCommand('switch', {
    description: 'Show the Switch connector status',
    handler: async (_args, ctx) => {
      if (!bridge) {
        ctx.ui.notify(
          'Switch connector: not connected. Check the pi log for a startup error, or restart the session.',
          'error'
        );
        return;
      }
      const { tools } = await bridge.client.listTools();
      ctx.ui.notify(
        `Switch connector: connected, ${tools.length} tool(s) registered ` +
          '(list_rooms / connect_to_room / select_agent tell you the rest - identity and room state ' +
          'live in the Switch server, not this command).',
        'info'
      );
    },
  });
}
