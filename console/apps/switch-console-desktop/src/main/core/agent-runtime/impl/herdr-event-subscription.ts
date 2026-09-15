import { randomUUID } from 'node:crypto';
import { createConnection, type Socket } from 'node:net';
import { resolveHerdrBin, type HerdrExec } from './herdr-session-host';

const EVENT_SUBSCRIPTION_PROTOCOL_MIN = 16;

export type HerdrAgentStatusEvent = {
  paneId: string;
  status: string;
};

type HerdrEndpoint = {
  protocol: number;
  socketPath: string | null;
};

function readEndpoint(stdout: string): HerdrEndpoint {
  const parsed = JSON.parse(stdout) as {
    client?: { protocol?: unknown };
    server?: { socket?: unknown };
  };
  const protocol = parsed.client?.protocol;
  if (typeof protocol !== 'number' || !Number.isInteger(protocol)) {
    throw new Error('herdr status did not include client.protocol');
  }
  const socket = parsed.server?.socket;
  return { protocol, socketPath: typeof socket === 'string' && socket ? socket : null };
}

export async function createHerdrAgentStatusSubscription(
  exec: HerdrExec,
  handlers: {
    onStatus: (event: HerdrAgentStatusEvent) => void;
    onAvailabilityChange: (available: boolean) => void;
  }
): Promise<HerdrAgentStatusSubscription | null> {
  const { stdout } = await exec(resolveHerdrBin(), ['status', '--json']);
  const endpoint = readEndpoint(stdout);
  if (endpoint.protocol < EVENT_SUBSCRIPTION_PROTOCOL_MIN || !endpoint.socketPath) return null;
  return new HerdrAgentStatusSubscription(endpoint.socketPath, handlers);
}

/**
 * Adapts Herdr's optional protocol-16 event stream. Switch Console keeps its
 * poll path as the fallback and receives only exact pane status changes here.
 */
export class HerdrAgentStatusSubscription {
  private socket: Socket | null = null;
  private paneIds: string[] = [];
  private available = false;
  private buffer = '';

  constructor(
    private readonly socketPath: string,
    private readonly handlers: {
      onStatus: (event: HerdrAgentStatusEvent) => void;
      onAvailabilityChange: (available: boolean) => void;
    }
  ) {}

  update(paneIds: readonly string[]): void {
    const next = [...new Set(paneIds)].sort();
    if (next.join('\n') === this.paneIds.join('\n') && this.socket) return;
    this.close();
    this.paneIds = next;
    if (!next.length) return;

    const socket = createConnection(this.socketPath);
    this.socket = socket;
    socket.setEncoding('utf8');
    socket.on('connect', () => {
      if (this.socket !== socket) return;
      socket.write(
        `${JSON.stringify({
          id: randomUUID(),
          method: 'events.subscribe',
          params: {
            subscriptions: this.paneIds.map((paneId) => ({
              type: 'pane.agent_status_changed',
              pane_id: paneId,
            })),
          },
        })}\n`
      );
      this.setAvailability(true);
    });
    socket.on('data', (chunk: string) => this.onData(socket, chunk));
    socket.on('error', () => this.fail(socket));
    socket.on('close', () => this.fail(socket));
  }

  isActiveFor(paneId: string): boolean {
    return this.available && this.paneIds.includes(paneId);
  }

  close(): void {
    const socket = this.socket;
    this.socket = null;
    this.buffer = '';
    this.setAvailability(false);
    socket?.destroy();
  }

  private onData(socket: Socket, chunk: string): void {
    if (this.socket !== socket) return;
    this.buffer += chunk;
    let newline = this.buffer.indexOf('\n');
    while (newline >= 0) {
      const line = this.buffer.slice(0, newline);
      this.buffer = this.buffer.slice(newline + 1);
      this.onLine(socket, line);
      newline = this.buffer.indexOf('\n');
    }
  }

  private onLine(socket: Socket, line: string): void {
    if (!line.trim()) return;
    let message: unknown;
    try {
      message = JSON.parse(line);
    } catch {
      this.fail(socket);
      return;
    }
    if (!message || typeof message !== 'object') return;
    const record = message as Record<string, unknown>;
    if (record.error || record.ok === false || record.success === false) {
      this.fail(socket);
      return;
    }
    if (record.event !== 'pane.agent_status_changed') return;
    const data = record.data;
    if (!data || typeof data !== 'object') return;
    const paneId = (data as Record<string, unknown>).pane_id;
    const status = (data as Record<string, unknown>).agent_status;
    if (
      typeof paneId !== 'string' ||
      !this.paneIds.includes(paneId) ||
      typeof status !== 'string'
    ) {
      return;
    }
    this.handlers.onStatus({ paneId, status });
  }

  private fail(socket: Socket): void {
    if (this.socket !== socket) return;
    this.socket = null;
    this.buffer = '';
    this.setAvailability(false);
    socket.destroy();
  }

  private setAvailability(available: boolean): void {
    if (this.available === available) return;
    this.available = available;
    this.handlers.onAvailabilityChange(available);
  }
}
