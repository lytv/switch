import { mkdtemp, rm } from 'node:fs/promises';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  createHerdrAgentStatusSubscription,
  HerdrAgentStatusSubscription,
} from './herdr-event-subscription';

const cleanup: Array<() => Promise<void>> = [];

afterEach(async () => {
  await Promise.all(cleanup.splice(0).map((remove) => remove()));
});

async function waitFor(check: () => void): Promise<void> {
  let lastError: unknown;
  for (let attempt = 0; attempt < 50; attempt += 1) {
    try {
      check();
      return;
    } catch (error) {
      lastError = error;
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
  }
  throw lastError;
}

describe('HerdrAgentStatusSubscription', () => {
  it('subscribes and forwards only an exact pane status event', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'herdr-events-'));
    const socketPath = join(directory, 'herdr.sock');
    const requests: unknown[] = [];
    const statuses: Array<{ paneId: string; status: string }> = [];
    const server = createServer((socket) => {
      socket.on('data', (chunk) => {
        requests.push(JSON.parse(chunk.toString()));
        socket.write(
          `${JSON.stringify({
            event: 'pane.agent_status_changed',
            data: { pane_id: 'other-pane', agent_status: 'working' },
          })}\n`
        );
        socket.write(
          `${JSON.stringify({
            event: 'pane.agent_status_changed',
            data: { pane_id: 'pane-1', agent_status: 'blocked' },
          })}\n`
        );
      });
    });
    await new Promise<void>((resolve, reject) => {
      server.once('error', reject);
      server.listen(socketPath, resolve);
    });
    cleanup.push(
      () =>
        new Promise<void>((resolve) => {
          server.close(() => {
            void rm(directory, { recursive: true, force: true }).then(resolve);
          });
        })
    );

    const subscription = new HerdrAgentStatusSubscription(socketPath, {
      onStatus: (event) => statuses.push(event),
      onAvailabilityChange: vi.fn(),
    });
    subscription.update(['pane-1']);

    await waitFor(() => expect(statuses).toEqual([{ paneId: 'pane-1', status: 'blocked' }]));
    expect(requests).toMatchObject([
      {
        method: 'events.subscribe',
        params: { subscriptions: [{ type: 'pane.agent_status_changed', pane_id: 'pane-1' }] },
      },
    ]);
    subscription.close();
  });

  it('keeps polling when protocol 16 events are unavailable', async () => {
    await expect(
      createHerdrAgentStatusSubscription(
        vi.fn(async () => ({ stdout: JSON.stringify({ client: { protocol: 15 } }), stderr: '' })),
        { onStatus: vi.fn(), onAvailabilityChange: vi.fn() }
      )
    ).resolves.toBeNull();
  });
});
