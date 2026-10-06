import { type ChildProcess, spawn } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import * as http from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';

/**
 * The working mark on a bridged message.
 *
 * The collaboration bridge draws that mark only when something POSTs
 * `/agents/{id}/runtime-state` with the message id. Switch Console does this
 * for sessions it launches. A session started outside Console never did, so
 * the mark stayed absent even though the agent answered. These spawn the
 * built runtime against a local stand-in for the bridge and assert the posts.
 *
 * `bin.ts` reads its config at module scope and binds stdio, so it cannot be
 * imported. The built artifact is what a host actually runs.
 */

const PKG_ROOT = join(import.meta.dirname, '..');
const BIN = join(PKG_ROOT, 'dist', 'bin.mjs');

const DEADLINE_MS = 20_000;

const INITIALIZE = {
  jsonrpc: '2.0',
  id: 1,
  method: 'initialize',
  params: {
    protocolVersion: '2024-11-05',
    capabilities: {},
    clientInfo: { name: 'runtime-state-test', version: '0' },
  },
};

type Running = {
  child: ChildProcess;
  root: string;
  stdout: () => string;
  stderr: () => string;
};

type Recorded = { method: string; url: string; body: string; authorization: string };

type Bridge = {
  endpoint: string;
  recorded: () => Recorded[];
  push: (event: Record<string, unknown>) => void;
};

const running: ChildProcess[] = [];
const servers: http.Server[] = [];
const sandboxes: string[] = [];

afterEach(() => {
  for (const child of running.splice(0)) child.kill('SIGKILL');
  for (const server of servers.splice(0)) server.close();
  for (const dir of sandboxes.splice(0)) {
    try {
      rmSync(dir, { recursive: true, force: true });
    } catch {
      // A just-killed child may still hold a file open; the OS reclaims tmp.
    }
  }
});

function sandbox(): string {
  const dir = mkdtempSync(join(tmpdir(), 'switch-runtime-state-'));
  sandboxes.push(dir);
  return dir;
}

/**
 * A stand-in for the agent bridge: the operation list, one tool, an event
 * stream this test can write into, and a record of every other POST.
 */
async function bridge(
  opts: { failRuntimeState?: boolean; singleStream?: boolean } = {}
): Promise<Bridge> {
  const recorded: Recorded[] = [];
  let sse: http.ServerResponse | null = null;
  let sseHasRoom = false;
  const queued: string[] = [];
  const streamIsLive = (): boolean => opts.singleStream === true || sseHasRoom;

  const server = http.createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on('data', (chunk: Buffer) => chunks.push(chunk));
    req.on('end', () => {
      const url = req.url ?? '/';
      const body = Buffer.concat(chunks).toString();
      const path = url.split('?')[0] ?? url;
      recorded.push({
        method: req.method ?? 'GET',
        url,
        body,
        authorization: String(req.headers.authorization ?? ''),
      });

      if (req.method === 'GET' && path.endsWith('/ops')) {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(
          JSON.stringify({
            operations: {
              connect_to_room: {
                description: 'Connect to a room',
                input_schema: {
                  type: 'object',
                  properties: { room_id: { type: 'string' } },
                },
              },
            },
          })
        );
        return;
      }

      if (req.method === 'GET' && path.endsWith('/events')) {
        res.writeHead(200, {
          'content-type': 'text/event-stream',
          'cache-control': 'no-cache',
        });
        // The runtime opens a stream, then reopens it once the room is known.
        // Frames belong on the live one. A Console-managed session does not
        // reopen, so its single stream is the live one.
        sse = res;
        sseHasRoom = url.includes('rooms=');
        if (streamIsLive()) {
          for (const frame of queued.splice(0)) res.write(frame);
        }
        return;
      }

      if (req.method === 'POST' && path.endsWith('/runtime-state') && opts.failRuntimeState) {
        res.writeHead(500, { 'content-type': 'text/plain' });
        res.end('runtime state refused');
        return;
      }

      if (req.method === 'POST' && path.endsWith('/ops/connect_to_room')) {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ result: { ok: true } }));
        return;
      }

      res.writeHead(200, { 'content-type': 'application/json' });
      res.end('{"ok":true}');
    });
  });
  servers.push(server);
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as { port: number };

  return {
    endpoint: `http://127.0.0.1:${port}`,
    recorded: () => recorded,
    push(event) {
      const frame = `id: 1\nevent: message\ndata: ${JSON.stringify(event)}\n\n`;
      if (sse && streamIsLive()) sse.write(frame);
      else queued.push(frame);
    },
  };
}

function start(env: Record<string, string>): Running {
  const root = sandbox();
  const child = spawn(process.execPath, [BIN], {
    cwd: root,
    env: { PATH: process.env.PATH ?? '', HOME: root, ...env },
    stdio: ['pipe', 'pipe', 'pipe'],
  });
  running.push(child);

  let out = '';
  let err = '';
  child.stdout.on('data', (c: Buffer) => {
    out += c.toString();
  });
  child.stderr.on('data', (c: Buffer) => {
    err += c.toString();
  });
  return { child, root, stdout: () => out, stderr: () => err };
}

function responseWithin(run: Running, id: number, ms: number): Promise<Record<string, unknown>> {
  const seek = (): Record<string, unknown> | null => {
    for (const line of run.stdout().split('\n')) {
      if (!line.trim()) continue;
      try {
        const message = JSON.parse(line) as { id?: number };
        if (message.id === id) return message as Record<string, unknown>;
      } catch {
        // Partial line; wait for the rest.
      }
    }
    return null;
  };

  return new Promise((resolve, reject) => {
    const already = seek();
    if (already) return resolve(already);
    const timer = setTimeout(() => reject(new Error(`no response to ${id} within ${ms}ms`)), ms);
    run.child.stdout!.on('data', () => {
      const found = seek();
      if (found) {
        clearTimeout(timer);
        resolve(found);
      }
    });
  });
}

async function handshake(run: Running): Promise<void> {
  run.child.stdin!.write(`${JSON.stringify(INITIALIZE)}\n`);
  await responseWithin(run, 1, DEADLINE_MS);
  run.child.stdin!.write(
    `${JSON.stringify({ jsonrpc: '2.0', method: 'notifications/initialized' })}\n`
  );
}

async function request(
  run: Running,
  id: number,
  method: string,
  params: Record<string, unknown> = {}
): Promise<Record<string, unknown>> {
  run.child.stdin!.write(`${JSON.stringify({ jsonrpc: '2.0', id, method, params })}\n`);
  return responseWithin(run, id, DEADLINE_MS);
}

async function until(label: string, pred: () => boolean): Promise<void> {
  const start = Date.now();
  while (!pred()) {
    if (Date.now() - start > DEADLINE_MS) throw new Error(`timed out waiting for ${label}`);
    await new Promise((r) => setTimeout(r, 20));
  }
}

function runtimeStateCalls(bridgeServer: Bridge): Recorded[] {
  return bridgeServer
    .recorded()
    .filter((call) => call.method === 'POST' && call.url.split('?')[0]?.endsWith('/runtime-state'));
}

function runtimeStates(
  bridgeServer: Bridge
): { room_id: string; state: string; thread_id: string | null }[] {
  return runtimeStateCalls(bridgeServer).map(
    (call) => JSON.parse(call.body) as { room_id: string; state: string; thread_id: string | null }
  );
}

function typing(bridgeServer: Bridge): { room_id: string; is_typing: boolean }[] {
  return bridgeServer
    .recorded()
    .filter((call) => call.method === 'POST' && call.url.split('?')[0]?.endsWith('/typing'))
    .map((call) => JSON.parse(call.body) as { room_id: string; is_typing: boolean });
}

async function connect(
  run: Running,
  bridgeServer: Bridge,
  roomId: string,
  opts: { managed?: boolean } = {}
): Promise<void> {
  await handshake(run);
  const connected = await request(run, 2, 'tools/call', {
    name: 'connect_to_room',
    arguments: { room_id: roomId },
  });
  expect(connected.error).toBeUndefined();
  expect(connected.result).not.toMatchObject({ isError: true });
  // An unmanaged session reopens the stream once the room is known. That
  // second request carries `rooms`. A managed session keeps the first stream.
  await until('event stream', () =>
    bridgeServer.recorded().some((call) => {
      if (call.method !== 'GET' || !call.url.includes('/events')) return false;
      return opts.managed === true || call.url.includes('rooms=');
    })
  );
}

function messageEvent(
  roomId: string,
  payload: { addressed: boolean; message_id: string; thread_id?: string; body: string }
): Record<string, unknown> {
  return {
    type: 'message',
    room_id: roomId,
    bridge_id: null,
    channel_type: null,
    payload: {
      sender: '@human:example',
      sender_name: 'Human',
      timestamp: 1_700_000_000_000,
      ...payload,
    },
  };
}

/** The hook port the runtime publishes for the host that spawned it. */
async function hookPort(root: string): Promise<number> {
  const file = join(root, '.switch', 'sessions', String(process.pid), 'port');
  await until('hook port', () => {
    try {
      return readFileSync(file, 'utf8').trim().length > 0;
    } catch {
      return false;
    }
  });
  return Number(readFileSync(file, 'utf8').trim());
}

describe('runtime state for sessions Console does not manage', { timeout: 20_000 }, () => {
  it('reports working on an addressed message and idle when the turn ends', async () => {
    const bridgeServer = await bridge();
    const run = start({
      SWITCH_API_ENDPOINT: bridgeServer.endpoint,
      SWITCH_API_TOKEN: 'tok-runtime-state',
      SWITCH_AGENT_ID: 'agent-runtime-state',
    });
    await connect(run, bridgeServer, 'room-1');

    bridgeServer.push(
      messageEvent('room-1', {
        addressed: true,
        message_id: 'msg-9',
        thread_id: 'thread-3',
        body: 'please look',
      })
    );

    await until('working report', () =>
      runtimeStates(bridgeServer).some((report) => report.state === 'working')
    );
    const working = runtimeStateCalls(bridgeServer);
    expect(working).toHaveLength(1);
    expect(working[0]?.authorization).toBe('Bearer tok-runtime-state');
    expect(working[0]?.url.split('?')[0]).toBe('/agents/agent-runtime-state/runtime-state');
    expect(runtimeStates(bridgeServer)).toEqual([
      { room_id: 'room-1', state: 'working', thread_id: 'thread-3' },
    ]);
    expect(typing(bridgeServer)).toContainEqual({ room_id: 'room-1', is_typing: true });
    await until('notification', () => run.stdout().includes('please look'));

    const port = await hookPort(run.root);
    const turnEnd = await fetch(`http://127.0.0.1:${port}/turn-end`, { method: 'POST' });
    expect(turnEnd.status).toBe(200);

    await until('idle report', () =>
      runtimeStates(bridgeServer).some((report) => report.state === 'idle')
    );
    expect(runtimeStates(bridgeServer).at(-1)).toEqual({
      room_id: 'room-1',
      state: 'idle',
      thread_id: 'thread-3',
    });
    expect(typing(bridgeServer)).toContainEqual({ room_id: 'room-1', is_typing: false });
  });

  it('uses the message id when the addressed message has no thread', async () => {
    const bridgeServer = await bridge();
    const run = start({
      SWITCH_API_ENDPOINT: bridgeServer.endpoint,
      SWITCH_API_TOKEN: 'tok-runtime-state',
      SWITCH_AGENT_ID: 'agent-runtime-state',
    });
    await connect(run, bridgeServer, 'room-1');

    bridgeServer.push(
      messageEvent('room-1', {
        addressed: true,
        message_id: 'msg-root',
        body: 'at the root',
      })
    );

    await until('working report', () => runtimeStates(bridgeServer).length > 0);
    expect(runtimeStates(bridgeServer)[0]).toEqual({
      room_id: 'room-1',
      state: 'working',
      thread_id: 'msg-root',
    });
  });

  it('does not report working for an unaddressed message', async () => {
    const bridgeServer = await bridge();
    const run = start({
      SWITCH_API_ENDPOINT: bridgeServer.endpoint,
      SWITCH_API_TOKEN: 'tok-runtime-state',
      SWITCH_AGENT_ID: 'agent-runtime-state',
    });
    await connect(run, bridgeServer, 'room-1');

    bridgeServer.push(
      messageEvent('room-1', {
        addressed: false,
        message_id: 'msg-chatter',
        body: 'just chatter',
      })
    );

    bridgeServer.push(
      messageEvent('room-1', {
        addressed: true,
        message_id: 'msg-real',
        thread_id: 'thread-real',
        body: 'this one counts',
      })
    );

    await until('working report', () => runtimeStates(bridgeServer).length > 0);
    expect(runtimeStates(bridgeServer)).toEqual([
      { room_id: 'room-1', state: 'working', thread_id: 'thread-real' },
    ]);
  });

  it('does not report runtime state when Switch Console manages the session', async () => {
    const bridgeServer = await bridge({ singleStream: true });
    const run = start({
      SWITCH_API_ENDPOINT: bridgeServer.endpoint,
      SWITCH_API_TOKEN: 'tok-runtime-state',
      SWITCH_AGENT_ID: 'agent-runtime-state',
      SWITCH_CHANNEL_DISABLE_POLL: '1',
    });
    await connect(run, bridgeServer, 'room-1', { managed: true });

    bridgeServer.push(
      messageEvent('room-1', {
        addressed: true,
        message_id: 'msg-9',
        thread_id: 'thread-3',
        body: 'console owns this',
      })
    );

    await until('event consumed', () =>
      bridgeServer.recorded().some((call) => {
        if (call.method !== 'POST' || !call.url.includes('/connection/beat')) return false;
        const beat = JSON.parse(call.body) as { cursor?: number };
        return (beat.cursor ?? 0) >= 1;
      })
    );

    const port = await hookPort(run.root);
    const turnEnd = await fetch(`http://127.0.0.1:${port}/turn-end`, { method: 'POST' });
    expect(turnEnd.status).toBe(200);
    await until('typing cleared', () =>
      typing(bridgeServer).some((report) => report.is_typing === false)
    );
    expect(runtimeStates(bridgeServer)).toEqual([]);
    expect(run.stdout()).not.toContain('console owns this');
  });

  it('still delivers the event when the runtime-state report fails', async () => {
    const bridgeServer = await bridge({ failRuntimeState: true });
    const run = start({
      SWITCH_API_ENDPOINT: bridgeServer.endpoint,
      SWITCH_API_TOKEN: 'tok-runtime-state',
      SWITCH_AGENT_ID: 'agent-runtime-state',
    });
    await connect(run, bridgeServer, 'room-1');

    bridgeServer.push(
      messageEvent('room-1', {
        addressed: true,
        message_id: 'msg-9',
        thread_id: 'thread-3',
        body: 'deliver me anyway',
      })
    );

    await until('failure logged', () => run.stderr().includes('set runtime state failed'));
    await until('notification', () => run.stdout().includes('deliver me anyway'));
    expect(run.child.exitCode).toBeNull();

    const port = await hookPort(run.root);
    const turnEnd = await fetch(`http://127.0.0.1:${port}/turn-end`, { method: 'POST' });
    expect(turnEnd.status).toBe(200);
  });
});
