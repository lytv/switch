import * as fs from 'node:fs/promises';
import * as http from 'node:http';
import { tmpdir } from 'node:os';
import * as path from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import {
  hookPathForTool,
  hookPortFilePath,
  notifyRuntimeHook,
  postHook,
  readHookPort,
} from './hooks';

describe('hookPathForTool', () => {
  it('maps read_context, assume_role and release_role to their runtime routes', () => {
    expect(hookPathForTool('read_context')).toBe('/read-context');
    expect(hookPathForTool('assume_role')).toBe('/assume-role');
    expect(hookPathForTool('release_role')).toBe('/release-role');
  });

  it('has no hook for tools the runtime already tracks itself, like connect_to_room', () => {
    expect(hookPathForTool('connect_to_room')).toBeNull();
  });

  it('has no hook for an unrelated tool', () => {
    expect(hookPathForTool('post_message')).toBeNull();
  });
});

describe('hookPortFilePath', () => {
  it('is keyed by the given session pid under <home>/.switch/sessions', () => {
    expect(hookPortFilePath(4242, '/home/u')).toBe('/home/u/.switch/sessions/4242/port');
  });
});

const roots: string[] = [];

afterEach(() => {
  for (const dir of roots.splice(0)) fs.rm(dir, { recursive: true, force: true });
});

async function tmpHome(): Promise<string> {
  const dir = await fs.mkdtemp(path.join(tmpdir(), 'pi-switch-hooks-test-'));
  roots.push(dir);
  return dir;
}

describe('readHookPort', () => {
  it('returns null when no port file has been published', async () => {
    const home = await tmpHome();
    expect(await readHookPort(1234, home)).toBeNull();
  });

  it('reads back a published port', async () => {
    const home = await tmpHome();
    const dir = path.join(home, '.switch', 'sessions', '1234');
    await fs.mkdir(dir, { recursive: true });
    await fs.writeFile(path.join(dir, 'port'), '54321');

    expect(await readHookPort(1234, home)).toBe(54321);
  });

  it('returns null for a garbage port file rather than throwing', async () => {
    const home = await tmpHome();
    const dir = path.join(home, '.switch', 'sessions', '1234');
    await fs.mkdir(dir, { recursive: true });
    await fs.writeFile(path.join(dir, 'port'), 'not-a-number');

    expect(await readHookPort(1234, home)).toBeNull();
  });
});

/** A tiny HTTP server that records every request it receives. */
function startRecordingServer(): Promise<{
  port: number;
  requests: { method: string; path: string; body: string }[];
  close(): Promise<void>;
}> {
  const requests: { method: string; path: string; body: string }[] = [];
  const server = http.createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on('data', (chunk) => chunks.push(chunk));
    req.on('end', () => {
      requests.push({
        method: req.method ?? '',
        path: req.url ?? '',
        body: Buffer.concat(chunks).toString('utf8'),
      });
      res.writeHead(200);
      res.end('ok');
    });
  });
  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => {
      const address = server.address();
      const port = typeof address === 'object' && address !== null ? address.port : 0;
      resolve({
        port,
        requests,
        close: () => new Promise((r) => server.close(() => r())),
      });
    });
  });
}

describe('postHook', () => {
  it('POSTs the given path and JSON body to the local hook listener', async () => {
    const server = await startRecordingServer();
    try {
      await postHook(server.port, '/read-context', { foo: 'bar' });

      expect(server.requests).toEqual([
        { method: 'POST', path: '/read-context', body: JSON.stringify({ foo: 'bar' }) },
      ]);
    } finally {
      await server.close();
    }
  });

  it('does not throw when nothing is listening on the port', async () => {
    await expect(postHook(1, '/read-context')).resolves.toBeUndefined();
  });
});

describe('notifyRuntimeHook', () => {
  it('is a silent no-op when the runtime has not published a port', async () => {
    const home = await tmpHome();
    await expect(notifyRuntimeHook(999, '/read-context', {}, home)).resolves.toBeUndefined();
  });

  it('resolves the published port and posts to it', async () => {
    const server = await startRecordingServer();
    try {
      const home = await tmpHome();
      const dir = path.join(home, '.switch', 'sessions', '555');
      await fs.mkdir(dir, { recursive: true });
      await fs.writeFile(path.join(dir, 'port'), String(server.port));

      await notifyRuntimeHook(555, '/assume-role', {}, home);

      expect(server.requests).toEqual([{ method: 'POST', path: '/assume-role', body: '{}' }]);
    } finally {
      await server.close();
    }
  });
});
