import { describe, expect, it, vi } from 'vitest';
import { HerdrInjectionSink } from './herdr-injection-sink';

const PASTE = '\u001b[200~line 1\nline 2\u001b[201~\r';

describe('HerdrInjectionSink', () => {
  it('acquires itself while the pane is injectable', () => {
    const sink = new HerdrInjectionSink('pane-1', vi.fn(), () => true, {
      preferAgentPrompt: true,
      promptTarget: () => null,
    });
    expect(sink.acquire()).toBe(sink);
  });

  it('returns null while the pane is blocked or gone', () => {
    const sink = new HerdrInjectionSink('pane-1', vi.fn(), () => false, {
      preferAgentPrompt: true,
      promptTarget: () => null,
    });
    expect(sink.acquire()).toBeNull();
  });

  it('prefers agent prompt for bracketed-paste payloads when an agent target exists', () => {
    const run = vi.fn();
    const sink = new HerdrInjectionSink('pane-1', run, () => true, {
      preferAgentPrompt: true,
      promptTarget: () => ({ agentId: 'agent-9' }),
    });

    sink.write(PASTE);

    expect(run).toHaveBeenCalledWith(['agent', 'prompt', 'agent-9', 'line 1\nline 2', '--wait']);
  });

  it('falls back to pane send-text when no prompt target is available', () => {
    const run = vi.fn();
    const sink = new HerdrInjectionSink('pane-1', run, () => true, {
      preferAgentPrompt: true,
      promptTarget: () => null,
    });

    sink.write(PASTE);

    expect(run).toHaveBeenCalledWith(['pane', 'send-text', 'pane-1', PASTE]);
  });

  it('does not send an extra Enter after agent prompt accepts the payload', () => {
    const run = vi.fn();
    const sink = new HerdrInjectionSink('pane-1', run, () => true, {
      preferAgentPrompt: true,
      promptTarget: () => ({ agentId: 'agent-9' }),
    });

    sink.write(PASTE);
    sink.write('\r');

    expect(run).toHaveBeenCalledTimes(1);
  });
});
