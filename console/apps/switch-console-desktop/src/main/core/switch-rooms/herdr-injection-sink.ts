import { execFile } from 'node:child_process';
import type { InjectionSink, InjectionTarget } from './injection-sink';
import type { RoomConnectionLogger } from './room-connection';

export type HerdrRun = (args: string[]) => void;

export function createHerdrRun(log: RoomConnectionLogger, herdrBin = 'herdr'): HerdrRun {
  return (args) => {
    execFile(herdrBin, args, (error) => {
      if (error) {
        log.warn('herdr: command failed', { args, error: String(error) });
      }
    });
  };
}

const BRACKET_PASTE_START = '\u001b[200~';
const BRACKET_PASTE_END = '\u001b[201~';

function asPromptText(payload: string): string | null {
  if (payload.startsWith(BRACKET_PASTE_START) && payload.endsWith(`${BRACKET_PASTE_END}\r`)) {
    return payload.slice(BRACKET_PASTE_START.length, -`${BRACKET_PASTE_END}\r`.length);
  }
  return null;
}

export interface HerdrPromptTarget {
  agentId: string;
}

export class HerdrInjectionSink implements InjectionSink, InjectionTarget {
  private skipNextSubmit = false;

  constructor(
    private readonly paneId: string,
    private readonly run: HerdrRun,
    private readonly isInjectable: () => boolean,
    private readonly options: {
      preferAgentPrompt: boolean;
      promptTarget: () => HerdrPromptTarget | null;
    }
  ) {}

  acquire(): InjectionTarget | null {
    return this.isInjectable() ? this : null;
  }

  write(data: string): void {
    if (this.skipNextSubmit && data === '\r') {
      this.skipNextSubmit = false;
      return;
    }
    const asPrompt = asPromptText(data);
    const target =
      this.options.preferAgentPrompt && asPrompt !== null ? this.options.promptTarget() : null;
    if (target && asPrompt !== null) {
      this.run(['agent', 'prompt', target.agentId, asPrompt, '--wait']);
      this.skipNextSubmit = true;
      return;
    }
    this.run(['pane', 'send-text', this.paneId, data]);
  }
}
