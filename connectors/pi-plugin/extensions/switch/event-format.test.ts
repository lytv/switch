import { describe, expect, it } from 'vitest';
import { chooseDeliveryMode, formatChannelEvent } from './event-format';

describe('formatChannelEvent', () => {
  it('frames the runtime content with a [Switch] prefix and room id', () => {
    const text = formatChannelEvent({
      content: '[alice]: are you there?',
      meta: { room_id: '!room:example.org', event_type: 'message' },
    });

    expect(text).toBe('[Switch] room !room:example.org: [alice]: are you there?');
  });

  it('falls back to a placeholder room id when meta carries none', () => {
    const text = formatChannelEvent({ content: 'hello', meta: {} });

    expect(text).toBe('[Switch] room unknown-room: hello');
  });

  it('appends attachment paths from meta', () => {
    const text = formatChannelEvent({
      content: '[bob]: see attached',
      meta: {
        room_id: '!room:example.org',
        image_path: '/tmp/a.png,/tmp/b.png',
        file_path: '/tmp/report.pdf',
        failed_attachments: 'broken.zip',
      },
    });

    expect(text).toBe(
      '[Switch] room !room:example.org: [bob]: see attached ' +
        '(image_path=/tmp/a.png,/tmp/b.png, file_path=/tmp/report.pdf, failed_attachments=broken.zip)'
    );
  });

  it('adds no parenthetical when there are no attachments', () => {
    const text = formatChannelEvent({ content: 'plain', meta: { room_id: '!r' } });

    expect(text).not.toContain('(');
  });
});

describe('chooseDeliveryMode', () => {
  it('delivers immediately when pi is idle', () => {
    expect(chooseDeliveryMode(true)).toBe('immediate');
  });

  it('steers into the in-flight turn when pi is busy', () => {
    expect(chooseDeliveryMode(false)).toBe('steer');
  });
});
