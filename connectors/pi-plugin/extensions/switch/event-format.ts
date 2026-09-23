/**
 * Turns a `notifications/claude/channel` payload from the Switch agent
 * runtime into the text pi injects as a turn, and decides how to deliver it.
 *
 * The runtime already renders `content` as a human-readable line (sender,
 * body, missed-count and gap annotations included - see `emitNotification`
 * in `switch-agent-runtime/src/bin.ts`). This module only adds the `[Switch]`
 * framing and room id the room-workflow skill promises, plus attachment
 * paths carried in `meta`. It does not re-derive anything the runtime already
 * decided.
 */

export interface ChannelNotificationParams {
  content: string;
  meta: Record<string, string>;
}

/** Render a channel notification as the text delivered into the pi session. */
export function formatChannelEvent({ content, meta }: ChannelNotificationParams): string {
  const roomId = meta.room_id ?? 'unknown-room';

  const attachments: string[] = [];
  if (meta.image_path) attachments.push(`image_path=${meta.image_path}`);
  if (meta.file_path) attachments.push(`file_path=${meta.file_path}`);
  if (meta.failed_attachments) attachments.push(`failed_attachments=${meta.failed_attachments}`);
  const suffix = attachments.length > 0 ? ` (${attachments.join(', ')})` : '';

  return `[Switch] room ${roomId}: ${content}${suffix}`;
}

export type DeliveryMode = 'immediate' | 'steer';

/**
 * Whether to deliver a Switch event as a fresh turn or steer it into the
 * turn already in flight.
 *
 * `pi.sendUserMessage()` requires `deliverAs` while streaming and rejects it
 * otherwise, so the caller branches on this rather than always passing the
 * same options.
 */
export function chooseDeliveryMode(isIdle: boolean): DeliveryMode {
  return isIdle ? 'immediate' : 'steer';
}
