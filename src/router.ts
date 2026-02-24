/**
 * Router and formatting utilities.
 *
 * Responsibilities in this file:
 * - Convert DB message rows into the XML-ish prompt format sent to Claude.
 * - Escape user-provided text so XML structure cannot be broken.
 * - Remove internal reasoning tags before sending assistant output to users.
 * - Resolve which channel instance owns a given chat JID.
 */
import { Channel, NewMessage } from './types.js';

/**
 * Escape XML special characters in free-form text.
 *
 * This protects prompt structure when message content includes characters like
 * `&`, `<`, `>`, or `"`.
 */
export function escapeXml(s: string): string {
  if (!s) return '';
  return s
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

/**
 * Build the prompt payload consumed by the container agent.
 *
 * Output shape:
 * <messages>
 *   <message sender="..." time="...">...</message>
 *   ...
 * </messages>
 */
export function formatMessages(messages: NewMessage[]): string {
  const lines = messages.map((m) =>
    `<message sender="${escapeXml(m.sender_name)}" time="${m.timestamp}">${escapeXml(m.content)}</message>`,
  );
  return `<messages>\n${lines.join('\n')}\n</messages>`;
}

/**
 * Remove `<internal>...</internal>` blocks from model output.
 *
 * Agents may emit these tags for internal-only text that should not be shown
 * to end users.
 */
export function stripInternalTags(text: string): string {
  return text.replace(/<internal>[\s\S]*?<\/internal>/g, '').trim();
}

/**
 * Final outbound formatter before channel send.
 * Returns an empty string when nothing user-visible remains.
 */
export function formatOutbound(rawText: string): string {
  const text = stripInternalTags(rawText);
  if (!text) return '';
  return text;
}

/**
 * Send a message through the connected channel that owns this JID.
 * Throws when no connected owner channel exists.
 */
export function routeOutbound(
  channels: Channel[],
  jid: string,
  text: string,
): Promise<void> {
  const channel = channels.find((c) => c.ownsJid(jid) && c.isConnected());
  if (!channel) throw new Error(`No channel for JID: ${jid}`);
  return channel.sendMessage(jid, text);
}

/**
 * Find the channel that owns a JID, regardless of connection state.
 * Used by callers that may set typing indicators or perform conditional sends.
 */
export function findChannel(
  channels: Channel[],
  jid: string,
): Channel | undefined {
  return channels.find((c) => c.ownsJid(jid));
}
