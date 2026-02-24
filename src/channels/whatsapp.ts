import { exec } from 'child_process';
import fs from 'fs';
import path from 'path';

import makeWASocket, {
  Browsers,
  DisconnectReason,
  WASocket,
  fetchLatestWaWebVersion,
  makeCacheableSignalKeyStore,
  useMultiFileAuthState,
} from '@whiskeysockets/baileys';

import { ASSISTANT_HAS_OWN_NUMBER, ASSISTANT_NAME, STORE_DIR } from '../config.js';
import {
  getLastGroupSync,
  setLastGroupSync,
  updateChatName,
} from '../db.js';
import { logger } from '../logger.js';
import { Channel, OnInboundMessage, OnChatMetadata, RegisteredGroup } from '../types.js';

// How often to sync WhatsApp group names to the database (24 hours in milliseconds)
const GROUP_SYNC_INTERVAL_MS = 24 * 60 * 60 * 1000;

// Interface defining the callbacks that index.ts provides to this channel
export interface WhatsAppChannelOpts {
  onMessage: OnInboundMessage; // Called when a new text/media message arrives
  onChatMetadata: OnChatMetadata; // Called when a group name/icon is discovered or changed
  registeredGroups: () => Record<string, RegisteredGroup>; // Function to get the live list of allowed VIP chats
}

// The main class that handles the WhatsApp connection, implementing the generic Channel interface
export class WhatsAppChannel implements Channel {
  name = 'whatsapp'; // Identifier for this channel

  private sock!: WASocket; // The actual Baileys WhatsApp socket connection
  private connected = false; // Tracks if we are currently connected to WhatsApp servers
  private lidToPhoneMap: Record<string, string> = {}; // Cache to map WhatsApp's internal LIDs to standard phone numbers
  private outgoingQueue: Array<{ jid: string; text: string }> = []; // Holds messages to send if WhatsApp disconnects
  private flushing = false; // Prevents overlapping runs when sending queued messages
  private groupSyncTimerStarted = false; // Ensures we only start the daily group name sync timer once

  private opts: WhatsAppChannelOpts; // Stores the callbacks passed in from index.ts

  // Constructor receives the callbacks from index.ts and saves them
  constructor(opts: WhatsAppChannelOpts) {
    this.opts = opts;
  }

  // Public method to start the connection process (returns a Promise so index.ts can await it)
  async connect(): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this.connectInternal(resolve).catch(reject);
    });
  }

  // The actual connection logic. Takes an optional callback for when the connection first opens
  private async connectInternal(onFirstOpen?: () => void): Promise<void> {
    // Determine where to save the WhatsApp login session files (keys, tokens)
    const authDir = path.join(STORE_DIR, 'auth');
    fs.mkdirSync(authDir, { recursive: true }); // Ensure the folder exists

    // Initialize Baileys' auth state manager (loads existing keys from disk or creates new ones)
    const { state, saveCreds } = await useMultiFileAuthState(authDir);

    // Fetch the latest WhatsApp Web version (falls back to default if unavailable)
    const { version } = await fetchLatestWaWebVersion({}).catch((err) => {
      logger.warn({ err }, 'Failed to fetch latest WA Web version, using default');
      return { version: undefined };
    });

    // Create the WhatsApp socket connection using the auth state
    this.sock = makeWASocket({
      version,
      auth: {
        creds: state.creds,
        // Wrap the keys in a cache to improve performance (Baileys recommendation)
        keys: makeCacheableSignalKeyStore(state.keys, logger),
      },
      printQRInTerminal: false, // We don't print the QR to the terminal directly; setup script handles it
      logger, // Use our Pino logger instead of the default console logger
      browser: Browsers.macOS('Chrome'), // Tell WhatsApp we are Chrome on a Mac (avoids some blocks)
    });

    // Listen for connection state changes (connecting, open, close, QR code ready)
    this.sock.ev.on('connection.update', (update) => {
      const { connection, lastDisconnect, qr } = update;

      // If WhatsApp gives us a QR code, it means we are logged out
      if (qr) {
        const msg =
          'WhatsApp authentication required. Run /setup in Claude Code.';
        logger.error(msg); // Log the error
        // Trigger a macOS desktop notification to alert the user
        exec(
          `osascript -e 'display notification "${msg}" with title "NanoClaw" sound name "Basso"'`,
        );
        // Exit the app because we can't do anything without authentication
        setTimeout(() => process.exit(1), 1000);
      }

      // If the connection dropped
      if (connection === 'close') {
        this.connected = false;
        // Get the error code from the disconnect reason
        const reason = (lastDisconnect?.error as { output?: { statusCode?: number } })?.output?.statusCode;
        // Should we reconnect? Yes, unless the user explicitly logged out via their phone
        const shouldReconnect = reason !== DisconnectReason.loggedOut;
        logger.info({ reason, shouldReconnect, queuedMessages: this.outgoingQueue.length }, 'Connection closed');

        if (shouldReconnect) {
          logger.info('Reconnecting...');
          // Try to connect again
          this.connectInternal().catch((err) => {
            logger.error({ err }, 'Failed to reconnect, retrying in 5s');
            // If the immediate reconnect fails, wait 5 seconds and try again
            setTimeout(() => {
              this.connectInternal().catch((err2) => {
                logger.error({ err: err2 }, 'Reconnection retry failed');
              });
            }, 5000);
          });
        } else {
          // The user tapped "Log out" on their phone linked devices screen
          logger.info('Logged out. Run /setup to re-authenticate.');
          process.exit(0); // App must die so user can run setup again
        }
      } else if (connection === 'open') {
        // Successfully connected to WhatsApp!
        this.connected = true;
        logger.info('Connected to WhatsApp');

        // Tell WhatsApp servers we are online so they send us typing indicators from others
        this.sock.sendPresenceUpdate('available').catch((err) => {
          logger.warn({ err }, 'Failed to send presence update');
        });

        // Build a mapping of internal IDs (LID) to phone numbers if this is a multi-device setup
        if (this.sock.user) {
          const phoneUser = this.sock.user.id.split(':')[0];
          const lidUser = this.sock.user.lid?.split(':')[0];
          if (lidUser && phoneUser) {
            this.lidToPhoneMap[lidUser] = `${phoneUser}@s.whatsapp.net`;
            logger.debug({ lidUser, phoneUser }, 'LID to phone mapping set');
          }
        }

        // If any messages failed to send while we were disconnected, send them now
        this.flushOutgoingQueue().catch((err) =>
          logger.error({ err }, 'Failed to flush outgoing queue'),
        );

        // Fetch the names of all the groups we are in right now
        this.syncGroupMetadata().catch((err) =>
          logger.error({ err }, 'Initial group sync failed'),
        );

        // Start a timer to re-fetch group names once every 24 hours
        if (!this.groupSyncTimerStarted) {
          this.groupSyncTimerStarted = true;
          setInterval(() => {
            this.syncGroupMetadata().catch((err) =>
              logger.error({ err }, 'Periodic group sync failed'),
            );
          }, GROUP_SYNC_INTERVAL_MS);
        }

        // If this was the very first connection, resolve the Promise from connect()
        if (onFirstOpen) {
          onFirstOpen();
          onFirstOpen = undefined;
        }
      }
    });

    // Save login credentials whenever WhatsApp rotates the security keys
    this.sock.ev.on('creds.update', saveCreds);

    // Listen for new incoming messages
    this.sock.ev.on('messages.upsert', async ({ messages }) => {
      for (const msg of messages) {
        if (!msg.message) continue; // Skip if it's an empty event
        const rawJid = msg.key.remoteJid;
        // Ignore status updates (WhatsApp Stories) or missing JIDs
        if (!rawJid || rawJid === 'status@broadcast') continue;

        // Convert weird LID IDs to normal phone numbers if needed
        const chatJid = await this.translateJid(rawJid);

        // Convert WhatsApp timestamp to standard ISO date string
        const timestamp = new Date(
          Number(msg.messageTimestamp) * 1000,
        ).toISOString();

        // Always notify index.ts that this chat exists, so it can record metadata (like isGroup)
        const isGroup = chatJid.endsWith('@g.us');
        this.opts.onChatMetadata(chatJid, timestamp, undefined, 'whatsapp', isGroup);

        // Ask index.ts: "Is this chat on the VIP list?"
        const groups = this.opts.registeredGroups();
        if (groups[chatJid]) {
          // It's a VIP group! Extract the actual text content from the message
          const content =
            msg.message?.conversation || // Standard text message
            msg.message?.extendedTextMessage?.text || // Message with preview link or reply
            msg.message?.imageMessage?.caption || // Text attached to an image
            msg.message?.videoMessage?.caption || // Text attached to a video
            '';

          // If there's no text (e.g. it was just a system message or unhandled media type), skip it
          if (!content) continue;

          // Figure out who sent it (the group member, or the person DMing us)
          const sender = msg.key.participant || msg.key.remoteJid || '';
          // Try to get their display name, fallback to their phone number
          const senderName = msg.pushName || sender.split('@')[0];

          // Did we (the bot's phone number) send this message?
          const fromMe = msg.key.fromMe || false;

          // Determine if this message was generated by the AI
          // If the bot has its own dedicated phone number, any message "fromMe" is from the bot.
          // If sharing a number, bot messages are prefixed with "Name: ", so check for that.
          const isBotMessage = ASSISTANT_HAS_OWN_NUMBER
            ? fromMe
            : content.startsWith(`${ASSISTANT_NAME}:`);

          // Send the fully parsed message back to index.ts so it can be saved in the DB
          this.opts.onMessage(chatJid, {
            id: msg.key.id || '',
            chat_jid: chatJid,
            sender,
            sender_name: senderName,
            content,
            timestamp,
            is_from_me: fromMe,
            is_bot_message: isBotMessage,
          });
        }
      }
    });
  }

  // Method called by index.ts to send a message out to WhatsApp
  async sendMessage(jid: string, text: string): Promise<void> {
    // Add the bot's name to the front of the message if we are sharing a phone number
    const prefixed = ASSISTANT_HAS_OWN_NUMBER
      ? text
      : `${ASSISTANT_NAME}: ${text}`;

    // If we've lost connection to WhatsApp, put the message in a waiting list
    if (!this.connected) {
      this.outgoingQueue.push({ jid, text: prefixed });
      logger.info({ jid, length: prefixed.length, queueSize: this.outgoingQueue.length }, 'WA disconnected, message queued');
      return;
    }

    try {
      // Actually send the text to WhatsApp
      await this.sock.sendMessage(jid, { text: prefixed });
      logger.info({ jid, length: prefixed.length }, 'Message sent');
    } catch (err) {
      // If sending fails (e.g., connection dropped mid-send), add it to the queue to try later
      this.outgoingQueue.push({ jid, text: prefixed });
      logger.warn({ jid, err, queueSize: this.outgoingQueue.length }, 'Failed to send, message queued');
    }
  }

  // Simple getter so index.ts knows if we are online
  isConnected(): boolean {
    return this.connected;
  }

  // Check if a given JID looks like a WhatsApp ID
  ownsJid(jid: string): boolean {
    return jid.endsWith('@g.us') || jid.endsWith('@s.whatsapp.net');
  }

  // Close the WhatsApp connection safely
  async disconnect(): Promise<void> {
    this.connected = false;
    this.sock?.end(undefined);
  }

  // Show the "typing..." indicator in WhatsApp
  async setTyping(jid: string, isTyping: boolean): Promise<void> {
    try {
      const status = isTyping ? 'composing' : 'paused';
      logger.debug({ jid, status }, 'Sending presence update');
      await this.sock.sendPresenceUpdate(status, jid);
    } catch (err) {
      logger.debug({ jid, err }, 'Failed to update typing status');
    }
  }

  /**
   * Sync group metadata from WhatsApp.
   * Fetches all participating groups and stores their names in the database.
   * Called on startup, daily, and on-demand via IPC.
   */
  async syncGroupMetadata(force = false): Promise<void> {
    // Unless forced, don't run if we already synced in the last 24 hours
    if (!force) {
      const lastSync = getLastGroupSync();
      if (lastSync) {
        const lastSyncTime = new Date(lastSync).getTime();
        if (Date.now() - lastSyncTime < GROUP_SYNC_INTERVAL_MS) {
          logger.debug({ lastSync }, 'Skipping group sync - synced recently');
          return;
        }
      }
    }

    try {
      logger.info('Syncing group metadata from WhatsApp...');
      // Ask WhatsApp servers for the list of every group we are a member of
      const groups = await this.sock.groupFetchAllParticipating();

      let count = 0;
      // Loop through the groups and save their names in our database
      for (const [jid, metadata] of Object.entries(groups)) {
        if (metadata.subject) { // subject is WhatsApp's term for Group Name
          updateChatName(jid, metadata.subject);
          count++;
        }
      }

      // Record the time so we don't sync again too soon
      setLastGroupSync();
      logger.info({ count }, 'Group metadata synced');
    } catch (err) {
      logger.error({ err }, 'Failed to sync group metadata');
    }
  }

  // Internal helper to handle weird WhatsApp multi-device IDs (LIDs)
  // Maps them back to normal phone number JIDs
  private async translateJid(jid: string): Promise<string> {
    if (!jid.endsWith('@lid')) return jid; // Only translate @lid addresses
    const lidUser = jid.split('@')[0].split(':')[0];

    // Check if we already looked up this LID before
    const cached = this.lidToPhoneMap[lidUser];
    if (cached) {
      logger.debug({ lidJid: jid, phoneJid: cached }, 'Translated LID to phone JID (cached)');
      return cached;
    }

    // Ask Baileys' internal database to resolve the LID to a phone number
    try {
      const pn = await this.sock.signalRepository?.lidMapping?.getPNForLID(jid);
      if (pn) {
        // Clean up the phone number format
        const phoneJid = `${pn.split('@')[0].split(':')[0]}@s.whatsapp.net`;
        this.lidToPhoneMap[lidUser] = phoneJid; // Cache it for next time
        logger.info({ lidJid: jid, phoneJid }, 'Translated LID to phone JID (signalRepository)');
        return phoneJid;
      }
    } catch (err) {
      logger.debug({ err, jid }, 'Failed to resolve LID via signalRepository');
    }

    return jid; // Fallback to returning the original LID if translation fails
  }

  // Sends out any messages that were queued up while offline
  private async flushOutgoingQueue(): Promise<void> {
    if (this.flushing || this.outgoingQueue.length === 0) return;
    this.flushing = true; // Lock so we don't run two flush loops at once
    try {
      logger.info({ count: this.outgoingQueue.length }, 'Flushing outgoing message queue');
      // Keep pulling the oldest message off the queue and sending it
      while (this.outgoingQueue.length > 0) {
        const item = this.outgoingQueue.shift()!;
        // Send directly — queued items are already prefixed by sendMessage
        await this.sock.sendMessage(item.jid, { text: item.text });
        logger.info({ jid: item.jid, length: item.text.length }, 'Queued message sent');
      }
    } finally {
      this.flushing = false; // Unlock
    }
  }
}
