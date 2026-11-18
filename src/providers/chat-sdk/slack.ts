import { createHash } from 'node:crypto';
import { SocketModeClient, LogLevel } from '@slack/socket-mode';
import {
  WebClient,
  WebAPIHTTPError,
  WebAPIRequestError,
  WebAPIPlatformError,
  WebAPIRateLimitedError,
} from '@slack/web-api';
import { get as emojiForName, which as nameForEmoji } from 'node-emoji';
import type { Pool } from 'pg';
import { z } from 'zod';
import type {
  InboundMessage,
  InboundReaction,
  MessagingAttachment,
  MessagingChannel,
  MessagingTransport,
} from '../../ports/messaging.ts';
import {
  MessagingAttachmentError,
  MessagingConnectionError,
  MessagingDeliveryError,
} from '../../ports/messaging.ts';
import { isMessageReaction } from '../../shared/emoji.ts';
import { slackMessages } from './format.ts';

const id = /^[A-Z][A-Z0-9]{1,63}$/;
const timestamp = /^\d{10,16}\.\d{6}$/;
const fileLimit = 20 * 1024 * 1024;
const emojiAliases: Record<string, string> = {
  thinking_face: 'thinking',
  hugging_face: 'hugs',
  face_with_rolling_eyes: 'roll_eyes',
};

function reactionEmoji(name: string) {
  const parsed = /^([a-z0-9_+-]+)(?:::skin-tone-([2-6]))?$/.exec(name);
  if (!parsed) return undefined;
  const base = emojiForName(
    parsed[1] === 'thumbsup'
      ? '+1'
      : parsed[1] === 'thumbsdown'
        ? '-1'
        : (emojiAliases[parsed[1]] ?? parsed[1]),
  );
  return base && base + (parsed[2] ? String.fromCodePoint(0x1f3f9 + Number(parsed[2])) : '');
}

function reactionName(emoji: string) {
  const base = nameForEmoji(emoji);
  if (!base) return undefined;
  const modifier = /\p{Emoji_Modifier}$/u.exec(emoji)?.[0];
  const name =
    (Object.keys(emojiAliases).find((name) => emojiAliases[name] === base) ?? base) +
    (modifier ? `::skin-tone-${modifier.codePointAt(0)! - 0x1f3f9}` : '');
  return reactionEmoji(name)?.replaceAll('\uFE0F', '') === emoji.replaceAll('\uFE0F', '')
    ? name
    : undefined;
}
const logger = {
  debug() {},
  info() {},
  warn() {},
  error() {},
  setLevel() {},
  getLevel: () => LogLevel.ERROR,
  setName() {},
};
export const slackInput = z
  .object({
    botToken: z
      .string()
      .trim()
      .regex(/^xoxb-[A-Za-z0-9-]+$/)
      .max(4096),
    appToken: z
      .string()
      .trim()
      .regex(/^xapp-[A-Za-z0-9-]+$/)
      .max(4096),
    teamId: z
      .string()
      .trim()
      .regex(/^T[A-Z0-9]{1,63}$/),
    allowedUserId: z
      .string()
      .trim()
      .regex(/^[UW][A-Z0-9]{1,63}$/),
  })
  .strict();

function failure(error: unknown) {
  if (error instanceof MessagingConnectionError) return error;
  const reason =
    error instanceof WebAPIPlatformError && /^[a-z_]{1,80}$/.test(error.data.error)
      ? error.data.error
      : error instanceof WebAPIHTTPError
        ? `HTTP ${error.statusCode}`
        : error instanceof WebAPIRateLimitedError
          ? `rate_limited (retry after ${Math.min(86400, Math.max(0, error.retryAfter))} seconds)`
          : error instanceof WebAPIRequestError
            ? 'network_request_failed'
            : 'connection_failed';
  return new MessagingConnectionError(`Slack failed: ${reason}.`);
}

function client(token: string, signal: AbortSignal, request: typeof fetch = fetch) {
  return new WebClient(token, {
    logger,
    retryConfig: { retries: 0 },
    rejectRateLimitedCalls: true,
    timeout: 15_000,
    fetch: (url, init) =>
      request(url, {
        ...init,
        redirect: 'error',
        signal: AbortSignal.any([signal, ...(init?.signal ? [init.signal] : [])]),
      }),
  });
}

class SlackSocket extends SocketModeClient {
  constructor(
    options: ConstructorParameters<typeof SocketModeClient>[0],
    private appId: string,
  ) {
    super(options);
  }

  override async start() {
    try {
      return await super.start();
    } catch (error) {
      const cause = failure(error);
      await this.disconnect().catch(() => {});
      this.emit('nod_start_failed', cause);
      return { ok: false };
    }
  }

  protected override async onWebSocketMessage(
    ...args: Parameters<SocketModeClient['onWebSocketMessage']>
  ) {
    try {
      const value = JSON.parse(
        typeof args[0] === 'string' ? args[0] : new TextDecoder().decode(args[0]),
      );
      if (value.type === 'hello' && value.connection_info?.app_id !== this.appId) {
        this.emit('nod_identity_failed');
        await this.disconnect();
        return;
      }
    } catch {
      return;
    }
    try {
      await super.onWebSocketMessage(...args);
    } catch (error) {
      await this.disconnect().catch(() => {});
      this.emit('nod_start_failed', failure(error));
    }
  }
}

function socket(
  appToken: string,
  appId: string,
  signal: AbortSignal,
  request: typeof fetch = fetch,
) {
  return new SlackSocket(
    {
      appToken,
      logger,
      autoReconnectEnabled: false,
      clientOptions: {
        retryConfig: { retries: 0 },
        rejectRateLimitedCalls: true,
        timeout: 15_000,
        fetch: (url, init) =>
          request(url, {
            ...init,
            redirect: 'error',
            signal: AbortSignal.any([signal, ...(init?.signal ? [init.signal] : [])]),
          }),
      },
    },
    appId,
  );
}

async function connect(connection: SlackSocket, signal: AbortSignal) {
  let cleanup = () => {};
  const connected = new Promise<void>((resolve, reject) => {
    const failed = (error: Error) => reject(error);
    const identityFailed = () =>
      reject(new MessagingConnectionError('Slack failed: app_identity_mismatch.'));
    const aborted = () =>
      reject(new MessagingConnectionError('Slack failed: connection_cancelled.'));
    connection.once('connected', resolve);
    connection.once('nod_start_failed', failed);
    connection.once('nod_identity_failed', identityFailed);
    signal.addEventListener('abort', aborted, { once: true });
    cleanup = () => {
      connection.removeListener('connected', resolve);
      connection.removeListener('nod_start_failed', failed);
      connection.removeListener('nod_identity_failed', identityFailed);
      signal.removeEventListener('abort', aborted);
    };
    if (signal.aborted) aborted();
  });
  try {
    await Promise.all([connection.start(), connected]);
  } finally {
    cleanup();
  }
}

async function identity(web: WebClient, input: z.infer<typeof slackInput>) {
  const me = await web.auth.test();
  if (me.team_id !== input.teamId || !me.bot_id || !me.user_id)
    throw new MessagingConnectionError('Slack failed: workspace_or_bot_mismatch.');
  const bot = await web.bots.info({ bot: me.bot_id });
  const appId = bot.bot?.app_id;
  if (!appId || !id.test(appId))
    throw new MessagingConnectionError('Slack failed: bot_identity_unavailable.');
  const owner = (await web.users.info({ user: input.allowedUserId })).user;
  if (
    !owner ||
    owner.id !== input.allowedUserId ||
    owner.team_id !== input.teamId ||
    owner.is_bot ||
    owner.is_app_user ||
    owner.deleted ||
    owner.id === me.user_id
  )
    throw new MessagingConnectionError('Slack failed: owner_mismatch.');
  const dm = (await web.conversations.open({ users: input.allowedUserId, return_im: true }))
    .channel;
  if (!dm?.id || !/^D[A-Z0-9]{1,63}$/.test(dm.id))
    throw new MessagingConnectionError('Slack failed: direct_message_unavailable.');
  const info = (await web.conversations.info({ channel: dm.id })).channel;
  if (
    !info?.is_im ||
    info.is_mpim ||
    info.is_shared ||
    info.is_ext_shared ||
    z.object({ user: z.literal(input.allowedUserId) }).safeParse(info).success === false
  )
    throw new MessagingConnectionError('Slack failed: direct_message_mismatch.');
  return { appId, botUserId: me.user_id, dmId: dm.id };
}

export async function verifySlack(input: unknown, request: typeof fetch = fetch) {
  const value = slackInput.parse(input);
  const signal = AbortSignal.timeout(20_000);
  let connection: SlackSocket | undefined;
  try {
    const verified = await identity(client(value.botToken, signal, request), value);
    connection = socket(value.appToken, verified.appId, signal, request);
    connection.on('error', () => {});
    await connect(connection, signal);
    return {
      environment: {
        SLACK_ENABLED: 'true',
        SLACK_BOT_TOKEN: value.botToken,
        SLACK_APP_TOKEN: value.appToken,
        SLACK_TEAM_ID: value.teamId,
        SLACK_ALLOWED_USER_ID: value.allowedUserId,
      },
      summary: {
        provider: 'slack' as const,
        enabled: true,
        mode: 'socket' as const,
        allowedUserId: value.allowedUserId,
        accountName: `Workspace ${value.teamId}`,
        accountHandle: `Member ${value.allowedUserId}`,
      },
    };
  } catch (error) {
    throw failure(error);
  } finally {
    await connection?.disconnect();
  }
}

const fileSchema = z.object({
  id: z.string().regex(/^F[A-Z0-9]{1,63}$/),
  name: z.string().min(1).max(255),
  mimetype: z.string().min(1).max(200),
  size: z.number().int().nonnegative(),
  url_private_download: z.string().max(2048).optional(),
  url_private: z.string().max(2048).optional(),
  subtype: z.string().optional(),
  mode: z.string().optional(),
  user: z.string().optional(),
});
const eventSchema = z.object({
  team_id: z.string(),
  api_app_id: z.string(),
  event_id: z.string().min(1).max(200),
  event: z.object({
    type: z.string(),
    user: z.string().optional(),
    bot_id: z.string().optional(),
    subtype: z.string().optional(),
    channel: z.string().optional(),
    channel_type: z.string().optional(),
    ts: z.string().optional(),
    thread_ts: z.string().optional(),
    text: z.string().max(100_000).optional(),
    files: z.array(z.unknown()).max(10).optional(),
    reaction: z.string().max(100).optional(),
    event_ts: z.string().regex(timestamp).optional(),
    item: z
      .object({ type: z.string(), channel: z.string(), ts: z.string().regex(timestamp) })
      .optional(),
  }),
});

function fileUrl(value: string | undefined, upload = false) {
  let url: URL;
  try {
    url = new URL(value ?? '');
  } catch {
    throw new MessagingConnectionError('Slack failed: file_address_unavailable.');
  }
  if (
    url.protocol !== 'https:' ||
    url.username ||
    url.password ||
    url.port ||
    url.hash ||
    !(upload
      ? url.hostname === 'files.slack.com'
      : url.hostname === 'files.slack.com' || url.hostname === 'slack-files.com')
  )
    throw new MessagingConnectionError('Slack failed: unsafe_file_address.');
  return url.href;
}

export function createSlackTransport(
  pool: Pool,
  environment: Record<string, string | undefined>,
): MessagingTransport {
  const parsed = slackInput.safeParse({
    botToken: environment.SLACK_BOT_TOKEN,
    appToken: environment.SLACK_APP_TOKEN,
    teamId: environment.SLACK_TEAM_ID,
    allowedUserId: environment.SLACK_ALLOWED_USER_ID,
  });
  const enabled = environment.SLACK_ENABLED === 'true';
  const channel: MessagingChannel = {
    id: 'slack',
    status: !enabled ? 'disabled' : parsed.success ? 'configured' : 'setup_required',
    missing:
      enabled && !parsed.success
        ? ['SLACK_BOT_TOKEN', 'SLACK_APP_TOKEN', 'SLACK_TEAM_ID', 'SLACK_ALLOWED_USER_ID'].filter(
            (name) => !environment[name],
          )
        : [],
    accountId: environment.SLACK_TEAM_ID,
    allowedSenderIds: parsed.success ? [parsed.data.allowedUserId] : [],
    supportsAttachments: true,
    available: false,
  };
  const lifetime = new AbortController();
  const web = parsed.success ? client(parsed.data.botToken, lifetime.signal) : undefined;
  let connection: SlackSocket | undefined;
  let verified: Awaited<ReturnType<typeof identity>> | undefined;
  let starting: Promise<void> | undefined;
  let replacementNeeded = false;
  let attempts = 0;
  let nextAttempt = 0;
  let closed = false;
  const incoming = new Map<
    string,
    { body: z.infer<typeof eventSchema>; ack: () => Promise<void> }
  >();
  const active = new Set<Promise<unknown>>();
  let ready: Promise<unknown> | undefined;
  const table = () =>
    (ready ??= pool
      .query(
        `CREATE TABLE IF NOT EXISTS channel_slack_reactions(account_id text NOT NULL,message_id text NOT NULL,event_at numeric NOT NULL,reaction text,PRIMARY KEY(account_id,message_id))`,
      )
      .catch((error) => {
        ready = undefined;
        throw error;
      }));

  function attachment(file: z.infer<typeof fileSchema>): MessagingAttachment {
    return {
      name: file.name,
      mimeType: file.mimetype,
      size: file.size,
      ...(file.mimetype.startsWith('audio/') ? { voice: true } : {}),
      async read(signal) {
        if (file.size > fileLimit) throw new MessagingAttachmentError(fileLimit);
        const operation = AbortSignal.any([
          lifetime.signal,
          AbortSignal.timeout(60_000),
          ...(signal ? [signal] : []),
        ]);
        const response = await fetch(fileUrl(file.url_private_download ?? file.url_private), {
          headers: { Authorization: `Bearer ${parsed.success ? parsed.data.botToken : ''}` },
          redirect: 'error',
          signal: operation,
        });
        if (!response.ok || !response.body)
          throw new MessagingConnectionError(
            `Slack file download failed: HTTP ${response.status}.`,
          );
        if (Number(response.headers.get('content-length') ?? 0) > fileLimit) {
          await response.body.cancel();
          throw new MessagingAttachmentError(fileLimit);
        }
        const reader = response.body.getReader();
        const chunks: Uint8Array[] = [];
        let size = 0;
        try {
          for (;;) {
            operation.throwIfAborted();
            const next = await reader.read();
            if (next.done) break;
            size += next.value.length;
            if (size > fileLimit) throw new MessagingAttachmentError(fileLimit);
            chunks.push(next.value);
          }
        } finally {
          await reader.cancel().catch(() => {});
          reader.releaseLock();
        }
        return Buffer.concat(chunks);
      },
    };
  }

  function incomingAttachment(value: unknown): MessagingAttachment {
    const file = fileSchema.safeParse(value);
    if (file.success) return attachment(file.data);
    return {
      name: 'Unavailable Slack file',
      mimeType: 'application/octet-stream',
      size: 0,
      async read() {
        throw new MessagingConnectionError('Slack failed: file_metadata_unavailable.');
      },
    };
  }

  async function start() {
    if (closed || !parsed.success || !enabled || starting) return starting;
    if (replacementNeeded) {
      await connection?.disconnect().catch(() => {});
      connection?.removeAllListeners();
      connection = undefined;
      replacementNeeded = false;
    }
    if (connection) return;
    if (attempts >= 5 || Date.now() < nextAttempt) return;
    attempts++;
    nextAttempt = Date.now() + Math.min(30_000, 2000 * 2 ** (attempts - 1));
    starting = (async () => {
      try {
        verified = await identity(web!, parsed.data);
        if (closed) return;
        connection = socket(parsed.data.appToken, verified.appId, lifetime.signal);
        connection.on('connected', () => {
          if (!closed) {
            channel.available = true;
            channel.error = undefined;
            attempts = 0;
          }
        });
        connection.on('disconnected', () => {
          channel.available = false;
          replacementNeeded = true;
        });
        connection.on('reconnecting', () => {
          channel.available = false;
        });
        connection.on('nod_start_failed', (error) => {
          channel.available = false;
          channel.error = failure(error).message;
          replacementNeeded = true;
        });
        connection.on('error', (error) => {
          channel.available = false;
          channel.error = failure(error).message;
        });
        connection.on('nod_identity_failed', () => {
          channel.available = false;
          channel.error = 'Slack failed: app_identity_mismatch.';
        });
        connection.on('slack_event', ({ type, body, ack }) => {
          const value = eventSchema.safeParse(body);
          if (closed) return;
          if (
            type !== 'events_api' ||
            !value.success ||
            value.data.team_id !== parsed.data.teamId ||
            value.data.api_app_id !== verified?.appId ||
            value.data.event.user !== parsed.data.allowedUserId ||
            value.data.event.bot_id ||
            value.data.event.user === verified.botUserId
          ) {
            void ack().catch(() => {});
            return;
          }
          const event = value.data.event;
          const dm = event.type === 'message' ? event.channel : event.item?.channel;
          if (
            dm !== verified.dmId ||
            (event.type === 'message' &&
              (event.channel_type !== 'im' ||
                (event.subtype && !['file_share', 'thread_broadcast'].includes(event.subtype)))) ||
            !['message', 'reaction_added', 'reaction_removed'].includes(event.type)
          ) {
            void ack().catch(() => {});
            return;
          }
          if (incoming.size < 100 || incoming.has(value.data.event_id))
            incoming.set(value.data.event_id, { body: value.data, ack });
        });
        await connect(connection, AbortSignal.any([lifetime.signal, AbortSignal.timeout(20_000)]));
      } catch (error) {
        channel.available = false;
        channel.error = failure(error).message;
        await connection?.disconnect().catch(() => {});
        connection?.removeAllListeners();
        connection = undefined;
        replacementNeeded = false;
        nextAttempt = Date.now() + Math.min(30_000, 2000 * 2 ** (attempts - 1));
        if (attempts >= 5) channel.error += ' Reconnect in Channels.';
        throw failure(error);
      }
    })().finally(() => {
      starting = undefined;
    });
    return starting;
  }

  function target(input: { accountId: string; threadId: string }) {
    if (
      closed ||
      !verified ||
      input.accountId !== channel.accountId ||
      input.threadId !== verified.dmId
    )
      throw new MessagingConnectionError(
        channel.error ?? 'Slack failed: direct_message_unavailable.',
      );
    return verified.dmId;
  }
  function messageId(value: string) {
    const [dm, ts, ...rest] = value.split(':');
    if (dm !== verified?.dmId || !timestamp.test(ts ?? '') || rest.length)
      throw new MessagingConnectionError('Slack failed: message_target_mismatch.');
    return ts;
  }
  async function operation<T>(action: () => Promise<T>): Promise<T> {
    lifetime.signal.throwIfAborted();
    const pending = action();
    active.add(pending);
    try {
      return await pending;
    } catch (error) {
      throw failure(error);
    } finally {
      active.delete(pending);
    }
  }

  return {
    channels: [channel],
    async poll(accept, _pair, react) {
      if (!enabled || !parsed.success || closed) return;
      await start();
      for (const [key, item] of incoming) {
        if (closed) return;
        const event = item.body.event;
        if (
          event.type === 'message' &&
          timestamp.test(event.ts ?? '') &&
          (!event.thread_ts || timestamp.test(event.thread_ts))
        ) {
          const text = (event.text ?? '')
            .replace(/<((?:https?:\/\/|mailto:)[^>|]+)(?:\|([^>]+))?>/g, (_, url, label) =>
              label ? `[${label}](${url})` : url,
            )
            .replaceAll('&lt;', '<')
            .replaceAll('&gt;', '>')
            .replaceAll('&amp;', '&');
          const command = /^nod\s+(usage|memory|tasks|status|stop)$/i.exec(text.trim());
          await accept({
            channelId: 'slack',
            accountId: parsed.data.teamId,
            senderId: parsed.data.allowedUserId,
            messageId: `${verified!.dmId}:${event.ts}`,
            threadId: verified!.dmId,
            text: command ? `/${command[1].toLowerCase()}` : text,
            sentAt: new Date(Number(event.ts) * 1000),
            ...(event.thread_ts && event.thread_ts !== event.ts
              ? { replyToMessageId: `${verified!.dmId}:${event.thread_ts}` }
              : {}),
            ...(event.files?.length ? { attachments: event.files.map(incomingAttachment) } : {}),
          } satisfies InboundMessage);
        } else if (react && event.item?.type === 'message' && event.reaction && event.event_ts) {
          const emoji = reactionEmoji(event.reaction);
          if (isMessageReaction(emoji)) {
            await table();
            const targetId = `${verified!.dmId}:${event.item.ts}`;
            const claimed = await pool.query(
              `INSERT INTO channel_slack_reactions(account_id,message_id,event_at,reaction) VALUES($1,$2,$3,$4)
              ON CONFLICT(account_id,message_id) DO UPDATE SET event_at=EXCLUDED.event_at,reaction=CASE WHEN $5 AND channel_slack_reactions.reaction IS DISTINCT FROM $6 THEN channel_slack_reactions.reaction ELSE EXCLUDED.reaction END
              WHERE channel_slack_reactions.event_at < EXCLUDED.event_at RETURNING reaction`,
              [
                createHash('sha256')
                  .update(`${parsed.data.teamId}:${parsed.data.allowedUserId}`)
                  .digest('hex'),
                targetId,
                event.event_ts,
                event.type === 'reaction_removed' ? null : emoji,
                event.type === 'reaction_removed',
                emoji,
              ],
            );
            if (
              claimed.rowCount &&
              (event.type === 'reaction_added' || claimed.rows[0].reaction === null)
            )
              await react({
                channelId: 'slack',
                accountId: parsed.data.teamId,
                senderId: parsed.data.allowedUserId,
                messageId: targetId,
                reaction: claimed.rows[0].reaction,
              } satisfies InboundReaction);
          }
        }
        await item.ack().catch(() => {});
        incoming.delete(key);
      }
    },
    async receive() {
      return { status: 404, body: { error: 'Slack Socket Mode does not use a webhook.' } };
    },
    async post(input) {
      const dm = target(input);
      if (input.replyToMessageId) messageId(input.replyToMessageId);
      const ids: string[] = [];
      try {
        const parts = slackMessages(
          input.text || input.attachments?.map((file) => file.name).join('\n') || '',
        );
        for (const text of parts) {
          const result = await operation(() =>
            web!.chat.postMessage({
              channel: dm,
              text,
              unfurl_links: false,
              unfurl_media: false,
              parse: 'none',
              link_names: false,
            }),
          );
          if (!result.ts || !timestamp.test(result.ts))
            throw new MessagingConnectionError('Slack failed: message_receipt_unavailable.');
          ids.push(`${dm}:${result.ts}`);
        }
        const unsent: string[] = [];
        let uncertain: MessagingConnectionError | undefined;
        for (const file of input.attachments ?? []) {
          let bytes: Uint8Array;
          try {
            if (!file.size || file.size > fileLimit) throw new MessagingAttachmentError(fileLimit);
            bytes = await file.read(lifetime.signal);
            if (bytes.length !== file.size || bytes.length > fileLimit)
              throw new MessagingAttachmentError(fileLimit);
          } catch {
            unsent.push(file.name);
            continue;
          }
          try {
            const upload = await operation(() =>
              web!.files.getUploadURLExternal({ filename: file.name, length: bytes.length }),
            );
            if (!upload.file_id)
              throw new MessagingConnectionError('Slack failed: upload_receipt_unavailable.');
            const response = await operation(() =>
              fetch(fileUrl(upload.upload_url, true), {
                method: 'POST',
                body: Buffer.from(bytes),
                redirect: 'error',
                signal: AbortSignal.any([lifetime.signal, AbortSignal.timeout(60_000)]),
              }),
            );
            if (!response.ok)
              throw new MessagingConnectionError(
                `Slack file upload failed: HTTP ${response.status}.`,
              );
            await operation(() =>
              web!.files.completeUploadExternal({
                files: [{ id: upload.file_id!, title: file.name }],
                channel_id: dm,
              }),
            );
            const shared = await operation(() => web!.files.info({ file: upload.file_id! }));
            const shares = z
              .object({
                shares: z
                  .object({
                    private: z
                      .record(z.string(), z.array(z.object({ ts: z.string().regex(timestamp) })))
                      .optional(),
                  })
                  .optional(),
              })
              .safeParse(shared.file);
            const receipts = shares.success ? (shares.data.shares?.private?.[dm] ?? []) : [];
            if (!receipts.length)
              throw new MessagingConnectionError('Slack failed: file_message_receipt_unavailable.');
            for (const share of receipts) ids.push(`${dm}:${share.ts}`);
          } catch (error) {
            uncertain ??= failure(error);
            if (closed) throw uncertain;
          }
        }
        if (unsent.length) {
          const notice = await input.attachmentFailure?.(unsent);
          if (!notice) throw new MessagingConnectionError('Slack failed: attachment_unavailable.');
          for (const text of slackMessages(notice)) {
            const sent = await operation(() =>
              web!.chat.postMessage({
                channel: dm,
                text,
                unfurl_links: false,
                unfurl_media: false,
                parse: 'none',
              }),
            );
            if (!sent.ts || !timestamp.test(sent.ts))
              throw new MessagingConnectionError('Slack failed: message_receipt_unavailable.');
            ids.push(`${dm}:${sent.ts}`);
          }
        }
        if (uncertain) throw uncertain;
        return { id: ids[0] ?? '', ids };
      } catch (error) {
        throw new MessagingDeliveryError(failure(error).message, ids);
      }
    },
    async react(input) {
      const dm = target(input);
      input.signal?.throwIfAborted();
      const ts = messageId(input.messageId);
      const desired = input.reaction ? reactionName(input.reaction) : undefined;
      if (input.reaction && !desired) return 'unsupported';
      const reactionWeb = client(
        parsed.success ? parsed.data.botToken : '',
        AbortSignal.any([lifetime.signal, ...(input.signal ? [input.signal] : [])]),
      );
      const current = await operation(() =>
        reactionWeb.reactions.get({ channel: dm, timestamp: ts, full: true }),
      );
      const previous = (current.message?.reactions ?? [])
        .filter((reaction) => reaction.users?.includes(verified!.botUserId))
        .map((reaction) => reaction.name)
        .filter((name): name is string => Boolean(name && /^[a-z0-9_+:-]{1,100}$/.test(name)));
      const change = async (name: string, remove: boolean) => {
        input.signal?.throwIfAborted();
        try {
          await operation(() =>
            remove
              ? reactionWeb.reactions.remove({ channel: dm, timestamp: ts, name })
              : reactionWeb.reactions.add({ channel: dm, timestamp: ts, name }),
          );
        } catch (error) {
          if (
            error instanceof MessagingConnectionError &&
            error.message === 'Slack failed: invalid_name.'
          )
            return 'unsupported' as const;
          if (!(
            error instanceof MessagingConnectionError &&
            (remove
              ? error.message === 'Slack failed: no_reaction.'
              : error.message === 'Slack failed: already_reacted.')
          ))
            throw error;
        }
      };
      for (const name of previous) {
        if (name !== desired && (await change(name, true)) === 'unsupported') return 'unsupported';
      }
      if (desired && !previous.includes(desired)) {
        if ((await change(desired, false)) === 'unsupported') return 'unsupported';
      }
      return 'sent';
    },
    async close() {
      closed = true;
      channel.available = false;
      lifetime.abort();
      incoming.clear();
      await connection?.disconnect();
      await starting?.catch(() => {});
      await Promise.allSettled(active);
      connection?.removeAllListeners();
    },
  };
}
